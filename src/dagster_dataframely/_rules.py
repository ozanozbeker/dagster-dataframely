"""`describe_rules` builds a record per rule with its declared severity, and `get_filter_severities` reads the same declaration on a Collection.

`validate_namespace` checks a schema's names and declarations.
Only this module splits a rule name at `|`, and `describe_rules` builds its records on every call (ADR-0008).
"""

import inspect
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Literal, Self, cast

import dagster as dg
import dataframely as dy
import polars as pl

# Private in Dataframely, as are `_validation_rules` and `Collection._filters`; `tests/test_upstream_characterization.py` pins all four.
from dataframely._rule import Rule, RuleFactory

from dagster_dataframely._naming import RESERVED_NAMESPACE, get_check_name_for_rule
from dagster_dataframely.errors import (
    CheckNameCollisionError,
    InvalidColumnNameError,
    InvalidSeverityError,
    ReservedColumnError,
)

DAGSTER_NAME = re.compile(r"[A-Za-z0-9_]+")
"""Restated because Dagster exports no name for it; `tests/test_upstream_characterization.py` pins it."""

type Severity = Literal["error", "warn"]

DECLARATION_KEY = "dagster_dataframely"
"""The `dy.Column(metadata=...)` key a column declares its rules' severities under, as dbt keeps Dagster's settings under `meta.dagster`."""

_CLASS_DECLARATION = "__dagster_dataframely__"

_SEVERITIES: tuple[Severity, ...] = ("error", "warn")

# The arguments whose rule Dataframely names differently.
_RULE_NAMES = {"nullable": "nullability", "allow_inf": "inf", "allow_nan": "nan"}


@dataclass(frozen=True)
class DescribedRule:
    """One Dataframely rule, with the names and docstring derived from it.

    `docs/pre-1.0.md` has its build cost, and why it is not a `NamedTuple`.
    """

    name: str
    check_name: str
    column: str | None
    rule_name: str | None
    description: str | None
    severity: Severity
    _rule: Rule

    @property
    def expr(self) -> pl.Expr:
        """The rule's expression, read on access, because a `@dy.rule()` builds it by calling its function."""
        return self._rule.expr

    @classmethod
    def from_rule(
        cls, schema: type[dy.Schema], name: str, rule: Rule, severity: Severity
    ) -> Self:
        """Describe one rule.

        `getattr` finds a `@dy.rule()`'s `RuleFactory` by name, and a column rule's `|` makes it miss.
        """
        column, delimiter, rule_name = name.partition("|")
        factory = getattr(schema, name, None)
        return cls(
            name=name,
            check_name=get_check_name_for_rule(name),
            column=column if delimiter else None,
            rule_name=rule_name if delimiter else None,
            description=(
                inspect.getdoc(factory.validation_fn)
                if isinstance(factory, RuleFactory)
                else None
            ),
            severity=severity,
            _rule=rule,
        )


def quote(names: Iterable[str]) -> str:
    """Quote and join names for an error message."""
    return ", ".join(f"'{name}'" for name in names)


def _parse_severity(value: object, place: str) -> Severity:
    """Return `value` if it is a severity, else raise."""
    if value in _SEVERITIES:
        return cast("Severity", value)
    hint: str = (
        f" Write the string '{value.value.lower()}' instead."
        if isinstance(value, dg.AssetCheckSeverity)
        else ""
    )
    raise InvalidSeverityError(
        place,
        f"sets the severity {value!r}, and a severity is 'error' or 'warn'.{hint}",
    )


def _resolve_named_rules(
    written: str, rules: list[str], place: str, owner: str, term: str
) -> list[str]:
    """Return the rules a written name stands for: itself, the rule its argument generates, or every `check__*` rule."""
    if written in rules:
        return [written]
    if _RULE_NAMES.get(written) in rules:
        return [_RULE_NAMES[written]]
    checks: list[str] = [rule for rule in rules if rule.startswith("check__")]
    if written == "check" and checks:
        return checks
    listed: str = (
        f"The {term}s of {owner} are {quote(rules)}."
        if rules
        else f"{owner} has no {term}s."
    )
    raise InvalidSeverityError(
        place, f"names '{written}', which is not a {term} of {owner}. {listed}"
    )


def _read_declared_severities(
    entry: object, rules: list[str], place: str, owner: str, term: str
) -> dict[str, Severity]:
    """Return the severities one declaration sets, keyed by rule name."""
    if entry is None:
        return {}
    if not isinstance(entry, Mapping):
        raise InvalidSeverityError(
            place,
            f"is a {type(entry).__name__}, not a mapping with the key 'severity'.",
        )
    unknown: list[str] = [str(key) for key in entry if key != "severity"]
    if unknown:
        raise InvalidSeverityError(
            place,
            f"has the key {quote(unknown)}, and the only key it reads is 'severity'.",
        )
    if "severity" not in entry:
        return {}
    value: object = entry["severity"]
    if isinstance(value, Mapping):
        return {
            rule: _parse_severity(severity, place)
            for written, severity in value.items()
            for rule in _resolve_named_rules(str(written), rules, place, owner, term)
        }
    severity: Severity = _parse_severity(value, place)
    if not rules:
        raise InvalidSeverityError(
            place,
            f"sets one severity for every {term} of {owner}, and {owner} has no {term}s.",
        )
    return dict.fromkeys(rules, severity)


def _reject_column_rules(entry: object, place: str, columns: Iterable[str]) -> None:
    """Raise if the class declaration names a column rule, which belongs in its column's metadata."""
    severity: object = entry.get("severity") if isinstance(entry, Mapping) else None
    if not isinstance(severity, Mapping):
        return
    for written in severity:
        column, delimiter, _ = str(written).partition("|")
        if delimiter and column in columns:
            raise InvalidSeverityError(
                place,
                f"names '{written}', a column rule. Declare it in the `{DECLARATION_KEY}` metadata of column '{column}' instead.",
            )


def _resolve_severities(
    schema: type[dy.Schema], names: Iterable[str]
) -> dict[str, Severity]:
    """Return every rule's severity, keyed by rule name: `"error"` unless the schema declares `"warn"`."""
    by_column: dict[str, list[str]] = {column: [] for column in schema.columns()}
    schema_level: list[str] = []
    for name in names:
        column, delimiter, rule_name = name.partition("|")
        if delimiter:
            by_column.setdefault(column, []).append(rule_name)
        else:
            schema_level.append(name)

    declared: dict[str, Severity] = {}
    for column, definition in schema.columns().items():
        place: str = f"The `{DECLARATION_KEY}` metadata of column '{column}' in {schema.__name__}"
        entry: object = (definition.metadata or {}).get(DECLARATION_KEY)
        for rule_name, severity in _read_declared_severities(
            entry, by_column[column], place, f"column '{column}'", "rule"
        ).items():
            declared[f"{column}|{rule_name}"] = severity
    attribute: object = getattr(schema, _CLASS_DECLARATION, None)
    place = f"`{_CLASS_DECLARATION}` on {schema.__name__}"
    _reject_column_rules(attribute, place, schema.columns())
    declared |= _read_declared_severities(
        attribute, schema_level, place, schema.__name__, "schema-level rule"
    )
    return {name: declared.get(name, "error") for name in names}


def describe_rules(schema: type[dy.Schema]) -> dict[str, DescribedRule]:
    """Return the schema's validation rules, keyed by rule name.

    `with_cast=False` drops the `<column>|dtype` rules, which repeat the column-schema check.
    """
    rules: dict[str, Rule] = schema._validation_rules(with_cast=False)  # noqa: SLF001
    severities: dict[str, Severity] = _resolve_severities(schema, rules)
    return {
        name: DescribedRule.from_rule(schema, name, rule, severities[name])
        for name, rule in rules.items()
    }


def get_filter_severities(
    collection: type[dy.Collection],
) -> dict[str, Severity]:
    """Return every collection filter's severity, keyed by its name: `"error"` unless the Collection declares `"warn"`."""
    filters: list[str] = list(collection._filters())  # noqa: SLF001
    place: str = f"`{_CLASS_DECLARATION}` on {collection.__name__}"
    declared: dict[str, Severity] = _read_declared_severities(
        getattr(collection, _CLASS_DECLARATION, None),
        filters,
        place,
        collection.__name__,
        "collection filter",
    )
    return {name: declared.get(name, "error") for name in filters}


def validate_namespace(schema: type[dy.Schema]) -> None:
    """Raise the three errors Dagster would otherwise report opaquely, or not at all, and `InvalidSeverityError` for a declaration `describe_rules` cannot read."""
    reserved: list[str] = [
        column for column in schema.columns() if column.startswith(RESERVED_NAMESPACE)
    ]
    if reserved:
        raise ReservedColumnError(schema.__name__, reserved)

    # This runs before the rule walk, which a `|` in a column name breaks (ADR-0008).
    invalid_names: list[str] = [
        column for column in schema.columns() if not DAGSTER_NAME.fullmatch(column)
    ]
    if invalid_names:
        raise InvalidColumnNameError(schema.__name__, invalid_names)

    seen: dict[str, str] = {}
    for rule in describe_rules(schema).values():
        if rule.check_name in seen:
            raise CheckNameCollisionError(
                schema.__name__, seen[rule.check_name], rule.name, rule.check_name
            )
        seen[rule.check_name] = rule.name
