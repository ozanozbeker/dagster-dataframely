"""Rule records, and the check on a schema's names.

Other modules read a rule only through `described_rules`, so only this one splits a rule name at `|`.
`described_rules` builds the records on every call (ADR-0008).
"""

import inspect
import re
from dataclasses import dataclass

import dataframely as dy
import polars as pl

# Private in Dataframely, as is `_validation_rules`; `tests/test_upstream_characterization.py` pins all three.
from dataframely._rule import Rule, RuleFactory

from dagster_dataframely._naming import RESERVED_NAMESPACE, check_name
from dagster_dataframely.errors import (
    CheckNameCollisionError,
    InvalidColumnNameError,
    ReservedColumnError,
)

DAGSTER_NAME = re.compile(r"[A-Za-z0-9_]+")
"""Restated because Dagster exports no name for it; `tests/test_upstream_characterization.py` pins it."""


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
    _rule: Rule

    @property
    def expr(self) -> pl.Expr:
        """The rule's expression, read on access, because a `@dy.rule()` builds it by calling its function."""
        return self._rule.expr


def _described_rule(schema: type[dy.Schema], name: str, rule: Rule) -> DescribedRule:
    """Describe one rule.

    `getattr` finds a `@dy.rule()`'s `RuleFactory` by name, and a column rule's `|` makes it miss.
    """
    column, delimiter, rule_name = name.partition("|")
    factory = getattr(schema, name, None)
    return DescribedRule(
        name=name,
        check_name=check_name(name),
        column=column if delimiter else None,
        rule_name=rule_name if delimiter else None,
        description=(
            inspect.getdoc(factory.validation_fn)
            if isinstance(factory, RuleFactory)
            else None
        ),
        _rule=rule,
    )


def described_rules(schema: type[dy.Schema]) -> dict[str, DescribedRule]:
    """Return the schema's validation rules, keyed by rule name.

    `with_cast=False` drops the `<column>|dtype` rules, which repeat the column-schema check.
    """
    return {
        name: _described_rule(schema, name, rule)
        for name, rule in schema._validation_rules(with_cast=False).items()  # noqa: SLF001
    }


def validate_namespace(schema: type[dy.Schema]) -> None:
    """Raise the three errors Dagster would otherwise report opaquely, or not at all."""
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
    for rule in described_rules(schema).values():
        if rule.check_name in seen:
            raise CheckNameCollisionError(
                schema.__name__, seen[rule.check_name], rule.name, rule.check_name
            )
        seen[rule.check_name] = rule.name
