"""The text this module renders a schema's rules as: column constraints and check descriptions.

`_metadata` puts the column constraints in the Columns tab, and `_checks` puts the descriptions on the asset checks.
Both follow the fallback orders in the user guide's *Naming* page.
"""

import re
from collections.abc import Sequence
from typing import Any

import dataframely as dy
import polars as pl

from dagster_dataframely._rules import DescribedRule, described_rules

_ANONYMOUS_CHECK = "custom check"

_OPERATORS = {
    "min": ">=",
    "max": "<=",
    "min_exclusive": ">",
    "max_exclusive": "<",
    "min_length": ">=",
    "max_length": "<=",
}

_PHRASES = {
    "nullability": "not null",
    "unique": "unique",
    "inf": "not infinite",
    "nan": "not NaN",
    "check": _ANONYMOUS_CHECK,
}

_FIRST_CLASS = frozenset({"nullability", "unique"})
"""These two rules become `TableColumnConstraints.nullable` and `.unique`."""

_DEFAULTED = frozenset({"inf", "nan"})
"""Rules every float column has, because `allow_inf` and `allow_nan` default to `False`."""

_NO_CONSTRAINT = _FIRST_CLASS | _DEFAULTED

_CHECK_RULE_NAME = re.compile(r"(?:inner_)*check(?:__.+)?")
"""Matches the names Dataframely gives the rules of a `check=` argument, on a column or on its elements."""


def _value(column: dy.Column, rule_name: str) -> Any:  # noqa: ANN401 - the type varies by constraint
    """Read the value of the constraint that generated a rule.

    Dataframely stores the value under the rule's name on a private mixin, as a characterization test pins (#16).
    """
    return getattr(column, rule_name)


def _length_unit(column: dy.Column) -> str:
    """Name what a length bound counts.

    Only `String` (bytes) and `List` (elements) take length bounds, as a characterization test pins (#16).
    """
    return "elements" if isinstance(column, dy.List) else "bytes"


def _named_check(rule_name: str) -> str:
    """Render a `check=` rule by its dict key."""
    key: str = rule_name.removeprefix("check__")
    # Dataframely numbers the lambdas on one column: `check__0`, `check__1`.
    return _ANONYMOUS_CHECK if key.isdigit() else key


def _column_constraint(column: dy.Column, rule_name: str) -> str | None:  # noqa: PLR0911 - one return per kind of constraint
    """Render one column rule, or `None` for a kind of rule this package does not render."""
    if rule_name in _PHRASES:
        return _PHRASES[rule_name]
    match rule_name:
        case "min" | "max" | "min_exclusive" | "max_exclusive":
            return f"{_OPERATORS[rule_name]} {_value(column, rule_name)}"
        case "min_length" | "max_length":
            return (
                f"length {_OPERATORS[rule_name]} {_value(column, rule_name)}"
                f" {_length_unit(column)}"
            )
        case "regex":
            return f"matches {_value(column, rule_name)}"
        case "resolution":
            return f"aligned to {_value(column, rule_name)}"
        case "is_in":
            values: str = ", ".join(str(value) for value in _value(column, rule_name))
            return f"in ({values})"
        case _ if rule_name.startswith("check__"):
            return _named_check(rule_name)
        case _ if rule_name.startswith("inner_") and isinstance(
            column, (dy.List, dy.Array)
        ):
            # Not `Struct`: its `inner_<field>_<rule>` names are ambiguous when a field name has an underscore.
            nested: str | None = _column_constraint(
                column.inner, rule_name.removeprefix("inner_")
            )
            return f"elements {nested}" if nested else None
        case _:
            return None


def _rule_text(schema: type[dy.Schema], rule: DescribedRule) -> str | None:
    """Render a rule's constraint or `check=` key, or `None` when it has neither.

    Every function here gets a rule's text from this one, so a check description repeats the text the Columns tab shows for the rule.
    """
    if rule.name == "primary_key":
        # With no key columns, a rule named `primary_key` is a `@dy.rule()`.
        keys: list[str] = schema.primary_key()
        return f"PK: {', '.join(keys)}" if keys else None
    # Both or neither: the delimiter that sets one sets the other.
    if rule.column is None or rule.rule_name is None:
        return None
    return _column_constraint(schema.columns()[rule.column], rule.rule_name)


def column_constraints(schema: type[dy.Schema]) -> dict[str, list[str]]:
    """Render every column's constraints."""
    constraints: dict[str, list[str]] = {name: [] for name in schema.columns()}
    for rule in described_rules(schema).values():
        if rule.column is None or rule.rule_name in _NO_CONSTRAINT:
            continue
        constraints[rule.column].append(
            _rule_text(schema, rule) or rule.rule_name or rule.name
        )
    return constraints


def table_constraints(schema: type[dy.Schema]) -> list[str]:
    """Render the constraints that belong to no single column."""
    return [
        _rule_text(schema, rule) or rule.name
        for rule in described_rules(schema).values()
        if rule.column is None
    ]


def _code_span(expr: pl.Expr) -> str:
    """Wrap an expression's text in a markdown code span, delimited by a backtick run longer than any inside it.

    Polars' expression text never starts or ends with a backtick, so the span needs no padding.
    """
    text: str = str(expr)
    longest: int = max(map(len, re.findall(r"`+", text)), default=0)
    delimiter: str = "`" * (longest + 1)
    return f"{delimiter}{text}{delimiter}"


def _with_expression(schema: type[dy.Schema], rule: DescribedRule, text: str) -> str:
    """Return `text`, followed by the rule's expression unless the rule renders as a constraint.

    A `check=` key renders as text, and it does not show what the rule tests.
    """
    # Reading an expression calls a `@dy.rule()`'s function, so only these rules read it: "The cost of DescribedRule records" in `docs/pre-1.0.md`.
    if _rule_text(schema, rule) is not None and not _CHECK_RULE_NAME.fullmatch(
        rule.rule_name or ""
    ):
        return text
    return f"{text} {_code_span(rule.expr)}"


def check_description(schema: type[dy.Schema], rule: DescribedRule) -> str:
    """Describe a rule's asset check."""
    if rule.description:
        return rule.description
    rendered: str | None = _rule_text(schema, rule)
    if rendered is None:
        text: str = rule.name
    elif rule.column is None:
        text = rendered
    else:
        text = f"{rule.column} {rendered}"
    return _with_expression(schema, rule, text)


def column_rule_summary(schema: type[dy.Schema], rules: Sequence[DescribedRule]) -> str:
    """Render every constraint one column's rules state, for their collapsed check."""
    return ", ".join(
        _with_expression(
            schema, rule, _rule_text(schema, rule) or rule.rule_name or rule.name
        )
        for rule in rules
    )
