"""One renderer for the Columns tab, the check names and the check descriptions, and the fallback each follows.

`USER_GUIDE.md` has the four fallback orders as a table.

The docstring stays out of the Columns tab and out of a collapsed check's description. Two sibling rules must not read in different voices because one author wrote a docstring and the other did not; from the UI that is a change of register with no visible cause. The rule name is the constant in both places, and the docstring reaches only the check description.

Values are rendered but never named. A bound is a parameter, so it belongs in a column constraint and in the per-evaluation `dy_rule__expr` metadata, never in a check name that tightening it would orphan.
"""

from collections.abc import Sequence
from typing import Any

import dataframely as dy

from dagster_dataframely._rules import DescribedRule, described_rules

_ANONYMOUS_CHECK = "custom check"
"""What an unnamed `check=` renders as. A lambda leaves nothing else to recover."""

_OPERATORS = {
    "min": ">=",
    "max": "<=",
    "min_exclusive": ">",
    "max_exclusive": "<",
    "min_length": ">=",
    "max_length": "<=",
}
"""The comparison each bound states. Dataframely names a column rule after the parameter that declared it and keeps the value on an attribute of the same name, so the rule name alone renders the whole constraint. A characterization test covers this (#16)."""

_PHRASES = {
    "nullability": "not null",
    "unique": "unique",
    "inf": "not infinite",
    "nan": "not NaN",
    "check": _ANONYMOUS_CHECK,
}
"""Constraints with no value to render, so the phrase is the constraint. `check` sits here because an anonymous lambda leaves nothing else to say."""

_FIRST_CLASS = frozenset({"nullability", "unique"})
"""Rules Dagster already models as `TableColumnConstraints.nullable` and `.unique`. They still render in the check descriptions; a constraint would say the same thing a second time in the same row."""

_DEFAULTED = frozenset({"inf", "nan"})
"""Rules Dataframely generates from a default, not from something an author wrote. `allow_inf` and `allow_nan` default to `False`, so every float column carries an `inf` and a `nan` rule nobody asked for. A constraint for either says nothing about intent: allowing the value removes the rule instead of inverting it. The question to ask of any future defaulted constraint: does the rule exist only while its flag sits at the default?"""

_NO_CONSTRAINT = _FIRST_CLASS | _DEFAULTED
"""The rules `column_constraints` skips: both sets above. They still reach the check name and the check description."""


def _value(column: dy.Column, rule_name: str) -> Any:  # noqa: ANN401 - the values are of every constraint's own type
    """Read the value of the constraint that generated a rule.

    By name, because the values live on private Dataframely mixins (`OrdinalMixin`, `IsInMixin`). Importing them to satisfy a type checker would depend on private structure for no runtime gain. A characterization test covers the regularity instead.

    Returns
    -------
    Whatever the author passed that parameter, at its own type: a number for a bound, a string for a regex, a sequence for `is_in`. Every caller renders it, so one accessor serves all of them.
    """
    return getattr(column, rule_name)


def _length_unit(column: dy.Column) -> str:
    """Name what a length bound counts.

    `String` measures bytes (`str.len_bytes()`) and `List` measures elements (`list.len()`), under the same two parameter names. Dispatching on the parameter would state one of them wrongly, and for `String` the wrong one is silent: an ASCII column agrees with a character count and a multibyte one does not.

    They are the only two column types that take the parameters. A characterization test asserts that (#16).
    """
    return "elements" if isinstance(column, dy.List) else "bytes"


def _named_check(rule_name: str) -> str:
    """Render a `check=` rule that carries a key.

    A `check={"lowercase": ...}` reaches the reader as `lowercase`. That is the nudge to name a check rather than pass a bare lambda.
    """
    key: str = rule_name.removeprefix("check__")
    # `check__0`, `check__1`: Dataframely's counter for several anonymous lambdas on one column. A position is not a name anybody wrote.
    return _ANONYMOUS_CHECK if key.isdigit() else key


def _column_constraint(column: dy.Column, rule_name: str) -> str | None:  # noqa: PLR0911 - one return per kind of constraint; a dispatch table of lambdas would be the same table read through one more indirection
    """Render one column rule as an operator, or `None` when it has no value to state.

    Returns
    -------
    The constraint as a phrase, or `None` for a kind of rule this package has not met. `None` is a fallback instruction, not an error: every place falls back to the rule name, which always exists.
    """
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
            # A `List` or an `Array` runs its inner column's rules over the elements, so the renderer recurses instead of growing a second vocabulary. A `Struct` is not here: its rules read `inner_<field>_<kind>`, and a field named `a_min` is indistinguishable from field `a` bounded by `min`.
            nested: str | None = _column_constraint(
                column.inner, rule_name.removeprefix("inner_")
            )
            return f"elements {nested}" if nested else None
        case _:
            # A kind of rule this package has not met. Every place falls back to its name.
            return None


def _rule_text(schema: type[dy.Schema], rule: DescribedRule) -> str | None:
    """Render the constraint a rule states, or `None` when it states none.

    Every place reads a rule from here, so a bound cannot say one thing in the Columns tab and another in the check list.

    Parameters
    ----------
    schema
        The schema the rule belongs to. Passed alongside the record because a constraint is read off the column's own attributes, and the primary key off the schema's.

    Returns
    -------
    The constraint, worded as an operator. `None` for a rule whose expression is arbitrary: a `@dy.rule()` body, or a column rule this package has not met.
    """
    if rule.name == "primary_key":
        # A schema with no key columns has no such rule, so the name belongs to a `@dy.rule()` and there is no key to state.
        keys: list[str] = schema.primary_key()
        return f"PK: {', '.join(keys)}" if keys else None
    # Both or neither: the delimiter that sets one sets the other.
    if rule.column is None or rule.rule_name is None:
        return None
    return _column_constraint(schema.columns()[rule.column], rule.rule_name)


def column_constraints(schema: type[dy.Schema]) -> dict[str, list[str]]:
    """Render every column's constraints.

    Read off the same rule dict the asset checks come from, so every constraint has a check behind it and every check about one column reaches that column's row.

    Returns
    -------
    One list per column, keyed by column name, in the schema's own column order and each column's own rule order. A column with nothing to show carries an empty list.
    """
    constraints: dict[str, list[str]] = {name: [] for name in schema.columns()}
    for rule in described_rules(schema).values():
        if rule.column is None or rule.rule_name in _NO_CONSTRAINT:
            continue
        # The fallback is the rule's own name rather than the `|`-delimited whole: the row already carries the column name. The whole name is what a check description falls back to, and what `dy_rule` reports, so the delimiter is visible there and not here.
        constraints[rule.column].append(
            _rule_text(schema, rule) or rule.rule_name or rule.name
        )
    return constraints


def table_constraints(schema: type[dy.Schema]) -> list[str]:
    """Render the constraints that belong to no single column.

    Dataframely models the primary key as one rule over a struct of every key column. Stating it once here distinguishes a composite key from two independent single-column ones. A `@dy.rule()` joins under its own name: its expression is arbitrary, and its docstring belongs to the description.

    Returns
    -------
    One entry per schema-level rule, in the schema's own rule order.
    """
    return [
        _rule_text(schema, rule) or rule.name
        for rule in described_rules(schema).values()
        if rule.column is None
    ]


def check_description(schema: type[dy.Schema], rule: DescribedRule) -> str:
    """Describe a rule's asset check, falling back until something holds.

    The docstring first, because an author who wrote one said something the schema cannot. Then the rendered constraint prefixed with the column, because a check list reads flat and a bare `>= 0` names nothing. Then the rule name, which always exists, so no check is described as nothing.

    Returns
    -------
    The check's description, never empty.
    """
    if rule.description:
        return rule.description
    rendered: str | None = _rule_text(schema, rule)
    if rendered is None:
        return rule.name
    if rule.column is None:
        return rendered
    return f"{rule.column} {rendered}"


def column_rule_summary(schema: type[dy.Schema], rules: Sequence[DescribedRule]) -> str:
    """Render every constraint one column's rules state, for the check that reports them as one.

    A collapsed check's description is the only place its members are visible before a run, so it renders all of them, including the two a column constraint skips. `not null` has to be said here because no column row sits beside this check to say it.

    The check names the column once, so no constraint repeats it. A rule with nothing structured to render falls back to its rule name.

    Returns
    -------
    The constraints, comma-separated.
    """
    return ", ".join(
        _rule_text(schema, rule) or rule.rule_name or rule.name for rule in rules
    )
