"""One renderer for the Columns tab, the check names and the check descriptions, and the fallback each follows.

A schema's constraints reach a data consumer as three kinds of text: a constraint, a name, and a description. Each place falls back differently.

| place | fallback |
| --- | --- |
| Columns tab | rendered constraint, then the rule name. Never the docstring. |
| check name | always `dy_rule__<rule>`. Identity, never prose. |
| check description | docstring, then `<column> <rendered>`, then the rule name. |
| collapsed check description | for one column's members: each rendered constraint, then that member's kind. Never the docstring. Members no column owns are listed by name; `_rule_sets` in `_checks` says why. |

The docstring stays out of the Columns tab and out of a collapsed check's description. Two sibling rules must not read in different voices because one author wrote a docstring and the other did not; from the UI that is a change of register with no visible cause. The rule name is the constant in both places, and the docstring reaches only the check description.

Values are rendered but never named. A bound is a parameter, so it belongs in a constraint and in the per-evaluation `dy_rule__expr` metadata, never in a check name that renaming would orphan.

A `check=` given a bare lambda has no name to recover, so it renders as `custom check` wherever it appears. Name your checks, `check={"lowercase": ...}`, and the data consumer sees the key.
"""

from collections.abc import Sequence
from typing import Any

import dataframely as dy

from dagster_dataframely._naming import (
    OwnedRule,
    owned_rule,
    rule_description,
    validation_rules,
)

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


def _value(column: dy.Column, kind: str) -> Any:  # noqa: ANN401 - the values are of every constraint's own type
    """Read the value of the constraint that generated a rule.

    By name, because the values live on private Dataframely mixins (`OrdinalMixin`, `IsInMixin`). Importing them to satisfy a type checker would depend on private structure for no runtime gain. A characterization test covers the regularity instead.

    Parameters
    ----------
    column
        The column that declared the constraint.
    kind
        The part of the rule name after `|`, which is also the parameter's name.

    Returns
    -------
    Whatever the author passed that parameter, at its own type: a number for a bound, a string for a regex, a sequence for `is_in`. Every caller renders it, so one accessor serves all of them.
    """
    return getattr(column, kind)


def _length_unit(column: dy.Column) -> str:
    """Name what a length bound counts.

    `String` measures bytes (`str.len_bytes()`) and `List` measures elements (`list.len()`), under the same two parameter names. Dispatching on the parameter would state one of them wrongly, and for `String` the wrong one is silent: an ASCII column agrees with a character count and a multibyte one does not.

    They are the only two column types that take the parameters. A characterization test asserts that (#16).
    """
    return "elements" if isinstance(column, dy.List) else "bytes"


def _named_check(kind: str) -> str:
    """Render a `check=` rule that carries a key.

    A `check={"lowercase": ...}` reaches the reader as `lowercase`. That is the nudge to name a check rather than pass a bare lambda.
    """
    key: str = kind.removeprefix("check__")
    # `check__0`, `check__1`: Dataframely's counter for several anonymous lambdas on one column. A position is not a name anybody wrote.
    return _ANONYMOUS_CHECK if key.isdigit() else key


def _column_constraint(column: dy.Column, kind: str) -> str | None:  # noqa: PLR0911 - one return per kind of constraint; a dispatch table of lambdas would be the same table read through one more indirection
    """Render one column rule as an operator, or `None` when it has no value to state.

    Parameters
    ----------
    column
        The column the rule belongs to.
    kind
        The part of the rule name after `|`, so `min` for `amount|min`.

    Returns
    -------
    The constraint as a phrase, or `None` for a kind of rule this package has not met. `None` is a fallback instruction, not an error: every place falls back to the rule name, which always exists.
    """
    if kind in _PHRASES:
        return _PHRASES[kind]
    match kind:
        case "min" | "max" | "min_exclusive" | "max_exclusive":
            return f"{_OPERATORS[kind]} {_value(column, kind)}"
        case "min_length" | "max_length":
            return (
                f"length {_OPERATORS[kind]} {_value(column, kind)}"
                f" {_length_unit(column)}"
            )
        case "regex":
            return f"matches {_value(column, kind)}"
        case "resolution":
            return f"aligned to {_value(column, kind)}"
        case "is_in":
            values: str = ", ".join(str(value) for value in _value(column, kind))
            return f"in ({values})"
        case _ if kind.startswith("check__"):
            return _named_check(kind)
        case _ if kind.startswith("inner_") and isinstance(column, (dy.List, dy.Array)):
            # A `List` or an `Array` runs its inner column's rules over the elements, so the renderer recurses instead of growing a second vocabulary. A `Struct` is not here: its rules read `inner_<field>_<kind>`, and a field named `a_min` is indistinguishable from field `a` bounded by `min`.
            nested: str | None = _column_constraint(
                column.inner, kind.removeprefix("inner_")
            )
            return f"elements {nested}" if nested else None
        case _:
            # A kind of rule this package has not met. Every place falls back to its name.
            return None


def rule_text(schema: type[dy.Schema], rule_name: str) -> str | None:
    """Render the constraint a rule states, or `None` when it states none.

    Every place reads a rule from here, so a bound cannot say one thing in the Columns tab and another in the check list.

    Parameters
    ----------
    schema
        The schema the rule belongs to.
    rule_name
        The rule name Dataframely reports, `|`-delimited for column rules.

    Returns
    -------
    The constraint, worded as an operator. `None` for a rule whose expression is arbitrary: a `@dy.rule()` body, or a column rule this package has not met.

    Examples
    --------
    ```python
    import dataframely as dy

    from dagster_dataframely._rendering import rule_text


    class Orders(dy.Schema):
        order_id = dy.String(primary_key=True)
        amount = dy.Float64(nullable=False, min=0.0)


    rule_text(Orders, "amount|min")  # '>= 0.0'
    rule_text(Orders, "primary_key")  # 'PK: order_id'
    ```
    """
    if rule_name == "primary_key":
        # A schema with no key columns has no such rule, so the name belongs to a `@dy.rule()` and there is no key to state.
        keys: list[str] = schema.primary_key()
        return f"PK: {', '.join(keys)}" if keys else None
    owned: OwnedRule | None = owned_rule(rule_name)
    if owned is None:
        return None
    return _column_constraint(schema.columns()[owned.column], owned.kind)


def column_constraints(schema: type[dy.Schema]) -> dict[str, list[str]]:
    """Render every column's constraints.

    Read off the same rule dict the asset checks come from, so every constraint has a check behind it and every check about one column reaches that column's row.

    Parameters
    ----------
    schema
        The schema to render.

    Returns
    -------
    One list per column, keyed by column name, in the schema's own column order and each column's own rule order. A column with nothing to show carries an empty list.
    """
    constraints: dict[str, list[str]] = {name: [] for name in schema.columns()}
    for rule_name in validation_rules(schema):
        owned: OwnedRule | None = owned_rule(rule_name)
        if owned is None or owned.kind in _NO_CONSTRAINT:
            continue
        # The fallback is the part of the rule name after `|`: the row already carries the column name, and this package shows Dataframely's `|` nowhere else.
        constraints[owned.column].append(rule_text(schema, rule_name) or owned.kind)
    return constraints


def table_constraints(schema: type[dy.Schema]) -> list[str]:
    """Render the constraints that belong to no single column.

    Dataframely models the primary key as one rule over a struct of every key column. Stating it once here distinguishes a composite key from two independent single-column ones. A `@dy.rule()` joins under its own name: its expression is arbitrary, and its docstring belongs to the description.

    Parameters
    ----------
    schema
        The schema to render.

    Returns
    -------
    One entry per schema-level rule, in the schema's own rule order.
    """
    return [
        rule_text(schema, rule_name) or rule_name
        for rule_name in validation_rules(schema)
        if owned_rule(rule_name) is None
    ]


def check_description(schema: type[dy.Schema], rule_name: str) -> str:
    """Describe a rule's asset check, falling back until something holds.

    The docstring first, because an author who wrote one said something the schema cannot. Then the rendered constraint prefixed with the column, because a check list reads flat and a bare `>= 0` names nothing. Then the rule name, which always exists, so no check is described as nothing.

    Parameters
    ----------
    schema
        The schema the rule belongs to.
    rule_name
        The rule name Dataframely reports.

    Returns
    -------
    The check's description, never empty.
    """
    docstring: str | None = rule_description(schema, rule_name)
    if docstring:
        return docstring
    rendered: str | None = rule_text(schema, rule_name)
    if rendered is None:
        return rule_name
    owned: OwnedRule | None = owned_rule(rule_name)
    if owned is None:
        return rendered
    return f"{owned.column} {rendered}"


def column_rule_summary(schema: type[dy.Schema], rule_names: Sequence[str]) -> str:
    """Render every constraint one column's rules state, for the check that reports them as one.

    A collapsed check's description is the only place its members are visible before a run, so it renders all of them, including the two a column constraint skips. `not null` has to be said here because no column row sits beside this check to say it.

    The check names the column once, so no constraint repeats it. A rule with nothing structured to render falls back to the part of its name after `|`.

    Parameters
    ----------
    schema
        The schema the rules belong to.
    rule_names
        One column's rules, in the schema's own order.

    Returns
    -------
    The constraints, comma-separated.

    Examples
    --------
    ```python
    import dataframely as dy

    from dagster_dataframely._rendering import column_rule_summary


    class Orders(dy.Schema):
        amount = dy.Float64(nullable=False, min=0.0)


    column_rule_summary(Orders, ["amount|nullability", "amount|min"])
    # 'not null, >= 0.0'
    ```
    """
    rendered: list[str] = []
    for rule_name in rule_names:
        owned: OwnedRule | None = owned_rule(rule_name)
        rendered.append(
            rule_text(schema, rule_name) or (owned.kind if owned else rule_name)
        )
    return ", ".join(rendered)
