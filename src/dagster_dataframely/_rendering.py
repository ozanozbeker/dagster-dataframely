"""One renderer for every surface that shows a rule, and the fallback each surface follows.

A schema's constraints reach a data consumer at three fidelities:

| surface | fallback |
| --- | --- |
| column constraint | rendered constraint → the rule's own name. Docstring excluded. |
| check name | always `dy_rule__<rule>`. Identity, never prose. |
| check description | docstring → `<column> <rendered>` → rule name. |
| collapsed check description | one column's members: every rendered constraint → that member's own kind. Docstring excluded, for the same reason a column constraint excludes it. The members no column owns are listed by name instead: a `@dy.rule()` has no constraint to render, and a rendered `primary_key` would put its own commas inside a comma-separated list. |

The register rule is what keeps the docstring out of a constraint: two sibling rules must not read in different voices merely because one author wrote a docstring and the other did not, which from the UI is a change of register with no visible cause. So the rule name is the constant on the constraint surfaces, and the docstring reaches only the description.

Values are rendered but never named: a bound is a parameter, so it belongs in a constraint and in the per-evaluation `dy_rule__expr` metadata, and never in a check name that renaming would orphan.

One rendering is deliberately unhelpful. A `check=` given a bare lambda has no name to recover, so it reads `custom check` wherever it appears: name your checks, `check={"lowercase": ...}`, and the key is what a data consumer sees.
"""

from collections.abc import Sequence
from typing import Any

import dataframely as dy

from dagster_dataframely._naming import (
    rule_description,
    split_rule,
    validation_rules,
)

#: What an unnamed `check=` renders as. There is nothing else to recover from a lambda.
_ANONYMOUS_CHECK = "custom check"

# The comparison each bound states. dataframely names a column rule after the parameter that declared it and keeps that parameter's value on an attribute of the same name, so the rule's own name is enough to render the whole constraint. Pinned by its own test (#16).
_OPERATORS = {
    "min": ">=",
    "max": "<=",
    "min_exclusive": ">",
    "max_exclusive": "<",
    "min_length": ">=",
    "max_length": "<=",
}

# Constraints whose whole statement is that they hold: there is no value to render, so the phrase is the constraint. `check` is here because an anonymous lambda leaves nothing else to say about itself.
_PHRASES = {
    "nullability": "not null",
    "unique": "unique",
    "inf": "not infinite",
    "nan": "not NaN",
    "check": _ANONYMOUS_CHECK,
}

# Rules Dagster already models first-class, as `TableColumnConstraints.nullable` and `.unique`. They still render on the check surfaces; a constraint would only say the same thing a second time, in the same row.
_FIRST_CLASS = frozenset({"nullability", "unique"})

# Rules dataframely generates from a default rather than from something an author wrote. `allow_inf` and `allow_nan` default to `False`, so every float column carries an `inf` and a `nan` rule nobody asked for. A constraint for either says nothing about intent, and it can never say anything else, because allowing the value removes the rule instead of inverting it. That is the test for any future defaulted constraint: does the rule exist only while its flag sits at the default?
_DEFAULTED = frozenset({"inf", "nan"})

_NO_CONSTRAINT = _FIRST_CLASS | _DEFAULTED


def _value(column: dy.Column, kind: str) -> Any:  # noqa: ANN401 - the values are of every constraint's own type
    """Reads the value of the constraint that generated a rule.

    By name rather than by attribute access, because these values live on dataframely mixins that are private (`OrdinalMixin`, `IsInMixin`). Importing them to satisfy a type checker would take a dependency on private structure for no runtime gain, so the regularity they provide is pinned by a test instead.

    Args:
        column: The column that declared the constraint.
        kind: The rule's own half of its name, which is also the parameter's.

    Returns:
        Whatever the author passed that parameter, at whatever type they passed it: a number for a bound, a string for a regex, a sequence for `is_in`. Every caller renders it rather than reasoning about it, which is what lets one accessor serve all of them.
    """
    return getattr(column, kind)


def _length_unit(column: dy.Column) -> str:
    """Names what a length bound counts.

    `String` measures bytes (`str.len_bytes()`) and `List` measures elements (`list.len()`), under the same two parameter names. Dispatching on the parameter would state one of them wrongly, and for `String` the wrong one is silent: an ASCII column agrees with a character count and a multibyte one does not.

    They are the only two column types that take the parameters at all, which is pinned by its own test (#16) rather than assumed here.
    """
    return "elements" if isinstance(column, dy.List) else "bytes"


def _named_check(kind: str) -> str:
    """Renders a `check=` rule that carries a key.

    A `check={"lowercase": ...}` reaches the reader as `lowercase`, which is the nudge to name a check rather than passing a bare lambda.
    """
    key: str = kind.removeprefix("check__")
    # `check__0`, `check__1`: dataframely's counter for several anonymous lambdas on one column. A position is not a name anybody wrote.
    return _ANONYMOUS_CHECK if key.isdigit() else key


def _column_constraint(column: dy.Column, kind: str) -> str | None:  # noqa: PLR0911 - one return per constraint shape; a dispatch table of lambdas would be the same table read through one more indirection
    """Renders one column rule as an operator, or `None` when it has no value to state.

    Args:
        column: The column the rule belongs to.
        kind: The rule's own half of its name, so `min` for `amount|min`.

    Returns:
        The constraint as a phrase, or `None` for a rule shape this package has not met. `None` is a fallback instruction rather than an error: the surfaces fall back to the rule's own name, which always exists.
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
            # A `List` or an `Array` runs its inner column's own rules over the elements, so the renderer recurses rather than growing a second vocabulary for them. A `Struct` is deliberately not here: its rules read `inner_<field>_<kind>`, and a field named `a_min` is indistinguishable from field `a` bounded by `min`.
            nested: str | None = _column_constraint(
                column.inner, kind.removeprefix("inner_")
            )
            return f"elements {nested}" if nested else None
        case _:
            # A rule shape this package has not met. Its name is what the surfaces fall back to.
            return None


def rule_text(schema: type[dy.Schema], rule_name: str) -> str | None:
    """Renders the constraint a rule states, or `None` when it states none.

    The one renderer. Every surface that shows a rule reads it from here, so a bound cannot say one thing in the Columns tab and another in the check list.

    Args:
        schema: The schema the rule belongs to.
        rule_name: The rule name dataframely reports, `|`-delimited for column rules.

    Returns:
        The constraint, worded as an operator. `None` for a rule whose expression is arbitrary: a `@dy.rule()` body, or a column rule this package has not met.

    Example:
        >>> import dataframely as dy
        >>> from dagster_dataframely._rendering import rule_text
        >>> class Orders(dy.Schema):
        ...     order_id = dy.String(primary_key=True)
        ...     amount = dy.Float64(nullable=False, min=0.0)
        >>> rule_text(Orders, "amount|min")
        '>= 0.0'
        >>> rule_text(Orders, "primary_key")
        'PK: order_id'
    """
    if rule_name == "primary_key":
        # A schema with no key columns has no such rule, so the name belongs to a `@dy.rule()` and there is no key to state.
        keys: list[str] = schema.primary_key()
        return f"PK: {', '.join(keys)}" if keys else None
    parts: tuple[str, str] | None = split_rule(rule_name)
    if parts is None:
        return None
    column_name, kind = parts
    return _column_constraint(schema.columns()[column_name], kind)


def column_constraints(schema: type[dy.Schema]) -> dict[str, list[str]]:
    """Renders every column's constraints.

    Read off the same rule dict the asset checks come from, so every constraint has a check behind it and every check that says something about one column reaches that column's row.

    Args:
        schema: The schema to render.

    Returns:
        One list per column, keyed by column name, in the schema's own column order and each column's own rule order. A column with nothing to show carries an empty list.
    """
    constraints: dict[str, list[str]] = {name: [] for name in schema.columns()}
    for rule_name in validation_rules(schema):
        parts: tuple[str, str] | None = split_rule(rule_name)
        if parts is None:
            continue
        column_name, kind = parts
        if kind in _NO_CONSTRAINT:
            continue
        # The fallback is the rule's own half of the name: the row already carries the column's half, and dataframely's `|` is a spelling this package shows nowhere else.
        constraints[column_name].append(rule_text(schema, rule_name) or kind)
    return constraints


def table_constraints(schema: type[dy.Schema]) -> list[str]:
    """Renders the constraints that belong to no single column.

    The primary key above all. dataframely models it as one rule over a struct of every key column, so stating it once, here, is what distinguishes a composite key from two independent single-column ones. A `@dy.rule()` joins it under its own name, because its expression is arbitrary and its docstring is the description's to use.

    Args:
        schema: The schema to render.

    Returns:
        One entry per schema-level rule, in the schema's own rule order.
    """
    return [
        rule_text(schema, rule_name) or rule_name
        for rule_name in validation_rules(schema)
        if split_rule(rule_name) is None
    ]


def check_description(schema: type[dy.Schema], rule_name: str) -> str:
    """Describes a rule's asset check, falling back until something holds.

    The docstring first, because an author who wrote one said something the schema cannot. Then the rendered constraint, prefixed with the column, because a check list reads flat and a bare `>= 0` names nothing. Then the rule's own name, which always exists, so no check is ever described as nothing.

    Args:
        schema: The schema the rule belongs to.
        rule_name: The rule name dataframely reports.

    Returns:
        The check's description, never empty.
    """
    docstring: str | None = rule_description(schema, rule_name)
    if docstring:
        return docstring
    rendered: str | None = rule_text(schema, rule_name)
    if rendered is None:
        return rule_name
    parts: tuple[str, str] | None = split_rule(rule_name)
    if parts is None:
        return rendered
    column_name, _ = parts
    return f"{column_name} {rendered}"


def column_rule_summary(schema: type[dy.Schema], rule_names: Sequence[str]) -> str:
    """Renders every constraint one column's rules state, for the check that reports them as one.

    A collapsed check's description is the only place its members are visible before a run, so this renders all of them, including the two a constraint skips. `not null` has to be said here: there is no column row beside this check saying it instead.

    The column is named once by the check, so it is not repeated per constraint, and a rule with nothing structured to render falls back to its own half of its name.

    Args:
        schema: The schema the rules belong to.
        rule_names: One column's rules, in the schema's own order.

    Returns:
        The constraints, comma-separated.

    Example:
        >>> import dataframely as dy
        >>> from dagster_dataframely._rendering import column_rule_summary
        >>> class Orders(dy.Schema):
        ...     amount = dy.Float64(nullable=False, min=0.0)
        >>> column_rule_summary(Orders, ["amount|nullability", "amount|min"])
        'not null, >= 0.0'
    """
    rendered: list[str] = []
    for rule_name in rule_names:
        parts: tuple[str, str] | None = split_rule(rule_name)
        rendered.append(
            rule_text(schema, rule_name) or (parts[1] if parts else rule_name)
        )
    return ", ".join(rendered)
