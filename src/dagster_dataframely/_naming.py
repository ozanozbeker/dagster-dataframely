"""The reserved namespace, the rule-name rewrite, and the two definition-time collision errors.

`dy_` is hardcoded, not configurable. Its value lies in being the same string in every project, so a setting would only let one project make its check names unrecognisable to the next.
"""

import inspect

import dataframely as dy

# Neither has a public equivalent. Covered by characterization tests (#16).
from dataframely._rule import Rule, RuleFactory

from dagster_dataframely.errors import CheckNameCollisionError, ReservedColumnError

# Spelled out again wherever a name is built, never interpolated: one grep for `dy_rule__` finds every producer and consumer.
RESERVED_PREFIX = "dy_"

COLUMN_SCHEMA_CHECK = "dy_schema__columns"
"""The column-schema check. Present at every granularity, always blocking."""

SCHEMA_RULES_CHECK = "dy_schema__rules"
"""The check the rules no single column owns report through when they are collapsed.

Not `dy_col__schema`, which would collide with a user column named `schema`, a column somebody has.
"""


def check_name(rule_name: str) -> str:
    """Rewrite a Dataframely rule name into an asset-check name.

    `amount|min` becomes `dy_rule__amount__min`, the same string wherever the rule shows up.

    The rewrite is forced. Every check spec becomes an op output named `<asset>_<check>`, and Dagster validates that against `^[A-Za-z0-9_]+$`, which `|` fails.

    Parameters
    ----------
    rule_name
        The rule name Dataframely reports, `|`-delimited for column rules.

    Returns
    -------
    The asset-check name, inside the reserved namespace.

    Examples
    --------
    ```python
    check_name("amount|min")  # 'dy_rule__amount__min'
    check_name("paid_orders_have_amount")  # 'dy_rule__paid_orders_have_amount'
    ```
    """
    return f"dy_rule__{rule_name.replace('|', '__')}"


def column_check_name(column: str) -> str:
    """Name the check that reports every rule on one column.

    Parameters
    ----------
    column
        The column the rules belong to.

    Returns
    -------
    The asset-check name, inside the reserved namespace.

    Examples
    --------
    ```python
    column_check_name("amount")  # 'dy_col__amount'
    ```
    """
    return f"dy_col__{column}"


def split_rule(rule_name: str) -> tuple[str, str] | None:
    """Split a column rule into the column it belongs to and its own kind.

    The one place the package reads Dataframely's delimiter instead of rewriting it. A rule no single column owns has no delimiter and returns `None`. A Python identifier cannot contain `|`, so the presence of `|` alone decides.

    Parameters
    ----------
    rule_name
        The rule name Dataframely reports.

    Returns
    -------
    The column name and the rule's own kind, or `None` for a rule no column owns.

    Examples
    --------
    ```python
    split_rule("amount|min")  # ('amount', 'min')
    split_rule("primary_key")  # None
    ```
    """
    column_name, delimiter, kind = rule_name.partition("|")
    return (column_name, kind) if delimiter else None


def validation_rules(schema: type[dy.Schema]) -> dict[str, Rule]:
    """Return the schema's validation rules, keyed by rule name.

    `with_cast=False` drops the `<column>|dtype` pseudo-rules. They would otherwise duplicate the column-schema check at a different severity and without blocking.

    Parameters
    ----------
    schema
        The schema to read rules from.

    Returns
    -------
    Each rule keyed by the name Dataframely gives it, `|`-delimited for column rules.
    """
    return schema._validation_rules(with_cast=False)  # noqa: SLF001


def rule_description(schema: type[dy.Schema], rule_name: str) -> str | None:
    """Return a rule's docstring, or `None` for a rule with no place to carry one.

    A `@dy.rule()` leaves its `RuleFactory` on the class, so the decorated function and its docstring stay reachable by name after the metaclass has built the `Rule`. Column rules are generated from column arguments and have no function. Their `|` makes the lookup miss, so no branch is needed.

    Parameters
    ----------
    schema
        The schema the rule belongs to.
    rule_name
        The rule name Dataframely reports.

    Returns
    -------
    The rule's docstring, dedented, or `None` if it has none.
    """
    factory = getattr(schema, rule_name, None)
    if not isinstance(factory, RuleFactory):
        return None
    return inspect.getdoc(factory.validation_fn)


def validate_namespace(schema: type[dy.Schema]) -> None:
    """Raise the two errors Dagster would otherwise report opaquely, or not at all.

    Parameters
    ----------
    schema
        The schema whose columns and rule names are being claimed.

    Raises
    ------
    ReservedColumnError
        A user column sits inside the reserved namespace.
    CheckNameCollisionError
        Two rules rewrite to the same asset-check name.
    """
    reserved: list[str] = [
        column for column in schema.columns() if column.startswith(RESERVED_PREFIX)
    ]
    if reserved:
        raise ReservedColumnError(schema.__name__, reserved, RESERVED_PREFIX)

    seen: dict[str, str] = {}
    for rule in validation_rules(schema):
        name: str = check_name(rule)
        if name in seen:
            raise CheckNameCollisionError(schema.__name__, seen[name], rule, name)
        seen[name] = rule
