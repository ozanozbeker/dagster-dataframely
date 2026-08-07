"""The reserved namespace, the rule-name rewrite, and the two definition-time collision errors.

`dy_` is hardcoded rather than configurable. A reserved namespace is not a preference; its whole value is being the same string in every project, so a knob would only let one project make its check names unrecognisable to the next.
"""

import inspect

import dataframely as dy

# Neither has a public equivalent. Pinned by their own tests (#16).
from dataframely._rule import Rule, RuleFactory

from dagster_dataframely.errors import CheckNameCollisionError, ReservedColumnError

# Spelled out again wherever a name is built, never interpolated: one grep for `dy_rule__` finds every producer and consumer.
RESERVED_PREFIX = "dy_"

#: The gate check. Present at every granularity, always blocking.
GATE_CHECK = "dy_schema__dtypes"


def check_name(rule_name: str) -> str:
    """Rewrites a dataframely rule name into an asset-check name.

    `amount|min` becomes `dy_rule__amount__min`, the same string wherever the rule shows up.

    The rewrite is forced, not chosen: every check spec becomes an op output named `<asset>_<check>`, and Dagster validates that against `^[A-Za-z0-9_]+$`, which `|` fails.

    Args:
        rule_name: The rule name dataframely reports, `|`-delimited for column rules.

    Returns:
        The asset-check name, inside the reserved namespace.

    Example:
        >>> check_name("amount|min")
        'dy_rule__amount__min'
        >>> check_name("paid_orders_have_amount")
        'dy_rule__paid_orders_have_amount'
    """
    return f"dy_rule__{rule_name.replace('|', '__')}"


def validation_rules(schema: type[dy.Schema]) -> dict[str, Rule]:
    """Returns the schema's validation rules, keyed by rule name.

    `with_cast=False` drops the `<column>|dtype` pseudo-rules, which would otherwise duplicate the gate at a different severity and without blocking.

    Args:
        schema: The schema to read rules from.

    Returns:
        Each rule keyed by the name dataframely gives it, `|`-delimited for column rules.
    """
    return schema._validation_rules(with_cast=False)  # noqa: SLF001


def rule_description(schema: type[dy.Schema], rule_name: str) -> str | None:
    """Returns a rule's docstring, or `None` for a rule that has no place to carry one.

    A `@dy.rule()` leaves its `RuleFactory` on the class, so the decorated function and its docstring stay reachable by name long after the metaclass has built the `Rule`. Column rules are generated from column arguments and have no function at all, and their `|` means the lookup misses rather than needing a branch.

    Args:
        schema: The schema the rule belongs to.
        rule_name: The rule name dataframely reports.

    Returns:
        The rule's docstring, dedented, or `None` if it has none.
    """
    factory = getattr(schema, rule_name, None)
    if not isinstance(factory, RuleFactory):
        return None
    return inspect.getdoc(factory.validation_fn)


def validate_namespace(schema: type[dy.Schema]) -> None:
    """Raises the two errors Dagster would otherwise report opaquely, or not at all.

    Args:
        schema: The schema whose columns and rule names are being claimed.

    Raises:
        ReservedColumnError: A user column sits inside the reserved namespace.
        CheckNameCollisionError: Two rules rewrite to the same asset-check name.
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
