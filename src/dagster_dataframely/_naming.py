"""The reserved namespace and the two names built out of it.

`dy_` is hardcoded, not configurable. Its value lies in being the same string in every project, so a setting would only let one project make its check names unrecognisable to the next.

Strings in, strings out. This module knows nothing about a schema, a rule or a frame, which is why it imports nothing: what a rule is and whether a schema may claim these names are `_rules`' questions, and it asks them through `check_name` below.
"""

# Spelled out again wherever a name is built, never interpolated: one grep for `dy_rule__` finds every producer and consumer.
RESERVED_NAMESPACE = "dy_"

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

    Returns
    -------
    The asset-check name, inside the reserved namespace.
    """
    return f"dy_rule__{rule_name.replace('|', '__')}"


def column_check_name(column: str) -> str:
    """Name the check that reports every rule on one column.

    Returns
    -------
    The asset-check name, inside the reserved namespace.
    """
    return f"dy_col__{column}"
