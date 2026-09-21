"""Group `granularity`: the same rules collapsed four ways.

Clean data throughout, so every check is green and the only thing that differs between the four is the shape of the check list itself. Open the Checks tabs side by side.

**Changing `check_granularity` on an asset that has already run orphans its check history.** The old check names stop being reported and their timelines end where the change landed, while the new ones start empty. Nothing migrates them, so it is a choice to make before an asset ships.
"""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders

GROUP = "granularity"


@dd.asset(
    Orders,
    group_name=GROUP,
    description=(
        "`check_granularity='rule'`, the default: one check per Dataframely rule, each with its "
        "own timeline.\n\n"
        "Every rule off thirteen columns, plus the blocking column-schema check. Compare the "
        "length of this list against the other three in this group."
    ),
)
def orders_by_rule(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """One check per rule."""
    return raw_orders


@dd.asset(
    Orders,
    group_name=GROUP,
    check_granularity="column",
    description=(
        "`check_granularity='column'`: one `dy_col__<column>` check per rule-bearing column.\n\n"
        "This is what makes a forty-column schema's check list readable. A column's rules land "
        "together whatever they are, and each collapsed check carries `dy_rules`, a row per "
        "member rule with its failure count and its expression.\n\n"
        "The rules no single column owns land in `dy_schema__rules`, which is what `schema_rules` "
        "decides. This asset takes the default."
    ),
)
def orders_by_column(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """One check per rule-bearing column."""
    return raw_orders


@dd.asset(
    Orders,
    group_name=GROUP,
    check_granularity="column",
    schema_rules="per_rule",
    description=(
        "The same `column` granularity with `schema_rules='per_rule'`.\n\n"
        "A schema-level rule is one no single column owns: `paid_orders_have_amount`, "
        "`line_numbers_are_dense` and the composite primary key. `collapsed`, the default, puts "
        "all of them in one `dy_schema__rules`; this gives each its own check and its own "
        "history.\n\n"
        "Diff its check list against `orders_by_column`. The `dy_col__*` checks are identical and "
        "the difference is entirely at the bottom, where one check becomes several.\n\n"
        "The setting is read at `column` granularity and nowhere else: `rule` already gives every "
        "rule a check, and `schema` has no second place to put one."
    ),
)
def orders_by_column_per_rule(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """Split the schema-level rules out, which is the other half of `column` granularity."""
    return raw_orders


@dd.asset(
    Orders,
    group_name=GROUP,
    check_granularity="schema",
    description=(
        "`check_granularity='schema'`: a single `dy_schema__rules` for every rule at once.\n\n"
        "Two checks left, and one of them is the column-schema check. One timeline for the whole "
        "schema.\n\n"
        "A collapsed check has no total failure count, because one row can break several rules "
        "and a sum would state a row count that is not one. The per-rule counts are in `dy_rules`."
    ),
)
def orders_by_schema(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """One check for every rule."""
    return raw_orders
