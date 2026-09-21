"""Group `partitions`: one dimension, two dimensions, and a fan-in over all of them.

`partitions_def` forwards to `@dg.asset` verbatim, so partitioning needed no code in the package and has no setting of its own. The quarantine is written under the same partition key, which is what stops it escaping its asset's partitioning.

Validation runs per partition on that partition's frame, so `dagster/row_count` is that partition's count and a partition whose frame drifts aborts at the column-schema check without touching any other partition's file.
"""

import datetime as dt

import dagster as dg
import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo import _data
from dagster_dataframely_demo.schema import Orders

GROUP = "partitions"

#: Five days, fixed rather than rolling, so a backfill stays small and a screenshot keeps its keys.
DAILY = dg.DailyPartitionsDefinition(start_date="2026-08-01", end_date="2026-08-06")

#: The same five days crossed with two regions: ten cells, each its own file.
GRID = dg.MultiPartitionsDefinition({
    "day": DAILY,
    "region": dg.StaticPartitionsDefinition(["eu", "us"]),
})


@dd.asset(
    Orders,
    group_name=GROUP,
    partitions_def=DAILY,
    description=(
        "The orders placed on one day, sliced out of the whole of `raw_orders`.\n\n"
        "An unpartitioned parent has no partitions to map, so every partition of this asset "
        "loads the entire base table and takes its own day out of it. That is the mapping "
        "Dagster applies by default, and it is why a partitioned asset can sit downstream of "
        "an unpartitioned one at all.\n\n"
        "It declares a `context` to reach `partition_key`, which is what a partitioned "
        "`@dg.asset` does too. Every check reports per partition, so the Checks tab carries a "
        "status per key, and a red partition says which day rather than which asset."
    ),
)
def daily_orders(
    context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
) -> pl.DataFrame:
    """One day's slice, taken out of the whole table by the partition key."""
    return _data.orders_on(raw_orders, dt.date.fromisoformat(context.partition_key))


@dd.asset(
    Orders,
    group_name=GROUP,
    partitions_def=GRID,
    quarantine=True,
    description=(
        "The same days crossed with two regions, so ten cells rather than five.\n\n"
        "`context.partition_key` is a `dg.MultiPartitionKey` here, a `str` subclass rendering as "
        "`2026-08-01|eu`. Read a dimension off `keys_by_dimension` rather than splitting that "
        "string: both the string and the paths on disk sort by dimension name, so renaming a "
        "dimension reorders them.\n\n"
        "Storage nests one directory per dimension, `regional_orders/2026-08-01/eu.parquet`, and "
        "the quarantine would mirror it cell for cell. This data is clean, so no cell writes one."
    ),
)
def regional_orders(
    context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
) -> pl.DataFrame:
    """One cell of the day-by-region grid."""
    cell = context.partition_key.keys_by_dimension
    return _data.orders_for(
        raw_orders, dt.date.fromisoformat(cell["day"]), cell["region"]
    )


#: Partitioned cell for cell like its parent, which the spec reads off the definition rather than being told.
regional_orders_quarantine = dd.quarantine_spec(
    Orders, regional_orders
).replace_attributes(
    group_name=GROUP,
    description=(
        "The grid's invalid rows, partitioned cell for cell exactly as the valid table is.\n\n"
        "Passing the definition rather than its key is what carries the partitions across, so "
        "the spec cannot disagree with its parent about them."
    ),
)


@dd.asset(
    Orders,
    group_name=GROUP,
    description=(
        "Every partition of `daily_orders` at once, assembled by the IO manager.\n\n"
        "An unpartitioned asset depending on the whole of a partitioned one gets a dict keyed by "
        "partition key. `dict[str, pl.LazyFrame]` is the lazy spelling: every value is that "
        "partition's scan rather than its rows, so the concat is one plan over every partition "
        "and nothing is read until `Schema.filter` executes it.\n\n"
        "Annotate the dict, not the frame. `pl.DataFrame` fails Dagster's type check after every "
        "partition has already been read."
    ),
)
def orders_rollup(daily_orders: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """Concatenate five days of scans into one plan."""
    return pl.concat(daily_orders.values())
