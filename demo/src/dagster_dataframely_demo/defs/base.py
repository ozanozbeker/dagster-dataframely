"""Group `base`: the four tables every other group reads from.

Plain `@dg.asset`s with no schema attached, which is the point twice over. They are the control for `catalog`, where the same rows under a schema fill a Columns tab these cannot. And they are where the demo's data enters the graph, so every other asset has a parent and each group reads as one chain rather than a scatter of roots.

One asset per frame shape in `_data.py`. The failure groups need their defect to arrive from upstream rather than be manufactured in the asset that demonstrates it: a table whose dtype drifted is something that happens to you, and an asset that casts its own column to break its own schema is not the case worth showing.
"""

import dagster as dg
import polars as pl

from dagster_dataframely_demo import _data

GROUP = "base"


@dg.asset(
    group_name=GROUP,
    description=(
        "Twelve clean order lines, declaring nothing about their own shape. "
        "A plain `@dg.asset`, so it is the control: open its Columns tab beside `orders` "
        "to see what the schema is worth. This one is empty until the asset has run, and "
        "then holds only what the IO manager could infer.\n\n"
        "Five days of `ordered_at` across ten orders, which is what lets `partitions` slice "
        "it by day and by region without a second table."
    ),
)
def raw_orders() -> pl.DataFrame:
    """Return the control: the same rows with no schema attached."""
    return _data.clean_orders()


@dg.asset(
    group_name=GROUP,
    description=(
        "The same twelve lines plus eight that break a rule, one rule per row bar two.\n\n"
        "`ORD-0015` trips three rules at once, which is what makes a quarantine's "
        "`dataframely/invalid_by_rules` table read as one broken row rather than three "
        "unrelated counts. `ORD-0016` skips line 2, so `line_numbers_are_dense` rejects both "
        "of that order's lines: the case where a rule takes down a group rather than a row.\n\n"
        "Read by `failure/quarantine` and `failure/no_quarantine`, which are the same rows "
        "with and without somewhere for the bad ones to go."
    ),
)
def defective_raw_orders() -> pl.DataFrame:
    """Twenty lines, eight of them invalid."""
    return _data.defective_orders()


@dg.asset(
    group_name=GROUP,
    description=(
        "Three lines, every one of them negative, so nothing survives a filter against "
        "`Orders`.\n\n"
        "The input for `failure/nothing_survives`, where the valid table is skipped rather "
        "than emptied."
    ),
)
def hopeless_raw_orders() -> pl.DataFrame:
    """Every row invalid, which is a different exit from most rows invalid."""
    return _data.hopeless_orders()


@dg.asset(
    group_name=GROUP,
    description=(
        "The clean lines with `quantity` arriving `Int32` widened to `Int64`.\n\n"
        "A pipeline defect rather than a data one, and it arrives from here rather than being "
        "cast inside the asset that fails on it: this is the upstream table whose dtype drifted. "
        "The input for `failure/column_schema`."
    ),
)
def mistyped_raw_orders() -> pl.DataFrame:
    """Return the dtype drift, one table upstream of the asset that refuses it."""
    return _data.mistyped_orders()
