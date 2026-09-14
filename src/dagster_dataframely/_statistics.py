"""The statistics a materialization carries: four tables, one per dtype group.

`USER_GUIDE.md` has the columns each group reports, and why the string group carries no value-bearing statistic.

Nothing here goes through `describe()`. It stringifies with per-source-dtype formatting and cannot be cast back: a `Date` mean renders as a datetime, a `Duration` mean as a clock time, and `min` mixes numbers, bare strings and dates in one column. Everything here is computed with typed Polars expressions and stringified once, at the display step.

Two rules divide the cells. `mean`, `std`, `p50` and `true_rate` are derived, so they round to four places. `min` and `max` exist in the data, so they show exactly. The UI must never display a number nobody stored.

This is its own module, not part of `_metadata`. That module holds what an asset declares before it runs. This one is computed from what a run wrote, and the two differ when the schema and the frame disagree.
"""

from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any

import dagster as dg
import polars as pl

from dagster_dataframely._samples import Cell, cell

_DERIVED = frozenset({"mean", "std", "p50", "true_rate"})
"""The derived cells, which round. Every other cell is a value out of the data, shown as it is."""

_PLACES = 4

_DURATION_STYLE = "polars"
"""Polars' own duration rendering, `8d` / `1m 30s` / `2h 5m`, the form its frame repr uses. The default gives ISO-8601 and a plain cast to `String` raises, so this format string is the only route to a span a human reads. Covered by a characterization test (#23)."""

_STRING_GROUP = (pl.String, pl.Categorical, pl.Enum, pl.Binary)
"""The dtypes whose useful statistics are all lengths and counts. Sharing one set of statistics makes them one table rather than four."""


def _cell(value: object) -> Cell:
    """Render one aggregate as a value a table record accepts.

    `_samples.cell` with one step ahead of it. The numeric group picks up `Decimal` columns, `min` and `max` on one return a Python `Decimal`, and `TableRecord` rejects it, so a money column would take the whole materialization down. A high-precision decimal loses digits through `float()`. That is display only; the exact value is still in the table it came from. A sampled row keeps a `Decimal` as a string instead, because it is the value somebody stored.

    Everything left is a date, a datetime or a time, and `cell` is where any of them becomes a string.
    """
    return cell(float(value) if isinstance(value, Decimal) else value)


def _rounded(value: object) -> Cell:
    """Render a derived statistic, rounding what `_cell` returns."""
    cell = _cell(value)
    return round(cell, _PLACES) if isinstance(cell, float) else cell


def _record(column: str, stats: Mapping[str, object]) -> dg.TableRecord:
    """Render one column's aggregates as a table row.

    The rounding rule lives here, not in the expressions, so it is stated once for every group instead of once per statistic.

    Returns
    -------
    One record: the column's name, then its statistics in the order they were computed, each rendered as a cell a table accepts.
    """
    return dg.TableRecord(
        {"column": column}
        | {
            name: _rounded(value) if name in _DERIVED else _cell(value)
            for name, value in stats.items()
        }
    )


def _numeric(name: str, _dtype: pl.DataType) -> pl.Expr:
    """Aggregate one `Int*`, `UInt*`, `Float*` or `Decimal` column."""
    column = pl.col(name)
    return pl.struct(
        count=column.len(),
        null_count=column.null_count(),
        mean=column.mean(),
        std=column.std(),
        min=column.min(),
        p50=column.median(),
        max=column.max(),
    ).alias(name)


def _temporal(name: str, dtype: pl.DataType) -> pl.Expr:
    """Aggregate one `Date`, `Datetime`, `Time` or `Duration` column.

    The span is a duration whatever the column is, because subtracting two dates, times or durations gives one. Polars' friendly form renders it; nobody reads a range off ISO-8601. A `Duration` column's own bounds render the same way, since they are the same kind of value as the span between them.

    The other three dtypes keep their values through the aggregate and become strings at the display step. So a `Date` renders as a date.
    """
    column = pl.col(name)
    low, high = column.min(), column.max()
    duration = dtype == pl.Duration
    return pl.struct(
        count=column.len(),
        null_count=column.null_count(),
        min=low.dt.to_string(_DURATION_STYLE) if duration else low,
        max=high.dt.to_string(_DURATION_STYLE) if duration else high,
        span=(high - low).dt.to_string(_DURATION_STYLE),
    ).alias(name)


def _string(name: str, dtype: pl.DataType) -> pl.Expr:
    """Aggregate one `String`, `Categorical`, `Enum` or `Binary` column.

    Lengths are bytes throughout. `Binary` has no other unit, and Dataframely's `max_length` bounds a `String` in bytes, so a constraint and this table agree.

    The cast lets one expression measure all four. It reads a label as the string it already is, and nothing it produces reaches the data. `Binary` is exempt because it has no string form to read.
    """
    column = pl.col(name)
    length = (
        column.bin.size()
        if dtype == pl.Binary
        else column.cast(pl.String).str.len_bytes()
    )
    return pl.struct(
        count=column.len(),
        null_count=column.null_count(),
        # Nulls dropped: `null_count` already states them, and a null is not a value the column holds.
        n_unique=column.drop_nulls().n_unique(),
        min_len=length.min(),
        max_len=length.max(),
        n_empty=(length == 0).sum(),
    ).alias(name)


def _boolean(name: str, _dtype: pl.DataType) -> pl.Expr:
    """Aggregate one `Bool` column.

    Both counts are stated, not one and a total: those two plus `null_count` add up to `count`, and a reader should not have to do that subtraction. The rate is over the non-null rows, as `mean` gives.
    """
    column = pl.col(name)
    return pl.struct(
        count=column.len(),
        null_count=column.null_count(),
        n_true=column.sum(),
        n_false=(~column).sum(),
        true_rate=column.mean(),
    ).alias(name)


_AGGREGATES: dict[str, Callable[[str, pl.DataType], pl.Expr]] = {
    "numeric": _numeric,
    "temporal": _temporal,
    "string": _string,
    "boolean": _boolean,
}
"""The groups and the aggregate each one runs, keyed by the name its metadata key ends in."""


def _group(dtype: pl.DataType) -> str | None:
    """Name the group a dtype belongs to.

    Polars' own predicates, not a transcribed dtype list, so a new integer width reaches the numeric group without touching this file.

    Returns
    -------
    The group, or `None` for a dtype that belongs to none.
    """
    if dtype.is_numeric():
        return "numeric"
    if dtype.is_temporal():
        return "temporal"
    if dtype in _STRING_GROUP:
        return "string"
    if dtype == pl.Boolean:
        return "boolean"
    return None


def _group_table(
    frame: pl.DataFrame,
    columns: Mapping[str, pl.DataType],
    aggregate: Callable[[str, pl.DataType], pl.Expr],
) -> dg.TableMetadataValue:
    """Render one group's table.

    One `select` aggregates every column of the group, so a wide frame costs one pass per group, not one per column. Each column returns one struct aliased to its own name, so its statistics stay together and no name needs mangling to sit beside another column's in one row.

    Returns
    -------
    The group's table, one row per column in the frame's own order.
    """
    # One row of structs, one struct per column. `Any` is the type a row of anything comes back as.
    stats: dict[str, Any] = frame.select(
        aggregate(name, dtype) for name, dtype in columns.items()
    ).row(0, named=True)
    return dg.MetadataValue.table([_record(name, stats[name]) for name in columns])


def statistics_metadata(
    frame: pl.DataFrame, *, enabled: bool
) -> dict[str, dg.TableMetadataValue]:
    """Summarize a frame as one table per dtype group present in it.

    Table values, not markdown. A table value renders as a full-featured HTML table in the UI; the same rows as markdown render as printed text. Judged in the running UI.

    Parameters
    ----------
    frame
        The frame that materialized. Eager, because the caller already collected it.
    enabled
        Whether to compute them at all. `False` returns before the frame is touched, so a setting turned off costs nothing.

    Returns
    -------
    One entry per group present, keyed `dataframely/valid_statistics/<group>`, each holding a row per column in the frame's own column order. Empty when the setting is off. No entry for a group the frame has no column of.
    """
    if not enabled:
        return {}
    groups: dict[str, dict[str, pl.DataType]] = {}
    for name, dtype in frame.collect_schema().items():
        group: str | None = _group(dtype)
        if group is not None:
            groups.setdefault(group, {})[name] = dtype
    return {
        f"dataframely/valid_statistics/{group}": _group_table(
            frame, columns, _AGGREGATES[group]
        )
        for group, columns in groups.items()
    }
