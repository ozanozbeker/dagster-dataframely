"""The statistics tables of a materialization, one per dtype group.

`_metadata` holds what an asset declares before it runs, and this module summarizes the valid rows a run writes.
Nothing here uses Polars' `describe()`, because it formats values per source dtype, and nothing can cast them back.
"""

from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any

import dagster as dg
import polars as pl

from dagster_dataframely._samples import Cell, cell

_DERIVED = frozenset({"mean", "std", "p50", "true_rate"})

_PLACES = 4

_DURATION_STYLE = "polars"
"""The default of `dt.to_string()` is ISO-8601, and a cast to `String` raises, as a characterization test pins (#23)."""

_STRING_GROUP = (pl.String, pl.Categorical, pl.Enum, pl.Binary)


def _cell(value: object) -> Cell:
    """Render one aggregate as a table cell.

    A `Decimal` becomes a float, because `TableRecord` rejects it.
    """
    return cell(float(value) if isinstance(value, Decimal) else value)


def _rounded(value: object) -> Cell:
    """Render a derived statistic, rounding what `_cell` returns."""
    rendered = _cell(value)
    return round(rendered, _PLACES) if isinstance(rendered, float) else rendered


def _record(column: str, aggregates: Mapping[str, object]) -> dg.TableRecord:
    """Render one column's aggregates as a table row."""
    return dg.TableRecord(
        {"column": column}
        | {
            name: _rounded(value) if name in _DERIVED else _cell(value)
            for name, value in aggregates.items()
        }
    )


def _numeric(name: str, _dtype: pl.DataType) -> pl.Expr:
    """Aggregate one numeric column."""
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
    """Aggregate one temporal column.

    A `Duration` column's bounds use the span's format, because they are the same kind of value.
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
    """Aggregate one column of the string group.

    Lengths are bytes, the unit of Dataframely's `max_length`, so this table matches the column constraints.
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
        # `null_count` already reports nulls, so `n_unique` excludes them.
        n_unique=column.drop_nulls().n_unique(),
        min_len=length.min(),
        max_len=length.max(),
        n_empty=(length == 0).sum(),
    ).alias(name)


def _boolean(name: str, _dtype: pl.DataType) -> pl.Expr:
    """Aggregate one `Boolean` column."""
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


def _group(dtype: pl.DataType) -> str | None:
    """Name the dtype group a dtype belongs to, or `None` when it belongs to none."""
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
    """Render one group's table."""
    row: dict[str, Any] = frame.select(
        aggregate(name, dtype) for name, dtype in columns.items()
    ).row(0, named=True)
    return dg.MetadataValue.table([_record(name, row[name]) for name in columns])


def statistics_metadata(
    frame: pl.DataFrame, *, enabled: bool
) -> dict[str, dg.TableMetadataValue]:
    """Summarize a frame as one table per dtype group present in it."""
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
