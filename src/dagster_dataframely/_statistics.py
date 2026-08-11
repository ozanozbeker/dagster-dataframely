"""The profile a materialization carries: four tables, one per dtype family.

A data consumer gets a `skimr`-style distribution read off the asset itself rather than reaching for a separate tool, on every materialization unless the `statistics` knob turns the pass off.

**Never through `describe()`.** It stringifies with per-source-dtype formatting and cannot be cast back: a `Date` mean renders as a datetime, a `Duration` mean as a clock time, and `min` mixes numbers, bare strings and dates in a single column. Everything here is computed with typed polars expressions and stringified once, deliberately, at the display step.

Two rules divide the cells. `mean`, `std`, `p50` and `true_rate` are derived, so they round to four places. `min` and `max` are values that exist in the data, so they are shown exactly: the UI must never display a number nobody stored.

**The string family carries no value-bearing statistic, deliberately.** A `min` or `max` on an email column would print real addresses permanently into a shared, exported event log, and lengths and cardinality catch what you would actually catch. The knob does not cover this: consenting to summary statistics is not consenting to raw values.

A column whose dtype belongs to no family reaches no table at all. A `List`, a `Struct` or an `Array` has nothing to say past a count and a null count, and two numbers do not earn a fifth table.

Its own module rather than `_metadata`'s. That one holds what an asset declares about its data before it has ever run; this is computed from what a run actually wrote, and the two answer to different things when the schema and the frame disagree.
"""

from collections.abc import Callable, Mapping
from decimal import Decimal
from typing import Any

import dagster as dg
import polars as pl

#: Cells nobody stored. Every other cell is a value out of the data, shown as it is.
_DERIVED = frozenset({"mean", "std", "p50", "true_rate"})

_PLACES = 4

#: polars' own duration rendering, `8d` / `1m 30s` / `2h 5m`, which is the form its own frame repr uses. The default gives ISO-8601 and a plain cast to `String` raises, so this format string is the only route to a span a human reads. Pinned by its own test (#23).
_DURATION_STYLE = "polars"

#: The dtypes whose useful statistics are all lengths and counts, which is what makes them one table rather than four.
_STRING_FAMILY = (pl.String, pl.Categorical, pl.Enum, pl.Binary)


def _cell(value: object) -> str | int | float | bool | None:
    """Renders one aggregate as something a table record accepts.

    `Decimal` is why this is not optional. The numeric family picks up `Decimal` columns, `min` and `max` on one return a Python `Decimal`, and `TableRecord` rejects it outright, so a money column would take the whole materialization down. A very high-precision decimal loses digits through `float()`; that is display only, and the exact value is still in the table it came from.

    Ints pass through as ints rather than being floated, because a count is not improved by reading `3.0`.

    Everything left is a date, a datetime or a time, and this is the one place any of them becomes a string.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    return str(value)


def _rounded(value: object) -> str | int | float | bool | None:
    """Renders a statistic that was computed rather than observed, rounding what `_cell` returns."""
    cell = _cell(value)
    return round(cell, _PLACES) if isinstance(cell, float) else cell


def _record(column: str, stats: Mapping[str, object]) -> dg.TableRecord:
    """Renders one column's aggregates as the row a table shows.

    The rounding rule lives here rather than in the expressions, so it is stated once for every family instead of once per statistic.

    Args:
        column: The column the row is about, which is the table's first cell.
        stats: The aggregates, keyed by statistic name, in the order the table shows them.
    """
    return dg.TableRecord(
        {"column": column}
        | {
            name: _rounded(value) if name in _DERIVED else _cell(value)
            for name, value in stats.items()
        }
    )


def _numeric(name: str, _dtype: pl.DataType) -> pl.Expr:
    """Aggregates one `Int*`, `UInt*`, `Float*` or `Decimal` column."""
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
    """Aggregates one `Date`, `Datetime`, `Time` or `Duration` column.

    The span is a duration whatever the column is: subtracting two dates, two times or two durations gives one, and it is rendered in polars' friendly form because ISO-8601 is not a thing anybody reads a range off. A `Duration` column's own bounds are rendered the same way, since they are the same kind of value as the span between them.

    The other three dtypes keep their values through the aggregate and become strings at the display step, which is what makes a `Date` render as a date.
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
    """Aggregates one `String`, `Categorical`, `Enum` or `Binary` column.

    Lengths are bytes throughout: `Binary` has no other unit, and bytes is what dataframely's own `max_length` bounds on a `String`, so a pill and this table agree.

    The cast is what lets one expression measure all four. It reads a label as the string it already is, and nothing it produces reaches the data; `Binary` is exempt because it has no string form to read.
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
        # Nulls dropped, because `null_count` already states them and a null is not a value the column holds.
        n_unique=column.drop_nulls().n_unique(),
        min_len=length.min(),
        max_len=length.max(),
        n_empty=(length == 0).sum(),
    ).alias(name)


def _boolean(name: str, _dtype: pl.DataType) -> pl.Expr:
    """Aggregates one `Bool` column.

    Both counts are stated rather than one and a total, because the two plus `null_count` are what add up to `count` and a reader should not have to do that subtraction. The rate is over the non-null rows, which is what `mean` gives.
    """
    column = pl.col(name)
    return pl.struct(
        count=column.len(),
        null_count=column.null_count(),
        n_true=column.sum(),
        n_false=(~column).sum(),
        true_rate=column.mean(),
    ).alias(name)


#: The families, and the aggregate each one runs. Keyed by the name its metadata key ends in.
_AGGREGATES: dict[str, Callable[[str, pl.DataType], pl.Expr]] = {
    "numeric": _numeric,
    "temporal": _temporal,
    "string": _string,
    "boolean": _boolean,
}


def _family(dtype: pl.DataType) -> str | None:
    """Names the family a dtype belongs to.

    polars' own predicates rather than a transcribed dtype list, so a new integer width lands in the numeric family without this file being touched.

    Args:
        dtype: The column's dtype.

    Returns:
        The family, or `None` for a dtype that belongs to none.
    """
    if dtype.is_numeric():
        return "numeric"
    if dtype.is_temporal():
        return "temporal"
    if dtype in _STRING_FAMILY:
        return "string"
    if dtype == pl.Boolean:
        return "boolean"
    return None


def _family_table(
    frame: pl.DataFrame,
    columns: Mapping[str, pl.DataType],
    aggregate: Callable[[str, pl.DataType], pl.Expr],
) -> dg.TableMetadataValue:
    """Renders one family's table.

    Every column of the family is aggregated in a single `select`, so a wide frame costs one pass per family rather than one per column. The struct each column returns is what keeps its statistics together: the alias is the column's own name, so nothing has to be mangled to fit two columns' cells side by side in one row.

    Args:
        frame: The frame that materialized.
        columns: The family's columns and their dtypes, in the frame's own order.
        aggregate: The family's aggregate, applied to each of them.
    """
    # One row of structs, one struct per column: `Any` is what a row of anything comes back as.
    stats: dict[str, Any] = frame.select(
        aggregate(name, dtype) for name, dtype in columns.items()
    ).row(0, named=True)
    return dg.MetadataValue.table([_record(name, stats[name]) for name in columns])


def statistics_metadata(
    frame: pl.DataFrame, *, enabled: bool
) -> dict[str, dg.TableMetadataValue]:
    """Profiles a frame as one table per dtype family present in it.

    Table values rather than markdown: a table value renders as a full-featured HTML table in the UI, while the same rows as markdown render as printed text. Judged in the running UI.

    Args:
        frame: The frame that materialized. Eager, because the caller already collected it.
        enabled: Whether to profile at all. `False` returns before the frame is touched, so a knob turned off costs nothing rather than costing a pass whose result is discarded.

    Returns:
        One entry per family present, keyed `stats/<family>`, each holding a row per column in the frame's own column order. Empty when the knob is off, and empty of any family the frame has no column of.
    """
    if not enabled:
        return {}
    families: dict[str, dict[str, pl.DataType]] = {}
    for name, dtype in frame.collect_schema().items():
        family: str | None = _family(dtype)
        if family is not None:
            families.setdefault(family, {})[name] = dtype
    return {
        f"stats/{family}": _family_table(frame, columns, _AGGREGATES[family])
        for family, columns in families.items()
    }
