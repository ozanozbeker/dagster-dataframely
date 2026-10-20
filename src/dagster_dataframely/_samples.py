"""`sample_rows` and `build_sample_metadata` render a frame's first rows as table metadata, for the row samples in `_runtime` and the failure examples in `_checks` and `_collection`.

Dagster stores metadata in its event log without redaction, so `sample_rows` always takes a row limit.
"""

import dagster as dg
import polars as pl

TABLE_SAMPLE_KEY = "dataframely/table_sample"

type Cell = str | int | float | bool | None
type Row = dict[str, Cell]


def render_cell(value: object) -> Cell:
    """Render one value as a `dg.TableRecord` cell.

    A `Decimal` becomes a string rather than a float, so it keeps its digits.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def sample_rows(frame: pl.DataFrame, limit: int) -> list[Row]:
    """Read the first `limit` rows as `dg.TableRecord` cells.

    A random sample would differ between two reads of the same frame.
    """
    return [
        {name: render_cell(value) for name, value in row.items()}
        for row in frame.head(limit).iter_rows(named=True)
    ]


def build_sample_metadata(
    key: str, rows: list[Row]
) -> dict[str, dg.TableMetadataValue]:
    """Return the table metadata entry for `rows`, or an empty dict when there are none.

    Dagster raises on a table value with no records and no schema.
    """
    if not rows:
        return {}
    return {key: dg.MetadataValue.table([dg.TableRecord(row) for row in rows])}
