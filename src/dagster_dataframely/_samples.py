"""The sample rows that `_checks` and `_runtime` write to metadata.

Sample rows go into the Dagster event log, which nothing redacts, so `sample_rows` always takes a row limit.
"""

import dagster as dg
import polars as pl

VALID_SAMPLE_KEY = "dataframely/valid_sample"

type Cell = str | int | float | bool | None
type Row = dict[str, Cell]


def cell(value: object) -> Cell:
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
        {name: cell(value) for name, value in row.items()}
        for row in frame.head(limit).iter_rows(named=True)
    ]


def sample_metadata(key: str, rows: list[Row]) -> dict[str, dg.TableMetadataValue]:
    """Return the metadata entry for a sample, or nothing when there are no rows.

    Dagster raises on a table value with no records and no schema.
    """
    if not rows:
        return {}
    return {key: dg.MetadataValue.table([dg.TableRecord(row) for row in rows])}
