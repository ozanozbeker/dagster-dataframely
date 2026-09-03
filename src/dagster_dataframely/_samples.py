"""The rows a run puts in front of a reader, bounded.

A check and a materialization want the same thing. A failing check says 43 rows failed `amount|min`, and the reader asks what three of them held. A materialization says 90,000 rows were written, and the reader asks what one looks like. A count answers neither. A handful of rows answers both.

Bounded by construction. There is no unbounded setting and no unbounded read. Nothing leaves this module without a caller saying how many rows it wanted. **These rows go into the Dagster event log, which is shared, exported and not redacted**, so somebody chooses the amount that lands there.

A sample is absent, never empty. Zero rows does not answer "what does a row look like", and an empty table in the UI would read as an answer. Dagster enforces the same from the other side: a table value with no records and no schema is an error, so an empty one would fail the run rather than show nothing.

Its own module rather than `_metadata`'s or `_statistics`'. `_metadata` holds what an asset declares before it runs. This module holds what a run held. And unlike a statistic, a cell here is a value out of the data, which decides the rendering rule in `_cell`.
"""

import dagster as dg
import polars as pl

VALID_SAMPLE_KEY = "dataframely/valid_sample"
"""The valid rows' materialization display key. Namespaced like `dataframely/valid_stats/*`, so everything this package writes sorts in one block apart from Dagster's and the IO manager's."""

type Cell = str | int | float | bool | None
"""What a `dg.TableRecord` cell may hold. Dagster states the union inline on the record's field and exports no name for it."""

type Row = dict[str, Cell]
"""One row, rendered."""


def _cell(value: object) -> Cell:
    """Render one value as something a table record accepts.

    Everything a record cannot hold becomes its string form, once, here. `Decimal`, `Datetime`, `Duration`, `Binary` and `List` all reach this from schemas the package tests.

    A `Decimal` becomes a string, not the `float` the statistics tables coerce it to. A statistic is a number nobody stored; this is the row itself. `10.00` must still read `10.00`, and a high-precision decimal must keep its digits.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def sample_rows(frame: pl.DataFrame, limit: int) -> list[Row]:
    """Read up to `limit` of a frame's rows as cells a table record accepts.

    The head, not a random draw. A sample somebody reports a bug against must be the same sample when they reopen the run, and `head` is the only draw a re-read reproduces.

    Parameters
    ----------
    frame
        The rows to sample from, in the order they should be shown.
    limit
        How many rows to take at most.

    Returns
    -------
    One mapping per row, keyed by column in the frame's own order. Empty when the limit is zero or the frame has no rows.
    """
    return [
        {name: _cell(value) for name, value in row.items()}
        for row in frame.head(limit).iter_rows(named=True)
    ]


def sample_metadata(key: str, rows: list[Row]) -> dict[str, dg.TableMetadataValue]:
    """Build the one metadata entry a sample lands under.

    Every check and materialization that shows sampled rows goes through here, so "absent, never empty" stays one decision.

    Table values rather than markdown: a table value renders as a full HTML table in the UI, and the same rows as markdown render as printed text.

    Parameters
    ----------
    key
        The metadata key to show the rows under.
    rows
        The rows, already sampled and rendered.

    Returns
    -------
    The one entry, or nothing when there are no rows: the limit was zero, the frame was empty, or nothing failed the rule.
    """
    if not rows:
        return {}
    return {key: dg.MetadataValue.table([dg.TableRecord(row) for row in rows])}
