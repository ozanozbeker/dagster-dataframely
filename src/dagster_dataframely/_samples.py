"""The rows a run puts in front of a reader, bounded.

Two surfaces want the same thing for the same reason. A red check says 43 rows failed `amount|min`, which raises the question of what three of them held. A materialization says 90,000 rows were written, which raises the question of what one of them looks like. A count answers neither. A handful of rows answers both.

**Bounded by construction.** There is no unbounded setting and no unbounded read. Nothing leaves this module without a caller saying how many rows it wanted. These rows go into the Dagster event log, which is shared, exported and not redacted, so somebody chooses the amount that lands there.

**A sample is absent, never empty.** Zero rows is no answer to "what does a row look like", and an empty table in the UI would read as one. Dagster enforces the same judgement from the other side: a table value with no records and no schema is an error, so an empty one would take a run down rather than show a reader nothing.

Its own module rather than `_metadata`'s or `_statistics`', for the reason `_statistics` gives and one more. That module holds what an asset declares before it has run. This one holds what a run actually held. And unlike a statistic, a cell here is a value out of the data, which decides the one rendering rule below.
"""

import dagster as dg
import polars as pl

#: The valid rows' materialization display key. Namespaced like `dataframely/valid_stats/*`, so everything this package writes sorts in one block apart from Dagster's and the IO manager's.
VALID_SAMPLE_KEY = "dataframely/valid_sample"

#: What a `dg.TableRecord` cell may hold. Dagster states the union inline on the record's own field rather than exporting a name for it.
type Cell = str | int | float | bool | None

#: One row, rendered.
type Row = dict[str, Cell]


def _cell(value: object) -> Cell:
    """Render one value as something a table record accepts.

    Everything a record cannot hold becomes its string form, once, here. A `Decimal`, a `Datetime`, a `Duration`, a `Binary` and a `List` all reach this from a schema the package already ships tests for.

    A `Decimal` deliberately becomes a string rather than the `float` the statistics tables coerce it to. Both are display renderings, but a statistic is a number nobody stored while this is the row itself. `10.00` has to still read `10.00`, and a high-precision decimal has to keep its digits.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def sample_rows(frame: pl.DataFrame, limit: int) -> list[Row]:
    """Read up to `limit` of a frame's rows as cells a table record accepts.

    The head rather than a random draw. A sample somebody reports a bug against has to be the same sample when they open the run again, and `head` is the only draw a re-read reproduces.

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

    Every surface that shows sampled rows goes through here, so "absent, never empty" stays one decision rather than three that could drift.

    Table values rather than markdown, for the reason the statistics tables give: a table value renders as a full-featured HTML table in the UI, and the same rows as markdown render as printed text.

    Parameters
    ----------
    key
        The metadata key to show the rows under.
    rows
        The rows, already sampled and rendered.

    Returns
    -------
    The one entry, or nothing at all when there are no rows: the limit was zero, the frame was empty, or nothing failed the rule this belongs to.
    """
    if not rows:
        return {}
    return {key: dg.MetadataValue.table([dg.TableRecord(row) for row in rows])}
