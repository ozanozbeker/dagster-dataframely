"""The rows a run puts in front of a reader, bounded.

Two surfaces want the same thing and want it for the same reason. A red check says `amount|min` rejected 43 rows, and the question that raises is what three of them held. A materialization says 90,000 rows landed, and the question that raises is what one of them looks like. Neither is answerable from a count, and both are answerable from a handful of rows.

**Bounded by construction.** There is no unbounded setting and no unbounded read: nothing leaves this module without a caller having said how many rows it wanted. These rows go into the Dagster event log, which is shared, exported and not redacted, so the amount that lands there is a number somebody chose.

**No sample is absent, never empty.** Zero rows is not a small answer to "what does a row look like", it is no answer, and an empty table in the UI would read as one. Dagster enforces the same judgement from the other side: a table value with no records and no schema is an error, so an empty one would take a run down rather than show a reader nothing.

Its own module rather than `_metadata`'s or `_statistics`', for the reason `_statistics` gives and one more. That one holds what an asset declares before it has run; this holds what a run actually held. And unlike a statistic, a cell here is a value out of the data, which is what decides the one rendering rule below.
"""

import dagster as dg
import polars as pl

#: The valid output's materialization display key. Short and unprefixed, like `stats/*` and unlike the fully qualified schema carrier: the difference in length is what tells a reader which keys are for them.
SAMPLE_KEY = "sample"

#: What a `dg.TableRecord` cell may hold. Dagster states the union inline on the record's own field rather than exporting a name for it.
Cell = str | int | float | bool | None

#: One row, rendered.
Row = dict[str, Cell]


def _cell(value: object) -> Cell:
    """Renders one value as something a table record accepts.

    Everything a record cannot hold becomes its string form, once, here: a `Decimal`, a `Datetime`, a `Duration`, a `Binary` and a `List` all reach this from a schema the package already ships tests for.

    A `Decimal` deliberately becomes a string rather than the `float` the statistics tables coerce it to. Both are display renderings, but a statistic is a number nobody stored while this is the row itself, so `10.00` has to still read `10.00` and a high-precision decimal has to keep its digits.
    """
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return str(value)


def sample_rows(frame: pl.DataFrame, limit: int) -> list[Row]:
    """Reads up to `limit` of a frame's rows as cells a table record accepts.

    The head rather than a random draw: a sample somebody reports a bug against has to be the same sample when they open the run again, and `head` is the only draw a re-read reproduces.

    Args:
        frame: The rows to sample from, in the order they should be shown.
        limit: How many rows to take at most.

    Returns:
        One mapping per row, keyed by column in the frame's own order. Empty when the limit is zero or the frame has no rows.
    """
    return [
        {name: _cell(value) for name, value in row.items()}
        for row in frame.head(limit).iter_rows(named=True)
    ]


def sample_metadata(key: str, rows: list[Row]) -> dict[str, dg.TableMetadataValue]:
    """Builds the one metadata entry a sample lands under.

    Every surface that shows sampled rows goes through here, which is what makes "absent, never empty" one decision rather than three that could drift.

    Table values rather than markdown, for the reason the statistics tables give: a table value renders as a full-featured HTML table in the UI and the same rows as markdown render as printed text.

    Args:
        key: The metadata key to show the rows under.
        rows: The rows, already sampled and rendered.

    Returns:
        The one entry, or nothing at all when there are no rows: the limit was zero, the frame was empty, or the rule this belongs to rejected nothing.
    """
    if not rows:
        return {}
    return {key: dg.MetadataValue.table([dg.TableRecord(row) for row in rows])}
