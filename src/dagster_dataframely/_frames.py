"""What the validation path reads off a frame before it decides anything about it.

Comparing a frame against its schema is about the frame alone. It reads no schema rule, no check name and no asset context. Every other step in `_runtime` needs all three, so this one sits apart from it.
"""

import dataframely as dy
import polars as pl


def column_schema_problems(
    schema: type[dy.Schema], frame: pl.DataFrame | pl.LazyFrame
) -> list[dict[str, str]]:
    """Compare the frame's columns and dtypes against the schema, naming every mismatch.

    An explicit pre-check, not a `try`/`except` around `filter`. The split takes a plan, so a column-schema mismatch would surface only at `collect_all`, after the plan had executed, as whatever Polars raises for the first column it tripped over. This runs first, executes nothing, and names every offending column at once.

    Only public API, and none of it executes. `collect_schema()` resolves a `LazyFrame`'s column schema without running it.

    Parameters
    ----------
    schema
        The schema the frame claims to match.
    frame
        The frame to compare, eager or lazy.

    Returns
    -------
    One mapping of `column`, `expected` and `actual` per offending column, empty when the frame matches. The same list feeds the failing check's metadata and `ColumnSchemaError`, so the two cannot disagree.
    """
    actual: pl.Schema = frame.collect_schema()
    return [
        {
            "column": name,
            "expected": str(column.dtype),
            "actual": str(actual[name]) if name in actual else "<missing>",
        }
        for name, column in schema.columns().items()
        if name not in actual or not column.validate_dtype(actual[name])
    ]
