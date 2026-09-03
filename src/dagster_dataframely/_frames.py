"""What the validation path reads off a frame before it decides anything about it.

The column-schema comparison reads no rule, check name, or asset context. Every other step in `_runtime` needs all three, so it lives apart.
"""

import dataframely as dy
import polars as pl


def column_schema_problems(
    schema: type[dy.Schema], frame: pl.DataFrame | pl.LazyFrame
) -> list[dict[str, str]]:
    """Compare the frame's columns and dtypes against the schema, naming every mismatch.

    An explicit pre-check, not a `try`/`except` around `filter`. `Schema.filter` takes a plan, so a mismatch would appear only at `collect_all`, after the plan had run, as whatever Polars raises for the first bad column. This runs first, executes nothing, and names every offending column at once. `collect_schema()` resolves a `LazyFrame`'s column schema without running it.

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
