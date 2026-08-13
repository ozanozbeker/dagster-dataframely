"""What the validation path and the write path both do to a frame on its way to disk.

Two callers, and the whole point of one module is that they cannot disagree. `dataframely_asset` compares a frame against its schema before filtering; the CSV manager runs the same comparison for an asset that attached a schema by hand, and sharing it is what stops two shape checks from drawing the line differently. `process` stages a lazy decorated function before validating it and the manager stages a lazy output before promoting it, so one prefix is what lets an operator sweeping a filled disk find every staging file this package makes with one glob.

Nothing belongs here that only one of the two callers needs.
"""

import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import dataframely as dy
import polars as pl

# Spelled once, which is the guarantee: two staging paths cannot name their directories differently.
_STAGING_PREFIX = "dagster_dataframely_"


def shape_problems(
    schema: type[dy.Schema], frame: pl.DataFrame | pl.LazyFrame
) -> list[dict[str, str]]:
    """Compares the frame's shape against the schema, naming every mismatch.

    An explicit pre-check rather than a `try`/`except` around `filter`, which would behave differently depending on what the decorated function returned: `filter(cast=False)` raises at call time on a `DataFrame`, but on a `LazyFrame` it returns cleanly and the same error surfaces only on the eventual collect. The decorator promises either return type works, so the shape check cannot be built on a difference between them.

    Only public API, and none of it executes: `collect_schema()` resolves a `LazyFrame`'s shape without running it.

    Args:
        schema: The schema the frame claims to match.
        frame: The frame to compare, eager or lazy.

    Returns:
        One mapping of `column`, `expected` and `actual` per offending column, empty when the frame matches. The same list feeds the failing check's metadata and `SchemaShapeError`, so the two cannot disagree.
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


@contextmanager
def staging(temp_dir: str | None) -> Iterator[Path]:
    """Opens a temp directory for a frame to land in, removed on the way out.

    Args:
        temp_dir: Where the directory goes, or `None` for wherever `tempfile` puts things. Resolved by the caller rather than here, because the two callers resolve at different moments: the decorator reads the setting where the asset is declared, and the IO manager reads it where the output is written.

    Yields:
        The directory. It and everything in it are gone once the block exits, so no exit can leave a staging file behind, including the two whose whole purpose is that nothing is written.

    Raises:
        FileNotFoundError: `temp_dir` names a directory that does not exist. It is not created, because the setting exists to move staging off a container's ephemeral disk and a mistyped path silently created there is the failure somebody set it to avoid.
    """
    with tempfile.TemporaryDirectory(dir=temp_dir, prefix=_STAGING_PREFIX) as directory:
        yield Path(directory)
