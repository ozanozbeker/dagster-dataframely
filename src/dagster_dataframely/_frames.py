"""What the validation path and the write path both do to a frame on its way to disk.

Two callers share this module so they cannot disagree. `dataframely_asset` compares a frame against its schema before filtering. The CSV manager runs the same comparison for an asset that attached a schema by hand. Sharing one comparison stops two shape checks drawing the line differently.

Both stage lazy frames, too. `process` stages a lazy decorated function before validating it, and the manager stages a lazy output before promoting it. One shared prefix lets an operator sweeping a filled disk find every staging file this package makes with a single glob.

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
    """Compare the frame's shape against the schema, naming every mismatch.

    An explicit pre-check, not a `try`/`except` around `filter`. The `try` would behave differently depending on what the decorated function returned. `filter(cast=False)` raises at call time on a `DataFrame`, but on a `LazyFrame` it returns cleanly and the same error surfaces only on the eventual collect. The decorator promises either return type works, so the shape check cannot rest on a difference between them.

    Only public API, and none of it executes. `collect_schema()` resolves a `LazyFrame`'s shape without running it.

    Parameters
    ----------
    schema
        The schema the frame claims to match.
    frame
        The frame to compare, eager or lazy.

    Returns
    -------
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
    """Open a temp directory to stage into, removed on the way out.

    Parameters
    ----------
    temp_dir
        Where the directory goes, or `None` for wherever `tempfile` puts things. The caller resolves it rather than this function, because the two callers resolve at different moments. The decorator reads the setting where the asset is declared. The IO manager reads it where the output is written.

    Yields
    ------
    The directory. It and everything in it are gone once the block exits, so no exit can leave a staging file behind. That includes the two exits whose whole purpose is that nothing is written.

    Raises
    ------
    FileNotFoundError
        `temp_dir` names a directory that does not exist. It is not created. The setting exists to move staging off a container's ephemeral disk, and a mistyped path silently created there is the failure somebody set it to avoid.
    """
    with tempfile.TemporaryDirectory(dir=temp_dir, prefix=_STAGING_PREFIX) as directory:
        yield Path(directory)
