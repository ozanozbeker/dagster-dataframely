"""The package's IO managers.

The asset body owns what the data is; the manager owns where and how it lands. So the manager emits only what varied this run: `path`, `bytes_written` and `dagster/storage_kind`.

Design decisions:
    - `dagster/column_schema` describes the data, not the write. The asset definition emits it, not the IO manager.
"""

from typing import TYPE_CHECKING, override

import polars as pl
from dagster import (
    ConfigurableIOManagerFactory,
    DagsterInvariantViolationError,
    MetadataValue,
    UPathIOManager,
)
from pydantic import Field
from upath import UPath

from dagster_dataframely.errors import UnwritableDtypeError

if TYPE_CHECKING:
    from dagster import InitResourceContext, InputContext, OutputContext

_STORAGE_KIND_KEY = "dagster/storage_kind"
_PARQUET_EXTENSION = ".parquet"

# Parquet's only refusal. Polars cannot nest an `Object`, so scanning top-level dtypes is enough.
_UNWRITABLE_DTYPES: tuple[pl.DataType | type[pl.DataType], ...] = (pl.Object,)


class _ParquetIOManager(UPathIOManager):
    """Writes polars frames to `.parquet`, built by the `DataframelyParquetIOManager` users bind."""

    extension = _PARQUET_EXTENSION

    def __init__(self, base_dir: str) -> None:
        """Roots the manager at `base_dir`, a directory or cloud URI."""
        super().__init__(base_path=UPath(base_dir))

    @override
    def handle_output(
        self, context: "OutputContext", obj: pl.DataFrame | pl.LazyFrame
    ) -> None:
        """Rejects what parquet cannot represent, then hands the frame to the base manager.

        Here rather than in `dump_to_path` so that a rejection writes nothing and creates no directory, and not at definition time because an asset cannot know which IO manager it will be bound to.
        """
        if not isinstance(obj, (pl.DataFrame, pl.LazyFrame)):
            wrong_type = f"This manager writes polars frames, but the output is a {type(obj).__name__}. Annotate the asset `-> None` if it manages its own storage, so that Dagster skips the IO manager entirely."
            raise DagsterInvariantViolationError(wrong_type)

        unwritable = {
            name: dtype
            for name, dtype in obj.collect_schema().items()
            if dtype in _UNWRITABLE_DTYPES
        }
        if unwritable:
            raise UnwritableDtypeError(extension=_PARQUET_EXTENSION, columns=unwritable)

        super().handle_output(context, obj)

    @override
    def dump_to_path(
        self, context: "OutputContext", obj: pl.DataFrame | pl.LazyFrame, path: UPath
    ) -> None:
        """Writes the frame, then stats the path for `bytes_written`.

        `get_metadata` never sees the path it was written to, so the size is taken here. It comes off the disk, so it reports the compression actually achieved rather than an in-memory estimate.
        """
        if isinstance(obj, pl.LazyFrame):
            context.log.warning(
                "Collecting a LazyFrame before the write. This manager supports polars DataFrame; "
                "sinking lazily is planned work, tracked in issue #27."
            )
        frame = obj.collect() if isinstance(obj, pl.LazyFrame) else obj
        with path.open("wb") as file:
            frame.write_parquet(file)

        context.add_output_metadata(
            {
                "bytes_written": MetadataValue.int(path.stat().st_size),
                _STORAGE_KIND_KEY: MetadataValue.text("parquet"),
            }
        )

    @override
    def load_from_path(self, context: "InputContext", path: UPath) -> pl.DataFrame:
        """Reads the file back eagerly, through the same filesystem the write went out on.

        `context` is unused: parquet is self-describing, so reading a file needs nothing from the asset definition.
        """
        with path.open("rb") as file:
            return pl.read_parquet(file)


class DataframelyParquetIOManager(ConfigurableIOManagerFactory[_ParquetIOManager]):
    """Stores polars frames as `.parquet` files under `base_dir`, locally or in cloud storage.

    `base_dir` is a universal-pathlib path, so `s3://bucket/prefix`, `gs://...` and `az://...` are written the way a local directory is, on credentials from the ambient environment. A cloud scheme needs its fsspec filesystem installed alongside this package: `s3fs`, `gcsfs` or `adlfs`.

    Every materialization carries `path`, `bytes_written` and `dagster/storage_kind`, and nothing else: no column schema, no data sample, no statistics pass. A dtype that parquet cannot represent raises `UnwritableDtypeError` before the write.

    Polars `DataFrame` is the supported type. A `LazyFrame` output is collected before the write, with a warning in the run log, and a read always returns a `DataFrame`. Sinking and scanning lazily is planned work, tracked in issue #27.

    Attributes:
        base_dir: Directory or cloud URI the manager writes parquet files under.

    Example:
        >>> import dagster as dg
        >>> import polars as pl
        >>> import dagster_dataframely as dd
        >>> @dg.asset
        ... def orders() -> pl.DataFrame:
        ...     return pl.DataFrame({"order_id": ["a", "b"]})
        >>> defs = dg.Definitions(
        ...     assets=[orders],
        ...     resources={
        ...         "io_manager": dd.DataframelyParquetIOManager(
        ...             base_dir="s3://my-bucket/warehouse"
        ...         )
        ...     },
        ... )
    """

    base_dir: str = Field(
        description="Directory or cloud URI the manager writes parquet files under."
    )

    @override
    def create_io_manager(self, context: "InitResourceContext") -> _ParquetIOManager:
        """Builds the manager that does the writing.

        `context` is unused: `base_dir` is the only source of the base path, so the `path` in the event log is always the one the user configured.
        """
        return _ParquetIOManager(base_dir=self.base_dir)
