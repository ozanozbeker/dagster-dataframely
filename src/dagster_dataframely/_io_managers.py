"""The package's IO managers.

The asset body owns what the data is; the manager owns where and how it lands. So the manager emits only what varied this run: `path`, `bytes_written` and `dagster/storage_kind`.

Design decisions:
    - `dagster/column_schema` describes the data, not the write. The asset definition emits it, not the IO manager.
    - Both managers share one spine, so what a materialization records is one implementation and cannot differ by format. What a format owns is exactly its refusals and its calls into polars.
    - **A scan is built on the open handle, not on the path.** Handed a file object, polars reads its bytes there and then, which is why the plan still collects after the handle closes. That gives up the transfer a scan of an `s3://` path would have pruned with range requests, and it buys two things worth more here: credentials stay one mechanism, fsspec's, rather than the second one polars' own object-store would need, and a missing file raises `FileNotFoundError` from the open, where `UPathIOManager` can still catch it and honour `allow_missing_partitions`. A scan of a path raises nothing until the caller collects, long after the manager has returned. So laziness buys decoding and materialization here, not transfer: the same bytes an eager read moves, and only the rows and columns a query keeps.
"""

from abc import abstractmethod
from collections.abc import Mapping
from typing import IO, TYPE_CHECKING, get_args, override

import polars as pl
from dagster import (
    ConfigurableIOManagerFactory,
    DagsterInvariantViolationError,
    MetadataValue,
    UPathIOManager,
)
from pydantic import Field
from upath import UPath

from dagster_dataframely import _csv_codecs
from dagster_dataframely._errors import SchemaGateError, UnwritableDtypeError
from dagster_dataframely._metadata import carried_schema, schema_dtypes
from dagster_dataframely._runtime import gate_problems

if TYPE_CHECKING:
    from dagster import InitResourceContext, InputContext, OutputContext

# Exported because nothing in this module's signatures says a read can return a dict: `load_from_path` is typed for a single file, while the `load_input` above it calls that hook once per partition key and assembles the results. A fan-in over every partition therefore lands on the obvious annotation, `pl.DataFrame`, which fails Dagster's type check after every partition has already been read.
# A plain assignment rather than a `type` statement, because Dagster resolves the annotation at runtime and rejects the `TypeAliasType` a PEP 695 alias produces. `tests/test_upstream_pins.py` pins that refusal.
# The names are `dagster-polars`', so a user arriving from there writes what they already know. Which of the two to write is decided the same way a single-partition read is: by the frame the annotation asks for.
DataFramePartitions = dict[str, pl.DataFrame]
LazyFramePartitions = dict[str, pl.LazyFrame]

_STORAGE_KIND_KEY = "dagster/storage_kind"

# Parquet's only refusal. Polars cannot nest an `Object`, so scanning top-level dtypes is enough.
_UNWRITABLE_DTYPES: tuple[pl.DataType | type[pl.DataType], ...] = (pl.Object,)


def _wants_lazy(context: "InputContext") -> bool:
    """Reports whether an input asks for a scan rather than a frame.

    Reads dispatch on the annotation, writes on the runtime type. The asymmetry is deliberate rather than an oversight: a write already holds the object, so `isinstance` is the honest test, while a read has no object yet and the annotation is the only signal for what to build.

    A fan-in annotates the shape rather than the element, so the wrapper comes off first. That is the same unwrapping `LazyFramePartitions` exists to spare a user.
    """
    annotation = context.dagster_type.typing_type
    return pl.LazyFrame in (annotation, *get_args(annotation))


class _FrameIOManager(UPathIOManager):
    """What every format in this package does identically: refuse, write, and record three keys.

    Attributes:
        storage_kind: The format this manager writes, in Dagster's own vocabulary. The base class's `extension` is derived from it, so a file's suffix, the badge in the event log and the format a refusal names are one string and cannot disagree.
    """

    storage_kind: str

    def __init__(self, base_dir: str) -> None:
        """Roots the manager at `base_dir`, a directory or cloud URI."""
        self.extension = self._suffix
        super().__init__(base_path=UPath(base_dir))

    @property
    def _suffix(self) -> str:
        """The extension, spelled once and narrowed.

        `UPathIOManager` types its own `extension` as `str | None` for a subclass that sets none. Every subclass here sets one, and the two readers that need it as a `str` read it from here.
        """
        return f".{self.storage_kind}"

    @abstractmethod
    def _unwritable(
        self, dtypes: Mapping[str, pl.DataType]
    ) -> Mapping[str, pl.DataType]:
        """Finds the columns this format cannot represent."""

    @abstractmethod
    def _write(
        self, context: "OutputContext", frame: pl.DataFrame, file: IO[bytes]
    ) -> None:
        """Writes the frame to an open handle on the resolved path."""

    def _check_declaration(
        self, context: "OutputContext", frame: pl.DataFrame | pl.LazyFrame
    ) -> None:
        """Refuses a frame that disagrees with what its asset declares about it.

        Nothing by default: a format whose read needs no declaration cannot be wrong about one.
        """

    @override
    def handle_output(
        self, context: "OutputContext", obj: pl.DataFrame | pl.LazyFrame
    ) -> None:
        """Rejects what the format cannot represent, then hands the frame to the base manager.

        Here rather than in `dump_to_path` so that a rejection writes nothing and creates no directory, and not at definition time because an asset cannot know which IO manager it will be bound to.
        """
        if not isinstance(obj, (pl.DataFrame, pl.LazyFrame)):
            wrong_type = f"This manager writes polars frames, but the output is a {type(obj).__name__}. Annotate the asset `-> None` if it manages its own storage, so that Dagster skips the IO manager entirely."
            raise DagsterInvariantViolationError(wrong_type)

        unwritable = self._unwritable(obj.collect_schema())
        if unwritable:
            raise UnwritableDtypeError(extension=self._suffix, columns=unwritable)

        self._check_declaration(context, obj)
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
        frame: pl.DataFrame = obj.collect() if isinstance(obj, pl.LazyFrame) else obj
        with path.open("wb") as file:
            self._write(context, frame, file)

        context.add_output_metadata(
            {
                "bytes_written": MetadataValue.int(path.stat().st_size),
                _STORAGE_KIND_KEY: MetadataValue.text(self.storage_kind),
            }
        )


class _ParquetIOManager(_FrameIOManager):
    """Writes polars frames to `.parquet`, built by the `DataframelyParquetIOManager` users bind."""

    storage_kind = "parquet"

    @override
    def _unwritable(
        self, dtypes: Mapping[str, pl.DataType]
    ) -> Mapping[str, pl.DataType]:
        return {
            name: dtype for name, dtype in dtypes.items() if dtype in _UNWRITABLE_DTYPES
        }

    @override
    def _write(
        self, context: "OutputContext", frame: pl.DataFrame, file: IO[bytes]
    ) -> None:
        """`context` is unused: parquet holds every dtype this manager accepts, so nothing has to be declared."""
        frame.write_parquet(file)

    @override
    def load_from_path(
        self, context: "InputContext", path: UPath
    ) -> pl.DataFrame | pl.LazyFrame:
        """Reads the file back through the same filesystem the write went out on, as a frame or as a scan.

        `context` is read for the annotation and nothing else: parquet is self-describing, so what to build is the only question the asset definition answers here.
        """
        with path.open("rb") as file:
            if _wants_lazy(context):
                return pl.scan_parquet(file)
            return pl.read_parquet(file)


class _CSVIOManager(_FrameIOManager):
    """Writes polars frames to `.csv`, built by the `DataframelyCSVIOManager` users bind."""

    storage_kind = "csv"

    @override
    def _unwritable(
        self, dtypes: Mapping[str, pl.DataType]
    ) -> Mapping[str, pl.DataType]:
        return _csv_codecs.unwritable(dtypes)

    @override
    def _check_declaration(
        self, context: "OutputContext", frame: pl.DataFrame | pl.LazyFrame
    ) -> None:
        """Refuses a frame the asset's own schema does not describe.

        The decode reads a column back into the dtype the schema declares, so a schema the frame disagrees with is not an inverse: a `Duration('ns')` written under a declared `Duration('us')` reads back a thousand times too long, with nothing in the file to catch it. That makes the declaration part of what gets written, and a false one a pipeline defect rather than a data one.

        `dataframely_asset` gates the same comparison at the door, so this only ever fires for an asset that attached a schema by hand. Both call `_runtime.gate_problems`, which is what stops one gate from admitting what the other refuses.
        """
        schema = carried_schema(context.definition_metadata)
        if schema is None:
            return
        problems = gate_problems(schema, frame)
        if problems:
            raise SchemaGateError(schema.__name__, problems)

    @override
    def _write(
        self, context: "OutputContext", frame: pl.DataFrame, file: IO[bytes]
    ) -> None:
        """Encodes what a cell cannot hold, names those columns in the run log, then writes plain CSV.

        The log is the only home the codec has. Materialization metadata is barred by the varied-this-run rule, and definition metadata cannot know which manager an asset will bind to, so neither can say that a column landed as base64.

        An encoded column no schema declares is a warning rather than an info line. It is written correctly and any CSV reader can still read it, but this package will hand it back as text, and that is worth saying at the moment it happens rather than at the read that surprises someone.
        """
        encoded, columns = _csv_codecs.encode(frame)
        if columns:
            schema = carried_schema(context.definition_metadata)
            declared: Mapping[str, pl.DataType] = (
                {} if schema is None else schema_dtypes(schema)
            )
            undeclared = {
                name: phrase for name, phrase in columns.items() if name not in declared
            }
            # Formatted here rather than left to the logger's own deferral, so the event
            # log a user reads holds the columns rather than a template.
            named = f"Encoded {len(columns)} column(s) that CSV cannot hold: {_csv_codecs.describe(columns)}."
            if undeclared:
                message = f"{named} No schema on this asset declares {_csv_codecs.describe(undeclared)}, so a read has nothing to decode that with and it comes back as text."
                context.log.warning(message)
            else:
                message = (
                    f"{named} The schema on this asset decodes them on the way back."
                )
                context.log.info(message)
        encoded.write_csv(file)

    @override
    def load_from_path(
        self, context: "InputContext", path: UPath
    ) -> pl.DataFrame | pl.LazyFrame:
        """Reads the file back against the schema the upstream asset declares, as a frame or as a scan.

        CSV is the sole reason a read path needs the schema at all. It arrives from the carrier in the upstream asset's definition metadata, so it costs no round trip to storage and cannot drift from the data, because it never came from the data.

        With no schema to read against, this is an ordinary inferred CSV read: every column arrives as whatever polars makes of the text.

        Going lazy costs the schema nothing. `scan_csv` takes the same `schema_overrides`, and every codec is an expression over `with_columns`, which behaves the same on both frame types, so the decode is the one written for the eager read.
        """
        upstream = context.upstream_output
        schema = (
            None if upstream is None else carried_schema(upstream.definition_metadata)
        )
        dtypes: Mapping[str, pl.DataType] = (
            {} if schema is None else schema_dtypes(schema)
        )
        overrides = _csv_codecs.read_schema(dtypes)

        with path.open("rb") as file:
            frame: pl.DataFrame | pl.LazyFrame = (
                pl.scan_csv(file, schema_overrides=overrides)
                if _wants_lazy(context)
                else pl.read_csv(file, schema_overrides=overrides)
            )

        decoded, columns = _csv_codecs.decode(frame, dtypes)
        if columns:
            # Present tense on both paths rather than past tense on one: a scan has queued the decode rather than run it, and the line is about which columns it covers either way.
            named = f"Decoding {len(columns)} column(s) CSV cannot hold: {_csv_codecs.describe(columns)}."
            context.log.info(named)
        return decoded


class DataframelyParquetIOManager(ConfigurableIOManagerFactory[_ParquetIOManager]):
    """Stores polars frames as `.parquet` files under `base_dir`, locally or in cloud storage.

    `base_dir` is a universal-pathlib path, so `s3://bucket/prefix`, `gs://...` and `az://...` are written the way a local directory is, on credentials from the ambient environment. A cloud scheme needs its fsspec filesystem installed alongside this package: `s3fs`, `gcsfs` or `adlfs`.

    Every materialization carries `path`, `bytes_written` and `dagster/storage_kind`, and nothing else: no column schema, no data sample, no statistics pass. A dtype that parquet cannot represent raises `UnwritableDtypeError` before the write.

    **A read dispatches on the input annotation.** `pl.LazyFrame` hands back an unexecuted scan, so a downstream `filter` or `select` prunes rows and columns before anything is decoded; `pl.DataFrame` reads the file whole, as before. The scan rides the same fsspec handle the eager read does, on the ambient credentials above and no second mechanism, and polars reads the file's bytes when the scan is built. So what a scan saves is decoding and materialization, not transfer.

    A write dispatches on the runtime type instead, because a write already holds the object: a `LazyFrame` output is collected before the write, with a warning in the run log. Sinking lazily is planned work, tracked in issue #27.

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


class DataframelyCSVIOManager(ConfigurableIOManagerFactory[_CSVIOManager]):
    """Stores polars frames as `.csv` files under `base_dir`, locally or in cloud storage.

    Everything `DataframelyParquetIOManager` does about storage, this does identically: the same universal-pathlib `base_dir`, the same three metadata keys, no column schema, and `UnwritableDtypeError` before the write for a dtype it cannot hold.

    What differs is that a CSV cell holds text. `Duration`, `List`, `Array`, `Struct` and `Binary` are encoded on the way out and decoded on the way back, each by an encoding with a declared inverse, so the frame read back compares equal to the frame written. A run log line names the encoded columns on both paths, because that is the only surface that can carry it.

    **The read needs the schema, so attach the asset's schema to reach it.** The decode reads the dtype off the carrier `dataframely_asset` puts in the asset's definition metadata, which `schema_metadata` also builds for a plain `@dg.asset`. Without one, the read is an ordinary inferred CSV read and an encoded column arrives as text. The schema never comes from a sidecar file and never from the data, so it costs no round trip and cannot drift.

    Two dtypes are refused rather than encoded: `Binary` and `Duration` *inside* a `List`, `Array` or `Struct`. polars cannot write the first to JSON and cannot read the second back, so encoding either one would land a file that no longer round-trips.

    A `pl.LazyFrame` input annotation reads back a scan here too, and it keeps everything above: `scan_csv` takes the same schema-driven dtypes, and the decode is the same expression over `with_columns`, run when the caller collects.

    Prefer parquet unless something downstream needs text. Parquet is self-describing, keeps every dtype natively, and needs no schema to read.

    Attributes:
        base_dir: Directory or cloud URI the manager writes CSV files under.

    Example:
        >>> import dagster as dg
        >>> import dataframely as dy
        >>> import polars as pl
        >>> import dagster_dataframely as dd
        >>> class Orders(dy.Schema):
        ...     order_id = dy.String(primary_key=True)
        ...     tags = dy.List(dy.String())
        >>> @dd.dataframely_asset(schema=Orders)
        ... def orders() -> pl.DataFrame:
        ...     return pl.DataFrame({"order_id": ["a"], "tags": [["new"]]})
        >>> defs = dg.Definitions(
        ...     assets=[orders],
        ...     resources={
        ...         "io_manager": dd.DataframelyCSVIOManager(
        ...             base_dir="s3://my-bucket/warehouse"
        ...         )
        ...     },
        ... )
    """

    base_dir: str = Field(
        description="Directory or cloud URI the manager writes CSV files under."
    )

    @override
    def create_io_manager(self, context: "InitResourceContext") -> _CSVIOManager:
        """Builds the manager that does the writing.

        `context` is unused: `base_dir` is the only source of the base path, so the `path` in the event log is always the one the user configured.
        """
        return _CSVIOManager(base_dir=self.base_dir)
