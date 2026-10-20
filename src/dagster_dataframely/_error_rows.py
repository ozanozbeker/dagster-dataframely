"""The error-rows key, path, writers and asset spec.

`dd.asset` records where its writer writes in the definition metadata, and `error_rows_spec` reads it back.
`error_rows_path` escapes partition keys as `FilesystemIOManager` does, not as `UPathIOManager` does, so the path for `../../etc/x` is still under `error_rows_dir`.
"""

import copy
from collections.abc import Callable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from pathlib import Path

import dagster as dg
import dataframely as dy
import polars as pl

# Dagster keeps these private; `tests/test_upstream_characterization.py` pins each.
from dagster._check import CheckError
from dagster._core.errors import DagsterInvalidPropertyError
from dagster._core.execution.plan.outputs import StepOutputHandle
from dagster._core.storage.upath_io_manager import (
    coerce_to_relative_parts,
    escape_dotdot_segments,
    escape_leading_slash,
)
from upath import UPath

from dagster_dataframely._metadata import error_rows_metadata
from dagster_dataframely._rules import validate_namespace
from dagster_dataframely._settings import ERROR_ROWS_DIR
from dagster_dataframely.errors import ErrorRowsDirError, ErrorRowsKeyCollisionError

type ErrorRowsWriter = Callable[[pl.DataFrame], str]
"""A writer: a function that writes the error rows and returns the error-rows address, such as an asset key or a file path, as a string."""

# A suffix, not a key part, because `DbIOManager` takes the table name from the last key part only (`docs/out-of-scope/dbt-test-configs.md#store_failures-writes-to-schema_dbt_test__audit`).
_LEAF_SUFFIX = "__error_rows"

_EXTENSION = ".parquet"

_KEY_ENTRY = "dataframely/error_rows_key"

_IO_MANAGER_KEY_ENTRY = "dataframely/error_rows_io_manager_key"


def _suffixed(parts: Sequence[str]) -> tuple[str, ...]:
    """Add `__error_rows` to the last part of a key."""
    return (*parts[:-1], f"{parts[-1]}{_LEAF_SUFFIX}")


def _error_rows_key(
    key: dg.AssetKey, error_rows_key: dg.AssetKey | str | Sequence[str] | None = None
) -> dg.AssetKey:
    """Return the error-rows key: `error_rows_key`, or else `key` with `__error_rows` added."""
    if error_rows_key is None:
        return dg.AssetKey(list(_suffixed(key.path)))
    if isinstance(error_rows_key, dg.AssetKey):
        return error_rows_key
    return dg.AssetKey(error_rows_key)


def writer_metadata(
    key: dg.AssetKey,
    io_manager_key: str | None,
    *,
    error_rows_key: dg.AssetKey | str | Sequence[str] | None,
    error_rows_io_manager_key: str | None,
) -> dict[str, dg.DagsterAssetMetadataValue | dg.TextMetadataValue]:
    """Return the definition metadata that records where a run writes the error rows, which `error_rows_spec` reads."""
    return {
        _KEY_ENTRY: dg.MetadataValue.asset(_error_rows_key(key, error_rows_key)),
        _IO_MANAGER_KEY_ENTRY: dg.MetadataValue.text(
            error_rows_io_manager_key or io_manager_key or "io_manager"
        ),
    }


def _executable_keys(context: dg.AssetExecutionContext) -> AbstractSet[dg.AssetKey]:
    """Return every asset key the run can materialize (ADR-0007)."""
    try:
        return context.repository_def.asset_graph.executable_asset_keys
    except CheckError:
        return context.job_def.asset_layer.asset_graph.executable_asset_keys
    except DagsterInvalidPropertyError:
        return frozenset()


def validate_error_rows_key(
    context: dg.AssetExecutionContext,
    *,
    error_rows_key: dg.AssetKey | str | Sequence[str] | None = None,
) -> None:
    """Raise if an asset already materializes the error-rows key.

    Call it at the start of a hand-wired asset's function when the asset writes error rows, as `dd.asset` does. In a direct invocation it does nothing.

    Parameters
    ----------
    error_rows_key
        The value the asset's writer receives. `None` checks `<name>__error_rows`.

    Raises
    ------
    ErrorRowsKeyCollisionError
        An asset in the code location materializes the error-rows key. The asset itself counts, so the function raises for an `error_rows_key` equal to the asset's own key.
    """
    key: dg.AssetKey = _error_rows_key(context.asset_key, error_rows_key)
    if key in _executable_keys(context):
        raise ErrorRowsKeyCollisionError(
            context.asset_key.to_user_string(), key.to_user_string()
        )


def _formatted_partition_key(partition_key: str) -> str:
    """Format a partition key as `UPathIOManager` does.

    Upstream's is a closure; `tests/test_upstream_characterization.py` pins this copy.
    """
    if isinstance(partition_key, dg.MultiPartitionKey):
        return "/".join(
            value for _, value in sorted(partition_key.keys_by_dimension.items())
        )
    return partition_key


def error_rows_path(
    key: dg.AssetKey,
    error_rows_dir: UPath | Path | str,
    partition_key: str | None = None,
    *,
    error_rows_key: dg.AssetKey | str | Sequence[str] | None = None,
) -> UPath:
    """Return the path of the parquet file `file_writer` writes an asset's error rows to.

    The layout matches `UPathIOManager`'s, so with `error_rows_dir` set to a manager's `base_dir`, the file is beside the asset's own file.

    Parameters
    ----------
    key
        The asset key of the asset whose rows failed validation, not the error-rows key.
    error_rows_dir
        Any path `UPath` accepts, such as a local directory or `s3://bucket/prefix`. Credentials for a remote path come from the environment.
    partition_key
        A `dg.MultiPartitionKey` becomes its dimension keys joined by `/`, in dimension-name order. Escaping a `..` and a leading `/` keeps the file under `error_rows_dir`.
    error_rows_key
        `None` uses `key` with `__error_rows` added to its last part.

    Returns
    -------
    `<error_rows_dir>/<key part>/.../<name>__error_rows.parquet`, or `<error_rows_dir>/<key part>/.../<name>__error_rows/<partition>.parquet` for a partition. With `error_rows_key`, its parts replace `<key part>/.../<name>__error_rows`. `error_rows_path` creates no parent directories.
    """
    path: UPath = UPath(error_rows_dir).joinpath(
        *coerce_to_relative_parts(UPath(*_error_rows_key(key, error_rows_key).path))
    )
    if partition_key is not None:
        path = path / escape_dotdot_segments(
            escape_leading_slash(_formatted_partition_key(partition_key))
        )
    return path.with_suffix(f"{path.suffix}{_EXTENSION}")


def delegating_writer(
    context: dg.AssetExecutionContext,
    *,
    error_rows_key: dg.AssetKey | str | Sequence[str] | None = None,
    error_rows_io_manager_key: str | None = None,
) -> ErrorRowsWriter:
    """Return a writer that passes the error rows to the asset's own IO manager, or to the one `error_rows_io_manager_key` names.

    The manager stores the rows under the error-rows key, as it stores any other asset: `PolarsParquetIOManager` writes a parquet file beside the table, and `DuckDBPolarsIOManager` writes a table beside it.
    After each write, the writer logs a materialization for the error-rows key, so the spec from `error_rows_spec` has a history.
    It logs one for the empty write of a clean run too, so the history matches the stored rows.
    It logs none when the run's code location has no spec for the key, so the catalog does not list a key that nobody declared.

    Parameters
    ----------
    context
        Its asset definition must have exactly one asset, as a `@dg.asset` does.
    error_rows_key
        `None` uses `<name>__error_rows`, with the asset's key prefix.
    error_rows_io_manager_key
        The op must require this resource key, because `context.resources` holds only the resources its op requires. The writer passes that IO manager its own config, because a `DbIOManager` reads its connection from the context. It passes the asset's definition metadata unchanged.

    Returns
    -------
    A writer that takes the error rows and returns their asset key as a string.

    Raises
    ------
    dagster._core.errors.DagsterInvalidPropertyError
        There is no step, which means a direct invocation. Use `file_writer` there.
    """
    step = context.get_step_execution_context()
    # `keys_by_output_name` omits check outputs; a characterization test pins it.
    (output_name,) = context.assets_def.keys_by_output_name
    handle = StepOutputHandle(step.step.key, output_name)
    original: dg.OutputContext = step.get_output_context(handle)
    if error_rows_io_manager_key is None:
        manager = step.get_io_manager(handle)
        resource_config = original.resource_config
    else:
        manager = getattr(context.resources, error_rows_io_manager_key)
        # Dagster's own `get_output_context` reads a manager's config the same way.
        resources = step.resolved_run_config.resources
        resource_config = resources[error_rows_io_manager_key].config
    key = _error_rows_key(context.asset_key, error_rows_key)
    # A run of one partition has a key and no range, and a single-run backfill the reverse.
    partitioned = context.has_partition_key or context.has_partition_key_range
    # Dagster logs the table once per partition in a range, so the error rows match it.
    partitions = list(context.partition_keys) if partitioned else [None]
    # Dagster defines `dagster/row_count` as the whole table's count.
    count_key = "dagster/partition_row_count" if partitioned else "dagster/row_count"
    # Only a code location shows that no spec exists, and a run of `dg.materialize` has none.
    try:
        logs_event = key in context.repository_def.asset_graph.unexecutable_asset_keys
    except CheckError:
        logs_event = True

    def address(frame: pl.DataFrame) -> str:
        # Copied per write, because `add_output_metadata` raises on a key an earlier write added.
        repointed = copy.copy(original)
        repointed._asset_key = key  # noqa: SLF001 - no setter; a characterization test pins it
        repointed._resource_config = resource_config  # noqa: SLF001 - the same
        manager.handle_output(repointed, frame)
        if logs_event:
            metadata = {**repointed.get_logged_metadata(), count_key: len(frame)}
            for partition in partitions:
                context.log_event(
                    dg.AssetMaterialization(
                        asset_key=key, partition=partition, metadata=metadata
                    )
                )
        return key.to_user_string()

    return address


def file_writer(
    key: dg.AssetKey,
    error_rows_dir: UPath | Path | str,
    partition_key: str | None = None,
    *,
    error_rows_key: dg.AssetKey | str | Sequence[str] | None = None,
) -> ErrorRowsWriter:
    """Return a writer that writes the error rows to a parquet file under `error_rows_dir`.

    Use it in a direct invocation, where `delegating_writer` raises because there is no step. The file is at `error_rows_path(key, error_rows_dir, partition_key, error_rows_key=error_rows_key)`.

    Parameters
    ----------
    key
        The asset key of the asset whose rows failed validation, not the error-rows key.
    error_rows_key
        `None` uses `key` with `__error_rows` added to its last part.

    Returns
    -------
    A writer that takes the error rows and returns the file's path as a string.
    """
    path: UPath = error_rows_path(
        key, error_rows_dir, partition_key, error_rows_key=error_rows_key
    )

    def address(frame: pl.DataFrame) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        # An open handle, because `write_parquet` cannot open every filesystem a `UPath` can.
        with path.open("wb") as file:
            frame.write_parquet(file)
        return str(path)

    return address


def error_rows_writer(
    context: dg.AssetExecutionContext,
    *,
    error_rows_key: dg.AssetKey | str | Sequence[str] | None = None,
    error_rows_io_manager_key: str | None = None,
) -> ErrorRowsWriter:
    """Return a writer that calls `delegating_writer` in a run and `file_writer` otherwise.

    Without a step it skips a zero-row frame, so a clean call needs no `error_rows_dir`.
    """

    def address(frame: pl.DataFrame) -> str:
        try:
            context.get_step_execution_context()
        except DagsterInvalidPropertyError:
            pass
        else:
            return delegating_writer(
                context,
                error_rows_key=error_rows_key,
                error_rows_io_manager_key=error_rows_io_manager_key,
            )(frame)
        if frame.is_empty():
            return ""
        error_rows_dir: str | None = ERROR_ROWS_DIR.resolve(None)
        if error_rows_dir is None:
            raise ErrorRowsDirError(context.asset_key.to_user_string())
        return file_writer(
            context.asset_key,
            error_rows_dir,
            context.partition_key if context.has_partition_key else None,
            error_rows_key=error_rows_key,
        )(frame)

    return address


def error_rows_spec(  # noqa: PLR0913 - takes the spec's attributes
    schema: type[dy.Schema],
    asset: dg.AssetsDefinition | dg.AssetKey | str | Sequence[str],
    *,
    partitions_def: dg.PartitionsDefinition | None = None,
    group_name: str | None = None,
    owners: Sequence[str] | None = None,
    tags: Mapping[str, str] | None = None,
) -> dg.AssetSpec:
    """Return an asset spec that adds an asset's error rows to the asset graph.

    The asset writes the error rows whether or not the spec exists.

    Parameters
    ----------
    asset
        The asset the error rows came from, as its definition or its asset key. Pass a key only for an asset you cannot import. A definition also sets the spec's partitions, group, owners and tags, and the error-rows key, IO manager key and definition metadata of its writer.
    partitions_def
        The error rows' partitions, when `asset` is a key.
    group_name
        The error rows' asset group. A definition's group is the default, and a value overrides it. Unlike `partitions_def`, passing it with a definition does not raise, because the group sets only where the catalog lists the error rows.
    owners
        The error rows' owners. A definition's owners are the default, and a value replaces them, as `group_name` does.
    tags
        The error rows' tags. A definition's tags are the default, and a value replaces them, as `group_name` does.

    Returns
    -------
    A spec keyed by the error-rows key that depends on `asset`, to pass to `dg.Definitions(assets=[...])`.

    Raises
    ------
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
    InvalidSeverityError
        The schema declares a severity this package cannot read.
    dg.DagsterInvariantViolationError
        `asset` is a definition and the call also passes `partitions_def`.

    Examples
    --------
    ```python
    #| echo: false
    #| output: false
    import dataframely as dy
    import polars as pl

    import dagster_dataframely as dd
    ```

    ```python
    class Orders(dy.Schema):
        order_id = dy.String(primary_key=True)
        amount = dy.Float64(nullable=False, min=0.0)


    @dd.asset(Orders, filter=True)
    def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return raw_orders.select("order_id", "amount")


    dd.error_rows_spec(Orders, orders).key
    ```

    Pass the spec, not its key, to `dg.Definitions(assets=[...])` with `orders`. A downstream asset can then declare the error rows as an input.
    """
    validate_namespace(schema)
    # The writer passes this metadata to the IO manager, so a load needs it too.
    definition_metadata: dict[str, object] = {}
    io_manager_key: str | None = None
    if isinstance(asset, dg.AssetsDefinition):
        if partitions_def is not None:
            both = f"`error_rows_spec` was given both the definition '{'/'.join(asset.key.path)}' and a `partitions_def`. The definition already states its partitions, so drop the argument. Pass a `partitions_def` only with a key, where there is no definition to read one off."
            raise dg.DagsterInvariantViolationError(both)
        parent, partitions_def = asset.key, asset.partitions_def
        definition_spec = asset.get_asset_spec()
        if group_name is None:
            group_name = asset.group_names_by_key[parent]
        if owners is None:
            owners = definition_spec.owners
        if tags is None:
            # Dagster stores each kind as a tag, and the spec has only the `polars` kind.
            tags = {
                name: value
                for name, value in definition_spec.tags.items()
                if not name.startswith("dagster/kind/")
            }
        definition_metadata = dict(definition_spec.metadata)
        recorded_key = definition_metadata.pop(_KEY_ENTRY, None)
        recorded_io_manager_key = definition_metadata.pop(_IO_MANAGER_KEY_ENTRY, None)
        key = (
            recorded_key.asset_key
            if isinstance(recorded_key, dg.DagsterAssetMetadataValue)
            else _error_rows_key(parent)
        )
        io_manager_key = (
            recorded_io_manager_key.text
            if isinstance(recorded_io_manager_key, dg.TextMetadataValue)
            else asset.get_io_manager_key_for_asset_key(parent)
        )
    else:
        parent = asset if isinstance(asset, dg.AssetKey) else dg.AssetKey(asset)
        key = _error_rows_key(parent)

    rendered: str = "/".join(parent.path)
    spec = dg.AssetSpec(
        key=key,
        deps=[parent],
        description=f"Rows from {rendered} that failed an 'error' rule, with one column per rule.",
        metadata={**definition_metadata, **error_rows_metadata(schema)},
        partitions_def=partitions_def,
        group_name=group_name,
        owners=owners,
        tags=tags,
        # Not the asset's kinds, which can name storage that `error_rows_io_manager_key` does not write to.
        kinds={"polars"},
    )
    return spec if io_manager_key is None else spec.with_io_manager_key(io_manager_key)
