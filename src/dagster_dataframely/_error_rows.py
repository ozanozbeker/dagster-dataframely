"""The functions in this module return the error rows, the error-rows key, the error rows' file path, the writers and the error rows' spec.

`dd.asset` records where its writer writes in the definition metadata, and `build_error_rows_spec` reads it back.
`get_error_rows` and `build_error_rows_spec` both build their columns from the schema's columns, `_get_rule_column_names` and `_RULE_COLUMN_DTYPE`, so the written error rows and the declared ones have the same names, order and dtypes.
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

from dagster_dataframely._metadata import COLUMN_SCHEMA_KEY, get_column_tags
from dagster_dataframely._rules import (
    describe_rules,
    get_error_rule_names,
    validate_namespace,
)
from dagster_dataframely._settings import ERROR_ROWS_DIR
from dagster_dataframely.errors import ErrorRowsDirError, ErrorRowsKeyCollisionError

type ErrorRowsWriter = Callable[[pl.DataFrame], str]
"""A writer: a function that writes the error rows and returns the error-rows address, such as an asset key or a file path, as a string."""

# A suffix, not a key part, because `DbIOManager` takes the table name from the last key part only (`docs/out-of-scope/dbt-test-configs.md#store_failures-writes-to-schema_dbt_test__audit`).
_LEAF_SUFFIX = "__error_rows"

_EXTENSION = ".parquet"

_KEY_ENTRY = "dataframely/error_rows_key"

_IO_MANAGER_KEY_ENTRY = "dataframely/error_rows_io_manager_key"

_VALID = "valid"

# The Delta writer panics on an `Enum` column, the dtype `FailureInfo.details()` gives a rule column.
_RULE_COLUMN_DTYPE = pl.String


def _get_rule_column_names(schema: type[dy.Schema]) -> dict[str, str]:
    """Return each rule's name in `FailureInfo.details()`, mapped to its rule column's name, in rule order."""
    return {rule.name: rule.check_name for rule in describe_rules(schema).values()}


def _with_rule_columns(schema: type[dy.Schema], details: pl.DataFrame) -> pl.DataFrame:
    """Select the schema's columns, then each rule column under its check name as `_RULE_COLUMN_DTYPE`.

    The `select` drops the `<column>|dtype` columns of a `FailureInfo` from `cast=True`, which the spec does not declare.
    """
    return details.select(
        *schema.column_names(),
        *(
            pl.col(rule).cast(_RULE_COLUMN_DTYPE).alias(name)
            for rule, name in _get_rule_column_names(schema).items()
        ),
    )


def separate_warned_and_error_rows(
    schema: type[dy.Schema], failure: dy.FailureInfo, errors: AbstractSet[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return the invalid rows that failed only `"warn"` rules, with the schema's columns, and the error rows as a writer receives them.

    `unknown` counts as failed; Dataframely reports it only with `cast=True` or for a `Collection`.
    """
    # Bound once: `details()` rebuilds the frame on every call.
    details: pl.DataFrame = failure.details()
    failed: pl.Expr = pl.any_horizontal(
        pl.lit(value=False), *(pl.col(rule) != _VALID for rule in errors)
    )
    return (
        details.filter(~failed).select(schema.column_names()),
        _with_rule_columns(schema, details.filter(failed)),
    )


def get_error_rows(schema: type[dy.Schema], failure: dy.FailureInfo) -> pl.DataFrame:
    """Return the rows that failed an `"error"` rule, as a writer receives them, with one rule column per rule.

    Parameters
    ----------
    schema
        The schema the rows were validated against, which sets each rule's severity.
    failure
        What `Schema.filter` returned beside the valid rows.

    Returns
    -------
    :
        The original columns in their own order, then a `String` rule column for every rule, named by `get_check_name_for_rule`. A rule column's value is `valid`, `invalid` or `unknown`.

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

    Examples
    --------
    ```python
    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema, marketplace_orders

    _, failure = OrdersSchema.filter(marketplace_orders, cast=False)
    error_rows = dd.wiring.get_error_rows(OrdersSchema, failure)
    error_rows.select("order_id", "amount", "dy_rule__amount__min")
    ```
    """
    validate_namespace(schema)
    _, error_rows = separate_warned_and_error_rows(
        schema, failure, get_error_rule_names(schema)
    )
    return error_rows


def get_error_rows_key(asset_key: dg.AssetKey) -> dg.AssetKey:
    """Return the default error-rows key of an asset.

    It takes no override, because an override is a `dg.AssetKey` to pass as it is.

    Parameters
    ----------
    asset_key
        The key of the asset whose rows fail validation.

    Returns
    -------
    :
        `asset_key` with `__error_rows` added to its last part. The key prefix stays.

    Examples
    --------
    ```python
    import dagster as dg

    import dagster_dataframely as dd

    dd.wiring.get_error_rows_key(dg.AssetKey(["sales", "orders"]))
    ```
    """
    return dg.AssetKey([*asset_key.path[:-1], f"{asset_key.path[-1]}{_LEAF_SUFFIX}"])


def build_writer_metadata(
    error_rows_key: dg.AssetKey,
    *,
    io_manager_key: str | None,
    error_rows_io_manager_key: str | None,
) -> dict[str, dg.DagsterAssetMetadataValue | dg.TextMetadataValue]:
    """Return the definition metadata that records where a run writes the error rows."""
    return {
        _KEY_ENTRY: dg.MetadataValue.asset(error_rows_key),
        _IO_MANAGER_KEY_ENTRY: dg.MetadataValue.text(
            error_rows_io_manager_key or io_manager_key or "io_manager"
        ),
    }


def _get_executable_keys(context: dg.AssetExecutionContext) -> AbstractSet[dg.AssetKey]:
    """Return every executable asset key in the code location, or in the job when the run has no code location (ADR-0007)."""
    try:
        return context.repository_def.asset_graph.executable_asset_keys
    except CheckError:
        return context.job_def.asset_layer.asset_graph.executable_asset_keys
    except DagsterInvalidPropertyError:
        return frozenset()


def validate_error_rows_key(
    context: dg.AssetExecutionContext, error_rows_key: dg.AssetKey
) -> None:
    """Raise if an asset already materializes the error-rows key.

    Call it at the start of a hand-wired asset's function when the asset writes error rows, as `dd.asset` does. In a direct invocation it does nothing.

    Parameters
    ----------
    context
        The running asset's context, whose code location the check reads.
    error_rows_key
        The key the asset's writer receives, such as the one `get_error_rows_key` returns.

    Raises
    ------
    ErrorRowsKeyCollisionError
        An asset in the code location materializes the error-rows key. The asset itself counts, so the function raises for an `error_rows_key` equal to the asset's own key.

    Examples
    --------
    ```python
    from collections.abc import Iterator

    import dagster as dg
    import polars as pl

    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema, storefront_orders


    @dg.asset(
        check_specs=dd.wiring.build_asset_check_specs(
            OrdersSchema, asset="orders", filter=True
        ),
    )
    def orders(
        context: dg.AssetExecutionContext,
    ) -> Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]:
        error_rows_key = dd.wiring.get_error_rows_key(context.asset_key)
        dd.wiring.validate_error_rows_key(context, error_rows_key)
        yield from dd.wiring.stream_results(
            OrdersSchema,
            storefront_orders,
            context=context,
            error_rows_writer=dd.wiring.build_io_manager_writer(
                context, error_rows_key
            ),
        )


    dg.materialize([orders]).success
    ```
    """
    if error_rows_key in _get_executable_keys(context):
        raise ErrorRowsKeyCollisionError(
            context.asset_key.to_user_string(), error_rows_key.to_user_string()
        )


def _format_partition_key(partition_key: str) -> str:
    """Format a partition key as `UPathIOManager` does.

    Upstream's is a closure; `tests/test_upstream_characterization.py` pins this copy.
    """
    if isinstance(partition_key, dg.MultiPartitionKey):
        return "/".join(
            value for _, value in sorted(partition_key.keys_by_dimension.items())
        )
    return partition_key


def get_error_rows_path(
    error_rows_key: dg.AssetKey,
    error_rows_dir: UPath | Path | str,
    partition_key: str | None = None,
) -> UPath:
    """Return the path of the parquet file `build_file_writer` writes an asset's error rows to.

    The layout matches `UPathIOManager`'s, so with `error_rows_dir` set to a manager's `base_dir`, the file is beside the asset's own file.

    Parameters
    ----------
    error_rows_key
        The key the writer writes under, such as the one `get_error_rows_key` returns.
    error_rows_dir
        Any path `UPath` accepts, such as a local directory or `s3://bucket/prefix`. Credentials for a remote path come from the environment.
    partition_key
        The partition the rows belong to, or `None` for an unpartitioned asset. A `dg.MultiPartitionKey` becomes its dimension keys joined by `/`, in dimension-name order. Escaping a `..` and a leading `/` keeps the file under `error_rows_dir`.

    Returns
    -------
    :
        `<error_rows_dir>/<key part>/.../<last part>.parquet`, or `<error_rows_dir>/<key part>/.../<last part>/<partition>.parquet` for a partition. `get_error_rows_path` creates no parent directories.

    Examples
    --------
    ```python
    import dagster as dg

    import dagster_dataframely as dd

    dd.wiring.get_error_rows_path(
        dd.wiring.get_error_rows_key(dg.AssetKey(["sales", "orders"])),
        "/data",
        "2026-08-01",
    )
    ```
    """
    path: UPath = UPath(error_rows_dir).joinpath(
        *coerce_to_relative_parts(UPath(*error_rows_key.path))
    )
    if partition_key is not None:
        # Both escapes match `FilesystemIOManager`, not `UPathIOManager`, so `../../etc/x` stays under `error_rows_dir`.
        path = path / escape_dotdot_segments(
            escape_leading_slash(_format_partition_key(partition_key))
        )
    return path.with_suffix(f"{path.suffix}{_EXTENSION}")


def get_row_count_key(context: dg.AssetExecutionContext) -> str:
    """Return the key Dagster defines for the row count of what the step materializes."""
    # A run of one partition has a key and no range, and a single-run backfill the reverse.
    partitioned = context.has_partition_key or context.has_partition_key_range
    # Dagster defines `dagster/row_count` as the whole table's count.
    return "dagster/partition_row_count" if partitioned else "dagster/row_count"


def build_io_manager_writer(
    context: dg.AssetExecutionContext,
    error_rows_key: dg.AssetKey,
    *,
    error_rows_io_manager_key: str | None = None,
) -> ErrorRowsWriter:
    """Return a writer that passes the error rows to the asset's own IO manager, or to the one `error_rows_io_manager_key` names.

    The manager stores the rows under the error-rows key, as it stores any other asset: `dd.data.ParquetIOManager` writes a Parquet file beside the table, and `DuckDBPolarsIOManager` writes a table beside it.
    After each write, the writer logs a materialization for the error-rows key, so the spec from `build_error_rows_spec` has a history.
    It logs one for the empty write of a clean run too, so the history matches the stored rows.
    It logs none when the run's code location has no spec for the key, so the catalog does not list a key that nobody declared.

    Parameters
    ----------
    context
        The running asset's context. Its asset definition must have exactly one asset, as a `@dg.asset` does.
    error_rows_key
        The key the IO manager stores the rows under, such as the one `get_error_rows_key` returns.
    error_rows_io_manager_key
        The resource key of another IO manager, or `None` for the asset's own. The op must require this resource key, because `context.resources` holds only the resources its op requires. The writer passes that IO manager its own config, because a `DbIOManager` reads its connection from the context. It passes the asset's definition metadata unchanged.

    Returns
    -------
    :
        A writer that takes the error rows and returns their asset key as a string.

    Raises
    ------
    dagster._core.errors.DagsterInvalidPropertyError
        There is no step, which means a direct invocation. Use `build_file_writer` there.

    Examples
    --------
    A hand-wired asset with `filter=True` builds the writer from its context and passes it to `stream_results`:

    ```python
    from collections.abc import Iterator

    import dagster as dg
    import polars as pl

    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema, marketplace_orders


    @dg.asset(
        metadata=dd.wiring.get_column_schema_metadata(OrdersSchema),
        check_specs=dd.wiring.build_asset_check_specs(
            OrdersSchema, asset="orders", filter=True
        ),
    )
    def orders(
        context: dg.AssetExecutionContext,
    ) -> Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]:
        yield from dd.wiring.stream_results(
            OrdersSchema,
            marketplace_orders,
            context=context,
            error_rows_writer=dd.wiring.build_io_manager_writer(
                context, dd.wiring.get_error_rows_key(context.asset_key)
            ),
        )


    dg.materialize([orders]).success
    ```
    """
    step = context.get_step_execution_context()
    handle = StepOutputHandle(
        step.step.key, context.output_for_asset_key(context.asset_key)
    )
    original: dg.OutputContext = step.get_output_context(handle)
    if error_rows_io_manager_key is None:
        manager = step.get_io_manager(handle)
        resource_config = original.resource_config
    else:
        manager = getattr(context.resources, error_rows_io_manager_key)
        # Dagster's own `get_output_context` reads a manager's config the same way.
        resources = step.resolved_run_config.resources
        resource_config = resources[error_rows_io_manager_key].config
    count_key = get_row_count_key(context)
    # Dagster logs one materialization per partition in a range, so the writer does too.
    partitions = (
        list(context.partition_keys)
        if count_key == "dagster/partition_row_count"
        else [None]
    )
    # A `dg.materialize` run has no code location to read the spec from, so the writer always logs the materialization.
    try:
        logs_event = (
            error_rows_key in context.repository_def.asset_graph.unexecutable_asset_keys
        )
    except CheckError:
        logs_event = True

    def write(frame: pl.DataFrame) -> str:
        # Copied per write, because `add_output_metadata` raises on a key an earlier write added.
        repointed = copy.copy(original)
        repointed._asset_key = error_rows_key  # noqa: SLF001 - no setter; a characterization test pins it
        repointed._resource_config = resource_config  # noqa: SLF001 - the same
        manager.handle_output(repointed, frame)
        if logs_event:
            metadata = {**repointed.get_logged_metadata(), count_key: len(frame)}
            for partition in partitions:
                context.log_event(
                    dg.AssetMaterialization(
                        asset_key=error_rows_key,
                        partition=partition,
                        metadata=metadata,
                    )
                )
        return error_rows_key.to_user_string()

    return write


def build_file_writer(
    context: dg.AssetExecutionContext,
    error_rows_key: dg.AssetKey,
    error_rows_dir: UPath | Path | str,
) -> ErrorRowsWriter:
    """Return a writer that writes the error rows to a parquet file under `error_rows_dir`.

    Use it in a direct invocation, where `build_io_manager_writer` raises because there is no step. The file is at `get_error_rows_path(error_rows_key, error_rows_dir, context.partition_key)`.

    Parameters
    ----------
    context
        The context the asset was invoked with. The writer reads only its partition key, so a context from `dg.build_asset_context()` works unbound. Without a partition key, the rows go to the unpartitioned path.
    error_rows_key
        The key the writer writes under, such as the one `get_error_rows_key` returns.
    error_rows_dir
        The directory the file goes under. Any path `UPath` accepts.

    Returns
    -------
    :
        A writer that takes the error rows and returns the file's path as a string.

    Raises
    ------
    dg.DagsterInvariantViolationError
        The context has a partition range and no partition key. The error rows do not record which partition each row came from, so they cannot be split into one file per partition.

    Examples
    --------
    ```python
    import tempfile

    import dagster as dg

    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema, marketplace_orders

    error_rows_dir = tempfile.mkdtemp()
    write = dd.wiring.build_file_writer(
        dg.build_asset_context(),
        dd.wiring.get_error_rows_key(dg.AssetKey("orders")),
        error_rows_dir,
    )
    _, failure = OrdersSchema.filter(marketplace_orders, cast=False)
    write(dd.wiring.get_error_rows(OrdersSchema, failure)).removeprefix(error_rows_dir)
    ```
    """
    # `UPathIOManager` raises on a range in a run too, and the rows do not record their partition.
    if context.has_partition_key_range and not context.has_partition_key:
        span = context.partition_key_range
        ranged = f"The context has the partition range '{span.start}' to '{span.end}', and `build_file_writer` writes the error rows under '{error_rows_key.to_user_string()}' to one partition's file. Invoke the asset once for each partition, with `dg.build_asset_context(partition_key=...)`."
        raise dg.DagsterInvariantViolationError(ranged)
    path: UPath = get_error_rows_path(
        error_rows_key,
        error_rows_dir,
        context.partition_key if context.has_partition_key else None,
    )

    def write(frame: pl.DataFrame) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        # An open handle, because `write_parquet` cannot open every filesystem a `UPath` can.
        with path.open("wb") as file:
            frame.write_parquet(file)
        return str(path)

    return write


def build_error_rows_writer(
    context: dg.AssetExecutionContext,
    error_rows_key: dg.AssetKey,
    *,
    error_rows_io_manager_key: str | None = None,
) -> ErrorRowsWriter:
    """Return a writer that calls `build_io_manager_writer` in a run and `build_file_writer` otherwise.

    Without a step it writes nothing for a zero-row frame, so a direct invocation with no error rows needs no `error_rows_dir`.
    """

    def write(frame: pl.DataFrame) -> str:
        try:
            context.get_step_execution_context()
        except DagsterInvalidPropertyError:
            pass
        else:
            return build_io_manager_writer(
                context,
                error_rows_key,
                error_rows_io_manager_key=error_rows_io_manager_key,
            )(frame)
        if frame.is_empty():
            return ""
        error_rows_dir: str | None = ERROR_ROWS_DIR.resolve(None)
        if error_rows_dir is None:
            raise ErrorRowsDirError(context.asset_key.to_user_string())
        return build_file_writer(context, error_rows_key, error_rows_dir)(frame)

    return write


def _build_error_rows_table_schema(schema: type[dy.Schema]) -> dg.TableSchema:
    """Return the error rows' column schema, without the constraints its rows fail."""
    return dg.TableSchema(
        columns=[
            dg.TableColumn(
                name=name,
                type=str(column.dtype),
                description=column.description,
                tags=get_column_tags(column),
            )
            for name, column in schema.columns().items()
        ]
        + [
            dg.TableColumn(
                name=check_name,
                type=str(_RULE_COLUMN_DTYPE),
                description=(
                    f"Whether the row is 'valid', 'invalid' or 'unknown' under rule '{rule}'."
                ),
            )
            for rule, check_name in _get_rule_column_names(schema).items()
        ]
    )


def build_error_rows_spec(  # noqa: PLR0913 - takes the spec's attributes
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
    schema
        The asset's schema. The spec's Columns tab has its columns, then one rule column per rule.
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
    :
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
    import polars as pl

    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema


    @dd.asset(OrdersSchema, filter=True)
    def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return raw_orders


    dd.build_error_rows_spec(OrdersSchema, orders).key
    ```

    Pass the spec, not its key, to `dg.Definitions(assets=[...])` with `orders`. A downstream asset can then declare the error rows as an input.
    """
    validate_namespace(schema)
    # The writer passes this metadata to the IO manager, so a load needs it too.
    definition_metadata: dict[str, object] = {}
    io_manager_key: str | None = None
    if isinstance(asset, dg.AssetsDefinition):
        if partitions_def is not None:
            both = f"`dd.build_error_rows_spec` received both the definition '{asset.key.to_user_string()}' and a `partitions_def`. The spec uses the definition's partitions, so remove the `partitions_def` argument. Pass a `partitions_def` only with an asset key."
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
            else get_error_rows_key(parent)
        )
        io_manager_key = (
            recorded_io_manager_key.text
            if isinstance(recorded_io_manager_key, dg.TextMetadataValue)
            else asset.get_io_manager_key_for_asset_key(parent)
        )
    else:
        parent = asset if isinstance(asset, dg.AssetKey) else dg.AssetKey(asset)
        key = get_error_rows_key(parent)

    spec = dg.AssetSpec(
        key=key,
        deps=[parent],
        description=f"Rows from {parent.to_user_string()} that failed an 'error' rule, with one column per rule.",
        metadata={
            **definition_metadata,
            COLUMN_SCHEMA_KEY: _build_error_rows_table_schema(schema),
        },
        partitions_def=partitions_def,
        group_name=group_name,
        owners=owners,
        tags=tags,
        # Not the asset's kinds, which can name storage that the error rows' IO manager does not write to.
        kinds={"polars"},
    )
    return spec if io_manager_key is None else spec.with_io_manager_key(io_manager_key)
