"""The quarantine's asset key, path, writers and asset spec.

`quarantine_path` escapes partition keys as `FilesystemIOManager` does, not as `UPathIOManager` does, so the path for `../../etc/x` is still under `quarantine_dir`.
"""

import copy
from collections.abc import Callable, Sequence
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

from dagster_dataframely._metadata import quarantine_metadata
from dagster_dataframely._rules import validate_namespace
from dagster_dataframely._settings import QUARANTINE_DIR
from dagster_dataframely.errors import QuarantineDirError, QuarantineKeyCollisionError

type QuarantineWriter = Callable[[pl.DataFrame], str]
"""A writer: a function that writes the invalid rows and returns the quarantine address, such as an asset key or a file path, as a string."""

_LEAF_SUFFIX = "_quarantine"

_EXTENSION = ".parquet"


def _suffixed(parts: Sequence[str]) -> tuple[str, ...]:
    """Add `_quarantine` to the last part of a key."""
    return (*parts[:-1], f"{parts[-1]}{_LEAF_SUFFIX}")


def _quarantine_key(key: dg.AssetKey) -> dg.AssetKey:
    """Return the quarantine's asset key."""
    return dg.AssetKey(list(_suffixed(key.path)))


def _executable_keys(context: dg.AssetExecutionContext) -> AbstractSet[dg.AssetKey]:
    """Return every asset key the run can materialize (ADR-0007)."""
    try:
        return context.repository_def.asset_graph.executable_asset_keys
    except CheckError:
        return context.job_def.asset_layer.asset_graph.executable_asset_keys
    except DagsterInvalidPropertyError:
        return frozenset()


def validate_quarantine_key(context: dg.AssetExecutionContext) -> None:
    """Raise if another asset already materializes the quarantine's asset key.

    Call it at the start of a hand-wired asset's function when the asset writes a quarantine, as `dd.asset` does. In a direct invocation it does nothing.

    Raises
    ------
    QuarantineKeyCollisionError
        Another asset in the code location materializes `<name>_quarantine`.
    """
    key: dg.AssetKey = _quarantine_key(context.asset_key)
    if key in _executable_keys(context):
        raise QuarantineKeyCollisionError(
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


def quarantine_path(
    key: dg.AssetKey,
    quarantine_dir: UPath | Path | str,
    partition_key: str | None = None,
) -> UPath:
    """Return the path of the parquet file `file_writer` writes an asset's invalid rows to.

    The layout matches `UPathIOManager`'s, so with `quarantine_dir` set to a manager's `base_dir`, the file is beside the asset's own file.

    Parameters
    ----------
    key
        The asset key of the asset whose rows failed validation, not the quarantine's key.
    quarantine_dir
        Any path `UPath` accepts, such as a local directory or `s3://bucket/prefix`. Credentials for a remote path come from the environment.
    partition_key
        A `dg.MultiPartitionKey` becomes its dimension keys joined by `/`, in dimension-name order. Escaping a `..` and a leading `/` keeps the file under `quarantine_dir`.

    Returns
    -------
    `<quarantine_dir>/<key part>/.../<name>_quarantine.parquet`, or `<quarantine_dir>/<key part>/.../<name>_quarantine/<partition>.parquet` for a partition. `quarantine_path` creates no parent directories.
    """
    path: UPath = UPath(quarantine_dir).joinpath(
        *_suffixed(coerce_to_relative_parts(UPath(*key.path)))
    )
    if partition_key is not None:
        path = path / escape_dotdot_segments(
            escape_leading_slash(_formatted_partition_key(partition_key))
        )
    return path.with_suffix(f"{path.suffix}{_EXTENSION}")


def delegating_writer(context: dg.AssetExecutionContext) -> QuarantineWriter:
    """Return a writer that passes the invalid rows to the asset's own IO manager.

    The manager stores the rows under the asset key `<name>_quarantine`, as it stores any other asset: `PolarsParquetIOManager` writes a parquet file beside the table, and `DuckDBPolarsIOManager` writes a table beside it.
    The writer discards the metadata the manager emits for this write.

    Parameters
    ----------
    context
        Its asset definition must have exactly one asset, as a `@dg.asset` does.

    Returns
    -------
    A writer that takes the invalid rows and returns the quarantine's asset key as a string.

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
    manager = step.get_io_manager(handle)
    key = _quarantine_key(context.asset_key)

    def address(frame: pl.DataFrame) -> str:
        # Copied per write, because `add_output_metadata` raises on a key an earlier write added.
        repointed = copy.copy(original)
        repointed._asset_key = key  # noqa: SLF001 - no setter; a characterization test pins it
        manager.handle_output(repointed, frame)
        return key.to_user_string()

    return address


def file_writer(
    key: dg.AssetKey,
    quarantine_dir: UPath | Path | str,
    partition_key: str | None = None,
) -> QuarantineWriter:
    """Return a writer that writes the invalid rows to a parquet file under `quarantine_dir`.

    Use it in a direct invocation, where `delegating_writer` raises because there is no step. The file is at `quarantine_path(key, quarantine_dir, partition_key)`.

    Parameters
    ----------
    key
        The asset key of the asset whose rows failed validation, not the quarantine's key.

    Returns
    -------
    A writer that takes the invalid rows and returns the file's path as a string.
    """
    path: UPath = quarantine_path(key, quarantine_dir, partition_key)

    def address(frame: pl.DataFrame) -> str:
        path.parent.mkdir(parents=True, exist_ok=True)
        # An open handle, because `write_parquet` cannot open every filesystem a `UPath` can.
        with path.open("wb") as file:
            frame.write_parquet(file)
        return str(path)

    return address


def quarantine_writer(context: dg.AssetExecutionContext) -> QuarantineWriter:
    """Return a writer that calls `delegating_writer` in a run and `file_writer` otherwise."""

    def address(frame: pl.DataFrame) -> str:
        try:
            context.get_step_execution_context()
        except DagsterInvalidPropertyError:
            pass
        else:
            return delegating_writer(context)(frame)
        quarantine_dir: str | None = QUARANTINE_DIR.resolve(None)
        if quarantine_dir is None:
            raise QuarantineDirError(context.asset_key.to_user_string())
        return file_writer(
            context.asset_key,
            quarantine_dir,
            context.partition_key if context.has_partition_key else None,
        )(frame)

    return address


def quarantine_spec(
    schema: type[dy.Schema],
    asset: dg.AssetsDefinition | dg.AssetKey | str | Sequence[str],
    *,
    partitions_def: dg.PartitionsDefinition | None = None,
) -> dg.AssetSpec:
    """Return an asset spec that adds an asset's quarantine to the asset graph.

    The asset writes the quarantine whether or not the spec exists. The spec never receives a materialization event, because nothing materializes it.

    Parameters
    ----------
    asset
        The asset the invalid rows came from, as its definition or its asset key. Pass a key only for an asset you cannot import. A definition also sets the spec's partitions.
    partitions_def
        The quarantine's partitions, when `asset` is a key.

    Returns
    -------
    A spec keyed `<name>_quarantine` that depends on `asset`, to pass to `dg.Definitions(assets=[...])`.

    Raises
    ------
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
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


    @dd.asset(Orders, quarantine=True)
    def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return raw_orders.select("order_id", "amount")


    dd.quarantine_spec(Orders, orders).key
    ```

    Pass the spec, not its key, to `dg.Definitions(assets=[...])` with `orders`. A downstream asset can then declare the quarantine as an input.
    """
    validate_namespace(schema)
    if isinstance(asset, dg.AssetsDefinition):
        if partitions_def is not None:
            both = f"`quarantine_spec` was given both the definition '{'/'.join(asset.key.path)}' and a `partitions_def`. The definition already states its partitions, so drop the argument. Pass a `partitions_def` only with a key, where there is no definition to read one off."
            raise dg.DagsterInvariantViolationError(both)
        parent, partitions_def = asset.key, asset.partitions_def
    else:
        parent = asset if isinstance(asset, dg.AssetKey) else dg.AssetKey(asset)

    rendered: str = "/".join(parent.path)
    return dg.AssetSpec(
        key=_quarantine_key(parent),
        deps=[parent],
        description=f"Invalid rows from {rendered}, with one column per rule.",
        metadata=quarantine_metadata(schema),
        partitions_def=partitions_def,
    )
