"""What a quarantine is called, where it goes, and who puts it there.

Four names, one rule. `_LEAF_SUFFIX` decides what a quarantine is called, and everything here is a way of using that name: a writer addresses the rows by it, a path spells it, a spec is keyed by it. They sit together because a spec keyed differently from the quarantine it describes is a dependency on nothing.

**The delegating writer is the usual route** (ADR-0006). It borrows the step's own output context, re-points it at `<name>_quarantine`, and hands the frame to the manager the asset is already bound to, so the rows land natively wherever that manager puts things: a parquet file beside the table under `UPathIOManager`, a second table beside it under `DbIOManager`. Nothing here knows which manager it is talking to.

**The file writer is the fallback**, for the case delegation cannot serve: direct invocation, where there is no step and therefore no manager to borrow. It writes parquet to `quarantine_path`, which mirrors `UPathIOManager`'s own path, so a root set to a manager's `base_dir` still lands each quarantine beside its table.

Whatever upstream already spells, this imports rather than restates. A second implementation of the same rule is a second answer waiting to differ. The multi-partition spelling is the one exception, because it lives in a closure inside `_get_paths_for_partitions` and cannot be reached. It is restated here and pinned against upstream's own path by a characterization test.

**A partition key is escaped as `FilesystemIOManager` escapes it, not as the `UPathIOManager` base class does.** The base escapes a leading `/` and leaves `..` alone, which is upstream's documented advice to override on a hierarchical filesystem. A key of `../../etc/x` would otherwise resolve out of the root and write wherever it landed, and a partition key can come from data where an asset key cannot. Here that matters more than parity, because this path is one the package writes itself rather than one it hands to a manager.
"""

import copy
from collections.abc import Callable, Sequence
from pathlib import Path

import dagster as dg
import dataframely as dy
import polars as pl

# What the delegating writer borrows. `StepOutputHandle` names the step's own output,
# `get_output_context` hands back the context that output was going to be written under,
# and `get_io_manager` hands back the manager that was going to write it. Both those
# methods are private, as is `OutputContext._asset_key`, and all of them are pinned by
# characterization tests (ADR-0006).
from dagster._core.execution.plan.outputs import StepOutputHandle

# The three escapes Dagster applies before it joins a path. None is exported from
# `dagster`; all three are covered by characterization tests (#104).
from dagster._core.storage.upath_io_manager import (
    coerce_to_relative_parts,
    escape_dotdot_segments,
    escape_leading_slash,
)
from upath import UPath

from dagster_dataframely._metadata import quarantine_metadata

#: What a writer hands back: wherever the invalid rows went, rendered for a reader. A string rather than a path, because the answer can be a database table.
type QuarantineWriter = Callable[[pl.DataFrame], str]

#: What keeps a quarantine from overwriting the table it came from when the two share a root. Spelled once, because the file's name and the spec's key have to be the same string.
_LEAF_SUFFIX = "_quarantine"

_EXTENSION = ".parquet"


def _suffixed(parts: Sequence[str]) -> tuple[str, ...]:
    """Suffix the last part of a key, leaving the prefix alone.

    The prefix is a directory on disk and a folder in the asset graph, and neither wants renaming. Only the name has to change, because only the name collides.
    """
    return (*parts[:-1], f"{parts[-1]}{_LEAF_SUFFIX}")


def _quarantine_key(key: dg.AssetKey) -> dg.AssetKey:
    """Name the quarantine's own asset key, which is the whole of its address under delegation.

    Every asset-key-addressed IO manager already turns a key into its own location: `UPathIOManager` into a path, `DbIOManager` into `<schema>.<table>` off the last part. So suffixing the leaf is all the placement rule there is, on every backend at once.
    """
    return dg.AssetKey(list(_suffixed(key.path)))


def _spelling(partition_key: str) -> str:
    """Render a partition key as `UPathIOManager` renders it in a path.

    A single-dimension key is already its own spelling. A multi-partition key is its dimension keys joined by `/`, ordered by dimension name and never by the order the dimensions were declared or the key was built.

    Restated from `UPathIOManager._get_paths_for_partitions`, where it is a closure with no import path. `test_upstream_characterization.py` pins it against a real manager's path, so upstream changing the spelling is a failing test rather than a quarantine nobody can find.
    """
    if isinstance(partition_key, dg.MultiPartitionKey):
        return "/".join(
            value for _, value in sorted(partition_key.keys_by_dimension.items())
        )
    return partition_key


def quarantine_path(
    key: dg.AssetKey,
    root: UPath | Path | str,
    partition_key: str | None = None,
) -> UPath:
    """Resolve the file an asset's invalid rows go to when no IO manager places them.

    The fallback address, not the usual one. A quarantine is normally written through the asset's own manager and lands wherever that manager puts things (ADR-0006). This is what answers when there is no manager to ask, which is direct invocation, or when the user named a root instead.

    ```text
    <root>/<key part>/.../<name>_quarantine.parquet
    <root>/<key part>/.../<name>_quarantine/<partition>.parquet
    ```

    Everything but the leaf is `UPathIOManager`'s own path, so a root set to a manager's `base_dir` lands each quarantine beside the table it came from. The leaf carries `_quarantine` so that sharing a root can never mean sharing a file, the same suffix the delegating writer puts on the asset key.

    A partition is a file under a directory rather than a longer file name, so a backfill of one partition rewrites one file.

    Parameters
    ----------
    key
        The asset key of the table the rows were rejected from, not the quarantine's own.
    root
        Where quarantines go. Anything `UPath` accepts, so a local directory and `s3://bucket/prefix` are the same call on credentials from the ambient environment.
    partition_key
        The partition being written, or `None` for an unpartitioned asset. A `dg.MultiPartitionKey` is spelled as its dimension keys in dimension-name order. A key that would climb out of the root is escaped rather than refused, so no partition can name a file the root does not contain.

    Returns
    -------
    The file's path, parents not created.

    Examples
    --------
    ```python
    import dagster as dg

    import dagster_dataframely as dd

    dd.wiring.quarantine_path(dg.AssetKey(["sales", "orders"]), "s3://bucket/warehouse")
    ```
    """
    path: UPath = UPath(root).joinpath(
        *_suffixed(coerce_to_relative_parts(UPath(*key.path)))
    )
    if partition_key is not None:
        path = path / escape_dotdot_segments(
            escape_leading_slash(_spelling(partition_key))
        )
    # Appended to whatever suffix the name already had, as `_with_extension` does, so an
    # asset named `orders.v2` keeps its own dot rather than losing it to `with_suffix`.
    return path.with_suffix(f"{path.suffix}{_EXTENSION}")


def delegating_writer(context: dg.AssetExecutionContext) -> QuarantineWriter:
    """Build the writer that hands the invalid rows to the asset's own IO manager.

    The quarantine then lands wherever that manager puts things, with no configuration and nothing here knowing which manager it is: a parquet file beside the table under `PolarsParquetIOManager`, a second table beside it under `DuckDBPolarsIOManager` (ADR-0006).

    **The output context is cloned, not built.** `DbIOManager` backends read the database and connection settings off the context at write time rather than off themselves, so a context assembled by hand would need a list of which fields matter, which is the per-manager knowledge delegation exists to avoid. A shallow copy of the step's real one carries every field instead, and only the asset key is re-pointed. Carrying a field is not interpreting it, and that is what keeps the write manager-blind. A partitioned asset on a database manager needs `partition_expr`, and it comes along on the definition metadata rather than by being read, as does the partition itself.

    **The metadata the manager emits still goes nowhere.** `add_output_metadata` rebinds the mapping on the object it is called on, so a manager writing its own `path` or `Query` writes it onto the clone. The step reads the original, which is why the manager's answer is dropped and the package reports the address itself (ADR-0006).

    **The manager is borrowed too**, off the same step output handle, rather than read off `context.resources`. That is what leaves the asset free of a `required_resource_keys` declaration, which Dagster validates at bind time and which would therefore make every direct invocation supply a manager it has no use for. It also means the asset's own `io_manager_key` is followed without this function ever learning what it is.

    The borrow happens here rather than inside the returned writer, so an asset with no step to borrow from fails while a caller can still choose the fallback, rather than at the moment the rows need writing.

    Parameters
    ----------
    context
        The executing asset's context. It must own exactly one asset, which is what `dg.asset` builds; check outputs are not asset outputs and are skipped.

    Returns
    -------
    A writer taking the invalid rows and returning the quarantine's asset key, rendered.

    Raises
    ------
    dagster._core.errors.DagsterInvalidPropertyError
        There is no step to borrow from, which is direct invocation. Callers that have a fallback catch this and reach for `file_writer` instead.
    """
    step = context.get_step_execution_context()
    # The asset's own output, never a check's: a check spec is an op output too, so the step has one more output per declared check.
    (output_name,) = [
        name
        for name, key in context.assets_def.keys_by_output_name.items()
        if key == context.asset_key
    ]
    handle = StepOutputHandle(step.step.key, output_name)
    original: dg.OutputContext = step.get_output_context(handle)
    manager = step.get_io_manager(handle)
    key = _quarantine_key(context.asset_key)

    def write(frame: pl.DataFrame) -> str:
        # Copied per call, so two writes cannot see each other's leftovers.
        borrowed = copy.copy(original)
        # The one field re-pointed, and the private attribute behind `OutputContext.asset_key`, which has no setter. Pinned by a characterization test, as the rest of the borrow is.
        borrowed._asset_key = key  # noqa: SLF001 - the whole of the re-point, pinned upstream
        manager.handle_output(borrowed, frame)
        return key.to_user_string()

    return write


def file_writer(
    key: dg.AssetKey,
    root: UPath | Path | str,
    partition_key: str | None = None,
) -> QuarantineWriter:
    """Build the writer that puts the invalid rows in a parquet file under `root`.

    The fallback, for when there is no manager to delegate to. That is direct invocation, where the asset is called rather than run and there is no step to borrow an output context from.

    Parquet and nothing else. The quarantine holds whatever dtypes the schema declares, and a format that cannot round-trip them would make the evidence disagree with the table it came from.

    Parameters
    ----------
    key
        The asset key of the table the rows were rejected from, not the quarantine's own. `quarantine_path` suffixes the leaf.
    root
        Where quarantines go. Anything `UPath` accepts.
    partition_key
        The partition being written, or `None` for an unpartitioned asset.

    Returns
    -------
    A writer taking the invalid rows and returning the file's path, rendered.
    """
    path: UPath = quarantine_path(key, root, partition_key)

    def write(frame: pl.DataFrame) -> str:
        # Created here rather than at build time, so a writer nothing calls leaves no empty directory behind.
        path.parent.mkdir(parents=True, exist_ok=True)
        # Through an open handle rather than by name, because `write_parquet` takes a path only for the local filesystem and a `UPath` may be anywhere.
        with path.open("wb") as file:
            frame.write_parquet(file)
        return str(path)

    return write


def build_quarantine_spec(
    schema: type[dy.Schema],
    asset: dg.AssetsDefinition | dg.AssetKey | str | Sequence[str],
    *,
    partitions_def: dg.PartitionsDefinition | None = None,
) -> dg.AssetSpec:
    """Build the quarantine spec that gives a quarantine its place in the graph.

    The package writes the quarantine whether or not this is called. What this adds is a node: a key a downstream asset can name as an input, a Columns tab saying what it holds, and a dependency edge back to the asset the invalid rows came from. Written through the asset's own manager, the spec's key resolves through that same manager, so the downstream read needs no code and no second manager from this package.

    It never receives a materialization event, which is honest: nothing materializes it.

    **The schema is passed rather than read off the definition.** Every other assembled part in `wiring` takes it first, and after ADR-0004 nothing on an `AssetsDefinition` carries the live class any more. Key, prefix and partitions still come off the definition, so the spec cannot disagree with its parent about any of those.

    Parameters
    ----------
    schema
        The schema that rejected the rows.
    asset
        The asset the invalid rows came from. A definition is the normal form and supplies its own partitions. The key forms are for an asset that cannot be imported, generated or foreign.
    partitions_def
        The quarantine's partitions, for a key form. A definition already states them, so passing both raises rather than choosing.

    Returns
    -------
    A spec to hand to `dg.Definitions(assets=[...])`.

    Raises
    ------
    dg.DagsterInvariantViolationError
        A definition and a `partitions_def` were both given. Dagster's own error rather than the package's, because two sources of one fact is a wiring mistake, not a data one.

    Examples
    --------
    ```python
    import dagster as dg
    import dataframely as dy
    import polars as pl

    import dagster_dataframely as dd


    class Orders(dy.Schema):
        order_id = dy.String(primary_key=True)


    @dd.dy_asset(Orders, quarantine=True)
    def orders() -> pl.DataFrame:
        return pl.DataFrame({"order_id": ["a"]})


    defs = dg.Definitions(assets=[orders, dd.build_quarantine_spec(Orders, orders)])
    ```
    """
    if isinstance(asset, dg.AssetsDefinition):
        if partitions_def is not None:
            both = f"`build_quarantine_spec` was given both the definition '{'/'.join(asset.key.path)}' and a `partitions_def`. The definition already states its partitions, so drop the argument. Pass a `partitions_def` only with a key, where there is no definition to read one off."
            raise dg.DagsterInvariantViolationError(both)
        parent, partitions_def = asset.key, asset.partitions_def
    else:
        parent = asset if isinstance(asset, dg.AssetKey) else dg.AssetKey(asset)

    rendered: str = "/".join(parent.path)
    return dg.AssetSpec(
        key=_quarantine_key(parent),
        deps=[parent],
        description=f"Invalid rows from {rendered}, with a column per rule saying why.",
        metadata=quarantine_metadata(schema),
        partitions_def=partitions_def,
    )
