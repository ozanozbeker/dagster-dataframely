"""What a quarantine is called, where it goes, and who puts it there.

`_LEAF_SUFFIX` decides what a quarantine is called. Everything here uses that name: a writer addresses the rows by it, a path carries it, a spec is keyed by it, and `validate_quarantine_key` proves no other asset already owns it. They sit together because a spec keyed differently from the quarantine it describes depends on nothing.

That last one is here because the suffix reserves a name in a space the package does not own. Dagster's asset keys are the user's too, so the reservation is the one this package cannot enforce by generating the string itself, and a run proves it rather than a load (ADR-0007).

`quarantine_writer` picks between the two writers, and the decorator is its only caller. It waits for the rows rather than deciding when it is built, because the answers differ only where there is no step and a call holding nothing back needs no directory at all (#115).

Whatever upstream already names, this module imports rather than restates. A second implementation of one rule is a second answer waiting to differ. The multi-partition format is the one exception: it lives in a closure inside `_get_paths_for_partitions` and cannot be imported. `_formatted_partition_key` restates it under upstream's own word, and a characterization test pins it against upstream's own path.

**A partition key is escaped as `FilesystemIOManager` escapes it, not as the `UPathIOManager` base class does.** The base escapes a leading `/` and leaves `..` alone; upstream documents that as the behaviour to override on a hierarchical filesystem. A key of `../../etc/x` would otherwise resolve out of the quarantine_dir and write wherever it landed, and a partition key can come from data where an asset key cannot. Safety beats parity here, because the package writes this path itself rather than handing it to a manager.
"""

import copy
from collections.abc import Callable, Sequence
from collections.abc import Set as AbstractSet
from pathlib import Path

import dagster as dg
import dataframely as dy
import polars as pl

# What `repository_def` raises when the run has no code location behind it, which is
# every in-process run. Dagster re-exports `dagster_shared`'s here, so the guard stays
# inside a declared dependency. Characterization tests pin it with the rest (ADR-0007).
from dagster._check import CheckError

# What every context property only a real step can answer raises. Here it says the
# quarantine key has no graph to be checked against, which is direct invocation.
from dagster._core.errors import DagsterInvalidPropertyError

# Where the delegating writer reads the step's output context and IO manager from.
# `StepOutputHandle` names the step's own output, `get_output_context` returns the
# context that output was going to be written under, and `get_io_manager` returns the
# manager that was going to write it. Both methods are private, as is
# `OutputContext._asset_key`. Characterization tests pin all three (ADR-0006).
from dagster._core.execution.plan.outputs import StepOutputHandle

# The three escapes Dagster applies before it joins a path. None is exported from
# `dagster`; characterization tests cover all three (#104).
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
"""What a writer returns: where the invalid rows went, rendered for a reader. A string, not a path, because the answer can be a database table."""

_LEAF_SUFFIX = "_quarantine"
"""Keeps a quarantine from overwriting the table it came from when the two share a quarantine_dir. Written once, because the file's name and the spec's key must be the same string."""

_EXTENSION = ".parquet"


def _suffixed(parts: Sequence[str]) -> tuple[str, ...]:
    """Suffix the last part of a key, leaving the prefix alone.

    The prefix is a directory on disk and a folder in the asset graph. Only the name collides, so only the name changes.
    """
    return (*parts[:-1], f"{parts[-1]}{_LEAF_SUFFIX}")


def _quarantine_key(key: dg.AssetKey) -> dg.AssetKey:
    """Name the quarantine's asset key, its whole address under delegation.

    Every asset-key-addressed IO manager turns a key into its own address: `UPathIOManager` into a path, `DbIOManager` into `<schema>.<table>` off the last part. So suffixing the leaf is the whole placement rule, on every backend at once.
    """
    return dg.AssetKey(list(_suffixed(key.path)))


def _executable_keys(context: dg.AssetExecutionContext) -> AbstractSet[dg.AssetKey]:
    """Return every asset key the run could materialize, as widely as it can see them.

    `repository_def` carries the whole code location, and a run launched from one always has it. In process there is no code location behind the run, so the job is everything: `dg.materialize` holds what it was handed, less whatever a selection dropped. Direct invocation has neither, and reaches `file_writer`, whose path resolves no asset key at all.

    Only executable keys count. An asset Dagster can materialize is the only thing that can write over a quarantine, and a spec from `quarantine_spec` is unexecutable, so upstream's own split exempts it with no marker of ours (ADR-0007).
    """
    try:
        return context.repository_def.asset_graph.executable_asset_keys
    except CheckError:
        return context.job_def.asset_layer.asset_graph.executable_asset_keys
    except DagsterInvalidPropertyError:
        return frozenset()


def validate_quarantine_key(context: dg.AssetExecutionContext) -> None:
    """Refuse a run whose quarantine key another asset already materializes.

    The suffix reserves `<name>_quarantine` in Dagster's key space, which belongs to the user as much as to this package. An asset declared there and a quarantine written there resolve to one address through one IO manager, so one write lands on the other and the run reports nothing (#114).

    Called before the decorated function, on every run of a quarantined asset. The key is a property of the declaration rather than of the rows, so it does not wait for them the way the writer's route does (#115), and a run holding nothing back still fails on a name that was always wrong.

    A hand-wired asset calls this itself, next to where it builds its writer.

    Parameters
    ----------
    context
        The executing asset's context. Direct invocation passes through untouched: there is no graph to read and no asset key to contend for.

    Raises
    ------
    QuarantineKeyCollisionError
        An asset the run can materialize already owns `<name>_quarantine`.
    """
    key: dg.AssetKey = _quarantine_key(context.asset_key)
    if key in _executable_keys(context):
        raise QuarantineKeyCollisionError(
            context.asset_key.to_user_string(), key.to_user_string()
        )


def _formatted_partition_key(partition_key: str) -> str:
    """Format a partition key as `UPathIOManager` formats it in a path.

    A single-dimension key is its own format. A multi-partition key is its dimension keys joined by `/`, ordered by dimension name, never by the order the dimensions were declared or the key was built.

    Restated from `UPathIOManager._get_paths_for_partitions`, whose `_formatted_multipartitioned_path` is a closure with no import path. `test_upstream_characterization.py` pins it against a real manager's path, so upstream changing the format fails a test instead of hiding a quarantine.
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
    """Resolve the file an asset's invalid rows go to when no IO manager places them.

    Everything but the leaf is `UPathIOManager`'s own path, so a quarantine_dir set to a manager's `base_dir` lands each quarantine beside its table. The leaf carries `_quarantine`, the same suffix the delegating writer puts on the asset key, so sharing a quarantine_dir never means sharing a file. `USER_GUIDE.md` has the layout.

    Parameters
    ----------
    key
        The asset key of the table the rows failed validation in, not the quarantine's own.
    quarantine_dir
        Where quarantines go. Anything `UPath` accepts, so a local directory and `s3://bucket/prefix` are the same call on credentials from the ambient environment.
    partition_key
        The partition being written, or `None` for an unpartitioned asset. A `dg.MultiPartitionKey` is formatted as its dimension keys in dimension-name order. A key that would climb out of the quarantine_dir is escaped, not refused, so no partition can name a file outside the quarantine_dir.

    Returns
    -------
    The file's path, parents not created.
    """
    path: UPath = UPath(quarantine_dir).joinpath(
        *_suffixed(coerce_to_relative_parts(UPath(*key.path)))
    )
    if partition_key is not None:
        path = path / escape_dotdot_segments(
            escape_leading_slash(_formatted_partition_key(partition_key))
        )
    # Appended to whatever suffix the name already has, as `_with_extension` does, so an
    # asset named `orders.v2` keeps its own dot instead of losing it to `with_suffix`.
    return path.with_suffix(f"{path.suffix}{_EXTENSION}")


def delegating_writer(context: dg.AssetExecutionContext) -> QuarantineWriter:
    """Build the writer that hands the invalid rows to the asset's own IO manager.

    The quarantine lands wherever that manager puts things, with no configuration and nothing here knowing which manager it is: a parquet file beside the table under `PolarsParquetIOManager`, a second table beside it under `DuckDBPolarsIOManager` (ADR-0006).

    The output context is shallow-copied, not built. `DbIOManager` backends read the database and connection settings off the context at write time, so a context assembled by hand would need a list of which fields matter. Delegation exists to avoid that per-manager knowledge. A shallow copy of the step's real context carries every field, and only the asset key is re-pointed. Carrying a field is not interpreting it, so the write stays manager-blind. A partitioned asset on a database manager needs `partition_expr`; it comes along on the definition metadata, as does the partition itself.

    The metadata the manager emits goes nowhere. `add_output_metadata` rebinds the mapping on the object it is called on, so a manager writing its own `path` or `Query` writes it onto the copy. The step reads the original. So the manager's answer is dropped and the package reports the address itself (ADR-0006).

    The manager comes off the same step output handle, not off `context.resources`. This leaves the asset free of a `required_resource_keys` declaration. Dagster validates that declaration at bind time, so every direct invocation would have to supply a manager it never uses. It also means the asset's own `io_manager_key` is followed without this function learning what it is.

    The step is read here, not inside the returned writer, so an asset running outside a step fails while a caller can still choose `file_writer`, rather than part-way through a write it cannot finish.

    Parameters
    ----------
    context
        The executing asset's context. It must own exactly one asset, as `dg.asset` builds; check outputs are not asset outputs and are skipped.

    Returns
    -------
    A writer taking the invalid rows and returning the quarantine's asset key, rendered.

    Raises
    ------
    dagster._core.errors.DagsterInvalidPropertyError
        There is no step, which is direct invocation. `quarantine_writer` catches this and uses `file_writer` instead.
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
        repointed = copy.copy(original)
        # The one field re-pointed. It is the private attribute behind `OutputContext.asset_key`, which has no setter. A characterization test pins it, as it does the rest of the copy.
        repointed._asset_key = key  # noqa: SLF001 - the whole of the re-point, pinned upstream
        manager.handle_output(repointed, frame)
        return key.to_user_string()

    return write


def file_writer(
    key: dg.AssetKey,
    quarantine_dir: UPath | Path | str,
    partition_key: str | None = None,
) -> QuarantineWriter:
    """Build the writer that puts the invalid rows in a parquet file under `quarantine_dir`.

    For direct invocation, where the asset is called rather than run and there is no step to borrow an output context from.

    Parquet only. The quarantine holds whatever dtypes the schema declares, and a format that cannot round-trip them would make the evidence disagree with the table it came from.

    Parameters
    ----------
    key
        The asset key of the table the rows failed validation in, not the quarantine's own. `quarantine_path` suffixes the leaf.

    Returns
    -------
    A writer taking the invalid rows and returning the file's path, rendered.
    """
    path: UPath = quarantine_path(key, quarantine_dir, partition_key)

    def write(frame: pl.DataFrame) -> str:
        # Created here, not at build time, so a writer nothing calls leaves no empty directory behind.
        path.parent.mkdir(parents=True, exist_ok=True)
        # Through an open handle, not by name: `write_parquet` takes a path only for the local filesystem, and a `UPath` may be anywhere.
        with path.open("wb") as file:
            frame.write_parquet(file)
        return str(path)

    return write


def quarantine_writer(context: dg.AssetExecutionContext) -> QuarantineWriter:
    """Build the writer that picks its own route when the invalid rows arrive.

    Delegation first. The asset's own IO manager puts the rows wherever it puts things, which needs no configuration and cannot disagree with where the valid table went (ADR-0006).

    `file_writer` answers the one case with no step: direct invocation, where the asset is called rather than run, so Dagster builds no step for `delegating_writer` to borrow the output context and IO manager from. A test that wants the real placement runs the asset.

    The step is asked for on its own, ahead of the writer. No predicate answers "is this a run", so the question has to be a call that raises. Wrapping the whole of `delegating_writer` in that guard would widen it: any other property raising the same error inside a real run would silently reroute the rows to a file.

    **The route is chosen inside the returned writer, not here.** `delegating_writer` reads its step where it is built, so a caller holding no fallback fails before a write it cannot finish. This one holds `file_writer` in reserve, so it waits for the rows instead (#115). A call whose every row is valid never asks where invalid ones would go, so it needs no quarantine_dir for rows that do not exist. And a deployment that sets `DAGSTER_DATAFRAMELY_QUARANTINE_DIR` after the module holding the asset imported is read, not ignored, which is what a test pointing the variable at a `tmp_path` does. `validation_results` calls its writer once, so the choice runs at most once either way.

    Returns
    -------
    A writer taking the invalid rows and returning where they went, rendered: the quarantine's asset key under `delegating_writer`, the file's path under `file_writer`. It raises `QuarantineDirError` when there is no manager to delegate to and no quarantine_dir to fall back on.
    """

    def write(frame: pl.DataFrame) -> str:
        try:
            context.get_step_execution_context()
        except DagsterInvalidPropertyError:
            # Not a run, so there is no step, no output context and no manager behind it.
            pass
        else:
            return delegating_writer(context)(frame)
        quarantine_dir: str | None = QUARANTINE_DIR.resolve(None)
        if quarantine_dir is None:
            raise QuarantineDirError(context.asset_key.to_user_string())
        return file_writer(
            context.asset_key,
            quarantine_dir,
            # Read behind the guard because `partition_key` raises on an unpartitioned asset rather than answering `None`.
            context.partition_key if context.has_partition_key else None,
        )(frame)

    return write


def quarantine_spec(
    schema: type[dy.Schema],
    asset: dg.AssetsDefinition | dg.AssetKey | str | Sequence[str],
    *,
    partitions_def: dg.PartitionsDefinition | None = None,
) -> dg.AssetSpec:
    """Build the spec that gives a quarantine its place in the graph.

    The package writes the quarantine whether or not this is called. This adds a node, and it never receives a materialization event, because nothing materializes it.

    The schema is passed, not read off the definition. Every other assembled part in `wiring` takes it first, and after ADR-0004 nothing on an `AssetsDefinition` carries the live class. Key, prefix and partitions still come off the definition, so the spec cannot disagree with its parent about any of them.

    Parameters
    ----------
    asset
        The asset the invalid rows came from. A definition is the normal form and supplies its own partitions. The key forms serve an asset that cannot be imported: generated or foreign.
    partitions_def
        The quarantine's partitions, for a key form. A definition already states them, so passing both raises.

    Returns
    -------
    A spec to hand to `dg.Definitions(assets=[...])`.

    Raises
    ------
    ReservedColumnError
        A user column sits inside the reserved namespace. Without the guard the rule columns of this spec's Columns tab collide with it, silently, at definition time.
    UnnameableColumnError
        A user column is spelled in characters Dagster refuses in a name.
    CheckNameCollisionError
        Two rules rewrite to the same check name, which this spec's Columns tab would also collapse.
    dg.DagsterInvariantViolationError
        A definition and a `partitions_def` were both given. Dagster's own error, not the package's, because two sources of one fact is a wiring mistake, not a data one.
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
        description=f"Invalid rows from {rendered}, with a column per rule saying why.",
        metadata=quarantine_metadata(schema),
        partitions_def=partitions_def,
    )
