"""What a quarantine is called, and where it goes when nothing else will place it.

A quarantine is normally placed by the asset's own IO manager, addressed by asset key (ADR-0006). Neither function here does that. What they share is the naming rule underneath it, `_LEAF_SUFFIX`, which is why they sit together: a spec keyed differently from the quarantine it describes is a dependency on nothing.

`build_quarantine_spec` gives a quarantine a node in the graph. `quarantine_path` is the fallback address, for the two cases delegation cannot serve: direct invocation, where there are no resources and so no manager, and a user who asked for the rows somewhere their manager would never put them.

The fallback mirrors `UPathIOManager`'s own path, so a root set to a manager's `base_dir` still lands each quarantine beside its table. Whatever upstream already spells, this imports rather than restates. A second implementation of the same rule is a second answer waiting to differ. The multi-partition spelling is the one exception, because it lives in a closure inside `_get_paths_for_partitions` and cannot be reached. It is restated here and pinned against upstream's own path by a characterization test.

**A partition key is escaped as `FilesystemIOManager` escapes it, not as the `UPathIOManager` base class does.** The base escapes a leading `/` and leaves `..` alone, which is upstream's documented advice to override on a hierarchical filesystem. A key of `../../etc/x` would otherwise resolve out of the root and write wherever it landed, and a partition key can come from data where an asset key cannot. Here that matters more than parity, because this path is one the package writes itself rather than one it hands to a manager.
"""

from collections.abc import Sequence
from pathlib import Path

import dagster as dg
import dataframely as dy

# The three escapes Dagster applies before it joins a path. None is exported from
# `dagster`; all three are covered by characterization tests (#104).
from dagster._core.storage.upath_io_manager import (
    coerce_to_relative_parts,
    escape_dotdot_segments,
    escape_leading_slash,
)
from upath import UPath

from dagster_dataframely._metadata import quarantine_metadata

#: What keeps a quarantine from overwriting the table it came from when the two share a root. Spelled once, because the file's name and the spec's key have to be the same string.
_LEAF_SUFFIX = "_quarantine"

_EXTENSION = ".parquet"


def _suffixed(parts: Sequence[str]) -> tuple[str, ...]:
    """Suffix the last part of a key, leaving the prefix alone.

    The prefix is a directory on disk and a folder in the asset graph, and neither wants renaming. Only the name has to change, because only the name collides.
    """
    return (*parts[:-1], f"{parts[-1]}{_LEAF_SUFFIX}")


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


    @dd.dataframely_asset(schema=Orders)
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
        key=dg.AssetKey(list(_suffixed(parent.path))),
        deps=[parent],
        description=f"Invalid rows from {rendered}, with a column per rule saying why.",
        metadata=quarantine_metadata(schema),
        partitions_def=partitions_def,
    )
