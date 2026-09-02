"""PROTOTYPE. The portable half: write a quarantine through the asset's own IO manager.

The question this answers
------------------------
Can a `dy_asset` hand its invalid rows to whatever IO manager the asset is already
bound to, with no code that knows which manager that is, so the quarantine lands
natively beside the valid table: a parquet file next to a parquet file, a DuckDB
table next to a DuckDB table? And does it still land when the run aborts?

The trick
---------
The quarantine's address is its asset key. Every asset-key-addressed manager already
turns a key into its own location, so nothing here needs a path rule or a backend
adapter. We borrow the step's real `OutputContext`, which already carries the correct
`resource_config`, `dagster_type` and `instance`, and re-point it at the quarantine's
key. Copying fields is not interpreting them, which is what keeps this manager-blind.

Everything reached for below is private Dagster API. That is a deliberate house call
(see the characterization tests in `tests/test_upstream_characterization.py`), not an
oversight.
"""

import dagster as dg
from dagster._core.execution.plan.outputs import StepOutputHandle

_LEAF_SUFFIX = "_quarantine"


def quarantine_key(key: dg.AssetKey) -> dg.AssetKey:
    """Suffix the last part of an asset key, leaving the prefix alone.

    The prefix is a directory on a filesystem and a schema in a database, and neither
    wants renaming. Only the name has to change, because only the name collides.
    """
    return dg.AssetKey([*key.path[:-1], f"{key.path[-1]}{_LEAF_SUFFIX}"])


def borrowed_output_context(
    context: dg.AssetExecutionContext, key: dg.AssetKey
) -> dg.OutputContext:
    """Clone the step's own output context, pointed at `key`.

    Every field copied here is one some manager reads and none this code understands.
    `resource_config` is the load-bearing one: `DbIOManager` backends read the database
    and connection settings off it at write time rather than off themselves, which is
    why reconstructing it by hand needed per-manager knowledge and borrowing it does not.
    """
    step = context.get_step_execution_context()
    (output_name,) = [output.name for output in step.step.step_outputs]
    original = step.get_output_context(StepOutputHandle(step.step.key, output_name))
    return dg.build_output_context(
        asset_key=key,
        partition_key=context.partition_key if context.has_partition_key else None,
        asset_partitions_def=original.asset_partitions_def
        if context.has_partition_key
        else None,
        dagster_type=original.dagster_type,
        definition_metadata=dict(original.definition_metadata or {}),
        resource_config=original.resource_config,
        instance=context.instance,
    )


def write_quarantine(context: dg.AssetExecutionContext, frame: object) -> dg.AssetKey:
    """Put the invalid rows wherever the asset's own manager puts things.

    Returns
    -------
    The key the rows were written under, for the materialization metadata to name.
    """
    key = quarantine_key(context.asset_key)
    context.resources.io_manager.handle_output(borrowed_output_context(context, key), frame)
    return key
