# 6. The quarantine is written by the asset's own IO manager

Accepted, 2026-09-02. Amends [ADR-0004](0004-the-quarantine-is-a-file-not-an-asset.md), which still decides that the quarantine is not an asset and that this package ships no IO manager.

## Context

ADR-0004 made the quarantine a parquet file under a root the user configures, and made the root's default the deployment setting `DAGSTER_DATAFRAMELY_QUARANTINE_DIR`. Adjacency, the file sitting beside the table it came from, was left to the user setting that root equal to their IO manager's `base_dir`.

Two things showed up while building it.

**Adjacency by configuration is adjacency nobody checks.** Point the root somewhere else and the quarantine still lands, just not beside the data, and nothing says so.

**A warehouse has no `base_dir` to equal.** A DuckDB, Snowflake or BigQuery shop stores tables, not files. For them the setting cannot express adjacency at all, so the invalid rows leave the warehouse and become a parquet file somewhere else. That is a regression from the sibling asset ADR-0004 replaced, which put them in a second table.

The rejected alternative in ADR-0004, "the IO manager writes the quarantine", was argued against *owning* a manager. It was never tried against *borrowing* the user's.

## Decision

**The quarantine is written through whatever IO manager the asset is already bound to, addressed by its own asset key.**

`quarantine_key` suffixes the last part of the asset's key, leaving the prefix alone: `analytics/orders` becomes `analytics/orders_quarantine`. The package then hands that key, and the invalid rows, to the manager the step was already going to write through.

Nothing in the package knows which manager that is. The quarantine's address is its asset key, and every asset-key-addressed manager already turns a key into its own location: `UPathIOManager` into a path, `DbIOManager` into `<schema>.<table>` off `asset_key.path[-1]`. So one suffix rule places the quarantine natively on every backend, and the partition layout comes along with it.

**The output context is cloned, not built.** `DbIOManager` backends read the database and connection settings off the output context at write time rather than off themselves, so a context assembled by hand needs a list of which fields matter, which is the per-manager knowledge this decision exists to avoid. Instead the step's real `OutputContext` is shallow-copied and its asset key re-pointed. Carrying a field is not interpreting it, and that is what keeps the write manager-blind.

**The manager is borrowed off the same step**, not read off `context.resources`. Amended while building (#106). The prototype reached for `context.resources.io_manager`, which needs the asset to declare `required_resource_keys`, and Dagster validates that at bind time: every direct invocation would then have to supply a manager it has no use for, which breaks the fallback this decision depends on. `StepExecutionContext.get_io_manager` answers off the step output handle already in hand, so the asset's own `io_manager_key` is followed without anything in the package learning what it is.

**`quarantine` becomes a bool, and nothing else.** ADR-0004 folded the flag and the location into `bool | str | Path` because `True` had to resolve to a root, and a `True` that resolved to nothing was an error invisible at the call site. That objection is gone: `True` now means "wherever this asset's manager puts things", which always resolves.

There is no location override. Three cases would need one, and they are recorded here so the design survives without the surface:

1. Same manager, a different key.
2. A different manager, named by its resource key.
3. No manager at all, a root plus the path rule.

None of them has a user asking for it, and a root is meaningless to a warehouse, so the three are not one filesystem case plus extras: they are two spellings of *where* and one of *who*. Spelling that wrong is a rename, and pre-1.0 a rename costs more than the wait. `QUARANTINE_DIR` still supplies case 3 for direct invocation, which is the only one the package needs to function.

**`quarantine_path` survives as case 3.** It keeps the `UPathIOManager` layout it already mirrors, and it is what direct invocation uses when no resources were supplied.

## Evidence

`prototypes/quarantine_sink/` ran twelve combinations green on dagster 1.13.20: `PolarsParquetIOManager` and `DuckDBPolarsIOManager`, partitioned and not, across the three exits that reject rows.

The abort case holds. `handle_output` is called inside the asset body before anything raises, so a run that dies still leaves the rows where a reader can open them, which is what ADR-0004 promised and the reason the file exists at all.

The two managers tested are one from each base class Dagster ships. Between them `UPathIOManager` and `DbIOManager` cover nearly every first-party manager, so this is not support for two integrations.

The matrix lifted into `tests/test_quarantine.py` when #106 landed, minus the artificial abort: a decorated asset runs its body before `process`, so the only exit that raises after writing is nothing-survived, and that is what proves the write happens first.

## Consequences

**Nothing reads `context.resources`, which ADR-0002 and ADR-0004 refused.** The refusal bought a decorated asset that is callable in a test with no resources, and borrowing the manager off the step keeps it whole: an asset declares no resource key, so a call binds with none. A call still has no step, so it falls back to case 3, the root and the path rule. A test that wants the real placement runs the asset, which is what testing placement means.

**A quarantined asset declares a `context` parameter** whether or not the decorated function asked for one, because the writer is built from the execution context and a wrapper cannot ask Dagster for a parameter it did not declare. Calling one therefore takes a `dg.build_asset_context()`. An asset without a quarantine keeps exactly the signature it was written with, so the common path is unchanged.

**Seven private Dagster APIs.** Four reach the step: `get_step_execution_context`, `StepOutputHandle`, `get_output_context` and `get_io_manager`. One re-points the clone: `OutputContext._asset_key`, which has no setter. Two serve the context parameter: `is_context_provided`, upstream's own rule for whether a decorated function asked for a context, and `DagsterInvalidPropertyError`, which is the whole of the signal that a call is not a run.

All seven are pinned by characterization tests, as `_naming` pins Dataframely's. An import-time failure here breaks the whole code location, not just the quarantine, which is the loud failure and the right one.

**Metadata the manager emits is dropped.** `add_output_metadata` rebinds the mapping on the object it is called on, and that object is the clone, which the step never reads. So the manager's own `path` or `Query` goes nowhere, and it cannot displace what the real output reports either. The package emits the quarantine's address itself, so a reader loses nothing, but the two are computed separately and could disagree.

**A partitioned asset on a database manager needs `partition_expr`.** `DbIOManager` raises without it. The user already declares it for their own partitioned table, and it is forwarded automatically because the borrowed context carries definition metadata.

**A quarantine spec gets simpler.** `build_quarantine_spec` (ADR-0004) keyed a spec that a downstream asset then had to route to a second IO manager. Written through the asset's own manager, the spec's key resolves through that same manager with no routing at all.

## Alternatives rejected

**Per-manager adapters**, one each for `dagster-polars` and `dagster-duckdb-polars`, reading `base_dir` or `database` and placing the quarantine ourselves. Rejected: the prototype needed no manager-specific code at all, so adapters would buy a support matrix and nothing else.

**Ship our own IO manager.** Rejected again, on ADR-0004's reasoning plus one more: it only helps users who bind it, and the warehouse case that motivated this decision is exactly the one it cannot serve.

**Keep the file as the only mechanism.** Rejected because it cannot put a warehouse's invalid rows in the warehouse, which is where that user will look for them.
