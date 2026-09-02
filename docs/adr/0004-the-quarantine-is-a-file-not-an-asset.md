# 4. The quarantine is a file, not an asset

Accepted, 2026-09-02. Supersedes [ADR-0003](0003-the-quarantines-only-parent-is-the-valid-asset.md).
Amended by [ADR-0006](0006-the-quarantine-is-written-by-the-assets-own-io-manager.md): the asset's own IO manager places the quarantine, so the root stops being the mechanism and survives only as the fallback for direct invocation. Everything else here stands.

## Context

Two weeks of daily use showed the sibling quarantine asset costs more than it returns. Every declaration is a `@dg.multi_asset` with two outs, `internal_asset_deps`, a rebuilt `dg.AssetOut`, and a second definition build to read the quarantine key back (ADR-0002, ADR-0003). The reader gets a second node in the graph that nothing ever consumes.

The rows still matter. A run that rejects rows has to leave them somewhere a person can open, with the rule columns that say why.

## Decision

**Invalid rows are written to a parquet file by the package itself.** The decorator builds one `dg.asset`, and `quarantine` is a declaration on it, `bool | str | Path`, default `False`. `True` takes the root from `DAGSTER_DATAFRAMELY_QUARANTINE_DIR` and fails at definition time when it is unset. A string or path is a root for that asset alone.

The path under the root mirrors `UPathIOManager`, with one forced difference in the leaf:

```text
<root>/<key part>/.../<name>_quarantine.parquet
<root>/<key part>/.../<name>_quarantine/<partition>.parquet
```

The leaf carries `_quarantine` so a root equal to the IO manager's `base_dir` puts the file beside the valid output instead of over it. Multi-partition keys are spelled as `UPathIOManager` spells them, dimension keys joined by `/` in dimension-name order.

**The file is written on every exit that rejected rows, aborts included.** As an out it was skipped on an abort because the out was never yielded. As a file it costs nothing structural, and the abort is when a reader most wants the rows.

**The valid materialization carries the quarantine's facts**: `dy_quarantine_path`, `dy_rejected_count`, `dy_rejected_rules`, `dy_rejected_sample`. On an abort there is no materialization, so the path rides on the check metadata.

**Graph presence is the user's declaration.** `build_quarantine_spec(asset | key, *, partitions_def=None)` returns a `dg.AssetSpec` keyed `<prefix>/<name>_quarantine` with `deps=[asset]`. With the root equal to `base_dir`, any `UPathIOManager` loads it for a downstream asset with no help from this package. It never receives a materialization event, which is honest: nothing materializes it.

**Package IO managers are removed.** The only reader of the schema carrier was the CSV manager, and dataframely v3 dropped its own I/O for the same reason this package now does: a second storage authority drifts from the first. `schema_metadata` shrinks to `dagster/column_schema`.

## Consequences

`process` takes `quarantine_root` and `partition_key` instead of `quarantine_key`. It stays context-free (ADR-0001); the decorator reads `partition_key` off the context, which is run state and not definition state, and direct invocation passes it through `build_asset_context(partition_key=...)`.

The failure tree is unchanged. Six exits, one severity per run derived from the exit, nothing-survived aborts with or without a quarantine.

Severity is a label, never a control. Dagster 1.13.20 reads it in two places: a failing check halts downstream only when `severity == ERROR and blocking`, and asset health buckets `WARN` failures apart from `ERROR` ones. Only the shape check is blocking, and it raises anyway.

Direct invocation with `quarantine=True` writes a real file. A test sets the env var to `tmp_path` or declares `quarantine=False`.

`_io_managers.py`, `_csv_codecs.py`, `_carrier.py`, both IO manager factories and their tests are deleted. The work is inventoried in an issue for a `dagster-polars` contribution.

## Alternatives rejected

**Keep the sibling asset.** The status quo. Rejected because the cost is paid on every declaration and the benefit, a graph node, was not used.

**The IO manager writes the quarantine.** The decorator hands the manager a value carrying both frames; the manager writes the sibling from its own path logic. Rejected because it only works with a manager this package owns, so quarantine would require our IO manager, and the package would stay in the storage business.

**Derive the root from the bound IO manager, falling back to a setting.** The file lands beside the output when the manager is a `UPathIOManager`. Rejected because `_get_path` and `_base_path` are private, the wrapper would read `context.resources` (breaking ADR-0002's direct invocation), and the path would follow different rules per environment. Equal roots by configuration give the same result with one rule.

**A `QuarantineSink` protocol with a fallback.** Same objections one layer down, plus a protocol with one implementer. Recorded as future work for a warehouse manager that wants an adjacent table.

**A `quarantine: bool` plus a separate `quarantine_dir` setting.** Rejected because `True` with no directory is an error the user cannot see from the declaration. Folding the root into the declaration makes the per-asset override and the deployment default one parameter.

**CSV as a second quarantine format.** Rejected: the file is evidence with boolean rule columns, and CSV loses dtypes. Parquet only.
