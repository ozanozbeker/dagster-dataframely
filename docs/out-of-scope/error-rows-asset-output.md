# The Error Rows as a Second Asset Output

With `filter=True`, `dd.asset` still builds one `dg.asset`.
It writes the error rows from inside the step through `delegating_writer`, not as a second `dg.AssetOut` of a `dg.multi_asset`.
`error_rows_spec` adds the error rows to the asset graph when the user wants them there.

## Why this is out of scope

[#132](https://github.com/ozanozbeker/dagster-dataframely/issues/132) proposed the second output on 2026-10-18.
It would remove `delegating_writer`, `file_writer`, `error_rows_writer`, `validate_error_rows_key`, `writer_metadata`, `error_rows_spec`, `ERROR_ROWS_DIR`, `ErrorRowsDirError`, `ErrorRowsKeyCollisionError` and the injected `context` parameter.
With them would go every private Dagster import except `CoercibleToAssetDep`.
ADR-0003 and ADR-0004 in [the pre-1.0 record](../pre-1.0.md#adr-0004-the-quarantine-is-a-file-not-an-asset) had already moved the error rows out of a second output, and #132 reopened them with facts the record did not test.

The maintainer rejected it on 2026-10-18.
The costs below affect the end user, and the gains affect the maintainer, and `CLAUDE.md` ranks the end user first.

The probes ran on dagster 1.13.24 and dagster-polars 0.27.12, with `PolarsParquetIOManager`.

### The function's own `context` calls raise

A `dg.multi_asset` with two asset outputs changes what the decorated function can call on `context`.
`context.asset_key` raises `DagsterInvariantViolationError`.
`context.add_asset_metadata(...)` without `asset_key=` raises it too.
"What a run produces" recommends `add_asset_metadata`, and it works on a plain `@dg.asset`.
So setting `filter=True` would break a function body that works with `filter=False`.

### The definition has two keys

`AssetsDefinition.key` raises `CheckError` on a definition with two asset keys, so `orders.key` fails.
`@dg.asset(deps=[orders])` makes the downstream asset depend on the table and on the error rows.

### The table cannot run alone

A selection of the table alone, such as `dg.define_asset_job("j", selection=["orders"])`, raises `DagsterInvalidDefinitionError` at load, because the definition does not support subsetting.
The user would need `AssetSelection.required_multi_asset_neighbors()`.
`can_subset=True` does not help.
A run that selects only the table cannot yield the error rows, so it would lose the rows that `filter=True` removes from the table.

### Every `filter=True` asset gets a node

A second output always puts the error rows in the asset graph.
Today `error_rows_spec` makes that the user's choice.

### `dd.asset` stops being one `dg.asset` call

ADR-0005 has `dd.asset` call `dg.asset` and forward every parameter it does not set.
With a second output, `filter` would choose between `dg.asset` and `dg.multi_asset`, and the parameters would split between `dg.AssetOut` and `dg.multi_asset`.

## What the probes settled

The lineage is not an obstacle. [#132](https://github.com/ozanozbeker/dagster-dataframely/issues/132) stated that an edge from the table to the error rows forces the step to yield the table first, so the `NoValidRowsError` outcome could not write the error rows.
That is wrong.
Dagster raises only when a step yields an asset after a child of that asset (`dagster/_core/execution/plan/execute_step.py`).
It does not require the step to yield the parent at all.
With `internal_asset_deps={"orders__error_rows": {orders}}`, a step that yielded only the error rows and then raised wrote them and recorded their materialization.
So a second output can have the lineage ADR-0003 chose: the error rows as a child of the table.

An output yielded before a later `raise` is still written, so a run that fails keeps its error rows ([#133](https://github.com/ozanozbeker/dagster-dataframely/issues/133)).
A direct invocation yields both frames, with no writer and no environment variable.

[#131](https://github.com/ozanozbeker/dagster-dataframely/issues/131) gives the error rows a materialization event from the current writer instead.

## If someone proposes this again

Re-run the probes if the dagster floor moves past 1.13.
Reconsider the design if Dagster resolves `context.asset_key`, `context.add_asset_metadata` and `AssetsDefinition.key` to one chosen output of a multi-asset.
Reconsider it also if a Dagster release breaks the private names that `tests/test_upstream_characterization.py` pins for the writer, and a fix costs more than the parity listed above.

## Prior requests

- [#132](https://github.com/ozanozbeker/dagster-dataframely/issues/132), "`filter=True` rebuilds a second asset output outside Dagster's output path".
