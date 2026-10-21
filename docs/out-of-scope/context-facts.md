# Reading More Arguments From the Context

Three kinds of `dd.wiring` argument stay arguments, although Dagster's execution context holds the value or one close to it:

- `error_rows_key` and `error_rows_io_manager_key`, in `validate_error_rows_key`, `build_io_manager_writer` and `build_file_writer`.
- `asset_key=`, in `evaluate_checks`.
- `check_granularity=`, in `stream_results` and `evaluate_checks`.

`stream_results` reads the asset key and the partition state from the context since #160, and `build_file_writer` reads the partition since #161.

## Why this is out of scope

Proposed as [#159](https://github.com/ozanozbeker/dagster-dataframely/issues/159), grilled on 2026-10-21, and rejected for these three.
The rule from #160 is that the package reads a fact from the context when an argument that repeats it can disagree with the run and nothing checks it.
None of the three meets that rule.
Measured on dagster 1.13.6, dataframely 3.0.0, polars 1.44.2.

### The context does not hold the error-rows key

The context holds the asset key, and `validate_error_rows_key` already reads it.
The error-rows key is the package's default for that asset key, or an override.

`dd.asset` records the key in its definition metadata, under an entry this package keeps private, and `build_error_rows_spec` reads it back.
The decorator's closure already holds the resolved key, so reading it from `context.assets_def.metadata_by_key` would change nothing there.

A hand-wired asset has no such entry.
From its context, the package could derive only the default, `get_error_rows_key(context.asset_key)`.
An override would still need an argument.
An optional argument would let the writer and the collision check read different keys, and #154 made the key required to prevent that.

`error_rows_io_manager_key` is recorded the same way, and stays an argument for the same reason.

### Dagster already checks a check result's asset key

A check result for an asset that no check in the step targets fails the step:

```text
DagsterInvariantViolationError: Received unexpected AssetCheckResult. It targets asset 'other'
which is not targeted by any of the checks currently being evaluated. Targeted assets: ['t'].
```

So a wrong `asset_key=` passed to `evaluate_checks` fails the run.

Reading the key from the context would also cost two things.
A `dg.multi_asset_check` over two assets calls `evaluate_checks` once for each, and its `context.check_specs` names both, so the function would need a rule to choose one.
An unbound `dg.build_asset_check_context()` raises `DagsterInvalidPropertyError` on `check_specs`.
So a direct call, such as the docstring example or a call in `tests/test_severity_runs.py`, would need a `dg.multi_asset_check` around it to bind the context.

### Dagster already checks a check result's name

A check result whose name the step does not declare fails the step:

```text
DagsterInvariantViolationError: Received unexpected AssetCheckResult. No checks currently being
evaluated target asset 't' and have name 'dy_col__x'. Checks being evaluated for this asset: {'a'}
```

So a `check_granularity` that disagrees with the declared specs fails the run.
That includes a hand-wired asset that passes `None` to both functions, in two processes whose `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY` differs.

The declared specs are in `context.assets_def.check_specs`, and a `dy_col__` name means `column`.
Inferring the setting from that prefix would make the check names a second record of it.
A schema whose rules are all schema-level has the same check names at both granularities, so the inference would also need a default.
`evaluate_checks` takes no context, so only `stream_results` could infer the setting, and the two functions would take it in two ways.
[Rule sets fixed at definition time](rule-sets-fixed-at-definition-time.md) records an earlier proposal that kept the parameter too.

### Easier hand-wiring is not a goal

`dd.asset` and `dd.build_rule_checks` resolve each of these values once and pass the same value to every call, so their users see no change.
Only someone hand-wiring an asset would pass fewer arguments.
[ADR-0001](../pre-1.0.md#adr-0001-validation_results-takes-asset-keys) records that hand-wiring does not change this package's design.

## If someone proposes this again

Two changes would reopen it.

The first is Dagster accepting a check result that matches no check in the step.
That reopens `asset_key=` and `check_granularity=`, because a wrong value would then pass.

The second is a public way for a hand-wired asset to record its error-rows key on its definition, such as an exported metadata builder.
That reopens `error_rows_key`, because the context would then hold the key for every asset.

## Prior requests

- [#159](https://github.com/ozanozbeker/dagster-dataframely/issues/159), "Read facts from Dagster's context instead of asking callers to pass them".
