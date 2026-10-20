# Changelog

Pre-1.0, so a `0.x` minor release is where a breaking change lands.
Each release below carries the migration, not just the list.

## 0.11.0 - unreleased

### Breaking in 0.11

**`schema_rules` is gone, and a schema-level rule keeps its own check at `column` granularity.**
`column` collapses the rules Dataframely generates on a column into `dy_col__<column>`.
A `@dy.rule()` or the primary key is written by you, so it now keeps its own `dy_rule__<rule>` check and its own history, as a dbt test does in dagster-dbt.
This is what `schema_rules="per_rule"` did.
`dy_schema__rules` is no longer declared, so its history ends with the last run before the upgrade.
Remove `schema_rules=`, `dd.SchemaRules` and `DAGSTER_DATAFRAMELY_SCHEMA_RULES`; nothing replaces them.
[`docs/out-of-scope/schema-rules.md`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/out-of-scope/schema-rules.md) has the reasons.

## 0.10.0 - 2026-10-18

### Breaking in 0.10

**A schema declares each rule's severity, and `filter=True` replaces `quarantine=True`.**
A rule is `"error"` unless the schema declares `"warn"`, so a schema that declares nothing validates as before.
A row that fails only `"warn"` rules is written to the table, and its check fails at `WARN`.
With `filter=True`, the package writes the rows that failed an `"error"` rule under `<name>__error_rows`.
[Rule severity](https://ozanozbeker.com/dagster-dataframely/user-guide/rule-severity.html) shows where a severity goes.

**The error rows have a new asset key.**
A run no longer writes to `<name>_quarantine`, and nothing removes the rows an earlier run wrote there.
To keep the old key, pass it as `error_rows_key`, such as `error_rows_key=["sales", "orders_quarantine"]`.

**A check for an `"error"` rule fails at `ERROR` with `filter=True`.**
In 0.9 the same check failed at `WARN` when the quarantine took the rows.
Downstream assets still run, because with `filter=True` no check is blocking.
With `filter=False`, a check for a `"warn"` rule is not blocking, where in 0.9 every check was.

**`dd.wiring.asset_check_results` no longer takes `severity`.**
It reads each rule's severity from the schema.

The check names are unchanged, so check history carries over.
Every other `quarantine` name becomes `error_rows`:

| 0.9 | 0.10 |
| --- | --- |
| `@dd.asset(Orders, quarantine=True)` | `@dd.asset(Orders, filter=True)` |
| `<name>_quarantine` | `<name>__error_rows` |
| `dd.quarantine_spec` | `dd.error_rows_spec` |
| `DAGSTER_DATAFRAMELY_QUARANTINE_DIR` | `DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR` |
| `dd.errors.QuarantineDirError` | `dd.errors.ErrorRowsDirError` |
| `dd.errors.QuarantineKeyCollisionError` | `dd.errors.ErrorRowsKeyCollisionError` |
| `dd.wiring.QuarantineWriter` | `dd.wiring.ErrorRowsWriter` |
| `dd.wiring.quarantine_frame` | `dd.wiring.error_rows_frame` |
| `dd.wiring.quarantine_path(..., quarantine_dir=...)` | `dd.wiring.error_rows_path(..., error_rows_dir=...)` |
| `dd.wiring.file_writer(..., quarantine_dir=...)` | `dd.wiring.file_writer(..., error_rows_dir=...)` |
| `dd.wiring.validation_results(..., quarantine_writer=...)` | `dd.wiring.validation_results(..., error_rows_writer=...)` |
| `dd.wiring.validate_quarantine_key` | `dd.wiring.validate_error_rows_key` |
| `dd.wiring.check_specs` | `dd.wiring.asset_check_specs` |
| `dd.wiring.check_results` | `dd.wiring.asset_check_results` |
| `dd.wiring.check_name` | `dd.wiring.asset_check_name` |
| `dataframely/quarantine_address` | `dataframely/error_rows_address` |
| `dataframely/invalid_count` | `dataframely/error_rows_count` |
| `dataframely/invalid_sample` | `dataframely/error_rows_sample` |
| `dataframely/invalid_by_rules` | `dataframely/error_rows_by_rules` |
| `dataframely/valid_sample` | `dataframely/table_sample` |
| `dataframely/valid_statistics/<group>` | `dataframely/table_statistics/<group>` |

### Added in 0.10

`dd.collection_checks(collection, members=...)` checks a Collection's filters over the tables its member assets wrote.
It returns one asset check for each filter on each member, named `dy_filter__<name>`.
The check reports a failing row and never removes it.
`InvalidMembersError` and `FailurePropagationError` are new with it.

`dd.testing.direct_invocation_result` reads what a direct call of an asset built with `dd.asset` returns.
A test gets the materialization and each check result by check name, with no cast.

`dd.asset` takes `error_rows_key` and `error_rows_io_manager_key`, to write the error rows under another key or through another IO manager.
`error_rows_spec` reads both from the definition.

A partitioned asset's checks have the asset's partitions, so each check evaluation records its partition.

`error_rows_spec` puts the error rows in the asset's group, so the catalog lists them beside the asset.
`group_name=` sets another group.

### Fixed in 0.10

A run with no error rows writes the error rows empty.
Before, they kept the last failing run's rows while the materialization reported a clean run.
A direct call skips the empty write, so a clean call still needs no `DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR`.

`error_rows_spec` copies the asset's definition metadata.
Before, a downstream load of the error rows failed for an asset with a non-default `io_manager_key`, or with `schema` or `partition_expr` metadata on a database IO manager.

### Documentation in 0.10

The guide has new pages for validation, rule severity, `filter=True` and Collection checks.
The site has a logo and the Nord palette, and every screenshot uses the light theme.

## 0.9.0 - 2026-09-22

### Breaking in 0.9

The floors move to `dagster>=1.13.24`, `dataframely>=3.1.2` and `polars>=1.44.2`.
The tests and every measurement in this repo run against those versions.

Two error classes are renamed.
Each is raised in the same cases as before.

| 0.8 | 0.9 |
| --- | --- |
| `dd.errors.NothingSurvivedError` | `dd.errors.NoValidRowsError` |
| `dd.errors.UnnameableColumnError` | `dd.errors.InvalidColumnNameError` |

Most error messages are reworded, so code that matches on message text needs updating.

### Documentation in 0.9

The documentation is a site now: <https://ozanozbeker.com/dagster-dataframely>.
The guide is one page per topic, and every code cell runs when the site builds, so an example that stops being true fails the build.
`USER_GUIDE.md` is gone.
A demo code location backs the examples, and a prek hook keeps the README's code blocks in sync with the demo's modules.

Every page, docstring, comment and error message is rewritten in plain language.
The package used to explain itself with metaphors that no upstream library uses, and it now says what the code does in Dagster's, Dataframely's and Polars' own words.
Nothing behaves differently.

The decisions and the measurements the package was built on are in one file, [`docs/pre-1.0.md`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/pre-1.0.md), which replaces `docs/adr/` and `docs/research/`.
Every claim in it was re-checked against the code, and a claim the code no longer supports was deleted rather than corrected.
`ARCHITECTURE.md` is gone, and its diagrams are in the guide.

## 0.8.0 - 2026-09-14

### Breaking in 0.8

`temp_dir` and `DAGSTER_DATAFRAMELY_TEMP_DIR` are gone.
A `LazyFrame` return is filtered in the engine, so there is no staging file to place: drop the argument and the variable, and nothing else changes.
The measurements are in [`docs/pre-1.0.md`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/pre-1.0.md#lazy-validation-and-lazy-storage).

Two functions are renamed, and nothing about either changes but the name.
A function here is named after what it returns, which [`CLAUDE.md`](CLAUDE.md) now writes down, and these two were the public exceptions.

| 0.7 | 0.8 |
| --- | --- |
| `dd.build_quarantine_spec` | `dd.quarantine_spec` |
| `dd.wiring.process` | `dd.wiring.validation_results` |

**The decorator is `dd.asset`.**
`dy_asset` put a user-typed name inside `dy_`, which otherwise names only what this package generates into Dagster: the check names, the rule columns and the check metadata keys.
Dagster owns the word `asset`, and Dataframely settles the same question the same way, shadowing 28 Polars names under its own alias.

| 0.7 | 0.8 |
| --- | --- |
| `@dd.dy_asset(Orders)` | `@dd.asset(Orders)` |
| `from dagster_dataframely import dy_asset` | `import dagster_dataframely as dd` |

Import the module, not the name.
A bare `from dagster_dataframely import asset` reads as Dagster's own, and the docs never show it.

A terminology pass renamed what this package had invented where Dagster, Dataframely or Polars already owned the word.
Nothing behaves differently, and no check history moves.

| 0.7 | 0.8 | the word it borrows |
| --- | --- | --- |
| `multi_column_rules=` | `schema_rules=` | Dataframely calls these schema-level rules, and a single-column `primary_key` is one, so "multi-column" was false |
| `dd.MultiColumnRules` | `dd.SchemaRules` | |
| `DAGSTER_DATAFRAMELY_MULTI_COLUMN_RULES` | `DAGSTER_DATAFRAMELY_SCHEMA_RULES` | |
| `schema_rules="schema"` | `schema_rules="collapsed"` | the old value repeated the setting's own name |
| `dataframely/valid_stats/<family>` | `dataframely/valid_statistics/<group>` | Polars' `DataTypeGroup` |

### Added in 0.8

`dd.wiring.check_results` answers every check `check_specs` declares, for an asset that writes its own storage.
It is `validation_results` without the write, so it never raises `ValidationAbortError` or `NothingSurvivedError`, and it takes `severity` rather than deriving one.

A run of a quarantined asset now fails with `QuarantineKeyCollisionError` before its body when another asset in the code location already materializes `<name>_quarantine` (ADR-0007).
Declaring `quarantine=True` beside such an asset used to lose the invalid rows in silence.

`ReservedColumnError` and `CheckNameCollisionError` now come from every public function that takes a schema, not only from `check_specs` (ADR-0008).

`UnnameableColumnError` joins them, for a column spelled in anything but `A-Za-z0-9_`.
A column comes by such a name through `dy.Column(alias=...)`, which Dataframely offers for a name that is not a Python identifier, so the fix is to rename the alias.
An alias holding Dataframely's own `|` delimiter used to surface as `KeyError` on a column nobody declared.

### Fixed in 0.8

`DAGSTER_DATAFRAMELY_QUARANTINE_DIR` is read where the invalid rows are written rather than where the asset is declared.
A deployment that sets it after the module imported is now read, and a call holding nothing back needs no directory at all.

`InvalidSettingError` no longer sends you to a `quarantine_dir=` argument that does not exist.
A setting with no argument now names the two sources it does resolve through, and says outright that there is no third.

### Documentation in 0.8

The README is a landing page and a quick start. [the user guide](https://ozanozbeker.com/dagster-dataframely/) is everything else, and this file is the upgrade log.

`dataframely/invalid_by_rules` names each rule the way the quarantine's own columns name it, at every granularity, and the guide used to say it names them the way the check list does.
Those agree only at `rule` granularity, so a reader at `column` or `schema` was being sent to a check name that does not exist.

## 0.7.0 - 2026-09-02

### Breaking in 0.7

The quarantine moves out of the graph and onto your own IO manager, and this package stops shipping storage.
Both are decisions: [ADR-0004](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/pre-1.0.md#adr-0004-the-quarantine-is-a-file-not-an-asset) and [ADR-0006](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/pre-1.0.md#adr-0006-the-assets-own-io-manager-writes-the-quarantine).

| 0.6 | 0.7 | note |
| --- | --- | --- |
| `dd.dataframely_asset(schema=Orders)` | `dd.dy_asset(Orders)` | the schema is positional; renamed again in 0.8 |
| `quarantine=dg.AssetOut()` | `quarantine=True` | it is a `bool`, and `True` needs no configuration |
| `dd.DataframelyParquetIOManager` | `dagster_polars.PolarsParquetIOManager` | this package ships no IO manager |
| `dd.DataframelyCSVIOManager` | none | the CSV codecs went with it; use parquet or a warehouse |
| the quarantine was a second out | `dd.build_quarantine_spec(Orders, orders)` | and only if you want it in the graph; renamed again in 0.8 |
| `dd.errors.SchemaShapeError` | `dd.errors.ColumnSchemaError` | "shape" was Polars' word for something else |
| `dd.errors.QuarantineSettingError` | none | nothing on the quarantine is configurable now |
| `dd.errors.UnwritableDtypeError` | none | it belonged to the CSV writer |
| `dd.wiring.quarantine_table_schema` | none | a quarantine spec carries its own Columns tab |
| `dd.wiring.process(..., quarantine_key=...)` | `dd.wiring.process(..., quarantine_writer=...)` | see [Hand-wiring](https://ozanozbeker.com/dagster-dataframely/user-guide/hand-wiring.html); renamed again in 0.8 |
| `dy_schema__dtypes` | `dy_schema__columns` | this orphans that check's history |
| `sample` | `dataframely/valid_sample` | |
| `stats/<family>` | `dataframely/valid_stats/<family>` | renamed again in 0.8 |
| `dy_quarantine_path` | `dataframely/quarantine_address` | it can be an asset key now, not just a file path |
| `dy_rejected_count` | `dataframely/invalid_count` | |
| `dy_rejected_sample` | `dataframely/invalid_sample` | |
| `dy_rejected_rules` | `dataframely/invalid_by_rules` | |

`dy_failed_count` and `dy_failed_sample` are unchanged.
So are `check_granularity`, `multi_column_rules`, `statistics`, `max_failure_samples` and `row_sample`, and every asset key and rule column, so your check history survives everything except the column-schema rename.

### Changed in 0.7

Two behaviours moved with the quarantine:

- **Invalid rows are written on every outcome that has them, aborts included.**
  As a second out they were skipped on an abort, because the out was never yielded.
- **`quarantine=True` adds a `context` parameter** to the asset, so a call takes a `dg.build_asset_context()` first.
  An asset without a quarantine is unchanged.
