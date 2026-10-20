# Changelog

Pre-1.0, so a `0.x` minor release is where a breaking change lands.
Each release below carries the migration, not just the list.

## 0.11.0 - 2026-10-20

### Breaking in 0.11

**Every function that returns something is named with a verb.**
A function is `build_*` when it returns a Dagster object, `get_*` when it derives a value from a schema, as dagster-dbt's `DagsterDbtTranslator` does, and `evaluate_*` when it reads data and reports pass or fail.
`stream_results` takes dagster-dbt's verb for a function a hand-wired body uses with `yield from`.
Nothing behaves differently, and no check name or check history moves.

| 0.10 | 0.11 |
| --- | --- |
| `dd.collection_checks` | `dd.build_collection_checks` |
| `dd.error_rows_spec` | `dd.build_error_rows_spec` |
| `dd.testing.direct_invocation_result(called)` | `dd.testing.DirectInvocationResult.from_invocation(called)` |
| `dd.wiring.validation_results(schema, frame, valid_key=...)` | `dd.wiring.stream_results(schema, frame, asset_key=...)` |
| `dd.wiring.AssetYield` | gone: write `Iterator[dg.MaterializeResult | dg.AssetCheckResult]` |
| `dd.wiring.asset_check_specs` | `dd.wiring.build_asset_check_specs` |
| `dd.wiring.asset_check_results` | `dd.wiring.evaluate_checks` |
| `dd.wiring.asset_check_name` | `dd.wiring.get_check_name_for_rule` |
| `dd.wiring.schema_code_version` | `dd.wiring.get_code_version` |
| `dd.wiring.schema_metadata` | `dd.wiring.get_column_schema_metadata` |
| `dd.wiring.table_schema` | `dd.wiring.get_table_schema` |
| `dd.wiring.error_rows_frame` | `dd.wiring.get_error_rows` |
| `dd.wiring.error_rows_path` | `dd.wiring.get_error_rows_path` |
| `dd.wiring.delegating_writer` | `dd.wiring.build_io_manager_writer` |
| `dd.wiring.file_writer` | `dd.wiring.build_file_writer` |

**The `schema` check granularity is gone.**
`check_granularity="schema"`, from the argument or from `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY`, raises `InvalidSettingError`.
Remove the argument or pass `column`, and the asset starts a new check history.
One check for every rule kept no per-rule history, blocked nothing the other two do not, and mixed `"warn"` and `"error"` rules in one result.
[`docs/out-of-scope/schema-granularity.md`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/out-of-scope/schema-granularity.md) has the reasons and the counts that keep `column`.

**`schema_rules` is gone, and a schema-level rule keeps its own check at `column` granularity.**
`column` collapses the rules Dataframely generates on a column into `dy_col__<column>`.
A `@dy.rule()` or the primary key is written by you, so it now keeps its own `dy_rule__<rule>` check and its own history, as a dbt test does in dagster-dbt.
This is what `schema_rules="per_rule"` did.
`dy_schema__rules` is no longer declared, so its history ends with the last run before the upgrade.
Remove `schema_rules=`, `dd.SchemaRules` and `DAGSTER_DATAFRAMELY_SCHEMA_RULES`; nothing replaces them.
[`docs/out-of-scope/schema-rules.md`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/out-of-scope/schema-rules.md) has the reasons.

**Every check-result metadata key is under `dataframely/`.**
`dy_` now covers only check names, rule columns and the `dy_rule` column in a collapsed check's failure examples.
`failed_row_count` follows dagster-dbt's `dagster_dbt/failed_row_count`.

| 0.10 | 0.11 |
| --- | --- |
| `dy_rule` | `dataframely/rule` |
| `dy_rule__expr` | `dataframely/rule_expr` |
| `dy_failed_count` | `dataframely/failed_row_count` |
| `dy_failed_sample` | `dataframely/failed_examples` |
| `dy_rules` | `dataframely/rules` |
| `dy_schema__errors` | `dataframely/column_schema_errors` |
| `dy_filter` | `dataframely/filter` |

**The failure rows a check shows are failure examples, Dataframely's word.**
`max_failure_samples=` is `max_failure_examples=`, and the variable is `DAGSTER_DATAFRAMELY_MAX_FAILURE_EXAMPLES`.
`row_sample` keeps its name, because Polars' word for the first rows of a table is a sample.

**A partitioned asset's row count is under `dagster/partition_row_count`.**
Dagster defines `dagster/row_count` as the whole table's count, so a partitioned materialization no longer writes it.
`stream_results` takes `partitioned=True` for a hand-wired partitioned asset, and `dd.asset` passes it when it has a `partitions_def`.
Dagster's Overview tab reads only `dagster/row_count`, so its row-count stat is empty for a partitioned asset unless you or the IO manager write that key.

**A run with `filter=True` logs a materialization for the error rows after each write.**
The spec from `build_error_rows_spec` then has a history and a partition status, and an asset downstream of it can automate on it.
A run launched from a code location without the spec logs nothing.
A run of `dg.materialize` or `execute_in_process` has no code location to read, so it logs the materialization with or without the spec, and code that expected one materialization from such a run now receives two.

**Every asset has a code version derived from its schema.**
With `code_version=None`, `dd.asset` sets the sha1 hexdigest of each column's name and dtype, then each rule's name, severity and serialized expression.
Every existing asset reports "has a new code version" once after the upgrade.
A Polars upgrade can do the same, because Polars documents its expression serialization as unstable between versions.
`dd.asset` now calls every `@dy.rule()` function when the asset is defined, so a function that raises raises there.
An `is_in` over a Python set of strings gives a different code version in each process, so write a list.

### Added in 0.11

`dd.build_rule_checks(schema, asset=...)` returns the schema's checks on a table that `dd.asset` did not build, as one `dg.AssetChecksDefinition`.
The checks read the table through the asset's own IO manager and write nothing, so a row that fails a rule stays in the table.
[Rule checks](https://ozanozbeker.com/dagster-dataframely/user-guide/rule-checks.html) covers it.

Every check result has a description with its counts: `3 rows failed.`, `No row failed.`, or the failing rules and their counts for a collapsed check.
The column-schema result names the columns that do not match.

A check description that names a `check=` key, `custom check` or a rule name shows the rule's Polars expression after it.

`dd.asset` writes the schema's column schema on every materialization, so the Columns tab keeps every constraint and the primary key under an IO manager that writes a column schema without them.
A `dagster/column_schema` the decorated function adds with `add_asset_metadata` is replaced.

`dd.asset` adds the `polars` kind to every asset, and `build_error_rows_spec` gives its spec the same kind.
Your `kinds` add to it and never replace it.

`build_error_rows_spec` copies the asset's owners and tags from a definition, and `owners=` and `tags=` replace them whole, as `group_name=` does.

### Fixed in 0.11

A single-run backfill logs one error-rows materialization per partition in the range, so Dagster shows each partition as materialized.
Before, the range got one materialization with no partition.

### Documentation in 0.11

*Schema checks* is [Rule checks](https://ozanozbeker.com/dagster-dataframely/user-guide/rule-checks.html), and *Advanced: `filter=True`* is [Filtering error rows](https://ozanozbeker.com/dagster-dataframely/user-guide/filtering-error-rows.html).
The partitioning page covers single-run backfills.
`docs/out-of-scope/` records three declined designs: the error rows as a second asset output, Dataframely's testing helpers in the testing guide, and the function's docstring before the schema's.

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
