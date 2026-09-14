# Changelog

Pre-1.0, so a `0.x` minor release is where a breaking change lands.
Each release below carries the migration, not just the list.

## Unreleased

### Breaking in 0.8

`temp_dir` and `DAGSTER_DATAFRAMELY_TEMP_DIR` are gone.
A `LazyFrame` return is filtered in the engine, so there is no staging file to place: drop the argument and the variable, and nothing else changes.
The measurements are in §12 of [`docs/research/lazyframe-end-to-end.md`](docs/research/lazyframe-end-to-end.md).

Two functions are renamed, and nothing about either changes but the name.
A function here is named after what it returns, which [`CLAUDE.md`](CLAUDE.md) now writes down, and these two were the public exceptions.

| 0.7 | 0.8 |
| --- | --- |
| `dd.build_quarantine_spec` | `dd.quarantine_spec` |
| `dd.wiring.process` | `dd.wiring.validation_results` |

**The decorator is `dd.asset`.** `dy_asset` put a user-typed name inside `dy_`, which otherwise names only what this package generates into Dagster: the check names, the rule columns and the check metadata keys.
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

The README is a landing page and a quick start. [`USER_GUIDE.md`](USER_GUIDE.md) is everything else, and this file is the upgrade log.

`dataframely/invalid_by_rules` names each rule the way the quarantine's own columns name it, at every granularity, and the guide used to say it names them the way the check list does.
Those agree only at `rule` granularity, so a reader at `column` or `schema` was being sent to a check name that does not exist.

## 0.7.0 - 2026-09-02

### Breaking in 0.7

The quarantine moves out of the graph and onto your own IO manager, and this package stops shipping storage.
Both are ADRs: [0004](docs/adr/0004-the-quarantine-is-a-file-not-an-asset.md) and [0006](docs/adr/0006-the-quarantine-is-written-by-the-assets-own-io-manager.md).

| 0.6 | 0.7 | note |
| --- | --- | --- |
| `dd.dataframely_asset(schema=Orders)` | `dd.asset(Orders)` | the schema is positional |
| `quarantine=dg.AssetOut()` | `quarantine=True` | it is a `bool`, and `True` needs no configuration |
| `dd.DataframelyParquetIOManager` | `dagster_polars.PolarsParquetIOManager` | this package ships no IO manager |
| `dd.DataframelyCSVIOManager` | none | the CSV codecs went with it; use parquet or a warehouse |
| the quarantine was a second out | `dd.quarantine_spec(Orders, orders)` | and only if you want it in the graph |
| `dd.errors.SchemaShapeError` | `dd.errors.ColumnSchemaError` | "shape" was Polars' word for something else |
| `dd.errors.QuarantineSettingError` | none | nothing on the quarantine is configurable now |
| `dd.errors.UnwritableDtypeError` | none | it belonged to the CSV writer |
| `dd.wiring.quarantine_table_schema` | none | a quarantine spec carries its own Columns tab |
| `dd.wiring.process(..., quarantine_key=...)` | `dd.wiring.validation_results(..., quarantine_writer=...)` | see [Hand-wiring](USER_GUIDE.md#hand-wiring) |
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
