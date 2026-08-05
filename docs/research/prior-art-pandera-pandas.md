# Prior art: how `dagster-pandera` and `dagster-pandas` attach a schema

Research for [#3](https://github.com/ozanozbeker/dagster-dataframely/issues/3).
Sources are the packages' own source in the `dagster-io/dagster` monorepo, the published docs, the issue tracker, and probes executed against the locally installed `dagster 1.13.16`.

Confidence markers used throughout:

- **[source]** — read in the package's own source, cited by permalink.
- **[executed]** — reproduced by running code against this repo's `.venv` (`dagster 1.13.16`, `polars 1.43.2`).
- **[docs]** — the published documentation claims it; not independently verified (and in one case, verified *false*).

## Answer

Both integrations attach a schema the same way: a **factory function that returns a `DagsterType`**, which the user passes as `dagster_type=` on an asset or op output.
Neither generates asset checks.
Neither has a lenient path.
Both are `@beta`, both are still released in lockstep with dagster core, and both have been feature-frozen for years — `dagster-pandera` since August 2025, `dagster-pandas` since July 2023.

The interesting split is fidelity, and it runs the *opposite* way from age:

- `dagster-pandas` (2019-era) renders its schema into a **markdown string** stuffed into `DagsterType.description`.
  Low fidelity, unqueryable.
- `dagster-pandera` (2022) renders it into a structured **`TableSchema`** under `DagsterType.metadata["schema"]`.
  Higher fidelity — but under a *private* metadata key on the `DagsterType`, not the ecosystem-standard `dagster/column_schema` key on the asset.
  So it feeds no downstream Dagster machinery.

The decisive finding for this project is not about either package's API, but about the substrate both chose.
**[executed]** A `DagsterType` type check runs **before** the IO manager, and failure raises `DagsterTypeCheckDidNotPass` — no output is stored, no materialization event is emitted, and there is no severity dial.
That is structurally incompatible with dataframely's `filter()` / `FailureInfo` model, whose entire point is that rejected rows are a durable artifact.
**[source]** Dagster's own current data-contracts guide has already moved off this substrate to asset checks.

## Evidence

Per-package first, then the cross-cutting facts about the `DagsterType` substrate that both of them sit on.

### `dagster-pandera`

The closer analogue to dataframely, and the one worth reading carefully.

#### Attachment mechanism

One public export, `pandera_schema_to_dagster_type`, marked `@beta`:

```python
__all__ = [
    "pandera_schema_to_dagster_type",
]
```

— [`dagster_pandera/__init__.py:323-325`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera/__init__.py#L323-L325) **[source]**

It accepts either a `DataFrameSchema` instance or a `DataFrameModel` subclass, normalises the model via `.to_schema()`, and returns:

```python
return DagsterType(
    type_check_fn=type_check_fn,
    name=name,
    description=norm_schema.description,
    metadata={
        "schema": MetadataValue.table_schema(tschema),
    },
    typing_type=pd.DataFrame,  # TODO: pending alternative dataframe support
)
```

— [`__init__.py:145-153`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera/__init__.py#L145-L153) **[source]**

The whole package is 325 lines.
Everything else is private helpers converting pandera constructs into `TableSchema` constructs.

#### UI fidelity

Structured, and reasonably good as far as it goes.
The mapping, verified by the package's own polars test:

| Pandera construct | Dagster construct |
| --- | --- |
| schema `description` | `DagsterType.description` |
| column name | `TableColumn.name` |
| column dtype | `TableColumn.type` (stringified, e.g. `"Int64"`) |
| column `description` | `TableColumn.description` |
| column `nullable` / `unique` | `TableColumnConstraints.nullable` / `.unique` |
| per-column `Check` | `TableColumnConstraints.other` — a **string** |
| dataframe-level `Check` / `Hypothesis` | `TableConstraints.other` — a **string** |

— [`dagster_pandera_tests/test_polars.py:120-160`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera_tests/test_polars.py) **[source]**

Checks degrade to free text.
`_pandera_check_to_column_constraint` prefers `check.description`, falls back to a hand-rolled operator table (`{"equal_to": "==", "less_than": "<", ...}`) that scrapes the operand back out of the check's error string with a regex, and otherwise falls back to `str(check)`:

```python
def _extract_operand(error_str: str) -> str:
    match = re.search(r"(?<=\().+(?=\))", error_str)
    return match.group(0) if match else ""
```

— [`__init__.py:292-320`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera/__init__.py#L292-L320) **[source]**

Regex-scraping another library's error strings to recover a predicate it already knows structurally is a smell worth naming.

Crucially, the schema goes on the **`DagsterType`** under the key `"schema"`, not on the **asset** under `dagster/column_schema`.
Dagster's canonical column-schema key is namespaced `dagster/column_schema` via `TableMetadataSet` — [`dagster/_core/definitions/metadata/metadata_set.py:174-199`](https://github.com/dagster-io/dagster/blob/master/python_modules/dagster/dagster/_core/definitions/metadata/metadata_set.py) **[source]** — and that is the key consumed by `build_column_schema_change_checks` **[source]**, by column lineage, and by the asset catalog.
`DagsterType.metadata` is carried into the `DagsterTypeSnap` (`dagster/_core/snap/dagster_types.py:35`) and rendered in the type detail view only.
So `dagster-pandera`'s schema is visible but inert: it does not become the asset's column schema and drives nothing downstream.

#### Asset checks

None.
No `AssetCheckSpec`, no `AssetCheckResult`, no `@asset_check` anywhere in the package.
The only signal is the binary `TypeCheck.success`.
**[source]**

#### Failure handling

Hard fail only.
`schema.validate(df, lazy=True)` collects all errors, and the resulting `SchemaErrors` is flattened to a description string:

```python
def _pandera_errors_to_type_check(
    error: pa_errors.SchemaErrors, _table_schema: TableSchema
) -> TypeCheck:
    return TypeCheck(
        success=False,
        description=str(error),
    )
```

— [`__init__.py:247-253`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera/__init__.py#L247-L253) **[source]**

Note the leading underscore on `_table_schema`: the parameter is accepted and discarded.

**The published documentation for this function is false, and has been since day one.**
The docstring — which the docs site renders verbatim — claims:

> If validation fails, the returned `TypeCheck` object will contain two pieces of metadata:
>
> - `num_failures` total number of validation errors.
> - `failure_sample` a table containing up to the first 10 validation errors.

— [`__init__.py:116-119`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera/__init__.py#L116-L119), reproduced on <https://docs.dagster.io/integrations/libraries/pandera/dagster-pandera> **[docs]**

No such metadata is ever attached.
I bisected the function back through every commit that touched the file: the stub body is identical in the original 2022-02-16 commit (`93219b73e`, "dagster-pandera (#6547)") and in current master.
**[source]** The constant that was clearly meant to describe that table, `PANDERA_FAILURE_CASES_SCHEMA` ([`__init__.py:215-244`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera/__init__.py#L215-L244)), is dead — a repo-wide code search returns exactly one hit, its own definition.
**[source]**

Four years of published, wrong documentation for the single most decision-relevant behaviour of the package.

#### Known rough edges (open issues)

| Issue | Opened | Still open | What it says |
| --- | --- | --- | --- |
| [#23714](https://github.com/dagster-io/dagster/issues/23714) | 2024-08-16 | yes | Polars schemas produce `typing_type == pandas.DataFrame`, breaking `PolarsParquetIOManager`. Workaround in thread is to poke the private `_typing_type`. |
| [#32510](https://github.com/dagster-io/dagster/issues/32510) | 2025-10-14 | yes | `raise_warning=True` on a pandera model is ignored; the user asks for `AssetCheckSeverity` control. |
| [#23000](https://github.com/dagster-io/dagster/issues/23000) | 2024-07-14 | yes | `Annotated` field types crash `to_schema()`. |
| [#22694](https://github.com/dagster-io/dagster/issues/22694) | 2024-06-25 | yes | Generic type hints unsupported. |
| [#8255](https://github.com/dagster-io/dagster/issues/8255) | 2022-06-08 | yes | Improve the Pandera guide. |

Two of these are load-bearing for this project.

**#23714 is still live in master.** `typing_type=pd.DataFrame` is hardcoded at [`__init__.py:152`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera/__init__.py#L152), unconditionally, even when the schema is a `pandera.polars.DataFrameSchema`.
**[source]** `typing_type` is exactly the field `dagster-polars` routes on.
Prior art shipped polars support in July 2024 (#23299) and got the one field that matters to a polars IO manager wrong, for two years.

**#32510 is the lenient-path question, and it is unanswerable on this substrate.** `TypeCheck` has a boolean `success` and nothing else.
There is no `WARN`.
The maintainer response pattern is also worth noting: on #23000, `danielgafni` (the `dagster-polars` author) states plainly, "I am not maintaining `dagster-pandera`."

#### Packaging

```toml
[project]
name = "dagster-pandera"
dependencies = [
    "dagster==1!0+dev",
    "pandas<3.0.0",
    "pandera>=0.24.0",
]

[project.optional-dependencies]
polars = ["polars>=1"]
```

— [`dagster-pandera/pyproject.toml`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/pyproject.toml) **[source]**

Three facts follow directly:

1. **It is its own package, not an extra of a storage integration.**
   It does not depend on `dagster-pandas` at all.
   Validation and storage were deliberately kept apart.
   This is the packaging precedent that most resembles what this project is considering.
2. **`pandas` is a mandatory dependency even for polars users.** `import pandas as pd` sits unguarded at [`__init__.py:6`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera/__init__.py#L6) and pandas is a hard requirement in `pyproject.toml`.
   Polars support was bolted onto a pandas-shaped package; the module-level `VALID_*_CLASSES` tuple-switching at [`__init__.py:60-79`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera/__init__.py#L60-L79) is the scar tissue.
3. **The dependency floor on the validation library is unbounded above and has already hard-broken once.**
   Dagster `1.11.0` / libraries `0.27.0` shipped: "[dagster-pandera] Adds support for version 0.24.0 of the `pandera` library to `dagster-pandera`, dropping support for pandera 0.23.1 and below." (`CHANGES.md:1399`) **[source]** No compatibility window, no shim.

For contrast, `dagster-polars` lives *outside* the monorepo, in [`dagster-io/community-integrations`](https://github.com/dagster-io/community-integrations/tree/main/libraries/dagster-polars), and ships patito as `patito>=0.8.3; extra == "patito"` (PyPI `dagster-polars` 0.27.12 metadata) **[source]**.
So the two available precedents genuinely disagree: the *closer analogue* (pandera) went own-package; the *storage integration this project depends on* (dagster-polars) went extra-of-storage.

#### Maintenance status

Not deprecated.
Still released: PyPI `dagster-pandera` 0.29.16, pinned `dagster==1.13.16` **[source]**.

But every commit touching the package since **2025-08-12** ("Remove dagster-pandera polars pin", #31720) is repo-wide chore work — ruff preview mode, pyright→ty migration, hatchling migration, Python 3.14 support, dropping 3.9, a `pandas<3` pin, and a docs reorg.
**[source]** The last change to the actual logic in `__init__.py` was **2025-06-20** (#30684, the pandera 0.24 bump).
The last *feature* was polars support, **2024-07-31** (#23299).

Alive as an artifact; dormant as a design.

### `dagster-pandas`

The older, larger, and lower-fidelity of the two.
Included mainly for the contrast and for its failure modes.

#### Attachment mechanism (`dagster-pandas`)

Two competing `DagsterType` factories, both `@beta`:

- `create_dagster_pandas_dataframe_type(name, description, columns, metadata_fn, dataframe_constraints, loader)` — [`data_frame.py:138-202`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandas/dagster_pandas/data_frame.py#L138-L202)
- `create_structured_dataframe_type(name, description, columns_validator, columns_aggregate_validator, dataframe_validator, loader)` — [`data_frame.py:205-281`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandas/dagster_pandas/data_frame.py#L205-L281)

Plus a pre-built `DataFrame` `DagsterType` with a config-driven `@dagster_type_loader` for csv/parquet/table/pickle — [`data_frame.py:31-81`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandas/dagster_pandas/data_frame.py#L31-L81). **[source]**

There is no schema *class*.
The schema is a list of `PandasColumn` objects built from nine typed factory helpers (`integer_column`, `float_column`, `datetime_column`, `string_column`, `categorical_column`, `boolean_column`, `numeric_column`, `exists`, …) — [`validation.py:77-334`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandas/dagster_pandas/validation.py) — over a 1,100-line hand-rolled constraint DSL in `constraints.py` with fifteen separate `@beta` markers.
**[source]**

Public surface: 18 exports. **[source]**

#### UI fidelity (`dagster-pandas`)

Markdown, not structure.
`create_dagster_pandas_dataframe_description` concatenates a bulleted markdown blob and hands it to `DagsterType(description=...)`:

```python
def create_dagster_pandas_dataframe_description(description, columns):
    title = "\n".join([description, "### Columns", ""])
    buildme = title
    for column in columns:
        buildme += f"{_build_column_header(column.name, column.constraints)}\n{_construct_constraint_list(column.constraints)}\n"
    return buildme
```

— [`data_frame.py:106-111`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandas/dagster_pandas/data_frame.py#L106-L111) **[source]**

No `TableSchema` is derived from the declared schema.
The package *does* ship `create_table_schema_metadata_from_dataframe`, but it reflects the schema off a **runtime dataframe**, and it is not wired into either factory — the user must call it themselves from `metadata_fn` — [`data_frame.py:114-135`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandas/dagster_pandas/data_frame.py#L114-L135).
**[source]**

That helper is, notably, the *only* part of `dagster-pandas` that Dagster's own modern documentation still reaches for (see below).

#### Asset checks and failure handling (`dagster-pandas`)

None, and hard-fail-only, same as pandera.
`create_dagster_pandas_dataframe_type` catches `ConstraintViolationException` and returns `TypeCheck(success=False, description=str(e))` — [`data_frame.py:188-189`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandas/dagster_pandas/data_frame.py#L188-L189).
`create_structured_dataframe_type` does slightly better: it aggregates results into per-bucket metadata (`dataframe-constraint-metadata`, `columns-constraint-metadata`, `column-aggregates-constraint-metadata`) as `MetadataValue.json` — [`data_frame.py:256-273`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandas/dagster_pandas/data_frame.py#L256-L273).
**[source]** This is the one place in either package where failure detail is actually attached as structured metadata rather than a string — and it is the *less* documented of the two factories.

#### Packaging and maintenance (`dagster-pandas`)

`dependencies = ["dagster==1!0+dev", "pandas<3.0.0"]` — [`pyproject.toml`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandas/pyproject.toml).
PyPI 0.29.16.
**[source]**

Unlike pandera, this package fuses validation *and* storage concerns in one place: the constraint DSL sits alongside `dataframe_loader`, a `@dagster_type_loader`.

Maintenance: worse than pandera.
The last functional change to `data_frame.py` was **2023-07-19** (#15304, removing the deprecated `event_metadata_fn`); everything since is chore.
**[source]** Three open issues, the newest from **2021**.
**[source]**

The git history contains one cautionary artifact worth surfacing.
On 2020-07-22 the package carried a deprecation warning (`a530254f3`, "update dagster_pandas deprecation warning"); twelve days later, on 2020-08-03, it was un-deprecated (`e25fa6599`, "Undeprecate `create_dagster_pandas_dataframe_type`").
**[source]** It has since sat `@beta` for six years without either resolution.

### Cross-cutting: what the `DagsterType` substrate can and cannot do

These were probed against the installed `dagster 1.13.16`, not read off docs.

#### 1. Type-check failure means nothing is persisted **[executed]**

`_type_check_and_store_output` calls `_type_check_output` *before* `_store_output` (`dagster/_core/execution/plan/execute_step.py:576-578`), and a failing check raises at line 427.
Running an asset whose `DagsterType` returns `TypeCheck(success=False, metadata={"num_failures": 3})` against an instrumented IO manager:

```text
success: False
io manager handle_output calls: []
materialization events: 0
  STEP_OUTPUT   Yielded output "result" of type "FailingSchema". Warning! Type check failed.
  STEP_FAILURE  Execution of step "a" failed.
```

`handle_output` is never invoked.
No materialization event exists.
The `TypeCheck.metadata` is carried on the raised `DagsterTypeCheckDidNotPass` and into `StepOutputData.type_check_data`, but there is no materialization for it to hang off.

#### 2. Asset checks can do what the type check cannot **[executed]**

The same shape as an asset check with `AssetCheckSeverity.WARN`:

```text
run success: True | stored: ['stored']
materializations: 1
check: warn_check passed: False severity: AssetCheckSeverity.WARN meta: {'num_failures': IntMetadataValue(value=3)}
```

Data persisted, asset materialized, failure recorded with severity and structured metadata, run green.
This is precisely what issue #32510 asks `dagster-pandera` for and cannot get.

#### 3. Deriving the `DagsterType` name from the schema creates a collision hazard **[executed]**

`pandera_schema_to_dagster_type` names the type after the schema (`Config.title` → `Config.name` → class name), verified by `test_name_extraction` **[source]**.
Dagster enforces global uniqueness of `DagsterType` *names* by object identity (`dagster/_core/types/dagster_type.py:1046-1059`).
Two calls to the factory with the same schema produce two distinct objects with the same name:

```text
ERROR: DagsterInvalidDefinitionError You have created two dagster types with the same name "MySchema".
       Dagster types have must have unique names.
```

The factory is therefore not idempotent, and callers must hoist the result to a module-level singleton.
Nothing in the API or the docs says so.
The anonymous fallback is worse: a module-global counter, `f"DagsterPanderaDataframe{i}"` for `i` in `itertools.count(1)` ([`__init__.py:156-157`](https://github.com/dagster-io/dagster/blob/master/python_modules/libraries/dagster-pandera/dagster_pandera/__init__.py#L156-L157)), which makes the type's UI-visible name depend on module import order.

#### 4. Dagster's own current guidance has already left this substrate **[source]**

The first-party guide at <https://docs.dagster.io/guides/test/data-contracts> uses neither factory.
Its snippet ([`docs_snippets/.../data-contracts/assets.py`](https://github.com/dagster-io/dagster/blob/master/examples/docs_snippets/docs_snippets/guides/build/assets/data-assets/quality-testing/data-contracts/assets.py)) does three things:

- attaches the schema as **runtime** metadata under the canonical key: `context.add_output_metadata({"dagster/column_schema": create_table_schema_metadata_from_dataframe(df)})`
- validates in a separate `@dg.asset_check`
- returns `severity=dg.AssetCheckSeverity.ERROR if not passed else dg.AssetCheckSeverity.WARN`

The only thing it imports from the prior art is the *reflection helper* from `dagster-pandas`.
Dagster's own answer to "how do I attach a data contract to an asset in 2026" is: canonical metadata key plus asset check.
Not `DagsterType`.

## What to copy / what to avoid

The decision-relevant payload.
Ordered roughly by how much each one constrains the design.

### Copy

1. **The packaging precedent from `dagster-pandera`: validation is its own package, not an extra of a storage integration.**
   Pandera — the closest analogue to dataframely in the Dagster ecosystem — is not `dagster-pandas[pandera]`.
   It is `dagster-pandera`, with no dependency on `dagster-pandas`.
   That is a real argument that `dagster-dataframely` as a standalone package is the *better-precedented* shape, even though `dagster-polars[patito]` is the shape the North Star points at.
   If the North Star holds, own the divergence explicitly: `dagster-polars[patito]` is a thin adapter, whereas pandera's separation exists because validation and storage have different release cadences and different dependency risk.
   Worth an ADR either way.
2. **Structured `TableSchema` over a markdown blob.**
   The move from `dagster-pandas`'s 2020-era markdown description to `dagster-pandera`'s 2022 `TableSchema` is the clearest directional signal in the survey.
   Dataframely's column metadata (dtype, nullability, primary key, per-column rules) maps onto `TableColumn` / `TableColumnConstraints` at least as cleanly as pandera's does.
3. **Lazy/collect-all validation as the default.** `schema.validate(df, lazy=True)` so the user sees every violation, not the first.
   Dataframely's `filter()` already collects everything into `FailureInfo`; do not throw that away by failing fast.
4. **`create_table_schema_metadata_from_dataframe` as a shape, not the code.**
   A small, boring, standalone "dataframe → `TableSchema`" helper is the single piece of prior art that Dagster's own modern docs still use.
   There is likely an analogous `dataframely.Schema → TableSchema` pure function worth exposing publicly and testing independently of any Dagster wiring.
5. **A tiny public surface.** `dagster-pandera` exports one symbol and is 325 lines; `dagster-pandas` exports eighteen across 1,800 lines.
   The small one is the one that got polars support, a docs page, and is still (barely) maintained.
   This corroborates the map's rejection of the archive's three-layer surface from an entirely independent direction.

### Avoid

1. **Do not make `DagsterType` the primary attachment mechanism.**
   Verified by execution: a failing type check runs before the IO manager, so nothing is stored and no materialization event is emitted.
   Dataframely's whole value proposition — `filter()` returning good rows *plus* a persistable `FailureInfo` — cannot be expressed on a substrate that discards the output on failure.
   If a `DagsterType` is offered at all, it should be an optional, secondary affordance, and the design should assume asset checks carry the semantics.
2. **Do not ship a hard-fail-only gate.**
   Issue #32510 is a user asking for exactly the lenient path, on a package that architecturally cannot provide one, unanswered since October 2025.
   `AssetCheckSeverity.WARN` plus `blocking=` is the dial that exists; a `DagsterType` has no dial at all.
   Failure policy must be a first-class design parameter, not a consequence of the attachment mechanism.
3. **Do not put the schema anywhere but `dagster/column_schema` on the asset.** `dagster-pandera`'s `DagsterType.metadata["schema"]` is visible in the type detail view and inert everywhere else: it does not feed `build_column_schema_change_checks`, column lineage, or the asset catalog.
   Use `TableMetadataSet` / the canonical namespaced key.
4. **Do not hardcode `typing_type`.**
   This is the exact bug in [#23714](https://github.com/dagster-io/dagster/issues/23714), open since August 2024 and live in master today, with users patching the private `_typing_type` in the wild.
   `dagster-polars` routes on `typing_type`; the map already flags this.
   Prior art proves it is easy to get wrong and hard to get fixed.
5. **Do not derive a globally-unique name from the schema without saying so.**
   Verified: two factory calls for the same schema in one job raise `DagsterInvalidDefinitionError`.
   Any factory returning a named `DagsterType` must be documented as returning a module-level singleton, or must not be a factory.
   And never use a module-global counter for anonymous names — it makes UI-visible identifiers depend on import order.
6. **Do not let the docstring outrun the implementation.** `dagster-pandera` has promised `num_failures` and `failure_sample` metadata on the docs site since 2022 and never once produced it.
   The map already has a ticket-shaped concern about README rot; this is what the failure mode looks like in a shipped, first-party package.
   If the spec's README claims a metadata key exists, something must execute that claim.
7. **Do not regex-scrape another library's error strings to recover structure.** `_extract_operand` reconstructs a check's operand from its rendered error message.
   Dataframely rules are Polars expressions; reach for the structured form or accept `str(rule)` honestly, but do not build a fragile middle path.
8. **Do not bolt a second dataframe library onto a package shaped for the first.** `dagster-pandera` requires `pandas` even for polars-only users, and switches on module-level tuples of valid classes.
   This project has one dataframe library; keep it that way, and let the map's "dagster-polars is a hard dependency" decision stand rather than reintroducing optional-backend branching.
9. **Do not read either package's docs as a spec.**
   Verified false in one case (`num_failures` / `failure_sample`) and stale in several others.
   Both packages are `@beta`, feature-frozen, and unowned — the `dagster-polars` maintainer states outright on #23000 that he does not maintain `dagster-pandera`.
   Whatever this project builds, it should not assume it can rely on these being fixed or extended upstream.

## Open questions this survey did not settle

- Whether the North Star (`dagster-polars[dataframely]`) or the pandera precedent (standalone `dagster-dataframely`) is the right packaging call.
  The two available precedents genuinely disagree, and this survey can only sharpen the trade-off, not resolve it.
- Whether a `DagsterType` should exist *at all* as a secondary affordance, given it cannot carry the failure semantics.
  There may still be a case for it purely as a `typing_type` carrier for `dagster-polars` routing — but note that is precisely the field the prior art got wrong.
- Granularity of generated asset checks (one per schema, one per column, one per rule).
  No prior art in this survey generates asset checks at all, so there is nothing to copy here — this is greenfield.
