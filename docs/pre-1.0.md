# Before 1.0

This is the record of the decisions and the measurements that shaped the package before 1.0.
It replaces the nine decision records that sat in `docs/adr/` and the seven notes that sat in `docs/research/`.

Every claim below was re-checked on 2026-09-22 against dagster 1.13.24, dataframely 3.1.2 and polars 1.44.2.
A claim the code no longer supports was deleted rather than corrected, along with every measurement of a strategy the package no longer runs and every open question a ticket now owns.
What is left is what a reader cannot reconstruct from the code: a rejected alternative, a number that decided something, and a constraint that upstream still imposes.

The decisions keep their ADR numbers, because `src/`, `tests/` and the guide cite them.
Their names are current: a decision that argued about `process` or `dy_asset` now says `validation_results` and `dd.asset`, and `CONTEXT.md` records the renames.

1.0 is the baseline.
This record is closed and nothing is added to it.
From 1.0, the reason a thing works the way it does lives in the module that implements it, and a declined design goes in `docs/out-of-scope/`.

## Decisions

### ADR-0001: validation_results takes asset keys

`validation_results` took `context: dg.AssetExecutionContext` and read it at exactly two places, both to resolve an asset key, so reaching any outcome in a test meant starting a Dagster run.
It now takes `valid_key: dg.AssetKey` and reads nothing off the execution context, which makes every outcome reachable by calling a function.
`src/dagster_dataframely/_runtime.py` holds the signature, and `TestOutcomeSelection` in `tests/test_asset_runtime.py` asserts all six outcomes with no run, no IO manager and no `tmp_path`.
ADR-0006 replaced the second parameter with `quarantine_writer`, which writes the invalid rows and returns the quarantine address, so `validation_results` receives the address alone.

The decision also set the principle that hand-wiring never changes the decorator's design, which `CLAUDE.md` and `docs/out-of-scope/rule-sets-fixed-at-definition-time.md` both cite.

### ADR-0002: the decorator resolves its key at definition time and yields every check standalone

Calling a decorated asset is Dagster's documented unit-testing path, and three blockers stopped it: the wrapper called `dg.AssetExecutionContext.get()`, then `context.asset_key_for_output`, and the runtime function bundled the check results onto the materialization.
The decorator now builds the asset key from `key_prefix` and the name when the definition is built, and `validation_results` yields every check result standalone.
`decorate` in `src/dagster_dataframely/_asset.py` builds the key, `tests/test_upstream_characterization.py` pins the standalone requirement, and both context blockers still raise on dagster 1.13.24.

ADR-0004 replaced the two-key half of this: one out means one key, and nothing reads `AssetsDefinition.keys`.
ADR-0006 replaced "the wrapper reads nothing off the execution context", because an asset with `quarantine=True` declares a `context` parameter that `validate_quarantine_key` and `delegating_writer` both need.

### ADR-0003: the quarantine's only parent is the valid asset

`@dg.multi_asset` gave both outs every input, so a declared quarantine rendered as an independent child of the upstream tables, which is false: neither out executes without the other.
The decorator built the definition twice and pointed the quarantine at the valid asset through `internal_asset_deps`.

ADR-0004 replaced the decision entirely: the quarantine is not an out, so nothing hangs off the valid asset, and `quarantine_spec` states the one edge a user wants in the graph.

### ADR-0004: the quarantine is a file, not an asset

The sibling quarantine out required a `@dg.multi_asset`, an `internal_asset_deps` map and a second definition build on every declaration, and it added a graph node that nothing consumed.
The decorator now builds one `dg.asset`, `quarantine` is a declaration on it, and the package writes the invalid rows on every outcome that has them, including the two that raise.
`validation_results` in `_runtime.py` calls the writer before anything below it can raise, and `quarantine_spec` in `_quarantine.py` returns the `dg.AssetSpec` that puts the quarantine in the graph when the user wants it there.
The package ships no IO manager, so `schema_metadata` returns one entry, `dagster/column_schema`.

ADR-0006 replaced the storage half: a run writes through the asset's own IO manager under the key `<name>_quarantine`, and `file_writer` writes a parquet file only in a direct invocation, where there is no step.

### ADR-0005: the decorator wraps dg.asset rather than stacking on it

`dd.asset` does three things, and `@dg.asset` accepts all three as arguments, so the open question was whether to ship a second decorator that stacks with Dagster's.
Both orders failed.
Stacking above `@dg.asset` needs three private APIs to add a check spec to a finished `AssetsDefinition`.
Stacking below it means the user passes `check_specs` and `metadata` themselves, names the schema three times, and repeats `check_granularity` on both sides or the specs and the results disagree.

`dd.asset` therefore calls `dg.asset` directly, which `src/dagster_dataframely/_asset.py` shows, and forwards every parameter it does not set or rule out.
`user_guide/declaring-an-asset.qmd` tables the six it withholds, and a test fails if `@dg.asset` gains or loses one.
On dagster 1.13.24 `AssetsDefinition.with_attributes` still has no `@public`, so the objection to stacking above still holds.

### ADR-0006: the asset's own IO manager writes the quarantine

The package passes the invalid rows to whichever IO manager the asset is already bound to, under the asset key `<name>_quarantine`.
A configured directory cannot place the rows beside a warehouse table, and an asset key resolves natively on every IO manager that stores by asset key.

`delegating_writer` in `src/dagster_dataframely/_quarantine.py` reads the step's own `OutputContext` and IO manager off a `StepOutputHandle`, shallow-copies that context and re-points `_asset_key`, so the package never reads which manager it is.
It copies the context rather than building one, because a `DbIOManager` backend reads its connection settings off the context at write time.
It reads the manager off the step rather than off `context.resources`, so an asset declares no resource key, and a direct invocation writes the rows with `file_writer` under `quarantine_dir`.
It uses seven private Dagster APIs, all of which still exist on dagster 1.13.24, and `tests/test_upstream_characterization.py` pins each one.

### ADR-0007: the quarantine's key is checked at run time, not at load

A run of an asset with `quarantine=True` fails before its decorated function when another executable asset in the code location materializes `<name>_quarantine`.
Moving the quarantine out of the asset graph removed Dagster's own duplicate-key error, so a collision overwrote the invalid rows and the run still succeeded (#114).

`validate_quarantine_key` in `src/dagster_dataframely/_quarantine.py` reads `context.repository_def.asset_graph.executable_asset_keys` and falls back to the job's asset graph when the run has no repository definition, and `dd.asset` calls it first in `compute`.
Only executable keys contend, so the spec from `quarantine_spec` is exempt by Dagster's own split, which `tests/test_upstream_characterization.py` pins.
A load-time validation was rejected because Dagster registers no third-party load-time validators, so it would only be a function the user remembers to call.
`validate_quarantine_key` is exported through `dd.wiring`, because a hand-wired asset has to make the call itself.

### ADR-0008: every public function that takes a schema validates it

`dy_` is reserved in the user's column space unconditionally, and every public function that takes a schema raises before it does anything else.
The guarantee is a property of the schema, not of what each function does with it, so `table_schema` and `schema_metadata` are not exempt even though they project only the user's own columns.

`validate_namespace` in `src/dagster_dataframely/_rules.py` raises `ReservedColumnError`, `InvalidColumnNameError` and `CheckNameCollisionError`, and `tests/test_reserved_namespace.py` lists the eight functions that call it rather than reflecting them off the package.
The invalid-name check runs ahead of the rule walk, because `|` is dataframely's own rule delimiter and `described_rules` would otherwise read part of the column as a rule name.
Nothing is cached, because a `functools.cache` keyed on a class holds a strong reference for the process lifetime, and the guard is under 5 percent of a `check_results` call on forty columns and three rows.
`dd.asset` calls the guard when the factory runs rather than when the decorator is applied, so `maker = dd.asset(Reserved)` raises on that line.

### ADR-0009: every fence stays plain markdown and the build adds the braces

Quarto executes a fence only when its info string is `{python}`, but `README.md` is also PyPI's long description and GitHub's front page, and neither renders that info string.
Every Python fence in the README and in a docstring therefore stays plain, and `scripts/brace-example-fences.py` adds the braces to the build directory's copies, which `great-docs.yml` registers under `pre_render:`.
Ruff's `docstring-code-format` formats only a plain fence, so the script is what lets a docstring example be both formatted and executed.

Losing the `pre_render:` key degrades silently, so the script counts the README's Python fences and fails unless the landing page ends with that many executable cells.
The guide's `.qmd` pages are the exception and carry the braces in source, because GitHub does not render a `.qmd` and `ruff.toml` maps the extension to markdown, so ruff formats their chunks either way.

## Measurements

### Extending dagster-polars

Issue #2 asked whether `dagster-polars` has an extension point a second validation library can attach to.
The packaging goal at the time was `dagster-polars[dataframely]`, matching `dagster-polars[patito]`.
Everything below is checked against `dagster-polars` 0.27.12.

**No documented extension point exists, and the undocumented one is a mutable global.**
`TYPE_ROUTERS` is a plain module-level list in `dagster_polars/io_managers/type_routers.py:228`, and `resolve_type_router` iterates it at call time.
Registering a router means appending to that list at import time.
The module is absent from `dagster_polars/__init__.py`'s `__all__`, absent from the API reference, and no test registers a third-party router.
There are no entry points either: a search for `entry_point`, `importlib.metadata` and `plugin` over the installed tree returns nothing.

**Appending works end to end.**
A router subclassing `BaseTypeRouter`, appended to `TYPE_ROUTERS`, ran its `dump` during `dg.materialize` through `PolarsParquetIOManager`, with `Schema.validate` inside it.
Two constraints came out of that probe and still hold.
`resolve_type_router` passes `(context, typing_type)` only, so a schema has to be recoverable from the `typing_type` object alone.
`PolarsTypeRouter.match` is `typing_type in [pl.DataFrame, pl.LazyFrame]`, an equality test rather than an `issubclass` test, so a router appended after it still matches a `pl.DataFrame` subclass.

**The metadata path is hard-coded and has no callback.**
`BasePolarsUPathIOManager.get_metadata` (`base.py:202`) calls `self._get_patito_metadata`, which does `import patito as pt` by name.
A `TypeRouter` has no metadata callback at all, so a third party who wants `dagster/column_schema` through this path has to subclass every concrete IO manager.

**Dataframely's typed frames cannot be Dagster annotations.**
This sits upstream of any extension point, and all three failures reproduce on dataframely 3.1.2:

- `resolve_dagster_type(dy.DataFrame[S])` raises `DagsterInvalidDefinitionError`, because `dy.DataFrame[S]` is a `typing._GenericAlias`.
- `dy.DataFrame` alone resolves, but `S.validate(df)` returns a plain `pl.DataFrame`, so Dagster's output type check fails before the IO manager runs.
- `resolve_type_router(dy.DataFrame)` raises `RuntimeError: Could not resolve type router`, because `PolarsTypeRouter`'s equality test misses.

The upstream issue, dagster#22694 "Support generic type hints", is still open, last updated 2025-11-04.
Its draft pull request, dagster#22676, closed stale in August 2025.

**What the package does as a result.**
It ships standalone and registers no type router.
`dagster-polars` is a dev dependency, used as one of two IO manager fixtures in the tests.
`src/` contains no `DagsterType` and no IO manager.
The quarantine writer passes the invalid rows to the asset's own IO manager, under the key `<name>_quarantine` (ADR-0006), so every manager that stores by asset key works and not only `dagster-polars`.

**The packaging goal is gone.**
`dagster-polars[dataframely]` appears nowhere in the repo outside this record.
If upstreaming returns as a question, community-integrations#202 is the place to start: it is still open, last updated 2026-05-04, and it records the maintainer's statement that type routers should become an IO manager constructor argument.
A search of `dagster-io/community-integrations` for "dataframely" still returns zero issues and zero pull requests.

### Prior art: dagster-pandera and dagster-pandas

Issue #3 asked how Dagster's own two validation integrations attach a schema.
Checked against `dagster_pandera/__init__.py` on `dagster-io/dagster` master and PyPI metadata for both packages.
Neither `dagster-pandera` nor `pandera` is installed in this repo's `.venv`.

**Both attach a schema as a `DagsterType` the user passes to `dagster_type=`.**
`dagster-pandera` exports one symbol, `pandera_schema_to_dagster_type`, and the module is still 325 lines on master.
Neither package generates an asset check: a search for `AssetCheck` across `dagster_pandera/__init__.py` returns zero hits.
Neither has a lenient path.

**A `DagsterType` check runs before the IO manager, and that determined this package's substrate.**
`_type_check_and_store_output` calls `_type_check_output` at `execute_step.py:576`, which raises at line 427, before `_store_output`.
An instrumented IO manager recorded zero `handle_output` calls, and the run produced zero materialization events.
The same failure expressed as an asset check with `AssetCheckSeverity.WARN` stored the output, emitted one materialization, recorded the failure with its metadata, and the run succeeded.
`Schema.filter` returns valid rows plus a `FailureInfo` that has to be written somewhere, which a `DagsterType` cannot express.

**`dagster-pandera` puts the schema under a private key.**
It writes `metadata={"schema": ...}` on the `DagsterType` (`__init__.py:149-151` on master), not `dagster/column_schema` on the asset.
Dagster's canonical key is `TableMetadataSet.column_schema`, and that is the key `build_column_schema_change_checks`, column lineage and the asset catalog read.
The pandera schema appears in the type detail view, and no other Dagster feature reads it.

**Three failure modes of the prior art are still present on master.**

- `typing_type=pd.DataFrame` is hard-coded at line 152, with the same `# TODO: pending alternative dataframe support`, even for a `pandera.polars` schema. dagster#23714 opened 2024-08-16 and is still open.
  Its fix, dagster#33780, is still open and unmerged.
- The docstring at lines 118-119 still states that a failing `TypeCheck` has `num_failures` and `failure_sample` metadata.
  `_pandera_errors_to_type_check` still returns `TypeCheck(success=False, description=str(error))` and nothing else, and `PANDERA_FAILURE_CASES_SCHEMA` at line 215 still has exactly one hit in a repo-wide code search, its own definition.
  The published documentation has been wrong for four years.
- Two `DagsterType`s with the same name raise `DagsterInvalidDefinitionError`.
  `pandera_schema_to_dagster_type` derives the name from the schema, so two calls for one schema fail, and nothing documents that.

**The lenient path is the request this substrate cannot satisfy.** dagster#32510 asks for `AssetCheckSeverity` control when a pandera model sets `raise_warning=True`.
It opened 2025-10-14 and is still open.
`TypeCheck` has a boolean `success` and no severity.

**Both packages still ship, and neither has changed its logic in over a year.**
`dagster-pandera` 0.29.24 and `dagster-pandas` 0.29.24 both uploaded 2026-09-21, each pinned to `dagster==1.13.24`.
Do not read either package's documentation as a specification.

**The packaging precedent held.**
`dagster-pandera` is its own package and does not depend on `dagster-pandas`; it requires `pandas<3.0.0` and `pandera>=0.24.0`, with polars as an extra.
`dagster-dataframely` took the same shape, and depends on dagster, dataframely, polars and universal-pathlib only.

**What the package does as a result.**

- `src/` contains no `DagsterType`, and `dd.asset` takes no `dagster_type` argument.
- The asset checks report the failures with a severity.
  `quarantine=True` sets `WARN` on the rule checks and `quarantine=False` sets `ERROR`.
  The column-schema check is always `ERROR` and blocking (ADR-0004).
- `schema_metadata` writes `dagster/column_schema`, so column lineage and the schema-change check read it.
- `wiring.table_schema` is the standalone `dy.Schema` to `dg.TableSchema` function, testable without any Dagster wiring.
- `_rendering.py` builds constraint text from the rule name and its column argument, and parses no error string from another library.
- `check_granularity` sets how many checks the rules collapse into: one per rule, one per column, or one for the schema.
  No prior art generated checks, so this had no precedent to follow.

### How a dy.Collection maps onto the Dagster asset model

Issue #4 asked how a Collection maps onto assets, and it is closed.
`dd.asset` raises `CollectionNotSupportedError` for a Collection, so none of this is in the package.
Issue #43 is open and starts from the findings below.

**Two facts constrain every design.**
`Collection.filter` and `Collection.validate` are classmethods taking a mapping of every required member.
Omit one and dataframely raises before any work runs: `ValueError: Input misses 1 required members: orders.` No API validates one member against the collection.

A row failing a rule in one member removes rows from another.
A customer whose only order fails `amount|min` is removed from `customers`, and `failure["customers"].counts()` returns `{"customer_must_have_order": 1}`.
The correct content of `customers` is not computable from the inputs of `customers` alone, so any design that materializes the members in separate steps writes wrong data.

A Collection with no `@dy.filter` still cascades.
`dy.CollectionMember(propagate_row_failures=True)` appends a rule column named `<member>|failure_propagation` to every non-ignored member's `FailureInfo`, and with no filter defined one invalid order still removes one customer.
So "this Collection has no filters" does not license treating its members as independent assets, and `_failure_propagating_members()` has to be read too.

**A cross-member rule has no single asset key.**
`Collection.filter` appends the filter's name as a rule column to every non-ignored member's `FailureInfo`, with a per-member count.
One banned customer produced one invalid customer row and two invalid order rows, under the same rule name, in two `FailureInfo` objects.
Dataframely already decomposes the rule per member.
`CollectionFilterResult.failure` is a dict keyed by member name, so mapping member name to asset key needs no derivation.
`FailureInfo.rule_columns` is now `FailureInfo._rule_columns`, so a probe written against 3.0.0 raises `AttributeError` on 3.1.2.

**One `@dg.multi_asset` with `outs=` is the recommendation.**
It gives one asset key per member, and one asset check per non-ignored member and cross-member rule.
It was verified end to end with parquet files on disk, per-member checks and a downstream asset depending on one member.
Four gotchas came with it:

- `specs=` does not work, so `outs=` is required.
  `@dg.multi_asset(specs=[dg.AssetSpec(key=m)])` gives each output the Dagster type `Nothing`, and yielding a frame raises `DagsterTypeCheckError`.
  `dg.AssetSpec` has no `io_manager_key` parameter and `dg.AssetOut` has one.
- Any Dagster type check that compares a `LazyFrame` with `!=` raises.
  `Nothing.type_check` runs `value != NoValueSentinel` at `dagster_type.py:487`, and Polars raises `TypeError: "'!='" comparison not supported for LazyFrame objects`.
- `Collection.optional_members()` maps to `AssetOut(is_required=False)`.
  An absent optional member yields no `Output`, and without the flag Dagster raises `DagsterStepOutputNotFoundError`.
  An ignored member (`ignored_in_filters=True`) stays in `required_members()` and still has a `FailureInfo` entry, so it gets the schema's checks and no cross-member check.
- `can_subset=True` is unsound.
  It saves no work, because `Collection.filter` still requires every member.
  It writes the selected member with the cascade applied and leaves the others as an earlier run wrote them, so the persisted members disagree.

Cross-member rules are also partition-local.
A customer whose only order is in another partition is invalid when the filter runs per partition and valid over the whole dataset.
That is a constraint on the user's data model that the package cannot check, so a partitioned Collection needs its own decision.

| Design | Cross-member guarantee | Per-member lineage | Result |
| --- | --- | --- | --- |
| A: `multi_asset` with `outs=` | yes, one `filter` over every member before any output | yes | recommended |
| B: N assets plus `multi_asset_check` | no, a check reports and does not change the rows | yes | ruled out |
| C: one asset whose value is the Collection | yes | no, one key for N tables | ruled out |
| D: `AssetSpec`, `can_subset`, `additional_deps` | no, `can_subset` breaks it | n/a | reduces to A |

Design B was built and run.
The check named the violation correctly and the invalid rows stayed on disk for every downstream consumer, because `blocking=True` stops downstream assets and does not remove the rows.
Design C works and gives one asset key, one materialization and one lineage node for what the user reads as several tables, which is most of what Dagster is for.

**The error message's advice is incomplete.**
`CollectionNotSupportedError` tells the user to declare one asset per member, each with the member's own schema.
That is design B, so it holds for a Collection with no filter and no `propagate_row_failures`, and it loses the cross-member guarantee for any other.
Issue #43 has to settle the wording along with the design, and it owns two more questions: whether one rejected entity writes N quarantines, one per member, or one keyed by the common primary key; and whether `Collection.filter(lazy=True)` works under Dagster's output handling, since `_validate_lazy_param` rejects `lazy=True` when the Collection has an eager member.

### Where Dagster keeps the metadata that fills the Columns tab

Issue #5 asked which metadata store the Columns tab reads, and it is closed.
The UI source was re-read at tag `1.13.24`.
`schema_metadata` writes `dagster/column_schema` to the definition bucket, and nothing in the package writes it at run time.
Everything below explains that choice.

**Three buckets, and the package uses one.**

| Bucket | The package writes it with | Where Dagster reads it |
| --- | --- | --- |
| Definition | `dg.asset(metadata=...)`, from `schema_metadata` | Columns section, Metadata accordion, `AssetNode.storageAddress` |
| Materialization | the event's metadata, from `dg.MaterializeResult` | Columns section when present, metadata plots |
| Output type | nothing | the lineage sidebar's Type accordion only |

The three are separate stores.
Definition metadata is never copied into the materialization event: an asset declared with `metadata={"defn_only_key": ...}` produced an event whose metadata did not contain that key.

**The precedence chain inside the materialization bucket.**
`execute_step.py:591` merges `{**output.metadata, **io_manager_metadata}`, so an IO manager beats `Output(metadata=)`, `MaterializeResult(metadata=)` and `add_output_metadata`.
Line 591 is not the last word: `execute_step.py:650-653` layers `context.add_asset_metadata()` over that, and `658-663` layers the partition-scoped call over that again.

This is why `tests/test_upstream_characterization.py::test_dagster_polars_writes_its_own_row_count_over_the_steps` exists.
`dd.asset` returns `dg.MaterializeResult(value=..., metadata={"dagster/row_count": ...})`, which is the weakest position, so `PolarsParquetIOManager` writes its own count over it.
The tests assert this package's count on what `validation_results` yields, not on the event.
One trap when reproducing this: a `MaterializeResult` with no `value` and an `Any` output type skips the IO manager entirely, so it appears to take precedence only because nothing competed.
Pass `value=`.

**What the Columns section reads.**
`buildConsolidatedColumnSchema.tsx` reads the materialization bucket and the definition bucket, never the output type.
The materialization schema is the base if one is present, and the definition schema then supplies `description` and `tags` and nothing else.
`type`, `nullable`, `unique` and `constraints.other` always come from the base.
Merged column names are lowercased.
A definition-only schema renders whole, which is what `user_guide/what-a-run-produces.qmd` states: the Columns tab comes from the definition, so the catalog shows it before the first run and after a failed one.

The gap opens when both buckets are filled, and a dagster-polars IO manager fills the other one.
`dagster_polars/io_managers/utils.py:154` writes `dagster/column_schema` from `df.collect_schema()`.
Running `dd.asset` under `PolarsParquetIOManager` shows the result: the definition has `[('orderID', 'String', [], nullable=False), ('amount', 'Float64', ['>= 0.0'], nullable=False)]` with the table constraint `PK: orderID`, and the materialization has the same two columns with no constraints and `nullable=True`.
So on the Overview tab, every column constraint this package renders disappears, the primary key with it, and `orderID` shows as `orderid`.
Nothing in the package writes to both buckets on one asset.
The user's IO manager does, and the package has no say in it.

**Rendering limits the Columns section imposes.**
`MAX_CONSTRAINT_TAG_CHARS = 30` in `metadata/TableSchema.tsx`, and a longer `constraints.other` string truncates to 27 characters plus an ellipsis, with the rest in a tooltip.
This is why `_rendering.py` renders `>= 0.0` and `PK: orderID` rather than the rule's expression, and why a `check=` rule renders its dict key.
A regular expression or a long `is_in` list still exceeds it.
The Overview Metadata table hides `dagster/column_schema`, `dagster/row_count`, `dagster/table_name` and `dagster/uri` when `hideEntriesShownOnOverview` is set, so the column schema appears in the Columns section and nowhere else on that page.
`AssetEventMetadataEntriesTable.tsx:148` merges definition rows under event rows for everything it does show.
Both come from reading the UI source, and the rendered result is unverified, because checking it needs a running UI.

**`dy.DataFrame[Schema]` cannot be an output annotation.**
`dy.DataFrame[S]` is a `typing._GenericAlias`, so `isinstance(T, type)` is `False`, `make_python_type_usable_as_dagster_type` raises `ParameterCheckError`, and using it as a return annotation raises `DagsterInvalidDefinitionError`.
A bare `-> dy.DataFrame` is accepted and resolves to a `TypeHintInferredDagsterType` with no schema.
This is a property of `typing._GenericAlias` and not of any Dagster version.

**Two mechanisms worth remembering.**
`TableMetadataSet` at `dagster._core.definitions.metadata` is the typed way to write these keys, and it is still not exported from the top-level `dagster` namespace.
Its fields at 1.13.24 are `column_schema`, `column_lineage`, `row_count`, `partition_row_count`, `table_name` and `storage_kind`.
`TableMetadataSet.extract_storage_address` is the source of `CONTEXT.md`'s use of the word address.
`dg.build_column_schema_change_checks` compares `dagster/column_schema` across an asset's two most recent materializations.
It reads the materialization bucket only, so it never fires on this package's definition metadata, and it is redundant beside `dy_schema__columns`, which is blocking and compares the frame against the declared schema before anything is written.

### Partitioned assets

Issue #25 asked what a partitioned asset actually does, because partitioning was forwarded rather than designed for.
It works, and it needed no code.
`tests/test_partitions.py` is the durable half: 17 tests, all passing, covering the two claims that are Dagster's behaviour rather than this package's.

#### Everything the package does per partition holds

Validation runs on each partition's frame.
`dagster/row_count` is that partition's valid row count.
A partition whose frame fails the column-schema check writes neither the table nor the quarantine and leaves every other partition's file alone.
The writer writes the quarantine under the same partition key as the asset.
All four are pinned in `tests/test_partitions.py` and stated in `user_guide/partitioning.qmd`.

One provenance detail changed.
`PolarsParquetIOManager` writes its own `dagster/row_count` over the step's, pinned by `tests/test_upstream_characterization.py::test_dagster_polars_writes_its_own_row_count_over_the_steps`, so the catalog value comes from the bound manager.
It equals the valid rows either way, because the manager counts the frame it writes.

#### Asset checks are not partitioned, and this is the one finding still worth acting on

`dg.AssetCheckResult` has no partition field.
Dagster fills the evaluation's partition only when the check's own spec has a `partitions_def`, which this package does not set.
So every evaluation on a partitioned asset has `partition=None`, and one history accumulates per asset and check pair rather than one per partition.
The last partition to run owns every rule's latest state, so a backfill ending on a clean partition hides a dirty one, and a backfill ending on a dirty partition marks the rule failed for an asset that is mostly fine.

The invalid rows are never lost.
The quarantine is written per partition with full per-row attribution, and only the checks are coarse.
Attribution by hand is possible: `target_materialization_data` is scoped to the step's partition, so a history row points at exactly one materialization, and that materialization has its partition key.

Three tests pin this: `test_every_partition_reports_its_own_checks_and_names_no_partition`, `test_a_backfill_appends_every_partition_to_one_check_history`, and `test_a_history_row_is_traceable_to_its_partition_through_the_materialization`.

#### A time-window partition strands a planned check row

A static partitions definition records one history row per run.
A daily one records two, and the second never resolves.
Dagster writes a planned row per check when the run starts, keyed by the partitions subset the run has, and a time-window run has one where a static run has `None`.
The evaluation then arrives with `partition=None`, `_update_asset_check_evaluation` matches on `(asset_key, check_name, run_id, partition)`, the partition clause misses, and the result is inserted as a second row.
A per-partition check view then reads never-executed on an asset whose checks all ran.

None of this is the package's doing: a stock `@dg.asset` with one `check_spec` and a daily partitions definition reproduces it.
`test_a_time_window_partition_leaves_the_planned_check_row_unresolved` pins it.
The `DeprecationWarning` from `datetime.utcfromtimestamp` inside Dagster's SQL event log still fires on 1.13.24, 21 times across the partition suite.
Issue #32 closed without an upstream issue.

#### The fix exists and waits on GA, not on the end of preview

`dg.AssetCheckSpec` accepts a `partitions_def`, and giving it the asset's own closes both findings together: the history is keyed per partition and no planned row is stranded.
Adoption was measured and is small.
Nothing is deleted, a check key's history stays continuous across the switch, the per-partition view starts empty and refills as each partition next runs, and what is left behind is a single `partition_key=None` entry that does not grow with how long adoption is deferred.

Constructing that spec still emits `PreviewWarning` on dagster 1.13.24, asserted as the whole warning list by `test_a_partitioned_asset_check_spec_is_still_in_preview`, so the suite fails when upstream promotes the parameter and the failure message tells beta from GA.
The floor moved from `dagster>=1.13.16` to `dagster>=1.13.24` and still has no upper bound, which is the reason to wait: a preview parameter that changes in a patch release would break a user's asset through no action of their own.
Beta permits the same behaviour changes under a quieter name, so the trigger is GA.
Tracked as issue #31, the only follow-up from this work still open.

Two properties of the parameter itself got tests, because they are properties of the surface rather than of adopting it.
A blocking check takes a `partitions_def`, stamps its partition and still ends the run when it fails, so the column-schema check needs no carve-out.
And Dagster does not enforce its own documented constraint that a spec's partitioning match its asset's, at construction, attach, load or run.
Both are in `tests/test_upstream_characterization.py`.

#### A single-run backfill raises before it writes

`backfill_policy` forwards like every other `dg.asset` parameter, but `dg.BackfillPolicy.single_run()` never reaches a write.
`UPathIOManager` resolves one path per output and raises a `check.invariant` on a range, naming the multi-run policy as the fix and arriving on the first run rather than after a wrong write.
The message formats `type(self)`, so it names whichever manager is bound.
`test_a_single_run_backfill_is_rejected_by_the_io_manager` asserts the two stable phrases and that nothing was written.

This is undocumented.
The README has no partitioning text at all, and `user_guide/partitioning.qmd` never mentions the single-run policy.

#### The fan-in shape is documented, not exported

An unpartitioned asset depending on every partition of a partitioned one receives one frame per partition, because the base manager calls `load_from_path` once per key.
The obvious annotation, `pl.DataFrame`, fails Dagster's type check after every partition has already been read.

The decision reversed twice and came back to where it started.
The prior art exports `DataFramePartitions`, so issue #35 shipped that name and reserved `LazyFramePartitions`, and the module-layout audit then deleted both.
What settles it is the direction an alias moves information.
`dict[str, pl.DataFrame]` states that the key is a partition key and the value is one frame; `DataFramePartitions` states neither.
The alias was exported to teach the shape, and a name that hides the shape cannot do that, so the guide paragraph was always what taught it.
Neither alias did anything at run time: `_wants_lazy` reads `get_args()` off the user's own annotation, and `get_args(dict[str, pl.LazyFrame])` is the same tuple either way.
`tests/test_public_surface.py` pins the six public names, and `user_guide/partitioning.qmd` shows `dict[str, pl.DataFrame]`, `dict[str, pl.LazyFrame]` and `dict[dg.MultiPartitionKey, pl.LazyFrame]`.

### Lazy validation and lazy storage

Issue #27 asked whether validation and storage can both stay lazy, from the decorated function to the destination.
The answer took three rounds: storage can, validation cannot, and validation is what the package provides.
Everything re-measured below ran on one M-series laptop, one run each unless the entry says otherwise.

#### Validation collects both sets of rows, so nothing streams to the destination

`dy.FailureInfo._df` is a `cached_property` that calls `self._lf.collect()` (`filter_result.py:108`).
`counts()`, `details()`, `invalid()`, `cooccurrence_counts()` and `__len__` all read it.
`validation_results` then determines the outcome from `bool(invalid_count) and (quarantine_writer is None or not len(valid))`, so both counts exist before any outcome does, and two outcomes write nothing.

The consequence was measured rather than assumed.
`Schema.validate(lf).sink_parquet(dest)` with one failing row at index 1.5M of 2M raises `ComputeError` with the full rule message and leaves `dest` at 0 bytes, which `read_parquet` rejects with "parquet: File out of specification: The file must end with PAR1".
`UPathIOManager.handle_output` writes to the final path with no temporary file and no rename.
`DuckDBPolarsTypeHandler.supported_types` is `[pl.DataFrame]`, so a lazy output cannot pass through it at all.
A run that streamed to the destination would replace the last written table with an empty file.
`_checks.filtered` therefore collects with `collect_all(engine="streaming")`.

#### The streaming collect_all is the fastest strategy, and the primary-key penalty did not return

The first round measured `collect_all` at 10.68s with a `primary_key` against 1.05s without, on polars 1.43.2, and concluded the schema decided the strategy.
Issue #118 re-raced on 1.44.1 and found the penalty gone.
Re-running the same benchmark confirms it on 1.44.2: 5x fan-out, unsorted primary key, 4M rows, peak resident set size from `ru_maxrss` in a fresh process per strategy.

| strategy | polars 1.44.1 | polars 1.44.2 |
| --- | --- | --- |
| eager | 0.29s / 676 MB | 0.32s / 705 MB |
| disk staging | 0.10s / 1033 MB | 0.12s / 1038 MB |
| `collect_all`, in-memory engine | 0.28s / 598 MB | 0.30s / 636 MB |
| `collect_all`, streaming engine | 0.08s / 757 MB | 0.07s / 781 MB |

Disk staging uses the most memory of the four with a primary key, which reverses the first round's reading.
The package runs one `collect_all` on the streaming engine for both return types, and 0.8 deleted `temp_dir`, `DAGSTER_DATAFRAMELY_TEMP_DIR` and the staging file.

Sinking both sets of rows from the plan in one `collect_all` remains the idea to reject.
Measured at #118: two lazy `sink_parquet` nodes and one aggregate pull the source through 3.00 times, against 1.00 for the two-frame call, even though `pl.explain_all` shows two `CACHE` nodes either way.

#### Naming the engine changes no result, and the engine name is not a setting

At #118, on 2M rows with three injected duplicates and 10% `min` failures, the streaming `collect_all` returned the same `counts()`, the same valid rows in the same order and the same invalid rows as the eager path.
Polars documents a fallback to the in-memory engine for any operation the streaming engine does not support, so naming an engine never fails a plan.
`tests/test_upstream_characterization.py::test_a_lazy_filter_still_defers_to_collect_all_and_forwards_the_engine` pins both facts: `collect_all` exists only on the lazy result, and polars raises `Invalid engine argument` for a name it does not accept, which proves the argument is forwarded.

#### Dataframely's lazy-validation guide was stale, and still is

The guide documents an `eager: bool` on `Schema.validate` and `Schema.filter`.
That parameter left in 3.0.0 (Quantco/dataframely#372) and the guide was not updated.
On 3.1.2 the signature is `(df, /, *, cast=False, **kwargs)`, so a `DataFrame` validates now and a `LazyFrame` appends to the plan.
`Collection.filter` and `Collection.validate` keep `lazy: bool`.

#### check_results makes the same call, and reaching past the accessor is not worth two private attributes

Issue #80 added `check_results`, which evaluates the checks and writes nothing.
It never reads the valid rows, so collecting them looked wasteful.
It is not.
Peak resident set size, three options, 2M rows and 14 columns scanned from parquet, two runs each.
The two columns used different frames, so compare within a column only.

| option | polars 1.44.1, 216 MB frame | polars 1.44.2, 256 MB frame |
| --- | --- | --- |
| `collect_all(engine="streaming")`, valid rows discarded | 785, 814 MB | 1115, 1080 MB |
| `.failure` alone, letting `FailureInfo` collect | 880, 872 MB | 1202, 1202 MB |
| `pl.collect_all([failure._lf], engine="streaming")` | 779, 762 MB | 966, 912 MB |

The accessor is the most expensive option in both rounds, because `FailureInfo._df` calls `collect()` with no arguments and runs on `auto`.
The engine matters more than which rows are collected.
The third option won by about 35 MB at #80, inside the run-to-run variance, and by about 150 MB now, outside it, so the original reason for declining it no longer carries.
The remaining reason stands on its own: it requires `_lf` and `_rule_columns`, two private dataframely attributes, and `check_results` calling exactly what `validation_results` calls keeps one code path to reason about.

#### Reaching the step's IO manager from inside the asset body works

This was traced while disk staging was live, to show that a file opened inside the asset body is still open when the manager writes.
Disk staging is gone, and the fact it established now carries a different weight: `delegating_writer` calls `manager.handle_output` from inside the asset body, before the step's own output is handled.
`tests/test_upstream_characterization.py` pins the three private APIs this needs, in `test_a_step_still_hands_over_the_output_context_and_manager_it_was_going_to_use`, `test_keys_by_output_name_still_omits_the_check_outputs` and `test_an_output_context_still_clones_and_re_points_by_attribute`.

#### Schema.filter projects to the schema's columns, in the schema's order

This was listed as not investigated while promoting a staged file was under consideration.
It is now answered and pinned by `tests/test_upstream_characterization.py::test_filter_projects_a_superset_to_the_schemas_columns_in_order`, on both the eager and the lazy path.
Dataframely does not document it, and `user_guide/the-failure-policy.qmd` states it.

### The cost of DescribedRule records

`described_rules` builds a record for every rule on every call, and each record splits the rule name at `|`, builds a check name and looks up a docstring whether or not the caller uses them.
Building the records takes about twice as long as reading dataframely's rule dictionary alone: 77 us against 43 us for a twelve-column schema.

`DescribedRule.expr` reads the rule's expression on access rather than at build time, because dataframely builds a `@dy.rule()` body as `Rule(expr=lambda: ...)`.
Reading every expression eagerly makes a forty-rule schema about seven times slower to build, and the multiple tracks how much work the rule bodies do.

`DescribedRule` is a frozen dataclass and not a `NamedTuple`, because a `NamedTuple` field cannot start with an underscore, so `_rule` would become a public `rule` beside `expr`.
A `NamedTuple` constructs each record about 2.8 times faster, which nothing needs.

Measured on 2026-09-22, after a first measurement for 9e0e2ad on 2026-09-13 against dagster 1.13.20, dataframely 3.0.0 and polars 1.44.1.
