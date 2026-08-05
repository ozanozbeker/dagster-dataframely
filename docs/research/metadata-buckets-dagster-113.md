# Metadata buckets on Dagster 1.13.16 — re-verification and survey

Resolves [#5](https://github.com/ozanozbeker/dagster-dataframely/issues/5).

Re-verifies the five metadata-bucket claims in `archive/dagster-dataframely-old/README.md` (written against `dagster>=1.11`) against the pinned `dagster 1.13.16`.

Evidence sources, in descending order of trust:

1. **Reproductions** run against `.venv/bin/python` (dagster 1.13.16, dataframely 3.0.0, polars 1.43.2).
2. **Installed Python source** under `.venv/lib/python3.12/site-packages/dagster/`.
3. **UI source pinned at the `1.13.16` git tag**, fetched via `gh api repos/dagster-io/dagster/contents/<path>?ref=1.13.16`.
   This settles *data flow* (which bucket feeds which component) but not *pixels*.
4. Docs, only where 1–3 are silent.

---

## Verdict table

| # | Claim | Verdict |
| - | ----- | ------- |
| 1 | `dagster/column_schema` lives in three buckets — definition, output type, materialization — each rendered in a different UI surface | **CONFIRMED** (one sub-claim corrected: the buckets *do* clash on the Catalog Overview) |
| 2 | Materialization bucket merges `{**output.metadata, **io_manager_metadata}`, so the IO manager wins any collision; subclassing the IO manager is the only way to win that bucket | **CHANGED** — the merge line is verbatim intact, but the conclusion is **false**. `context.add_asset_metadata()` is applied *after* the IO manager and beats it. Also true at 1.11.0, so the archive was wrong when written, not stale. |
| 3 | Type accordion ← output-type; Metadata accordion ← definition; metadata plots + Catalog Metadata section ← materialization | **CONFIRMED with two corrections** — (a) the Catalog Metadata section is definition+materialization merged, not materialization alone; (b) it **explicitly hides** `dagster/column_schema`. Data flow verified from UI source; visual rendering **UNVERIFIABLE** here. |
| 4 | Catalog Columns view: output-type gives dtypes, definition adds descriptions, materialization adds constraint pills; only materialization is complete | **CHANGED** — the output-type bucket contributes **nothing** to the Columns view. The real rule is: materialization schema is the base if present (else definition), and only `description` + `tags` are overlaid from the definition. Plus an undocumented gotcha: **merged column names are lowercased**. |
| 5 | `dy.DataFrame[Schema]` is a `typing._GenericAlias`, so `make_python_type_usable_as_dagster_type` rejects it and it cannot be an output annotation | **CONFIRMED** — reproduced verbatim on dagster 1.13.16 / dataframely 3.0.0 |

**Highest-value finding: claim 2.**
The archive's central architectural justification for `DataframelyPolarsParquetIOManager` ("subclassing the IO manager is the only way to win the materialization bucket") is false.
`context.add_asset_metadata()` in the asset body wins.

---

## Claim 1 — three metadata buckets

Testing whether a `dagster/column_schema` can live in three distinct, non-interchangeable buckets.

### Verdict: CONFIRMED, with one sub-claim corrected

The three buckets are three genuinely distinct stores.
Reproduced by writing a *different* `dagster/column_schema` to each on one asset and reading all three back:

```text
DEFINITION  AssetsDefinition.metadata_by_key           -> definition
DEFINITION  AssetSpec.metadata                         -> definition
OUTPUT_TYPE OutputDefinition.dagster_type.metadata     -> output_type
MATERIALIZATION event metadata                         -> io_manager
SNAPSHOT    DagsterTypeSnap MyType                     -> {'dagster/column_schema': 'output_type'}
```

Storage locations in installed source:

| Bucket | Python store | Snapshot / GraphQL surface |
| ------ | ------------ | -------------------------- |
| Definition | `AssetSpec.metadata` → `AssetNodeSnap.metadata` | `AssetNode.metadataEntries`, resolved in `dagster_graphql/schema/asset_graph.py:1235` from `self._asset_node_snap.metadata` |
| Output type | `DagsterType.metadata` (`_core/types/dagster_type.py:212`) → `DagsterTypeSnap.metadata` (`_core/snap/dagster_types.py:80`) | `AssetNode.type`, resolved in `asset_graph.py:1411` from the op's `output_def.dagster_type_key` |
| Materialization | the `AssetMaterialization` event's `metadata` | `assetMaterializations { metadataEntries }` |

Definition metadata is **never** copied into the materialization event.
Reproduced: an asset with `@dg.asset(metadata={"defn_only_key": ...})` produced a materialization event whose metadata keys were only what the IO manager emitted — `defn_only_key` was absent.

### Correction to the archive's phrasing

The archive says the buckets "do not clash across buckets; only within the materialization bucket do sources collide."
**That is not true at 1.13.16.**
The Catalog Overview page merges definition and materialization metadata in *two* places:

- `buildConsolidatedColumnSchema.tsx` — merges the two column schemas for the Columns section (see claim 4).
- `AssetEventMetadataEntriesTable.tsx:148` — `uniqBy([...observationRows, ...eventRows, ...definitionRows], e => e.entry.label)` for the Metadata section.
  Array order determines the winner: observation > materialization > definition.

Only the **output-type** bucket is genuinely isolated — it is read only by `DagsterTypeSummary`.

---

## Claim 2 — IO manager wins collisions in the materialization bucket

Testing whether an IO manager's metadata beats every other source within the materialization bucket.

### Verdict: CHANGED (the cited line is intact; the conclusion drawn from it is false)

Evidence below, in order.

#### The cited line is verbatim intact

`.venv/lib/python3.12/site-packages/dagster/_core/execution/plan/execute_step.py:591`, inside `_get_output_asset_events`:

```python
    # Metadata scoped to all events for this asset.
    key_scoped_metadata = {**output.metadata, **io_manager_metadata}
```

`output.metadata` at this point already includes anything from `context.add_output_metadata()` in the *asset body* — folded in at `execute_step.py:255-259`:

```python
            metadata = step_context.get_output_metadata(output.output_name)
            with disable_dagster_warnings():
                output = output.with_metadata(
                    metadata={**output.metadata, **normalize_metadata(metadata or {})}
                )
```

`io_manager_metadata` is `manager_metadata`, accumulated from the IO manager's `context.add_output_metadata()` calls and yielded dicts (`execute_step.py:828`, `835`, `846`).
So yes: **the IO manager beats `add_output_metadata`, `Output(metadata=)` and `MaterializeResult(metadata=)`.**

#### But line 591 is not the last word

Sixty lines later, `execute_step.py:650-654`:

```python
    unpartitioned_asset_metadata = step_context.get_asset_metadata(asset_key=asset_key)
    all_unpartitioned_asset_metadata = {
        **key_scoped_metadata,
        **(unpartitioned_asset_metadata or {}),
    }
```

and for partitioned assets, `execute_step.py:661-664` layers partition-scoped metadata on top of that.
`get_asset_metadata` is fed by `context.add_asset_metadata()` (`_core/execution/context/system.py:732-778`).

**Full precedence chain in the materialization bucket on 1.13.16, lowest to highest:**

```text
Output(metadata=) / MaterializeResult(metadata=) / context.add_output_metadata()   (weakest)
  < io_manager_metadata (context.add_output_metadata inside handle_output)
    < context.add_asset_metadata(metadata=...)
      < context.add_asset_metadata(metadata=..., partition_key=...)                (strongest)
```

#### Reproduction

`dg.materialize` against an IO manager that unconditionally emits `{"dagster/column_schema": <its own>}`, reading back the actual materialization events:

```text
all_three        col=add_asset_metadata     keys=['aam', 'aom', 'dagster/column_schema', 'who']
defn_only        col=io_manager             keys=['dagster/column_schema', 'who']
mr_vs_aam        col=add_asset_metadata     keys=['dagster/column_schema', 'who']
mr_with_value    col=io_manager             keys=['dagster/column_schema', 'who']
```

- `all_three` sets the key via `add_output_metadata`, `add_asset_metadata`, *and* the IO manager → `add_asset_metadata` wins.
- `mr_vs_aam` returns `MaterializeResult(value=1, metadata=...)` and also calls `add_asset_metadata` → `add_asset_metadata` wins.
- `mr_with_value` returns `MaterializeResult(value=1, metadata=...)` only → the IO manager wins.

`add_asset_metadata` is stable public API on `AssetExecutionContext` in 1.13.16 — not `@beta`, not `@preview`, not deprecated.

> One trap: `MaterializeResult` with **no** `value` and an `Any` output type **skips the IO manager entirely** (`execute_step.py:770-794`), so `MaterializeResult(metadata=...)` alone appears to "beat" the IO manager only because the IO manager never ran.
> Pass `value=` for a real collision.

#### Was this a 1.11 → 1.13 change?

**No.** Fetching `execute_step.py` at tag `1.11.0` shows the same structure at the same line numbers (`590` merge, `649-651` asset-metadata overlay).
The archive's conclusion was already wrong when it was written.

#### Design consequence

`DataframelyPolarsParquetIOManager` does **not** have to exist merely to win the `dagster/column_schema` key.
Any asset body can call `context.add_asset_metadata({"dagster/column_schema": ...})` and beat every dagster-polars IO manager.
That removes IO-manager subclassing as a *forced* dependency of the UI story — it may still be wanted for other reasons (casting on store, quarantine paths), but the metadata argument no longer carries it.

Related, and also useful: an IO manager can read the *definition* bucket at write time.
`OutputContext.definition_metadata` and `OutputContext.asset_spec` both expose the asset's definition metadata inside `handle_output` (reproduced).
`OutputContext.metadata` is deprecated in favour of `definition_metadata` (removal in 2.0).
This is a second route for smuggling a schema to an IO manager without the archive's `dagster_type=` mechanism, and the 1.13.16 changelog explicitly calls it out under Documentation: *"Clarified the distinction between definition-time and runtime metadata, and documented how to access asset definition metadata from a custom I/O manager."*

---

## Claim 3 — which accordion is fed by which bucket

Testing the mapping from each bucket to the UI surface that renders it.

### Verdict: CONFIRMED with two corrections; pixel rendering UNVERIFIABLE

`dagster-webserver` / `dagster-graphql` are not installed in this venv and were not installed (hard constraint: do not modify `.venv`).
Everything below is read from the UI source at the `1.13.16` tag, which is authoritative on *data flow* but cannot confirm what a human sees.

#### Lineage sidebar, Type accordion ← output-type bucket — CONFIRMED

`js_modules/ui-core/src/asset-graph/SidebarAssetInfo.tsx`:

```text
 90:  const {assetMetadata, assetType} = metadataForAssetNode(asset);
242:  {assetType && <TypeSidebarSection assetType={assetType} />}
268:  const TypeSidebarSection = ({assetType}) => (
270:    <SidebarSection title="Type"><DagsterTypeSummary type={assetType} /></SidebarSection>
```

`metadataForAssetNode` (`assets/AssetMetadata.tsx`) returns `assetType = assetNode.type`, which the GraphQL resolver derives from the op's output `dagster_type_key`.
`DagsterTypeSummary` (`dagstertype/DagsterType.tsx`) renders the **full `<TableSchema>` component** — the same filterable table with type tags, `non-nullable` / `unique` / arbitrary-constraint pills, descriptions, and column tags.
Confirms the archive.

Two nuances the archive did not record:

- `DagsterTypeSummary` selects the entry by `gqlTypePredicate('TableSchemaMetadataEntry')` — **any** `TableSchema`-typed entry, regardless of key.
  The `dagster/column_schema` key is not required in the output-type bucket.
- `dagsterTypeKind()` renames the type to `"<name> (table)"` in the header whenever such an entry
  is present.

#### Lineage sidebar, Metadata accordion ← definition bucket — CONFIRMED

`SidebarAssetInfo.tsx:221-239`:

```text
221:  {assetMetadata.length > 0 && (
222:    <SidebarSection title="Metadata">
234:      <AssetMetadataTable assetMetadata={assetMetadata} ... />
```

`assetMetadata` is `assetNode.metadataEntries` — the definition bucket, and nothing else.
The `assetMetadata.length > 0` guard confirms the archive's *"assets without definition metadata have no Metadata accordion at all."*

A `TableSchema` entry here renders as a **`[Show Table Schema]` dialog link** rather than inline — `metadata/MetadataEntry.tsx:250-271` — unless `expandSmallValues` is set *and* the schema has fewer than 5 columns.
Confirms the archive's "link that pops up the full table".

#### Metadata plots ← materialization bucket, numeric only — CONFIRMED

`AssetSidebarActivitySummary.tsx` renders `AssetPartitionMetadataPlots` / `AssetTimeMetadataPlots`, and `AssetEventMetadataPlots.tsx:29-30` filters to `__typename === 'MaterializationEvent' || 'ObservationEvent'`, with the empty state reading *"Include numeric metadata entries in your materializations and observations…"*.
Confirms both the bucket and the archive's aside that table-typed metadata is not plotted.

#### Catalog Metadata section — CORRECTED

`assets/overview/AssetNodeOverview.tsx:242-263` renders the Metadata section as `<AssetEventMetadataEntriesTable ... definitionMetadata={assetMetadata} event={materialization || observation} hideEntriesShownOnOverview />`.

Two corrections to the archive:

1. **It is not materialization-only.**
   It merges definition and event metadata (`AssetEventMetadataEntriesTable.tsx:148`), tagging each row with a source icon (`'materialization'` / `'observation'` / `'asset'`) and a "Loaded … / Materialized …" tooltip.
   On a key collision the event row wins.
2. **`dagster/column_schema` is explicitly excluded from it.** `AssetEventMetadataEntriesTable.tsx:328-352`:

   ```ts
   if (
     hideEntriesShownOnOverview &&
     (isCanonicalColumnSchemaEntry(entry) ||
       isCanonicalRowCountMetadataEntry(entry) ||
       isCanonicalTableNameEntry(entry) ||
       isCanonicalUriEntry(entry))
   ) {
     return true;
   }
   ```

   `hideEntriesShownOnOverview` is passed on the Catalog Overview page.
   So on the asset detail Overview tab, the column schema appears **only** in the Columns section — never as a row in the Metadata section.
   `dagster/column_lineage` and `dagster/code_references` are hidden unconditionally on every page.

#### What a human would need to click to close the remaining gap

Run `dagster dev` against a project containing one asset with a distinguishable `dagster/column_schema` in each of the three buckets, then check:

1. **Asset lineage graph → select node → right sidebar.**
   Does a **Type** accordion appear, and does it show the *output-type* schema as a filterable table with constraint pills?
   Does a **Metadata** accordion appear with a `[Show Table Schema]` link carrying the *definition* schema?
2. **Asset catalog → asset → Overview tab.**
   Confirm the **Columns** section shows the *materialization* schema (with definition descriptions overlaid), and that the **Metadata** section does *not* list a `dagster/column_schema` row.
3. **Overview → Metadata section → "Plots" toggle.**
   Confirm only numeric entries plot.
4. **Column-name casing** — see claim 4.
   Emit a definition schema and a materialization schema that both contain a column named `CustomerID` and confirm whether the Columns section displays `customerid`.

---

## Claim 4 — Catalog Columns completeness by bucket

Testing which bucket, or combination, produces a complete Catalog Columns view.

### Verdict: CHANGED

The Catalog Overview "Columns" section is built by `js_modules/ui-core/src/assets/buildConsolidatedColumnSchema.tsx`, called from `AssetNodeOverview.tsx:100-104` as `buildConsolidatedColumnSchema({materialization, definition: assetNode, definitionLoadTimestamp})`.
Full body at tag `1.13.16`:

```ts
  const materializationTableSchema = materialization?.metadataEntries?.find(
    isCanonicalColumnSchemaEntry,
  );
  const definitionTableSchema = definition?.metadataEntries?.find(isCanonicalColumnSchemaEntry);
  let tableSchema = materializationTableSchema ?? definitionTableSchema;

  // Merge the descriptions from the definition table schema with the materialization table schema
  if (materializationTableSchema && definitionTableSchema) {
    const definitionsTableColumnsByName = Object.fromEntries(
      definitionTableSchema.schema.columns.map((column) => [column.name.toLowerCase(), column]),
    );
    const mergedColumns = materializationTableSchema.schema.columns.map((column) => {
      const definitionsCol = definitionsTableColumnsByName[column.name.toLowerCase()];
      const description = definitionsCol?.description || column.description;
      const tags = definitionsCol?.tags || column.tags;
      return {...column, name: column.name.toLowerCase(), description, tags};
    });
    tableSchema = {
      ...materializationTableSchema,
      schema: {...materializationTableSchema.schema, columns: mergedColumns},
    };
  }
```

Four departures from the archive:

1. **The output-type bucket contributes nothing.**
   The function reads only `materialization.metadataEntries` and `assetNode.metadataEntries` (definition).
   The archive's "the output-type bucket shows dtypes only" is wrong for this view — `dagster_type=` metadata is invisible in the Catalog Columns section entirely.
   It only ever reaches the Lineage sidebar's Type accordion.
2. **It is winner-takes-base, not additive.**
   If a materialization schema exists it is the base and the definition schema is consulted *only* for `description` and `tags`.
   `type`, `constraints.nullable`, `constraints.unique`, and `constraints.other` always come from the base.
3. **A definition-only schema is fully rendered.**
   With no materialization schema, `tableSchema = definitionTableSchema` unchanged — so constraint pills, table-level `constraints.other` (rendered above the table, `TableSchema.tsx:92-101`), and dtypes all appear.
   The archive's "only the materialization bucket is complete" therefore does not hold: a definition-only schema is complete on its own.
   The gap only opens once *both* exist, at which point the materialization schema silently discards the definition's constraints.
4. **Column names are lowercased whenever both schemas are present** (`name: column.name.toLowerCase()`), and matching is case-insensitive.
   A dataframely schema with a mixed-case column name will render as `customerid` in the Columns section if — and only if — the same key is emitted in both buckets.
   Sharp, undocumented, and easy to trip over if the design decides to write the schema to both buckets "for safety".

Column-level rendering itself (`metadata/TableSchema.tsx:113-142`) matches the archive: name + tag chips, `TypeTag`, `non-nullable` when `!constraints.nullable`, `unique` when `constraints.unique`, then each `constraints.other` string as an `ArbitraryConstraintTag` (**truncated at 30 characters** with a tooltip, `MAX_CONSTRAINT_TAG_CHARS = 30`) — relevant, since `@dy.rule()` expressions and `check: <expr>` strings will routinely exceed 30 chars.

---

## Claim 5 — `dy.DataFrame[Schema]` cannot be an output annotation

Testing whether dataframely's generic alias can serve as a Dagster output annotation.

### Verdict: CONFIRMED

Reproduced on dataframely 3.0.0 / dagster 1.13.16:

```text
dataframely 3.0.0
type(T) = <class 'typing._GenericAlias'>
isinstance(T, type) = False
typing._GenericAlias? True
make_python_type_usable_as_dagster_type -> ParameterCheckError
    Param "python_type" is not a type. Got dataframely._typing.DataFrame[__main__.S]
    which is type <class 'typing._GenericAlias'>.
```

And as a return annotation:

```text
@dg.asset
def bad() -> dy.DataFrame[S]: ...
-> DagsterInvalidDefinitionError: Problem using type
   'dataframely._typing.DataFrame[__main__.S]' from return type annotation,
   correct the issue or explicitly set the dagster_type via Out().
```

The archive's third sub-point also holds: a bare `-> dy.DataFrame` *is* accepted, resolving to a `TypeHintInferredDagsterType` that carries no schema.

All three legs of the claim stand.
Note this is a property of `typing._GenericAlias`, not of dagster's version — nothing in 1.13 relaxes `check.is_callable`/`check.class_param` here.

---

## Survey: what is new or newly relevant in 1.13 for publishing a schema

Mechanisms beyond the five claims above that bear on getting a schema into the UI.

### `context.add_asset_metadata()` — the top of the materialization precedence chain

Covered under claim 2.
Stable, public, asset-scoped, and partition-scoped (`add_asset_metadata(metadata, asset_key=..., partition_key=...)`).
It is the only in-body mechanism that beats a third-party IO manager, and it is partition-aware — directly relevant to the map's open "Partitioned assets" fog, since a per-partition schema or row count can be attached without touching the IO manager.

### `OutputContext.definition_metadata` / `OutputContext.asset_spec`

An IO manager can read the asset's full definition metadata *and* the whole `AssetSpec` (metadata, kinds, tags, partitions_def) inside `handle_output`.
Verified by reproduction.
In `load_input`, the upstream equivalent is `context.upstream_output.definition_metadata`.
This is a live alternative to the archive's `dagster_type=`-as-schema-carrier: put the schema (or a reference to it) in definition metadata, and the IO manager recovers it at write time without a custom `DagsterType`.
`OutputContext.metadata` is deprecated in favour of `definition_metadata`.

### `TableMetadataSet` — the typed way to write these keys

`from dagster._core.definitions.metadata import TableMetadataSet` (not exported from the top-level `dagster` namespace as of 1.13.16).
Fields at `_core/definitions/metadata/metadata_set.py:174-195`:

| Field | Emitted key |
| ----- | ----------- |
| `column_schema: TableSchema \| None` | `dagster/column_schema` |
| `column_lineage: TableColumnLineage \| None` | `dagster/column_lineage` |
| `row_count: int \| None` | `dagster/row_count` |
| `partition_row_count: int \| None` | `dagster/partition_row_count` |
| `table_name: str \| None` | `dagster/table_name` (legacy alias `dagster/relation_identifier`) |
| `storage_kind: str \| None` | `dagster/storage_kind` — added during the 1.13 line |

It splats: `MaterializeResult(metadata={**TableMetadataSet(column_schema=...), ...})`.
Using it over a bare string key buys type-checking and forward-compatible key renames (it already handles the `relation_identifier` → `table_name` rename).

### Metadata keys the 1.13.16 UI treats specially

From `metadata/TableSchema.tsx:32-52` and `metadata/MetadataEntry.tsx`:

| Key | UI treatment |
| --- | ------------ |
| `dagster/column_schema` | Columns section (Catalog Overview); `[Show Table Schema]` dialog elsewhere; **hidden** from the Overview Metadata table |
| `dagster/column_lineage` | powers column lineage; **never** shown as a metadata row, on any page |
| `dagster/code_references` | powers "Open in editor"; **never** shown as a metadata row |
| `dagster/table_name` / `dagster/relation_identifier` | storage address in the Definition section; hidden from the Overview Metadata table |
| `dagster/uri` | storage address; hidden from the Overview Metadata table |
| `dagster/row_count` | row-count stat on Overview; hidden from the Overview Metadata table |
| `dagster/storage_kind` | storage-kind chip on the asset node |
| `dagster/kind/*` (tags, not metadata) | kind chips on the asset node — `AssetSpec(kinds={"polars"})` produces `dagster/kind/polars` |

`AssetNode.storageAddress` is resolved server-side from **definition** metadata only (`asset_graph.py:1245`, via `TableMetadataSet.extract_storage_address`) — so `dagster/table_name` and `dagster/storage_kind` must go in the definition bucket to drive that surface.

### `AssetSpec` / `AssetCheckSpec` metadata surfaces

- `AssetSpec(key, deps, description, metadata, skippable, group_name, code_version,
  automation_condition, owners, tags, kinds, partitions_def, freshness_policy, is_virtual)` —
  `metadata` is the definition bucket.
- `AssetSpec.merge_attributes(...)` / `AssetSpec.replace_attributes(...)` and the module-level `dg.map_asset_specs(...)` give a clean, non-invasive way to *decorate existing assets* with a schema.
  This is a much lighter front door than the archive's `@dd.asset` `__annotations__` rewriting: a `dagster_dataframely` helper could be a `map_asset_specs` transform over anyone's assets.
- `AssetCheckSpec(name, asset, description, additional_deps, blocking, metadata,
  automation_condition, partitions_def)` — carries definition-time `metadata`, and
  `partitions_def` is new in the 1.13 line (**partitioned asset checks** shipped in 1.13.0).
- `AssetCheckResult(passed, asset_key, check_name, metadata, severity, description)` — runtime check metadata.
  Nothing in 1.13 gives check metadata a *special* `dagster/column_schema` rendering; a `TableSchema` there renders as a generic `[Show Table Schema]` entry.

### Table-schema rendering changes

No structural change to `TableSchema`/`TableColumn`/`TableColumnConstraints` between 1.11 and 1.13.
The relevant 1.13-era items are:

- `dagster/storage_kind` added to `TableMetadataSet`.
- Row-count metadata now displayed even when zero; and a fix so `dagster/row_count` set by an
  *observation* (not a materialization) shows on the Overview page.
- The BI integrations now auto-emit `dagster/table_name` and `dagster/storage_kind`, which is why
  those keys gained dedicated (and hidden-from-table) UI treatment.

### Components / `dg` CLI bearing on this

- **`MetadataChecksComponent`** — `dagster.components.lib.metadata_checks_component.MetadataChecksComponent`, shipped in core.
  Declarative YAML for two check factories:
  - `type: column_schema_change` → `dg.build_column_schema_change_checks(assets=[...], severity=...)`
  - `type: metadata_bounds` → `dg.build_metadata_bounds_checks(...)`

  `build_column_schema_change_checks` (`@beta`) compares `TableMetadataSet.extract(metadata).column_schema` across an asset's two most recent **materializations** and reports added / removed / retyped columns.
  It reads the **materialization bucket only** — definition metadata will not satisfy it.
  This is free schema-drift detection for any asset the integration causes to emit `dagster/column_schema` at materialization time, and it is a concrete argument for putting the schema in the materialization bucket even if it is statically known.

- **`AssetAttributesModel`** (`dagster.components.AssetAttributesModel`) has a `metadata` field alongside `deps`, `description`, `group_name`, `owners`, `tags`, `kinds`, `automation_condition`, `partitions_def`, `freshness_policy`, `key`, `key_prefix`.
  Together with component `post_processing`, this is the YAML-side route for attaching definition-bucket metadata to assets produced by *any* component.
  If `dagster-dataframely` wants a Components story, exposing a resolvable "schema" attribute that lowers to `metadata` is the shape the ecosystem expects.

- `dg` itself (`dagster-dg-cli`) is **not installed** in this venv; dagster registers itself as a
  `dagster_dg_cli.registry_modules` entry point, so any component this package ships would be
  discoverable via its own entry point in `pyproject.toml`.

### Open questions this ticket did not settle

- Anything requiring the running UI (see the click-list under claim 3).
- Whether the Columns-section lowercasing is a bug or intentional.
  It is worth an upstream issue either way, and worth avoiding by **not** writing `dagster/column_schema` to both the definition and materialization buckets on the same asset.
- Whether `dagster-polars`' IO managers emit `dagster/column_schema` at all on the current release (`dagster-polars` is not installed; the archive asserts "every dagster-polars one does").
  Given claim 2's correction the answer matters less than it did, but it still determines whether a collision exists to lose.

---

## Reproduction scripts

Throwaway, kept out of the repo, under `/tmp/ddresearch/`: `repro_buckets.py`, `repro2.py` (precedence), `repro3.py` (three distinct stores), `repro4.py` (`OutputContext.definition_metadata` / `asset_spec`).
UI sources fetched at tag `1.13.16` with `gh api "repos/dagster-io/dagster/contents/<path>?ref=1.13.16" --jq .content | base64 -d`.
