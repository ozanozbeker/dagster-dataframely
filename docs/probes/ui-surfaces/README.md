# UI-surface probe

A set of controlled specimens that render the *same* logical orders schema through every candidate mechanism, so a human can compare Dagster UI surfaces side by side.

Built while working [#10](https://github.com/ozanozbeker/dagster-dataframely/issues/10), which asks which UI surfaces the schema must reach and at what fidelity.
It closes the click-list that research [#5](https://github.com/ozanozbeker/dagster-dataframely/issues/5) left open — everything #5 marked UNVERIFIABLE needed a running webserver, which this provides.

**Kept deliberately.**
The original charting note said prototypes are throwaway; this one is not.
It is the basis for a "how this package compares" section in the project's own docs, so the prior-art comparison can be regenerated and screenshotted rather than described from memory.

## Running it

The probe has its own dependency set — it installs `dagster-pandera` and `dagster-polars[patito]`, which are **not** dependencies of this package and must not leak into the repo's `.venv`.

```bash
cd docs/probes/ui-surfaces
uv venv --python 3.12
uv pip install -r pyproject.toml

export DAGSTER_HOME="$PWD/dagster_home" && mkdir -p "$DAGSTER_HOME"
export DISABLE_PANDERA_IMPORT_WARNING=True

# Pre-materialize so every catalog surface is populated before you look.
./.venv/bin/dagster asset materialize --select '*' -f probe.py
./.venv/bin/dagster dev -f probe.py -p 3333
```

## The specimens

Assets are grouped so they sort in the UI.

| Group | Asset | What it isolates |
| ----- | ----- | ---------------- |
| `a_prior_art` | `pandera_orders` | dagster-pandera: schema as a `DagsterType`. Lineage **Type** accordion only. |
| | `patito_orders` | dagster-polars[patito]: `DagsterType` metadata *and* a dagster-polars materialization schema. |
| `b_bucket` | `bucket_definition_only` | Definition bucket alone. |
| | `bucket_materialization_only` | Materialization bucket alone, via `context.add_asset_metadata()`. |
| | `bucket_both` | Both buckets, *identical* schema — isolates lowercasing from schema content. |
| | `bucket_defn_plus_polars_iom` | Our definition schema + a real `PolarsParquetIOManager`. The degraded case. |
| `c_pills` | `pills_rule_names` | `constraints.other` as check names: `dy_rule__amount__min`. |
| | `pills_human` | As operator strings: `>= 0`. The shape dagster-pandera ships. |
| | `pills_raw_expr` | As raw polars expression reprs. Exercises the truncation limit. |
| | `pills_none` | `nullable`/`unique` only. The shape dagster-polars[patito] ships. |
| `d_checks` | `orders` + `orders_quarantine` | Per-rule checks ([#8](https://github.com/ozanozbeker/dagster-dataframely/issues/8)) beside the quarantine frame ([#9](https://github.com/ozanozbeker/dagster-dataframely/issues/9)). |
| `e_ruletext` | `ruletext_unified` | One renderer, one fallback ladder: constraint → docstring → rule name. Pills carry the value and truncate. **Chosen.** |
| | `ruletext_per_surface` | Pills degrade on purpose to fit the cap (`matches regex`, `in 3 values`); the value moves to a prose check description. Rejected. |
| `f_primarykey` | `pk_table` / `pk_column` / `pk_both` | The first three candidates. All rejected. |
| | `pk_conditional_single` / `_composite` | Table-level row only when the key is composite. Rejected — `composite primary key (…)` ate the whole pill. |
| | `pk_top_single` / `_composite` | **Chosen.** `PK: order_id, customer_id` stated once, table-level, no per-column pill, and `unique` deliberately not claimed. |
| `g_extras` | `extras_keys_bare` / `_dy` / `_full` | Three metadata key-naming schemes side by side, plus the `dagster_dataframely/schema` carrier row via `ObjectMetadataValue`. |
| | `extras_md_variants` | The same table five ways — markdown with/without trailing newlines, and `MetadataValue.table` — isolating why markdown renders as printed text. |
| | `extras_stats_iso` / `_human` | All four stats families as `MetadataValue.table`. **Chosen: `_human`**, with durations as `8d` / `1m 30s`. |
| | `extras_one_stats_table` | The same numbers merged into one row, for a density comparison. Rejected. |

Groups `e`–`g` use a second schema, `OrdersRich`, which deliberately contains one instance of every hard-to-render rule shape: a long regex, a `@dy.rule()` with a docstring and one without, a named `check={...}` and an anonymous `check=lambda`, `is_in`, and a composite primary key.

## What it established

Observed directly in the running UI on dagster 1.13.16, and not obtainable any other way:

- **`dagster-pandera` has no Catalog Columns tab at all**, and no schema anywhere in the catalog view — only Description, Metadata (`path`), and Lineage.
  Visual confirmation of #5's claim 4: the output-type bucket feeds the Lineage **Type** accordion and nothing else.
- **Lowercasing is caused by both buckets being populated, whoever populates them.** `bucket_both` carries two *identical* schemas that are both ours, and its Catalog Columns tab still lowercases `OrderID` / `CustomerID`.
  It is not a dagster-polars artifact, and contesting the key cannot fix it.
- **Pill loss is a separate effect, caused by whose schema is the base.** `bucket_both` keeps every
  pill (base = ours); `bucket_defn_plus_polars_iom` loses them (base = dagster-polars', which is
  constraint-free) while keeping dtypes, descriptions and column tags.
- **The Lineage Metadata accordion is definition-only and never merged** — full fidelity, correct casing, on every asset including the degraded one.
  The Latest-materialization accordion shows the materialization bucket raw, also with original casing.
- **Constraint pills truncate at ~26 rendered characters**, but hovering shows the full value in a tooltip.
  The same is true of long asset-check names in the catalog Checks tab; the check list in the lineage accordion gives each rule its own row and comfortably fits ~120 characters.
- **`dy_rule__<col>__<rule>` reads poorly as a constraint pill.**
  It tells you a minimum exists but not what it is.
  Operator strings (`pills_human`) read far better on this surface.
- **`MetadataValue.table` renders as a full-featured HTML table; `MetadataValue.md` renders as printed text.**
  `extras_md_variants` rules out the markdown source as the cause — trailing newlines, blank lines and a heading all render the same, and the polars output is clean GFM with zero non-ASCII characters.
- **Sibling rules rendering in different voices is a defect, not a feature.** `A cancelled order must carry a zero amount.` beside `priority_orders_ship_domestic` — same surface, different register, for a reason invisible from the UI.
  This forced the rule that the name is the constant and the docstring only ever reaches the check description.
- **Marking a composite-primary-key column `unique` is a false statement about the data.** `{"a": ["x","x","y"], "b": [1,2,1]}` passes a composite PK — only the tuple is unique.
  The probe asserted this incorrectly until `f_primarykey` was built out.
