# Front-door comparison (wayfinder #11)

**Question.**
What is the smallest thing that attaches a schema's metadata to an asset and generates its check specs, while composing with the package's IO managers and any `@dg.asset` shape?

**Verdict (settled — [#11](https://github.com/ozanozbeker/dagster-dataframely/issues/11)).**
**Shape A — shipped as `@dataframely_asset` — is the front door; the kit it wraps is public API (which *is* shape C).**
The door covers the standard shapes with one declaration; the public parts cover every exotic shape the door doesn't.
Shape B is rejected.
A Dagster Component is rejected as a *front door* but remains buildable on top of A later.

The name is `dataframely_asset`, not the prototyped `@dd.asset`: `dd.` sits one keystroke from `dg.` in the same call-site position, and the first-party precedent (`@dbt_assets`) uses a distinct, greppable name.
Collections get their own decorator when [#13](https://github.com/ozanozbeker/dagster-dataframely/issues/13) is worked — different underpinnings per [#4](https://github.com/ozanozbeker/dagster-dataframely/issues/4), so not a mode switch on this one.
`door_typed.py` is the door as it ships; the `shape_*.py` files preserve the comparison that chose it.

## The call sites (quarantine variant, where the shapes diverge most)

### Shape A — one declaration

```python
@dataframely_asset(schema=Orders, quarantine=dg.AssetOut(), group_name="sales")
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return transform(raw_orders)
```

Schema stated **once**.
Asset name stated **zero** extra times.
The failure policy is the structural presence of `quarantine=` (#6/#9).
Everything else is `@dg.multi_asset`'s own vocabulary, passed through verbatim.

### Shape B — layered decorator (rejected)

```python
@dg.asset(
    check_specs=dd.check_specs(Orders, asset="orders"),  # schema #1, name #1
    metadata=dd.schema_metadata(Orders),  # schema #2
)
@dd.validated(Orders)  # schema #3
def orders() -> pl.DataFrame: ...
```

Three statements of the schema with nothing detecting drift between them — and **the quarantine cannot exist**: `@dg.asset` has one out, so declaring the second asset (#6's consent) means abandoning the shape entirely.
Flipping the decorator order raises at import with a misleading error (`Expected Tuple annotation for multiple outputs...`).
A door that dissolves exactly when the failure policy gets interesting is not a door.

### Shape C — the kit, user-wired

```python
@dg.multi_asset(
    outs={
        "orders": dg.AssetOut(is_required=False, metadata=dd.schema_metadata(Orders)),
        "orders_quarantine": dg.AssetOut(
            is_required=False,
            metadata={"dagster/column_schema": dd.quarantine_table_schema(Orders)},
        ),
    },
    check_specs=dd.check_specs(Orders, asset="orders"),
)
def orders(context):
    yield from dd.process(
        Orders,
        transform(),
        context=context,
        good_out="orders",
        quarantine_out="orders_quarantine",
    )
```

Works — verified: both outs materialize, checks WARN, run green.
But the schema is stated **4×** and the asset name **5×**, the user must know to set `is_required=False` on both outs (forget it and #6's skip semantics silently break), and the `<asset>_quarantine` sibling default (#9) doesn't exist — every name is hand-typed.
This is the right *floor*, not the right *door*.

## What execution proved (dagster 1.13.16, dataframely 3.0.0, in-process)

| case | expected (#6/#7) | observed |
| --- | --- | --- |
| clean, abort policy | green, quarantine absent | ✅ 14 checks pass, only good out materialized |
| mixed, quarantine declared | both outs, WARN, run green | ✅ 3 checks FAIL at WARN, both outs materialized |
| mixed, abort policy | ERROR checks, raise, nothing kept | ✅ 3 checks FAIL at ERROR, zero materializations |
| hopeless, quarantine declared | quarantine only, run fails | ✅ quarantine materialized, good out skipped, ERROR |
| wrong dtype | gate fails before filter | ✅ only `dy_schema__dtypes` evaluates (1 of 14), ERROR, nothing materialized |

Definition-time facts, verified by construction:

- **Upstream ins bind through the wrapper** — `functools.wraps` preserves the signature Dagster inspects, so `def orders(raw_orders: pl.DataFrame)` gets its upstream with no `ins=` restatement.
  This was shape A's feasibility risk; it holds.
- **Key handling**: the door constructs the good key **once** and hands it to both the out and the specs — `key_prefix="warehouse"` yields `warehouse/orders` with aligned checks, and the default quarantine sibling inherits the prefix (`warehouse/orders_quarantine`) per #9, while `AssetOut(key=)` overrides completely (`quarantine/orders`).
  No re-derivation of Dagster's resolution (the archive's `_asset_key` sin never appears).
- **Both loud errors fire with culprits named**: `amount__min` (rule) vs `amount|min`
  (column) collision, and a user column inside the reserved `dy_` namespace.
- **Env tier works**: `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY=column` flips the same
  schema to `dy_col__*` + `dy_schema__rules` specs.
- **Shape C's key desync is loud, not silent** — Dagster rejects a check spec whose key doesn't match the out (`Invalid asset key ... in check spec`).
  C's cost is repetition and missing defaults, not hidden breakage. (Schema drift between two seams — passing different schemas — remains undetected in B and C.)
- **Context annotation trap**: under `from __future__ import annotations`, a `context: dg.AssetExecutionContext` parameter is *rejected* by Dagster (its qualified-name check sees the literal string).
  The prototype now runs without the future import and shape C demonstrates the working spelling; the trap remains a documented user-side hazard, and shape A's call sites structurally avoid it — the wrapper gets context via `AssetExecutionContext.get()` and user functions never take one.

## Scorecard

| criterion (source) | A `@dataframely_asset` | B layered | C helpers |
| --- | --- | --- | --- |
| One distinction = failure policy (#6) | ✅ `quarantine=` presence | ❌ inexpressible | ⚠️ hand-wired outs |
| `quarantine=dg.AssetOut(...)` shape (#9) | ✅ | ❌ | ⚠️ user builds dict |
| Sibling default + prefix inheritance (#9) | ✅ | — | ❌ hand-typed |
| Check specs derived statically (#8) | ✅ | ✅ (restated) | ✅ (restated) |
| Collision/namespace errors before Dagster's (#8/#9) | ✅ | ✅ | ✅ |
| Schema stated once | ✅ 1× | ❌ 3× | ❌ 4× |
| Plain `pl.DataFrame` annotation, no rewriting (#2/#14) | ✅ | ✅ | ✅ |
| Composes with any IO manager (metadata channel, #14) | ✅ | ✅ | ✅ |
| Any `@dg.asset` shape reachable | ⚠️ via public kit | ❌ | ✅ by definition |
| Smallest package surface | ⚠️ decorator + kit | kit + wrapper | kit only |
| At home next to the North Star | ✅ see below | ❌ | ⚠️ floor, not door |

**On the North Star test.** dagster-pandera answered with a single factory because its entire output is *one artifact* (`DagsterType`) slotting into an existing parameter.
Our front door coordinates **four artifacts** — outs, check specs, definition metadata, and the wrapped runtime — and no existing `@dg.asset` parameter accepts that bundle.
The smallest container that holds all four is a decorator.
The first-party precedent is `@dbt_assets`: a package-owned decorator wrapping a multi-asset.
A also passes the archive-sins test: no `__annotations__` rewriting, no `_asset_key` re-derivation, no `DagsterType`, no IO-manager policing.

## Rejected: Dagster Component (reasoned, not prototyped)

A Component is a YAML-driven definitions builder aimed at low-code surfaces.
The front door's center is a *user-authored Python transform* plus a *Python schema class* — a Component could only reference both by import path, splitting the call site across two files for an audience that is already writing Python.
It is not the smallest thing; it is a plausible *additive layer on top of the door* if a config-driven surface is ever wanted. (`dagster_type=` and IO-manager-only were already ruled out by #3/#7/#12/#14.)

## Sub-decisions this ticket owns (settled)

1. **Env-var convention**: `DAGSTER_DATAFRAMELY_<SETTING>` — e.g. `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY`.
   Fully qualified per #10's display-key/machine-key rule (env vars are machine surface); collision-proof where a `DY_*` prefix is not.
   The tier chain validates on resolve: a value outside the setting's vocabulary — from any tier — raises a loud `InvalidSettingError` naming the tiers, rather than flowing through as a silent misconfiguration.
2. **Good half handed on eager**: the wrapper collects the re-wrapped `LazyFrame` before materializing.
   `filter` already collected internally (map fact), and `dagster/row_count` needs the length regardless; the user's transform may still accept/return either frame type.
   This settles #6's open return-type call and retires the last of the lazy fog.
   The collect is *dataframely's*, not ours — a lazy transform still optimizes and executes as one plan, so the eager hand-off costs nothing extra, and `sink_*` has no streaming path left to preserve.
   Sink-first-then-validate was considered and rejected: it inverts #6's write order, so a failed run would have already overwritten the last-known-good snapshot — the same hazard that killed `block` as a policy rung.
   **README obligation**: state plainly that validation materializes and sinks are not on the supported path.
   Positioning: the package's habitat is post-landing transformation (bronze→silver→gold), where the data is already on your side; ingestion-scale and larger-than-memory work belongs to other tools, or to future upstream streaming validation.
3. **`dy_csv__encoding` lands in docs + a CSV-manager log line at encode/read time.**
   This is a *transparency* obligation from #14 ("visible, never silent"), not a mechanism: the CSV round-trip needs nothing stored per-asset, because the schema carrier (#14/#10) already reaches the IO manager on read and *is* the decode key — the codec table is fixed package behaviour.
   Definition metadata is the wrong home because the decorator cannot know which IO manager will bind (a Parquet-bound asset would carry a false CSV claim); materialization metadata was already barred by #10's rule (a).
   A `context.log.info` naming the encoded columns is visible-never-silent at exactly the moment it applies, and logs are not metadata, so no rule is touched.
4. **Granularity bucket names**: `column` mode emits `dy_col__<column>` per rule-bearing column plus `dy_schema__rules` for multi-column rules (default placement, #8); `schema` mode emits the single `dy_schema__rules`.
   Both live under the reserved sibling-prefix convention (#9).
   The multi-column bucket cannot be `dy_col__schema`: a user column legitimately named `schema` would collide with it.
5. **The door's signature is explicit and fully typed — no `**kwargs`.**
   Door-owned: `schema=`, `quarantine=`, `check_granularity=`, `multi_column_rules=`, `key_prefix=`.
   Forwarded to `@dg.multi_asset`, each declared with its runtime-real type: `name`, `ins`, `deps`, `description`, `config_schema`, `required_resource_keys`, `partitions_def`, `hooks`, `backfill_policy`, `op_tags`, `resource_defs`, `group_name`, `retry_policy`, `code_version`, `pool`.
   A `**kwargs` passthrough (the archive's shape, and the first prototype's) erases autocomplete and type checking; with the explicit list, `group_nme="sales"` is a **static** error (verified: pyrefly `unexpected-keyword`) rather than an import-time `TypeError`.
   The owned surfaces `outs`/`check_specs`/`specs` simply do not exist as parameters — statically unpassable, so no runtime guard is needed — and `can_subset` is deliberately absent (#4: subsetting executes but saves nothing).
   Cost: the door lags a new `multi_asset` parameter until the package adds it; the public kit is the escape hatch, since raw `@dg.multi_asset` always has everything.
6. **The package carries no `from __future__ import annotations`.**
   At a 3.12 floor PEP 563 buys only unquoted forward references, while turning every user-facing annotation into a string for Dagster's runtime introspection — the context-annotation trap above is PEP 563's doing — and on 3.14+ it would opt the package out of PEP 649's native lazy annotations.
   The counter-trap is real and was hit in this prototype: without PEP 563, typing-only names like `dg.CoercibleToAssetKey` (absent at runtime) cannot appear unquoted in annotations, so the package uses runtime-real spellings (`str | dg.AssetKey`) or quoted forward references instead.
7. **Signature drift is a pin-and-assert, checked in CI.**
   The explicit forwarded list in (5) is the one thing that can silently fall out of step with Dagster, so a test asserts it in **both directions** against `inspect.signature(dg.multi_asset)`: every forwarded parameter still exists (catches renames and removals), and Dagster has no parameter the door does not know about (catches additions, so new features are noticed rather than silently lagged).
   Both pass on 1.13.16.
   This is the fourth pin-and-assert on the map, joining `Schema._validation_rules`, `FailureInfo.details()`, and the `ObjectMetadataValue` import path — it amends the version-compatibility fog patch rather than opening anything new.
   Delivery is a scheduled CI job running the suite against latest dependencies (the normal per-PR job runs against `uv.lock`), configured to fail on Dagster deprecation warnings — the earliest tripwire, given Dagster's post-1.0 deprecate-before-remove discipline.

## Known deltas between prototype and spec

Sketched, not implemented, all pre-decided elsewhere: opt-in stats families (`stats/{numeric,...}` from #10), bounded per-rule failure samples (off by default per #8 and #14), the full pill/description renderer ladder (#10), and Collection support (dict-keyed `quarantine=`, #13).
None of them press on the shape choice.
