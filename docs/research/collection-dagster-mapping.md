# How a `dy.Collection` maps onto the Dagster asset model

Research for [#4](https://github.com/ozanozbeker/dagster-dataframely/issues/4).
Verified against the installed `dagster 1.13.16`, `dataframely 3.0.0`, `polars 1.43.2`, plus `dagster-polars 0.27.12` (downloaded to `/tmp` and imported on the same interpreter).

Every claim below is tagged **[RAN]** (verified by constructing and executing the thing) or **[READ]** (inferred from library source or docs).
Scratch scripts lived in `/tmp/dydg/`.

---

## Answer / Recommendation

**Collections do fit — but only in one shape, and the fit is narrower than it looks.**

Recommended shape: **one `@dg.multi_asset` with `outs=`, one asset key per member, plus one asset check per (member × collection filter) pair.**
This is candidate shape A, with the specific correction that `specs=` does not work and `outs=` is required.

The three questions the ticket asked, answered:

| Question | Answer |
| --- | --- |
| Where does a cross-member rule report? | **Against every non-ignored member, once each.** This is dataframely's own answer, not a design choice we get to make — `Collection.filter` appends a rule column named after the filter to *every* non-ignored member's `FailureInfo`, with per-member row counts. A rule spanning `orders` and `customers` therefore becomes **two** checks: `customers/<rule>` and `orders/<rule>`. **[RAN]** |
| How does `CollectionFilterResult` map to per-asset results? | **Cleanly, and this is the strongest argument for shape A.** `CollectionFilterResult` is `(result: C, failure: dict[str, FailureInfo])`. The `failure` dict is *already keyed by member name*. Map member name → asset key and you get: `result.<member>` → that asset's output, `failure[<member>]` → that asset's quarantine artifact and check metadata. No re-derivation, no invention. **[RAN]** |
| How does `Collection.write_parquet`'s directory layout interact with IO manager paths? | **In the recommended shape it never comes up** — each member goes through the ordinary per-asset-key IO manager and `Collection.write_parquet` is never called. It only matters for shape C, where it composes *perfectly* with `UPathIOManager` if `extension = None`, and collides *destructively* with any `.parquet` extension (including `dagster-polars`' own). **[RAN]** |

Three things should be **ruled out**, with evidence below:

1. **Shape B (N assets + a `multi_asset_check` carrying the cross-member rules) cannot deliver the collection guarantee.**
   Checks observe; `Collection.filter` mutates.
   A check can report that `customers` contains rows the collection invariant forbids, but the rows stay on disk.
2. **Shape C (a single asset whose value *is* the Collection) works mechanically but collapses N tables into one asset key** — no per-table lineage, no downstream dep on just `orders` — and **cannot be carried by `dagster-polars` at all** (two independent hard failures, below).
   Since `dagster-polars` is a hard dependency and upstreaming is the North Star, this is disqualifying rather than merely awkward.
3. **`can_subset=True` on a Collection multi-asset is a lie** and should not be offered.
   It executes, but it produces mutually inconsistent persisted members.

And two things should be scoped **out of v1**:

4. **Partitioned collections.**
   Cross-member rules run per-partition.
   If an entity's rows span partitions, the rule quarantines rows that are valid globally.
   Sound only when the collection's entities are partition-local — a constraint we cannot verify for the user.
5. **The N-times-quarantine question.**
   One logically-rejected entity produces rows in *N* members' `FailureInfo`.
   That is correct (each member's rows really were removed) but it means "rows quarantined" is not an entity count, and any UI summing it across assets double-counts.
   Needs a DX decision, not more research.

**Bottom line for the map:** Collection support is *additive* — nothing in the `Schema`-level design depends on it.
If the map wants v1 small, deferring Collections costs nothing structural.
But if it is in scope, shape A is the answer and the other three candidates should be closed.

---

## The two facts that decide everything

Two hands-on findings constrain every candidate shape.
Everything below follows from them.

### Fact 1: `Collection.filter` is atomic across all required members **[RAN]**

`filter` and `validate` are classmethods taking a mapping of *all* members.
Omit a required member and you get a hard error before any work happens:

```python
ShopData.filter({"customers": customers})
# ValueError: Input misses 1 required members: orders.
```

Source: `dataframely/collection/collection.py::_validate_input_keys` (called first in both `filter` and `validate`).
Note `required_members()` = all non-optional members, *including* members marked `ignored_in_filters=True`.
**[READ + RAN]**

There is no "validate just this member against the collection" API.
Every collection-level operation needs the whole collection in hand.

### Fact 2: row-level failures **cascade between members** **[RAN]**

This is the finding that rules out treating members as independent assets.
A schema-level failure in `orders` silently removes rows from `customers`:

```python
class ShopData(dy.Collection):
    customers: dy.LazyFrame[CustomerSchema]  # customer_id PK
    orders: dy.LazyFrame[OrderSchema]  # (customer_id, order_id) PK, amount min=0

    @dy.filter()
    def customer_must_have_order(self) -> pl.LazyFrame:
        return self.customers.join(self.orders, on="customer_id", how="semi")


customers = pl.DataFrame({"customer_id": [1, 2], "name": ["a", "b"]})
orders = pl.DataFrame(
    {"customer_id": [1, 2], "order_id": [10, 20], "amount": [5.0, -1.0]}
)  # customer 2's ONLY order is invalid

res = ShopData.filter({"customers": customers, "orders": orders})
```

Actual output:

```text
surviving customers            -> [{'customer_id': 1, 'name': 'a'}]
failure['customers'].counts()  -> {'customer_must_have_order': 1}
failure['orders'].counts()     -> {'amount|min': 1}
```

Customer 2 was dropped from `customers` because a rule violation in a *different table* orphaned it.
The correct content of the `customers` asset is not computable from `customers`' inputs alone.
Any design that materializes `customers` and `orders` in separate steps is wrong by construction.

---

## Where cross-member rules report: verified

`Collection.filter` appends the filter name as a rule column to **every non-ignored member's** `FailureInfo`, and each member gets its own count.
From the probe above:

```text
failure[customers] rule_columns = ['primary_key', 'customer_id|nullability',
                                   'name|nullability', 'customer_must_have_order']
failure[orders]    rule_columns = ['primary_key', ..., 'amount|min',
                                   'customer_must_have_order']
```

A second probe with a rule that rejects an entity present in both members: **[RAN]**

```text
failure[customers]: counts={'name_not_banned': 1} rows=1
failure[orders]:    counts={'name_not_banned': 2} rows=2
```

One banned customer → 1 quarantined customer row **and** 2 quarantined order rows, under the same rule name, in two different `FailureInfo` objects.

**So there is no single asset key for a cross-member rule, and asking for one is the wrong question.** dataframely already decomposes the rule per member.
The Dagster mapping is:

```python
CROSS_MEMBER_RULES = [
    *Collection._filters(),
    *(f"{m}|failure_propagation" for m in Collection._failure_propagating_members()),
]

check_specs = [
    dg.AssetCheckSpec(
        name=f"collection_{rule}",
        asset=dg.AssetKey(member),
        additional_deps=[dg.AssetKey(o) for o in MEMBERS if o != member],
    )
    for member in Collection.non_ignored_members()
    for rule in CROSS_MEMBER_RULES
]
```

`AssetCheckSpec` accepts `additional_deps` so each per-member check declares the other members it logically depends on (already established on the map).
Signature confirmed to also carry `blocking` — relevant below.
**[RAN, via signature inspection]**

### Fact 2b: a Collection with **zero** filters can still couple its members **[RAN]**

`CollectionMember(propagate_row_failures=True)` is a second, independent cross-member mechanism.
A collection with no `@dy.filter` at all still cascades:

```python
class Shop(dy.Collection):
    customers: dy.LazyFrame[CS]
    orders: Annotated[
        dy.LazyFrame[OS], dy.CollectionMember(propagate_row_failures=True)
    ]
    # no @dy.filter defined anywhere
```

```text
filters: []
propagating members: {'orders'}
surviving customers: [{'cid': 1, 'name': 'a'}]        # cid=2 removed
failure[customers].counts() -> {'orders|failure_propagation': 1}
failure[orders].counts()    -> {'amt|min': 1}
```

It surfaces exactly like a filter: a synthetic rule column named `<member>|failure_propagation` appended to every non-ignored member's `FailureInfo`.
So the check-spec generation rule is not "one check per `@dy.filter`" but **one check per (non-ignored member × cross-member rule)**, where cross-member rules = `Collection._filters()` **plus** `{f"{m}|failure_propagation" for m in Collection._failure_propagating_members()}`.

The consequence for the design is sharper than it looks: **you cannot use "this collection has no filters" as a licence to treat its members as independent assets.** `_failure_propagating_members()` must be checked too. (Confirmed in source: `CollectionMeta.__new__` requires an overlapping primary key if *either* filters or failure-propagation are present — `collection/_base.py:137-148`.)

---

## Shape A — `@dg.multi_asset`, one asset per member — **RECOMMENDED**

The recommended shape, verified end to end against real parquet output.

### It works end to end **[RAN]**

Full materialization with per-member checks, real parquet on disk, and a downstream asset depending on only one member:

```python
@dg.multi_asset(
    outs={m: dg.AssetOut(key=m, dagster_type=pl.LazyFrame) for m in MEMBERS},
    check_specs=[...],  # as above
)
def shop_multi():
    res = Shop.filter({"customers": cust, "orders": ords})
    for m in MEMBERS:
        n = res.failure[m].counts().get("name_not_banned", 0)
        yield dg.Output(
            getattr(res.result, m),
            output_name=m,
            metadata={"quarantined_rows": res.failure[m].invalid().height},
        )
        yield dg.AssetCheckResult(
            asset_key=dg.AssetKey(m),
            check_name="collection_name_not_banned",
            passed=n == 0,
            metadata={"rows_removed": n},
        )


@dg.asset
def only_orders(orders) -> int:
    return orders.collect().height
```

Result:

```text
success: True
files on disk: ['customers.parquet', 'orders.parquet']
check customers/collection_name_not_banned: passed=False {'rows_removed': 1}
check orders/collection_name_not_banned:    passed=False {'rows_removed': 2}
downstream asset depending on ONLY `orders` ran: True
```

Per-member lineage, per-member storage, per-member checks, downstream selectivity.
All present.

### Gotcha 1: `specs=` does not work — you must use `outs=` **[RAN]**

`@dg.multi_asset(specs=[dg.AssetSpec(key=m) ...])` gives each output the Dagster type `Nothing`, so yielding an actual value fails:

```text
DagsterTypeCheckError: Error occurred while type-checking output "customers" of op
"shop_multi", with Python type <class 'polars.lazyframe.frame.LazyFrame'> and
Dagster type Nothing:
TypeError: "'!='" comparison not supported for LazyFrame objects
```

(The `!=` crash is a secondary bug — Dagster's `Nothing` type-check does `value != NoValueSentinel`, which polars `LazyFrame` refuses.
Worth knowing: **any** Dagster type check that compares a `LazyFrame` by `!=` blows up with a confusing error.)

`specs=` is for `MaterializeResult`-only multi-assets.
To route member values through IO managers you need `outs={name: dg.AssetOut(...)}`.
Note `dg.AssetSpec` has no `io_manager_key` parameter at all (params: `key, deps, description, metadata, skippable, group_name, code_version, automation_condition, owners, tags, kinds, partitions_def, freshness_policy, is_virtual`), whereas `dg.AssetOut` does.
**[RAN, via signature inspection]**

### Gotcha 2: optional members need `is_required=False` **[RAN]**

A `dy.LazyFrame[S] | None` member that is absent yields no `Output`.
Without `is_required=False` Dagster rejects the run:

```text
DagsterStepOutputNotFoundError: Core compute for op "shop_multi" did not return an
output for non-optional output "promos"
```

With `dg.AssetOut(..., is_required=(m not in Collection.optional_members()))` it succeeds and only the present members materialize:

```text
success: True
materialized: ['customers', 'orders']
files: ['customers.parquet', 'orders.parquet']
```

So the mapping is exact: `Collection.optional_members()` → `AssetOut(is_required=False)`.
**Ignored members** (`ignored_in_filters=True`) get an ordinary required `AssetOut`; they are still in `required_members()`, still get a `FailureInfo` entry, but are excluded from `common_primary_key()` and from every cross-member check — so they get schema checks only, no collection checks.
**[RAN]**

### Gotcha 3: `can_subset=True` executes but is semantically unsound **[RAN]**

```python
r = dg.materialize([shop_data], selection=dg.AssetSelection.assets("customers"))
# subset success: True
# selected inside compute: [['customers']]
# materialized: ['customers']   rows=1
```

It runs.
But look at what it cost and what it broke:

- **It saved nothing.** `Collection.filter` still required *all* required members (`ValueError: Input misses 1 required members: orders.` if you honestly pass only the selected one).
  The full computation happens regardless; subsetting only discards outputs.
- **It broke the invariant.** `customers` was written with the cascade applied (1 row), while `orders` on disk is whatever a previous run left there.
  The two persisted members are now mutually inconsistent — precisely the thing a Collection exists to prevent.

Recommendation: do **not** set `can_subset=True` on a generated Collection multi-asset, and document why.
Shape D ("something using `AssetSpec` / `can_subset` / `additional_deps`") reduces to this: `additional_deps` is already used by shape A's check specs, `AssetSpec` is the wrong output mechanism (gotcha 1), and `can_subset` is a trap.
Shape D adds nothing.

### Gotcha 4: cross-member rules are partition-local **[RAN]**

Common primary key `cid`; one customer whose only order falls in partition `d2`:

```text
whole dataset  -> surviving customers: [{'cid': 7}]
partition 'd1' -> surviving customers: []   quarantined: {'customer_must_have_order': 1}
```

The same row is valid globally and quarantined when the filter runs per-partition.
A partitioned Collection multi-asset is only sound when every entity's rows across all members land in the same partition.
That is a real constraint on the user's data model that the integration cannot check.
Recommend scoping partitioned collections out of v1 and saying so.

---

## Shape B — N assets + `@dg.multi_asset_check` — **RULED OUT**

Built and ran it: independent `customers` and `orders` assets each doing their own `Schema.filter`, then a `multi_asset_check` re-loading both and running `Collection.filter(..., skip_member_validation=True)` to report what the cross-member rule *would* remove.
**[RAN]**

```text
customers/collection_customer_must_have_order: passed=False {'rows_that_should_be_removed': 2}
orders/collection_customer_must_have_order:    passed=True  {'rows_that_should_be_removed': 0}

PERSISTED customers (what downstream consumers actually read):
  customer_id=1 | customer_id=2 | customer_id=3
```

The check correctly identified the violation.
The data on disk is still invalid — customer 3 has no orders and customer 2's only order was rejected, yet both are sitting in the `customers` asset for every downstream consumer to read.

This is the categorical difference: **`Collection.filter` is a mutation; an asset check is an observation.**
Shape B converts dataframely's guarantee ("the members you hold are mutually consistent") into a notification ("the members you hold are not mutually consistent").
That is strictly weaker than what `dy.Collection` promises, and it is weaker than what shape A delivers for free.

Dagster's own docs confirm this is inherent, not an artifact of how I built the probe: checks "run after the asset has been materialized", and by default "if a parent's asset check fails during a run, the run will continue and downstream assets will be materialized".
**[READ]**

`blocking=True` on the check specs (parameter exists on `AssetCheckSpec` **[RAN, signature]**) partially mitigates — with it, "if the `orders_id_has_no_nulls` check fails, the downstream `augmented_orders` asset won't be materialized" **[READ]** — but the bad rows are still persisted, and any consumer outside the Dagster graph reads them.
Mitigation, not a fix.

Secondary cost: the check must re-read both members from storage and re-run the join, so the cross-member work is done twice per run.

---

## Shape C — a single asset whose value *is* the Collection — **RULED OUT (but instructive)**

Ruled out against dagster-polars 0.27.12, but its failures are worth recording.

### The directory layout composes beautifully with `UPathIOManager` **[RAN]**

```python
class CollectionIOManager(dg.UPathIOManager):
    extension = None  # the "file" is a directory

    def dump_to_path(self, context, obj, path):
        path.mkdir(parents=True, exist_ok=True)
        obj.write_parquet(str(path))

    def load_from_path(self, context, path):
        return ShopData.scan_parquet(str(path))
```

All three cases worked first try:

```text
unpartitioned:   shop/customers.parquet, shop/orders.parquet
partitioned:     shop_p/2024-01-01/customers.parquet, shop_p/2024-01-01/orders.parquet
fan-in over all partitions -> dict{'2024-01-01': ShopData, '2024-01-02': ShopData}
```

This answers the ticket's IO-manager question directly.
`UPathIOManager._get_path` is `base / *asset_key.path` (+ extension), and `_get_paths_for_partitions` appends the partition key.
With `extension = None` those are directory paths, and `Collection.write_parquet`'s `<member>.parquet` naming slots underneath with **zero** collision.
Dagster's path handling and dataframely's directory layout are genuinely compatible.
**[RAN + READ: `dagster/_core/storage/upath_io_manager.py:201-297`]**

### But it collides destructively with any `.parquet` extension **[RAN]**

`PolarsParquetIOManager.extension = ".parquet"` (`dagster_polars/io_managers/parquet.py:280`).
Subclassing it and keeping that extension:

```text
store/shop.parquet            (DIRECTORY)
store/shop.parquet/customers.parquet
store/shop.parquet/orders.parquet
```

A *directory* named `shop.parquet`.
Polars then globs it:

- Incompatible member schemas → confusing error:
  `SchemaError: extra column in file outside of expected schema: order_id ... File containing
  extra column: '.../shop.parquet/orders.parquet'`
- **Compatible member schemas → silent wrong data.**
  Two members with schema `{a: Int64}`: `pl.scan_parquet(dir).collect()` returns a 2-row concatenation.
  No error, no warning.

Any tool, notebook, or downstream `PolarsParquetIOManager` pointed at that key reads garbage.
This is the strongest concrete argument that the collection directory must **not** live at a path that looks like a single file.
If shape C is ever revived, `extension = None` is mandatory.

### `dagster-polars` cannot carry a Collection — two independent hard failures **[RAN]**

1. Type routing rejects it outright:

   ```text
   RuntimeError: Could not resolve type router for <TypeHintInferredDagsterType ...>
     at dagster_polars/io_managers/type_routers.py:250, from base.py:156 (dump_to_path)
   ```

   `TYPE_ROUTERS = [TypeRouter, OptionalTypeRouter, DictTypeRouter, (PatitoTypeRouter), PolarsTypeRouter]` matches only `Any`/`None`, `Optional[...]`, `dict`/`Mapping`, patito models, and `pl.DataFrame`/`pl.LazyFrame`.
   A `dy.Collection` subclass matches nothing.
   `TYPE_ROUTERS` is a bare module-level list with no registration hook — a third party can only `.append()` to it by monkey-patching.
   **[READ: `dagster_polars/io_managers/type_routers.py:227-250`]**

2. Even with routing bypassed, metadata generation assumes a single frame:

   ```text
   DagsterExecutionHandleOutputError: AttributeError: 'ShopData' object has no attribute
   'collect_schema'
     at dagster_polars/io_managers/utils.py:67 (get_metadata_schema),
        base.py:209 (get_metadata)
   ```

   Note `DictTypeRouter` sounds promising but is *not* a directory-of-named-frames router —
   its docstring is "Handles loading partitions as dictionaries of DataFrames" and it just
   delegates to the parent router on a single path. **[READ:
   `dagster_polars/io_managers/type_routers.py:133-146`]**

So a Collection IO manager would have to override `dump_to_path`, `load_from_path`, *and* `get_metadata` — i.e. every method that does work — inheriting only `base_dir` and `storage_options` config plumbing.
The abstract seam `dagster-polars` exposes (`write_df_to_path` / `sink_df_to_path` / `scan_df_from_path`) is single-frame by signature **[READ: `base.py:103-124`]**, and a Collection is N frames.
Given that `dagster-polars` is a hard dependency and `dagster-polars[dataframely]` is the North Star, "we cannot reuse any of its format logic" is a strong signal that Collection-as-asset-value is not the upstreamable shape.

### And the real cost: one asset key for N tables

Even setting the IO manager aside, shape C gives one asset key, one materialization event, one lineage node for what the user thinks of as several tables.
Downstream assets cannot depend on just `orders`.
The Dagster UI shows one box.
Column-level metadata (see the sibling metadata-buckets research) has nowhere sensible to go.
That is a large loss of exactly the thing Dagster is for.

Shape C stays viable *only* for a use case where the collection genuinely is one atomic artifact that is always consumed whole.
That is a narrow case and it can be built by a user in ~15 lines (the `CollectionIOManager` above), so the integration does not need to ship it.

---

## Summary table

| Shape | Delivers the collection guarantee? | Per-member lineage? | Composes with dagster-polars? | Verdict |
| --- | --- | --- | --- | --- |
| A — `multi_asset` + `outs=`, N assets, N×F checks | **Yes** (filter runs once, over all members, before any output) | **Yes** | Yes — each member is a plain `pl.LazyFrame` through the normal IO manager | **Recommended** |
| B — N assets + `multi_asset_check` | **No** — observes, does not repair | Yes | Yes | Ruled out |
| C — one asset whose value is the Collection | Yes | **No** — one key for N tables | **No** — two hard failures | Ruled out |
| D — `AssetSpec` / `can_subset` / `additional_deps` | `can_subset` actively breaks it | — | — | Reduces to A; adds nothing |

---

## Open questions this research did not settle

- **Quarantine DX for the N-times problem.**
  One rejected entity produces `FailureInfo` rows in N members.
  Does the integration write N quarantine artifacts (one per asset key, matching dataframely exactly), or one entity-level artifact keyed by the common primary key?
  Verified the *fact*; the *decision* is a design call.
  Interacts with the failure-policy ticket.
- **Whether Collection support belongs in v1 at all.**
  Shape A is sound but carries four documented caveats (no subsetting, no partitions, N-times quarantine, `outs=` not `specs=`).
  Collections are strictly additive to the `Schema` design, so deferring is free.
  This is a scope call for the map, not a research question.
- **Not tested: `dg.Definitions`-level validation of a generated multi-asset + check set.**
  All probes used `dg.materialize` directly.
  Definitions-load-time errors (duplicate keys, check specs naming assets outside the def) were not exercised.
- **Not tested: `Collection.filter(lazy=True)` inside an asset.** `LazyFilterResult` / `collect_all()` behaviour under Dagster's output handling is untouched here; it belongs with the lazy-vs-eager fog on the map.
  Note `_validate_lazy_param` rejects `lazy=True` outright if the collection has any eager (`dy.DataFrame[...]`) member.
  **[READ: `collection.py:1020-1025`]**
*(`propagate_row_failures` was initially listed here as unverified; it has since been run — see the section above.)*

---

## Sources

Hands-on, against the installed environment (`.venv/bin/python`, dagster 1.13.16, dataframely 3.0.0, polars 1.43.2) — scripts in `/tmp/dydg/`.

Library source read directly:

- `dataframely/collection/collection.py` — `filter`, `validate`, `write_parquet`,
  `read_parquet`, `scan_parquet`, `_validate_input_keys`, `_validate_lazy_param`
- `dataframely/collection/_base.py` — `CollectionMember`, `MemberInfo`, `required_members`,
  `optional_members`, `ignored_members`, `common_primary_key`
- `dataframely/collection/filter_result.py` — `CollectionFilterResult`
- `dagster/_core/storage/upath_io_manager.py:201-297` — `_get_path`, `_with_extension`,
  `_get_paths_for_partitions`
- `dagster_polars 0.27.12` — `io_managers/base.py`, `io_managers/parquet.py`,
  `io_managers/type_routers.py` (wheel unpacked to `/tmp/dgp`)

Docs:

- [dataframely — Primary keys](https://dataframely.readthedocs.io/v1.14.0/sites/features/primary-keys.html)
  and its source `docs/guides/features/primary-keys.md`: "The central idea behind `Collection` is to unify multiple tables relating to the same set of underlying entities.
  This is useful because it allows us to write `filter`s that use information from multiple tables to identify whether the underlying entity is valid or not.
  If any `filter`s are defined, dataframely requires the tables in a `Collection` to have an overlapping primary key."
- [dataframely — `dataframely.collection` API](https://dataframely.readthedocs.io/v1.14.0/_api/dataframely.collection.html):
  `write_parquet` "writes one parquet file per member into the provided directory.
  Each parquet file is named `<member>.parquet`."
- [Dagster — Defining assets](https://docs.dagster.io/guides/build/assets/defining-assets):
  `@multi_asset` is for "when you need to generate multiple assets from a single operation", e.g. "using the same in-memory object to compute multiple assets".
- [Dagster — Asset checks](https://docs.dagster.io/guides/test/asset-checks): "The asset check will run after the asset has been materialized"; by default "if a parent's asset check fails during a run, the run will continue and downstream assets will be materialized"; with `blocking=True`, "the downstream `augmented_orders` asset won't be materialized".
  With `@multi_asset_check`, "both asset checks will run in a single operation after the asset has been materialized."
