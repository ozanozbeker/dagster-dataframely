# 5. The decorator wraps `dg.asset` rather than stacking on it

Accepted, 2026-09-02.

## Context

With the quarantine gone from the graph (ADR-0004), the decorator has three jobs: put `dagster/column_schema` on the asset, declare one check spec per rule set, and wrap the body so it validates and yields results. All three are things `@dg.asset` accepts as arguments. The obvious question is whether the package should be a second decorator stacked with `@dg.asset`, so the user keeps seeing Dagster's own decorator.

## Decision

`@dy_asset(Schema, quarantine=False, **dg.asset kwargs)` calls `dg.asset(check_specs=..., metadata=..., ...)` on the wrapped body. Every `dg.asset` argument is forwarded. There is no stacked form.

## Why not stacked

Both stacking orders were tried on dagster 1.13.20.

**Above `@dg.asset`** means receiving a finished `AssetsDefinition`. Check specs are op outputs: `@dg.asset(check_specs=[...])` adds an `Out` per check and maps it in `check_specs_by_output_name`. The public constructors, `from_op` and `from_graph`, cannot add one to an op-backed definition, and `with_attributes` is not `@public`. The compute function cannot be swapped either: `OpDefinition.with_replaced_properties` keeps `compute_fn`, and building a new `OpDefinition` loses `DecoratedOpFunction`, the adapter that turns `(context, inputs)` into the user's signature. Rebuilding the definition also drops what `@dg.asset` set (`from_op` returned group `default` for an asset declared in `sales`). Three private APIs and an attribute copy that rots per release.

**Below `@dg.asset`** means wrapping the raw function. `@dg.asset` reads only its signature, so the lower decorator cannot hand it `check_specs` or `metadata`. The user passes both, names the schema three times, and repeats `check_granularity` on each side or the specs and results disagree. A forgotten spec fails on the first run, not at load. The lower decorator learns the asset key only at run time, so it reads the context, which ADR-0002 removed to make a decorated asset callable in a test.

**Checks as a separate `AssetChecksDefinition`** (`build_*_checks` style) runs after the asset and sees only the written rows. With a quarantine declared, the invalid rows are already gone, so every check would pass forever, and validation would run twice.

## Consequences

The pieces stay exported under `dd.wiring`, so the below-stack form is available to anyone who wants `@dg.asset` visible, at the cost described above. The README shows it as hand-wiring, not as a second happy path.

A stacked decorator can be added later without changing this one. Removing one after people use it cannot, which is why it is not added now.
