# 3. The quarantine's only parent is the valid asset

Accepted, 2026-08-13.

## Context

`@dg.multi_asset` gives every out every input, and there is no per-out vocabulary for saying otherwise: `dg.AssetOut` has no `deps`. So a quarantined asset rendered two children of the same parents, on dagster 1.13.16:

```text
deps=["other"], ins={"raw": dg.AssetIn(["upstream_frame"])}
  orders            <- ['other', 'upstream_frame']
  orders_quarantine <- ['other', 'upstream_frame']
```

That is not merely untidy. It says the quarantine is an independent child of the upstream tables, reachable and materializable on its own, and that is false: the two are outs of one step and neither can execute without the other.

`internal_asset_deps` is the only lever, and it refuses a partial map. Naming the quarantine alone fails:

```text
Invalid asset dependencies: `{AssetKey(['other']), AssetKey(['upstream'])}` specified as asset
inputs, but are not specified in `internal_asset_deps`.
```

So the decorator has to state the valid out's input set too, and that set is Dagster's to derive: six coercible `deps` forms, an `AssetsDefinition` among them contributing every key it owns, `dg.AssetIn(key_prefix=)` composing with the *parameter* name, and `context` dropped by position.

## Decision

**The quarantine's only parent is the valid asset, unconditionally**, whenever one is declared. The valid asset keeps every parent it had.

**The definition is built twice on that path.** The first build supplies the valid out's inputs through `AssetsDefinition.asset_deps`, which is `@public`; the second passes them back with the quarantine pointed at the valid key. Same move as ADR-0002 and for the same reason: read Dagster's answer rather than derive a second one that can disagree with it. Roughly 0.25 ms, and only where a quarantine exists.

`internal_asset_deps` is keyed by output name, so a quarantine that named its own key or `key_prefix` costs nothing extra.

**The edge is asset-grained, and that is a claim about rows this package knowingly does not make.** No row in the quarantine came from the valid table; `Schema.filter` splits one frame into two siblings. Column-lineage tooling reading the quarantine will trace its columns back through the valid table. Accepted, because the shape it replaces makes the worse claim.

## Consequences

`CONTEXT.md`'s **Quarantine** entry now carries both axes: sibling in the definition, downstream in the graph.

Staleness, measured on a clean run that skipped the quarantine:

| | before | after |
| --- | --- | --- |
| asset has upstream | quarantine `STALE` | quarantine `STALE` |
| asset has no upstream | quarantine `FRESH` | quarantine `STALE` |

Dagster's reason is `has a new dependency materialization`. With any upstream at all the quarantine was already stale on the same runs, so the new cost lands only on an asset that reads its own source. The README states it.

Hand-wiring gets the same shape by writing the map itself, and the README's first arrangement carries it, since that arrangement claims to be what the decorator builds. It exposes one wrinkle the decorator does not have: `internal_asset_deps` is read at definition time where there is no context, so the valid key there is spelled by hand and must carry the out's prefix. That fails at import with `Invalid asset dependencies ... Each specified asset key must be associated with an input to the asset or produced by this asset`, so it is loud. Per ADR-0001, this did not shape the decision.

Nothing a run sees changes. Both outs still execute in one step, verified with an upstream asset in the graph, which is the shape that could plausibly have been read as a cycle.

Automation is unmoved. No condition can sit on the quarantine, since `automation_condition` and `freshness_policy` are contested settings, and the valid out's own deps did not change. A user's `eager()` asset pointed at the quarantine's key was measured across four ticks either side of the change and requested identically, firing on the runs that quarantined rows and staying quiet on the clean ones. `AutomationCondition.any_downstream_conditions()` is the one condition that reads the graph downward, and it is the one this was not able to make fire in either shape, so nothing is claimed about it.

## Alternatives rejected

**Re-derive the valid out's inputs in the decorator.** One pass, no discarded object. Rejected because it reimplements Dagster's input resolution, and its failure mode is silent: a map missing a key is still a valid map, and `internal_asset_deps` is exactly what Dagster validates inputs against, so the mistake surfaces as `Invalid asset dependencies` on a user's definition naming a key they never wrote.

**Build once and reconstruct.** `AssetsDefinition.__init__` does take `asset_deps`. Rejected because it is not `@public` and takes 23 parameters, so reconstruction means transcribing all 23, and a Dagster release adding a 24th drops it silently. That is `_rebuild`'s hazard without the property that makes `_rebuild` safe.

**`map_asset_specs` with `spec.replace_attributes(deps=...)`.** Rejected with evidence: dropping the shared inputs from the quarantine spec turns them into `Nothing` inputs and the rebuild dies on `@op 'c' decorated function has parameter 'upstream' that is one of the input_defs of type 'Nothing'`. A no-op map is fine, so the API is not the problem; expressing this is.

**Keep the quarantine's parents and add the valid asset on top.** The honest shape at row level. Rejected on legibility: every upstream table then draws two edges into the same pair of assets, which is the reading the change exists to remove.

**A flag to turn the edge off.** Rejected: declaring `quarantine=dg.AssetOut()` is already the whole consent, and the cases that might want it do not survive. A quarantine in another group or ownership domain is exactly where the edge earns its keep, and an automation condition downstream of the quarantine still fires on the quarantine's own materialization.

**Two `@dg.asset`s, or a `@dg.graph_multi_asset`.** Two assets is two ops, so the decorated function runs once per table, measured. Any non-determinism then means the quarantine holds rows the run that wrote the valid table never rejected, and the two tables stop being one split of one frame. `graph_multi_asset` keeps one compute but cannot express this: it has no `internal_asset_deps`, no `deps`, no `description`, and eight of the fourteen parameters the decorator forwards are absent, and ADR-0002's direct invocation goes with it.

The surface question this raised, whether `quarantine=` should take a package-owned container instead of `dg.AssetOut`, is filed as #91. It changes nothing here: the map is keyed by output name.
