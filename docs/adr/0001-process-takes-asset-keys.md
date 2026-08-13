# 1. `process` takes asset keys, not the execution context

Accepted, 2026-08-12. Superseded in part by [ADR-0002](0002-the-decorator-resolves-its-keys-at-definition-time.md), which is marked below at the two paragraphs it replaces. The decision itself stands.

## Context

`process` is the package's exported runtime entry. The decorator calls it, and hand-wiring calls it directly.

It used to take `context: dg.AssetExecutionContext` alongside the output names `valid_out` and `quarantine_out`, and it read that context at exactly two places, both of them `asset_key_for_output`. Nothing else on the context was touched: no logging, no partition key, no resources, no instance.

That one parameter decided how the package could be tested. `process` has three phases and five exits, and the exits are where the failure policy lives: whether a run writes both tables, one table, or nothing, and whether its checks warn or error. Reaching any of them meant starting a Dagster run, because the context can only be obtained inside one. At the time of this decision that was 81 tests and 91 `dg.materialize` calls across three files.

An output name is not always its asset key. An out that declares `key_prefix` has a key its name does not spell, and `asset_key_for_output` is what reconciles the two.

## Decision

`process` takes `valid_key: dg.AssetKey` and `quarantine_key: dg.AssetKey | None`. The context parameter and both output-name parameters are gone.

The decorator binds the context once in its wrapper and resolves both keys through `asset_key_for_output`. Hand-wiring resolves its own, and the README shows that form rather than a hand-built key.

Behind it sits a principle that decided several of the smaller questions: **hand-wiring does not shape the decorator's design.** Hand-wiring is the advanced path, taken by someone who has already stepped outside what the decorator offers. Where the two pull in different directions, the decorator wins.

## Consequences

The five exits are now reachable by calling a function. `TestExitSelection` in `tests/test_asset_runtime.py` does that, with no run, no IO manager and no `tmp_path`. Writing it surfaced one fact no existing test stated: check results are bundled onto the valid materialization where there is one and yielded standalone where the valid out is skipped, and a run flattens both into a single event stream so the difference is invisible from there.

> **Superseded by ADR-0002.** The bundling is gone: every check result is yielded standalone at every exit, because direct invocation is satisfied only by a standalone one.

Hand-wiring can now pass a key no out owns. This fails on the first yield with `DagsterInvariantViolationError: Asset key ... not found in AssetsDefinition`; the step fails and nothing is written. That was verified before the decision, not assumed.

The change is breaking, and `process` is public. It lands in a `0.x` minor, per the policy the README states.

Nothing in `CONTEXT.md` changed. `valid_key` and `quarantine_key` are built from terms it already defines.

## Alternatives rejected

**Keep the context and the output names.** The names are what a hand-wiring user writes in `outs={...}`, so passing them is the friendlier call site, and the package would keep doing the reconciliation. Rejected because it preserves exactly the dependency that forces every exit test through a run, in order to save one line at a call site taken by advanced users.

**Accept the context as an optional parameter, resolving names when given.** Rejected for the same reason and one more: `process` would still carry the context path, so the tests could keep routing around the new interface and the change would deliver nothing until the option was removed.

**Export a helper that turns a context and output names into the pair.** Rejected as a second exported name bought to save one line.

**Close over the valid key at definition time.** The decorator already builds that key and passes it to the out, so it is guaranteed correct and needs no lookup. Rejected because the quarantine key cannot always be known at definition time: `_quarantine_out` may keep the user's own `key_prefix`, and Dagster derives the key from it. Two mechanisms answering one question in one function would need explaining every time it is read, and the saving is one dictionary lookup per run against a validation pass.

> **Superseded by ADR-0002**, which adopts a variant: both keys resolve at definition time, and the quarantine's is read off the finished `AssetsDefinition` rather than rebuilt. The objection above is what rules out rebuilding it, and it still holds. What changed is that the lookup no longer has to happen inside a run, which is what made the decorator directly invocable.

**A deprecation shim.** Rejected: the package is pre-1.0, the README tells users to pin `>=0.1,<0.2`, and a shim would keep the context parameter alive, which is the thing being removed.
