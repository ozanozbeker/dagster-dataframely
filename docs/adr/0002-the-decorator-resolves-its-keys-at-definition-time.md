# 2. The decorator resolves its keys at definition time and yields every check standalone

Accepted, 2026-08-12.

Supersedes part of [ADR-0001](0001-process-takes-asset-keys.md).

## Context

Calling a `@dataframely_asset` is Dagster's documented unit-testing path, and it did not work. Three blockers, each hidden behind the last, on dagster 1.13.16:

```text
1. the wrapper calls dg.AssetExecutionContext.get()
   DagsterInvariantViolationError: No current AssetExecutionContext in scope.

2. the wrapper calls context.asset_key_for_output(...)
   DagsterInvalidPropertyError: The job_def property is not set on the context
   when an asset or op is directly invoked.

3. process bundles check results onto the valid MaterializeResult
   DagsterInvariantViolationError: Invocation of op "plain" did not return an
   output for non-optional output "plain_dy_schema__dtypes"
```

So a unit test had to go through `dg.materialize`, standing up an IO manager and an instance for what should be a function call. That cost was paid by every user writing a test for their own transform, not only by this package's suite.

None of the three can be cleared by the caller. `dg.build_asset_context()` does not set the ContextVar `.get()` reads, and Dagster sets it in exactly one place, during real step execution. Blocker 2 reproduces on a bare `@dg.multi_asset` taking a `context` parameter, so accepting an opt-in context argument would not close it either. Blocker 3 reproduces on a bare `@dg.multi_asset` whose results ride on `check_results=[...]`, and the same results yielded standalone pass.

One thing was never broken: a transform may declare `context`, bare or annotated, alone or alongside upstream frame inputs. `functools.wraps` puts the transform's signature in front of Dagster, so the parameter binds as it does anywhere else. It was untested, and the documentation denied it.

## Decision

**The wrapper reads nothing off the execution context.** Both asset keys resolve once, when the definition is built. The valid key is the one the decorator built the out with. The quarantine key is read off the finished `AssetsDefinition`, which is Dagster's own answer rather than a second derivation of it: a quarantine that declared its own `key_prefix` has a key only Dagster builds.

`AssetsDefinition.keys_by_output_name` spells that directly and carries no `@public`. `AssetsDefinition.keys` is `@public` and answers the same question at the cost of a set subtraction, which works because there are at most two outs: removing the valid key leaves exactly the quarantine. The public accessor wins, and `_asset.py` says why where it is read.

**`process` yields every check result standalone**, at every exit. Nothing is bundled onto a materialization. Every result this package builds already carries an explicit `asset_key`, so a standalone yield is fully addressed.

Neither change touches `process`'s signature. ADR-0001's decision stands, and is what made this possible: a `process` still taking a context would have kept blocker 1 for hand-wiring and for the decorator alike.

## Consequences

Calling a decorated asset returns its yields, with no run, no IO manager and no instance. `MaterializeResult` carries the frame on `value`, so a call hands back the validated frame, the quarantine frame, and every check outcome:

```python
events = list(orders(raw_orders))
tables = {e.asset_key: e.value for e in events if isinstance(e, dg.MaterializeResult)}
checks = {e.check_name: e.passed for e in events if isinstance(e, dg.AssetCheckResult)}
```

A transform declaring `context` is invoked as `orders(dg.build_asset_context(partition_key="2026-01-02"), raw_orders)`. A shape drift raises `SchemaShapeError` out of the call rather than wrapped in a step failure.

`tests/test_direct_invocation.py` asserts every shape twice, once by calling and once through `dg.materialize`. The property worth having is not that a call yields keys but that it yields the same keys a run writes under, so all four ways a quarantine key is decided are covered: absent, derived, derived under the quarantine's own prefix, and named outright.

Nothing a run sees changes. The same asset keys, check names, severities and metadata, and the same five exits. What moves is where the checks sit in the event stream, since a bundled result is emitted with its materialization and a standalone one after it.

Two upstream facts this now rests on are characterized in `tests/test_upstream_characterization.py`: direct invocation is satisfied only by a standalone `AssetCheckResult`, and `AssetsDefinition.keys` still holds the keys Dagster derived.

Unbundling removes the blocker `process` owned, so a hand-wired asset is directly invocable too, as long as it resolves its own keys without reading the execution context. `wiring.py`'s example still reads one, because it is written for a run. That is welcome rather than aimed at, per ADR-0001's principle that hand-wiring does not shape the decorator.

## Alternatives rejected

**An opt-in `context` parameter, detected on the transform and passed through.** The obvious reading of the original report. Rejected because it closes nothing: blocker 2 reproduces on a bare `@dg.multi_asset` taking a `context`, so the wrapper's own `asset_key_for_output` call would still fail on a direct call. It also buys nothing, because the parameter already works.

**Rebuild the quarantine key at definition time from `key_prefix` and the name.** The decorator already computes the default sibling key, so extending it to cover the user's own prefix is a few lines. Rejected because it is a second derivation of a key Dagster derives, and the two can disagree. Reading Dagster's answer keeps one mechanism, which is the objection ADR-0001 raised against closing over the valid key alone.

**Keep the bundling and document `dg.materialize` as the testing path.** Rejected: it is the status quo, and the cost lands on every user who writes a test rather than on this package once.

**Yield the checks before the materializations.** Rejected as a coin flip that changes event order for no reason. Every out that survived, then every check, is the order at all five exits.
