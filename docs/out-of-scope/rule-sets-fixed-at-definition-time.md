# Rule Sets Fixed at Definition Time

`check_granularity` stays a parameter of the run-time functions.
`build_asset_check_specs` calls `_build_rule_sets` where the user declares the asset, and `evaluate_rule_checks` calls it again while the asset runs.
The two calls must build the same rule sets, or a check reports for rules its spec does not describe.
Tests assert that they match, and no value that both calls read guarantees it.

## Why this is out of scope

Proposed as candidate 2 of the 2026-08-12 architecture review, reviewed on 2026-09-13, and rejected.
The proposal was to build the rule sets once, at definition time, into one value that provides the outs, the check specs and the run.
It made three claims, and all three were wrong.

### A test already asserts that the rule sets match

The main claim was that nothing checks that the two calls build the same rule sets.
Two tests do, the first from [#80](https://github.com/ozanozbeker/dagster-dataframely/issues/80), which closed while this proposal was open:

- `test_the_results_match_the_specs_exactly_at_every_granularity`, in `tests/test_check_results.py`, asserts at every granularity that `build_asset_check_specs` and `evaluate_checks` name the same checks, in the same order.
- `test_a_skip_yields_a_result_for_every_check_the_asset_declares`, in `tests/test_asset_runtime.py`, asserts that a skip yields a result for every check the asset declares, at every granularity.

The tests would miss only a bug in the decorator that makes the rule sets differ on a run with rows, at a non-default granularity, while a skip at that granularity still passes.
In `stream_results`, the skip and a run with rows call `evaluate_rule_checks` with the same `check_granularity`, and `_build_rule_sets` never reads the `FailureInfo`, so no such bug is possible.

### Hand-wiring composing one part is not a goal

Most of the proposal's benefit was that someone hand-wiring an asset would compose one part instead of seven.
That is not an open design question.
The guide's [Hand-wiring](https://ozanozbeker.com/dagster-dataframely/user-guide/hand-wiring.html) page already states the policy: "This package will add nothing to `dd.wiring` to make reassembling the decorator easier."
The same page states that the `dd.wiring` functions do less than `dd.asset`: you pass them the settings and asset keys that `dd.asset` resolves at definition time.

[ADR-0001](../pre-1.0.md#adr-0001-validation_results-takes-asset-keys) records the principle: hand-wiring does not change the decorator's design.
A proposal whose main benefit is easier hand-wiring has to reverse both, and this one did not try to.

### The decorator's closure is already inspectable

The proposal took `tests/test_asset_definition.py`, 1,103 lines and 74 tests at the time, as a sign that a test cannot inspect anything in `_asset.decorate` without building an asset.
The file shows the opposite.
Its tests read the `AssetsDefinition` that `dd.asset` returns, and none of them runs an asset.
`dd.wiring.build_asset_check_specs` is public, so a test that needs only the rule sets can call it in one line.

The tests go through the decorator because the decorator's wiring is what they test.
The file is long because it covers a lot, not because the closure makes anything hard to test.

## This is not #65

[#65](https://github.com/ozanozbeker/dagster-dataframely/issues/65) would have passed the settings as one resolved value.
[One resolved settings value](one-settings-value.md) records its rejection, for reasons of its own.

It would have changed how `dd.asset` passes the settings, and kept both `_build_rule_sets` calls.
This proposal would have changed what `dd.asset` passes: with the rule sets fixed at definition time, a run would never receive `check_granularity`.
The case below that would reopen this proposal would also reopen #65.

## If someone proposes this again

The current design has one drawback: `check_granularity` is a parameter of several functions, which have to change together.
`build_asset_check_specs`, `stream_results` and `evaluate_checks` take it, `evaluate_rule_checks` receives it from the last two, and a fourth public function would have to take it by hand.

`dd.build_rule_checks` takes `check_granularity` as well, since [#143](https://github.com/ozanozbeker/dagster-dataframely/issues/143).
It resolves `check_granularity` once and passes the value to `build_asset_check_specs` and `evaluate_checks`, as `dd.asset` does, so a user has no two calls to keep consistent.
`test_the_run_yields_a_result_for_exactly_the_specs`, in `tests/test_rule_checks.py`, asserts at every granularity that its results name its specs, so it does not reopen this proposal.

A second setting that changes the rule sets, or a fourth public function that a user must pass `check_granularity` to by hand, would reopen it.
Either would add more places that change together than one parametrized test covers, and a single resolved value would then justify its work.
A change to the hand-wiring policy would also reopen it, for a different reason.

## Prior requests

- [#67](https://github.com/ozanozbeker/dagster-dataframely/issues/67), "Build the rule sets once, not once per side of the definition/run split", candidate 2 of the 2026-08-12 architecture review.
