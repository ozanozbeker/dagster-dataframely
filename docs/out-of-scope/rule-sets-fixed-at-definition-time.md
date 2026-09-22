# Rule Sets Fixed at Definition Time

`check_granularity` and `schema_rules` stay parameters of the run-time functions.
`check_specs` calls `_rule_sets` where the user declares the asset, and `rule_results` calls it again while the asset runs.
The two calls must group the schema's rules the same way, or a check reports for rules its spec does not describe.
Tests assert that they match, and no value that both calls read guarantees it.

## Why this is out of scope

Proposed as candidate 2 of the 2026-08-12 architecture review, reviewed on 2026-09-13, and rejected.
The proposal was to build the grouping once, at definition time, into one value that provides the outs, the check specs and the run.
It made three claims, and all three were wrong.

### A test already asserts that the groupings match

The main claim was that nothing checks that the two groupings match.
Three tests do, the first two from [#80](https://github.com/ozanozbeker/dagster-dataframely/issues/80), which closed while this proposal was open:

- `test_the_results_answer_exactly_the_specs_at_every_granularity`, in `tests/test_check_results.py`, asserts at all three granularities that `check_specs` and `check_results` name the same checks, in the same order.
- `test_schema_rules_reach_the_results_too` covers the second setting, which only `column` granularity reads.
- `test_a_skip_answers_whatever_check_list_the_asset_declared`, in `tests/test_asset_runtime.py`, asserts that a skip yields a result for every check the asset declares, at all three granularities.

The tests would miss only a bug in the decorator that makes the groupings differ on a run with rows, at a non-default granularity, while a skip at that granularity still passes.
In `validation_results`, the skip and a run with rows call `rule_results` with the same `check_granularity` and `schema_rules`, and `_rule_sets` never reads the `FailureInfo`, so no such bug is possible.

### Hand-wiring composing one part is not a goal

Most of the proposal's benefit was that someone hand-wiring an asset would compose one part instead of seven.
That is not an open design question.
The guide's [Hand-wiring](https://ozanozbeker.com/dagster-dataframely/user-guide/hand-wiring.html) page already states the policy: "This package will add nothing to `dd.wiring` to make reassembling the decorator easier."
The same page states that the `dd.wiring` functions do less than `dd.asset`: you pass them the settings and asset keys that `dd.asset` resolves once, at definition time, and passes to both `check_specs` and `validation_results`.

[ADR-0001](../pre-1.0.md#adr-0001-validation_results-takes-asset-keys) records the principle: hand-wiring does not change the decorator's design.
A proposal whose main benefit is easier hand-wiring has to reverse both, and this one did not try to.

### The decorator's closure is already inspectable

The proposal took `tests/test_asset_definition.py`, 1,103 lines and 74 tests at the time, as a sign that a test cannot inspect anything in `_asset.decorate` without building an asset.
The file shows the opposite.
Its tests read the `AssetsDefinition` that `dd.asset` returns, and none of them runs an asset.
`check_specs` is public, so a test that needs only the grouping can call it in one line.

The tests go through the decorator because the decorator's wiring is what they test.
The file is long because it covers a lot, not because the closure makes anything hard to test.

## This is not #65

[#65](https://github.com/ozanozbeker/dagster-dataframely/issues/65) would pass all six settings as one resolved value, and it stays open at its full scope.

It changes how `dd.asset` passes the settings, and keeps both `_rule_sets` calls.
This proposal would have changed what `dd.asset` passes: with the grouping fixed at definition time, a run would never receive the two check settings.
This note does not reject passing one resolved value, only fixing the grouping as the reason to build one.

## If someone proposes this again

The current design has one drawback: the two grouping settings are parameters of several functions, which have to change together.
`check_specs`, `validation_results` and `check_results` take them, `rule_results` takes them from the last two, and a fourth public function would have to take them by hand.

A third grouping setting, or a fourth public function that needs the two, would reopen it.
Either would add more places that change together than one parametrized test covers, and a single resolved value would then justify its work.
A change to the hand-wiring policy would also reopen it, for a different reason.

## Prior requests

- [#67](https://github.com/ozanozbeker/dagster-dataframely/issues/67), "Build the rule sets once, not once per side of the definition/run split", candidate 2 of the 2026-08-12 architecture review.
