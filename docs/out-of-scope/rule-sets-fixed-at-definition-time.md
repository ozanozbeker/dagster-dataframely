# Rule Sets Fixed at Definition Time

`check_granularity` and `schema_rules` stay parameters of the run-time functions.
`_rule_sets` is called twice: once from `check_specs` where the asset is declared, and once from `rule_results` while it runs.
The two must group the schema's rules identically, or a check reports for a set of rules its spec never claimed.
That agreement is held by a test, not by a value both sides read from.

## Why this is out of scope

Proposed as candidate 2 of the 2026-08-12 architecture review, grilled on 2026-09-13, and rejected.
The proposal was to build the grouping once, at definition time, into a value owning the outs, the check specs and the run.
It rested on three claims, and none of them survived.

### The agreement is already enforced

The headline claim was that nothing enforces the two groupings matching.
Something does. [#80](https://github.com/ozanozbeker/dagster-dataframely/issues/80) landed while this sat open, and `tests/test_check_results.py` pins the names and their order:

```python
@pytest.mark.parametrize("granularity", ["rule", "column", "schema"])
def test_the_results_answer_exactly_the_specs_at_every_granularity(
    granularity: Granularity,
):
    specs = check_specs(Orders, asset=KEY, check_granularity=granularity)
    results = _results(mixed_orders(), check_granularity=granularity)

    assert [result.check_name for result in results] == [spec.name for spec in specs]
```

`test_schema_rules_reach_the_results_too` covers the second setting, which only `column` granularity reads.
`test_a_skip_answers_whatever_check_list_the_asset_declared` pins the decorator's own relay, parametrized the same way.

A bug that broke the agreement would have to break it on a data-bearing run at non-default granularity while the skip at that same granularity still passed.
Both paths call `rule_results` with identical arguments, and `_rule_sets` never reads the `FailureInfo`, so there is no such bug to write.

### Hand-wiring composing one part is not a goal

Most of the proposal's payoff was that a hand-wirer would compose one part instead of seven.
That is not an open design question.
The guide's [Hand-wiring](https://ozanozbeker.com/dagster-dataframely/user-guide/hand-wiring.html) section already answers it: "Nothing will be added to `dd.wiring` to make reassembling the decorator easier."
It documents the current threading as deliberate in the same breath, saying `validation_results` stops short of the decorator on purpose, because the decorator resolves the settings once at definition time and hands the same values to both sides.

[ADR-0001](../adr/0001-process-takes-asset-keys.md) is the principle underneath: hand-wiring does not shape the decorator's design.
A proposal whose main benefit lands on the hand-wirer has to overturn both, and this one did not set out to.

### The decorator's closure is already inspectable

The proposal read `tests/test_asset_definition.py` at 1,103 lines and 74 tests as evidence that nothing in `_asset.decorate` can be inspected without building an asset.
It reads the wrong way round.
That file opens "Definition-time behaviour of `@dd.asset`, asserted without running anything", and nothing in it runs.
`check_specs` is public and directly callable, so a test wanting the grouping alone can have it in one line.

The tests reach through the decorator because the decorator's wiring is the thing under test.
Length follows from what is covered, not from a closure holding anything hostage.

## This is not #65

[#65](https://github.com/ozanozbeker/dagster-dataframely/issues/65) puts all six settings behind a single resolved value, and it stays open at full size.

The two look alike and are not.
That issue changes how settings travel and leaves the two `_rule_sets` calls where they are.
This one changes what travels, by fixing the grouping so the two check settings never reach a run at all.
Nothing here rejects carrying a resolved value; it rejects fixing the grouping as the way to earn one.

## If this is reconsidered

The cost this leaves standing is a lockstep edit.
Three public functions carry the two grouping settings today, `check_specs`, `validation_results` and `check_results`, plus `rule_results` behind them, and a fourth would join them by hand.

The evidence that reopens it is a third grouping setting, or a fourth public function needing the two.
Either raises the lockstep cost past what one parametrized test pays for, and the value gets cheap at that point.
A change to the hand-wiring policy would do it too, for a different reason.

## Prior requests

- [#67](https://github.com/ozanozbeker/dagster-dataframely/issues/67), "Build the rule sets once, not once per side of the definition/run split", candidate 2 of the 2026-08-12 architecture review.
