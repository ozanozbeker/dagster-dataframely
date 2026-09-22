# Wiring Argument Type Guards

`check_results` does not check the type of its `frame` argument.
An argument of another type fails in `_column_schema_problems`, two calls below, on `collect_schema()`.

`validation_results` calls `_require_frame`, and that guard is not a precedent.
It exists for the decorator: Dagster does not check the decorated function's return value, which `dd.asset` passes to `validation_results`.
Someone hand-wiring an asset gets the same check by calling the same function.
In the documented use, a type checker or Dagster checks the frame passed to `check_results`, so that reason does not apply.

## Why this is out of scope

Proposed as [#124](https://github.com/ozanozbeker/dagster-dataframely/issues/124), reviewed on 2026-09-13, and rejected.
It offered three options: one guard with one message for both callers, a separate guard for `check_results`, or no guard, with the reason written down.
The maintainer chose the third.
Measured on dagster 1.13.20, dataframely 3.0.0, polars 1.44.1.

### Dagster already checks the case the proposal named

The proposal accepted that a type checker covers a hand-written call, and named the one case it does not: a frame that an IO manager loads.
That is the case `check_results` exists for, so it mattered.

Dagster covers it: it checks a loaded input against the check function's parameter annotation, before the function runs:

```text
orders: pl.DataFrame, and the manager returns a dict

DagsterTypeCheckDidNotPass: Type check failed for step input "orders" - expected type
"DataFrame". Description: Value of type <class 'dict'> failed type check for Dagster type
DataFrame, expected value to be of Python type polars.dataframe.frame.DataFrame.
```

The message names the input, the expected type and the actual type, and the check function never runs.
`tests/test_upstream_characterization.py` asserts both, in `test_a_check_input_is_still_type_checked_against_its_annotation`.

The package's own error appears only where the parameter has no annotation, or where you annotate it `Any`:

```text
orders, or orders: Any

AttributeError: 'dict' object has no attribute 'collect_schema'
```

The guide's hand-wiring example annotates the parameter, so code that follows it gets Dagster's check.

This is the main reason for the decision.
`_require_frame` exists because Dagster does not check the decorated function's return value, and Dagster does check an annotated check input.

### A shared guard would need two behaviours for `None`

The proposal named a vaguer message as the only drawback of the simplest option, one guard for both callers.
The drawback is larger.
`_require_frame` accepts `None` and returns, because `None` is the decorator's skip.
`check_results` has to reject `None`: it writes nothing, so it has no partition to skip, and its annotation excludes the value.
One function can do both only with a parameter that selects the behaviour, and its message would then state two meanings for one object.

### Hand-wiring composing one part is not a goal

A guard here would only help someone assembling `wiring` functions by hand.
[ADR-0001](../pre-1.0.md#adr-0001-validation_results-takes-asset-keys) records that hand-wiring does not change this package's design, and [the guide's hand-wiring page](https://ozanozbeker.com/dagster-dataframely/user-guide/hand-wiring.html) states: "This package will add nothing to `dd.wiring` to make reassembling the decorator easier."
[Rule sets fixed at definition time](rule-sets-fixed-at-definition-time.md) rejected a larger proposal for the same reason.

## What this package does reject

Two other guards look like precedents, and neither is one.

The maintainer accepted [ADR-0008](../pre-1.0.md#adr-0008-every-public-function-that-takes-a-schema-validates-it) the same day, and it put `validate_namespace(schema)` at the top of `check_results`, one line above where this guard would have been.
It rejects a property that no annotation can express.
A schema declaring a `dy_` column satisfies `type[dy.Schema]`, and only reading its columns at run time shows the problem.

`dd.asset` rejects a `dy.Collection` passed to it as a schema, and a type checker reports that one too: `dy.Collection` does not subclass `dy.Schema`.
It covers only a `Collection`, the one likely wrong argument, because that is the other Dataframely class a user might pass as a schema.
Any other wrong type fails as it already did.
A user is not likely to pass a dict where a frame goes.

This package rejects two things: what a type checker cannot check, and the one likely wrong argument.
A frame's type is neither.
An annotation expresses it, and two tools check it: a type checker and Dagster's input type check.

## If someone proposes this again

Two changes would reopen it, and the number of user reports is not one of them.

The first is Dagster weakening or removing its type check on an annotated check parameter.
The characterization test above fails the day that happens.

The second is a `wiring` function that takes a value no type checker can check, such as one read from the environment or annotated `Any`.
That would remove the reason for this decision, and justify a run-time check like the one that raises `InvalidSettingError` for a setting's value.

## Prior requests

- [#124](https://github.com/ozanozbeker/dagster-dataframely/issues/124), "check_results takes any object and fails two frames deep".
