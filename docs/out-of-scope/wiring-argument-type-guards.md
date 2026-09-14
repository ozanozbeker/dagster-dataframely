# Wiring Argument Type Guards

`check_results` refuses nothing about its `frame` argument.
A wrong type reaches `column_schema_problems` and fails on `collect_schema()`, two frames inside the package.

One `wiring` part does refuse a frame for its type, and it is not a precedent.
`validation_results` runs `_require_frame`, which belongs to the decorator: `dd.asset` routes the decorated function's return value through it, and Dagster cannot hold that function to its return annotation.
A hand-wirer inherits the refusal by sharing the code path.
Nothing dynamic reaches `check_results`, so the reason does not carry.

## Why this is out of scope

Proposed as [#124](https://github.com/ozanozbeker/dagster-dataframely/issues/124), grilled on 2026-09-13, and rejected.
The proposal offered three ways and named the choice as the ticket: one guard reading for both callers, a second guard of its own, or none with the absence written down.
The third won.
Measured on dagster 1.13.20, dataframely 3.0.0, polars 1.44.1.

### Dagster already closes the exposure the proposal named

The proposal granted that a type checker covers the hand-written call site, and rested on the one case it does not reach: a frame arriving from an IO manager load rather than from the caller's own hand.
That is the arrangement `check_results` exists for, so the case mattered.

Dagster covers it.
It checks a loaded input against the check function's parameter annotation, before the body runs:

```text
orders: pl.DataFrame, and the manager returns a dict

DagsterTypeCheckDidNotPass: Type check failed for step input "orders" - expected type
"DataFrame". Description: Value of type <class 'dict'> failed type check for Dagster type
DataFrame, expected value to be of Python type polars.dataframe.frame.DataFrame.
```

The message names the input, the expected type and the actual type, and the check function never runs.
`tests/test_upstream_characterization.py` pins that.

The package's own failure returns only where the parameter carries no annotation, or carries `Any`:

```text
orders, or orders: Any

AttributeError: 'dict' object has no attribute 'collect_schema'
```

Both the user guide and the `check_results` docstring teach the annotated form, so a reader who follows either is on the enforced path.

The decision turns on this.
`_require_frame` justifies itself with "Dagster calls the decorated function dynamically, so it cannot enforce the return annotation".
Here Dagster does enforce it, so the reason the existing guard exists does not reach.

### A shared guard would have to disagree with itself about `None`

The cheapest of the three ways was one guard reading for both callers, and the proposal priced it as a vaguer message.
It costs more than that.
`_require_frame` accepts `None` and returns, because `None` is the decorator's skip.
`check_results` has to refuse it: it writes nothing, so it has no partition to skip, and its annotation excludes the value.
One function cannot hold both verdicts without a parameter deciding which, and its message then states two meanings for one object.

### Hand-wiring composing one part is not a goal

A guard here is ergonomics for someone assembling `wiring` parts by hand.
[ADR-0001](../adr/0001-process-takes-asset-keys.md) settles that hand-wiring does not shape this package's design, and [the guide's hand-wiring page](https://ozanozbeker.com/dagster-dataframely/user-guide/hand-wiring.html) says "Nothing will be added to `dd.wiring` to make reassembling the decorator easier".
[Rule sets fixed at definition time](rule-sets-fixed-at-definition-time.md) rejected a larger proposal on the same ground.

## What this package does refuse

Two other guards sit close enough to read as precedent.
Neither is.

[ADR-0008](../adr/0008-every-public-function-that-takes-a-schema-validates-it.md) landed the same day and put `validate_namespace(schema)` at the top of `check_results`, one line above where this guard would have gone.
It refuses a property no annotation can express.
A schema declaring a `dy_` column satisfies `type[dy.Schema]` perfectly, and only a run-time read of its columns tells you otherwise.

`dd.asset` refuses a `dy.Collection` handed to it as a schema, and a type checker does see that one: `dy.Collection` does not subclass `dy.Schema`.
Its comment draws the line.
"Only a `Collection` is refused here.
Anything else keeps failing however it already fails."
It buys one near-miss, because Dataframely's other schema-shaped class is the thing a user reaches for next.
Nobody reaches for a dict where a frame goes.

The rule underneath: this package refuses what a type checker cannot see, plus the single near-miss it expects a user to make.
A frame's type is neither.
An annotation expresses it, and two enforcers read it.

## If this is reconsidered

Two things reopen it, and a count of user reports is not one of them.

Dagster weakening or dropping the input type check on an annotated check parameter.
The characterization test is the tripwire, and it fails the day that happens.

A `wiring` part taking a value no type checker sees, such as one read from the environment or annotated `Any`.
That breaks the premise this rests on, and earns a guard on the terms the settings values already have one.

## Prior requests

- [#124](https://github.com/ozanozbeker/dagster-dataframely/issues/124), "check_results takes any object and fails two frames deep".
