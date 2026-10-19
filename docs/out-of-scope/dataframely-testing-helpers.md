# Dataframely's Testing Helpers in the Testing Guide

The testing guide builds its fixtures from the demo's data, and it shows neither `Schema.sample` nor `dy.testing.validation_mask`.
A test of an asset built with `dd.asset` can still call either one, because both are dataframely's public API.

## Why this is out of scope

Proposed as [#144](https://github.com/ozanozbeker/dagster-dataframely/issues/144) on 2026-10-18 from the integration assessment, and rejected the same day.
The proposal was one example of each helper on the testing page.
`Orders.sample` would supply a passing frame, an override would make one of its rows fail, and `validation_mask` would mark the failing rows.
The probes below ran on dataframely 3.1.2 and polars 1.44.2, against the demo's `Orders`.

### `Orders.sample` needs three overrides

`Orders.sample` raises before it samples a row:

```text
ValueError: Samples cannot be generated for columns with custom checks.
```

`email` and `note` each set `check=`, and dataframely cannot sample a column with a custom check.
Overriding both is not enough.
Each sampled `order_id` is unique, so each order has one line, and `line_numbers_are_dense` passes only where `line_no` is 1.
With `email` and `note` overridden, sampling 5 rows raised after 10,000 iterations, in 19.5 seconds.
With `line_no` set to 1 as well, it returned 5 valid rows:

```python
Orders.sample(
    5,
    overrides={"email": ["a@x.com"] * 5, "note": [None] * 5, "line_no": [1] * 5},
)
```

The page already gets a passing frame in one call to `storefront_orders()`.
So `sample` would replace that call with one that sets 3 of the schema's 14 columns by hand.

### An override cannot make a row fail

`Schema.sample` resamples every row that fails a rule, until every row passes.
So an override that breaks a rule makes `sample` raise, and it never returns the failing row.
With `amount` set to `-1.00` on the first of the 5 rows above, it raised after 10,000 iterations, in 114 seconds:

```text
ValueError: After sampling for 10000 iterations, 9 rules failed validation:
```

A test that needs failing rows edits a passing frame instead.
The page's `marketplace_orders()` already has 8 lines that fail an `"error"` rule.

### A test of an asset has no `FailureInfo`

`dy.testing.validation_mask(df, failure)` takes the `FailureInfo` that `Schema.filter` returns.
`dd.testing.direct_invocation_result` returns the materialization and the check results, and the writer writes the error rows to a parquet file.
None of the three is a `FailureInfo`.
So a test would have to call `Orders.filter` itself, and that tests the schema, not the asset.
The guide covers this package's behaviour, and dataframely documents how to test a schema.

## If someone proposes this again

A user would reopen `sample` with a fixture that the demo's data cannot supply, or with a dataframely release that samples a column with a custom check.

A user would reopen `validation_mask` with a reason for `direct_invocation_result` to return a `FailureInfo`.
That is a change to the package, so it needs its own issue first.

## Prior requests

- [#144](https://github.com/ozanozbeker/dagster-dataframely/issues/144), "The testing guide does not mention `Schema.sample` or `dy.testing.validation_mask`".
