# Dataframely's Testing Helpers in the Testing Guide

The testing guide shows neither `Schema.sample` nor `dy.testing.validation_mask`, and it builds its fixtures from `dd.data`.
A test of an asset built with `dd.asset` can still call either one, because both are dataframely's public API.

## Why this is out of scope

Issue [#144](https://github.com/ozanozbeker/dagster-dataframely/issues/144) proposed this on 2026-10-18, from the integration assessment.
The maintainer rejected it the same day.
The proposal was one example of each helper on the testing page.
`OrdersSchema.sample` would supply a passing frame, an override would make one of its rows fail, and `validation_mask` would mark the failing rows.
The probes below ran on dataframely 3.1.2 and polars 1.44.2, against `OrdersSchema` from `dd.data`.

### `OrdersSchema.sample` needs three overrides

`OrdersSchema.sample` raises before it samples a row:

```text
ValueError: Samples cannot be generated for columns with custom checks.
```

`email` and `note` each set `check=`, and dataframely cannot sample a column with a custom check.
Overriding both is not enough.
Each sampled `order_id` is unique, so each order has one line, and `line_numbers_are_dense` passes only where `line_no` is 1.
With `email` and `note` overridden, sampling 5 rows raised after 10,000 iterations, in 19.5 seconds.
With `line_no` set to 1 as well, it returned 5 valid rows:

```python
OrdersSchema.sample(
    5,
    overrides={"email": ["a@x.com"] * 5, "note": [None] * 5, "line_no": [1] * 5},
)
```

The page already imports a passing frame, `storefront_orders`, from `dd.data`.
So `sample` would replace that import with a call that sets 3 of the schema's 13 columns by hand.

### An override cannot make a row fail

`Schema.sample` resamples every row that fails a rule, until every row passes.
So an override that breaks a rule makes `sample` raise, and it never returns the failing row.
With `amount` set to `-1.00` on the first of the 5 rows above, it raised after 10,000 iterations, in 114 seconds:

```text
ValueError: After sampling for 10000 iterations, 9 rules failed validation:
```

A test that needs failing rows edits a passing frame instead.
`marketplace_orders` from `dd.data` already has 8 lines that fail an `"error"` rule.

### A test of an asset has no `FailureInfo`

`dy.testing.validation_mask(df, failure)` takes the `FailureInfo` that `Schema.filter` returns.
`dd.testing.DirectInvocationResult.from_invocation` returns the materialization and the check results, and `build_file_writer` writes the error rows to a parquet file.
None of the three is a `FailureInfo`.
So a test would have to call `OrdersSchema.filter` itself, and that tests the schema, not the asset.
The guide covers this package's behaviour, and dataframely documents how to test a schema.

## If someone proposes this again

A user would reopen `sample` with a fixture that `dd.data` cannot supply, or with a dataframely release that samples a column with a custom check.

A user would reopen `validation_mask` with a reason for `DirectInvocationResult` to hold a `FailureInfo`.
That is a change to the package, so it needs its own issue first.

## Prior requests

- [#144](https://github.com/ozanozbeker/dagster-dataframely/issues/144), "The testing guide does not mention `Schema.sample` or `dy.testing.validation_mask`".
