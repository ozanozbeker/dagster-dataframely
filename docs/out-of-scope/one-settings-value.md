# One Resolved Settings Value

The `wiring` functions take each setting as a separate keyword argument, and each function resolves the settings it reads.
`dd.asset` resolves its five settings once, where you declare the asset, and passes the resolved values to `asset_check_specs` and `validation_results`.
No public value holds the resolved settings.

## Why this is out of scope

Proposed as [#65](https://github.com/ozanozbeker/dagster-dataframely/issues/65) on 2026-08-12 and shelved the same day.
Its status was reviewed on 2026-09-13.
The maintainer moved it to the v1.0.0 milestone on 2026-10-15 and rejected it the same day.
The counts below are from `ec0b583`.

The proposal was one public `Settings` class.
`Settings.resolve()` would read each setting's sources and return a frozen value, and constructing one directly would require every field.
`asset_check_specs` and the run-time function would take `settings=` in place of the separate arguments.
`dd.asset` would keep its keyword arguments, so a user of the decorator would see no change.

### Only shorter signatures are left

The proposal listed four benefits.

The first was a correctness problem.
`error_rows_dir` resolved in the decorator and again where the invalid rows are written, so a worker's environment overrode the code location's. [#115](https://github.com/ozanozbeker/dagster-dataframely/issues/115) made that the intended behaviour.
`dd.asset` now resolves `error_rows_dir` only to raise on a malformed value, and `_error_rows.py` reads it again when a direct invocation writes the invalid rows.

The other three measure the size of the code:

- `_asset.py`, `_checks.py`, `_runtime.py` and `_error_rows.py` call `resolve` on a setting 13 times.
- Three functions suppress `PLR0913` because they take the settings: `validation_results`, `rule_check_results` and `asset_check_results`.
- Four signatures take settings: those three and `asset_check_specs`.

A `Settings` value would shorten those signatures and remove those suppressions.
It would change nothing a user of `dd.asset` writes or reads.

### It adds a public type for hand-wiring alone

`asset_check_specs`, `asset_check_results` and `validation_results` are public, so the value they take would be public too.
`dd.asset` keeps its keyword arguments, so only hand-wiring would construct a `Settings`.
After 1.0, removing that type is a breaking change.

The type would also no longer hold every setting.
`ERROR_ROWS_DIR` sets `takes_argument=False`, so `dd.asset` takes no argument for it, and a `Settings` would hold five values.
A sixth field would mean something different from the other five.

### Nothing requires a breaking change after 1.0

The proposal named its own reason to revisit: more settings, or more functions that take them.
Neither requires a breaking change after 1.0.

A new setting is a new keyword argument, with a `None` default, on each function that reads it.
That is an additive change.

`settings=` can also be added after 1.0, as an optional keyword argument beside the separate ones.
Only removing the separate arguments is a breaking change, and that can wait for a major version that has another reason to break.
So settling this before 1.0 does not prevent any later option.

## If someone proposes this again

The package gaining settings, or functions that take them, would reopen it.
[Rule sets fixed at definition time](rule-sets-fixed-at-definition-time.md) names the nearest case: a third grouping setting, or a fourth public function that takes `check_granularity` and `schema_rules`.

Build it as an additive change.
Add `settings: Settings | None = None` to each `wiring` function that takes settings, keep the separate keyword arguments, and raise when a call passes both.
Removing the separate arguments waits for a major version.

## Prior requests

- [#65](https://github.com/ozanozbeker/dagster-dataframely/issues/65), "Carry one resolved Settings value instead of six separate arguments".
