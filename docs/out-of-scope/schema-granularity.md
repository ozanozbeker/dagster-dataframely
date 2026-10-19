# Schema Check Granularity

`check_granularity` accepts `rule` and `column`.
It does not accept `schema`, which gave one check, `dy_schema__rules`, for every rule of the schema.
At `column` granularity, `dy_schema__rules` still reports the schema-level rules when `schema_rules="collapsed"`.

## Why this is out of scope

The maintainer agreed to drop `schema` in a design review on 2026-10-19, and [#148](https://github.com/ozanozbeker/dagster-dataframely/issues/148) removed it.
Issue [#8](https://github.com/ozanozbeker/dagster-dataframely/issues/8) chose the three granularities, and [#21](https://github.com/ozanozbeker/dagster-dataframely/issues/21) built them.
The counts below are from `c65797f`, on dataframely 3.1.2.

### It kept no per-rule history

[#8](https://github.com/ozanozbeker/dagster-dataframely/issues/8) chose `rule` as the default because "per-rule history is the entire reason to prefer asset checks over a log line".
One check for every rule kept none of that history.
The same issue named `schema` "the first mode to drop if maintaining three proves a burden".

### Its check blocked nothing new

With `filter=False`, a failing `"error"` rule already blocks downstream assets at `rule` and `column` granularity.
One `dy_schema__rules` check blocked in the same cases.
With `filter=True`, no rule's check is blocking at any granularity.

### It mixed severities

One check held every `"warn"` and `"error"` rule of the schema, and it had one severity.
When a `"warn"` rule and an `"error"` rule failed in the same run, the check failed at `ERROR`, and nothing showed the `WARN`.

### Only the demo used it

The demo's `legacy_orders` was the one asset at `schema` granularity.
Each new check result feature still handled it, as [#141](https://github.com/ozanozbeker/dagster-dataframely/issues/141) did for the result description.

### `column` stays, because Dataframely generates rules nobody wrote

dagster-dbt makes one asset check per dbt data test, and has no setting that collapses them.
A dbt model has one test per entry in its YAML, so a model with no tests has no checks.
Dataframely generates rules from the column types and their defaults, so a schema with no constraints written still has rules.
These are the check counts from `asset_check_specs`, without the column-schema check:

| schema, with no constraints written | `rule` | `column` | `schema` |
| --- | --- | --- | --- |
| 40 `Float64` columns | 120 | 40 | 1 |
| 40 `String` columns | 40 | 40 | 1 |
| one `Struct` with 10 fields | 11 | 1 | 1 |

`column` reduces the checks where Dataframely generates several rules per column.
`schema` reduced them further only by keeping no per-rule history.

### Removing it saves little code

`column` still collapses rules, so the collapsed metadata, the collapsed description and the grouping of rules into checks all stay.
The removal saves one mode that each new check result feature handles and tests.

## If someone proposes this again

A user who wants one pass or fail result per asset can read the run's status, or the asset's health, which any failing check sets.
A request for fewer checks on a wide schema is a request for `column`, or for a grouping between `column` and `rule`, not for one check per schema.

## Prior requests

- [#8](https://github.com/ozanozbeker/dagster-dataframely/issues/8), "Check granularity: one check per rule, or per schema?"
- [#148](https://github.com/ozanozbeker/dagster-dataframely/issues/148), "Drop the schema check granularity"
