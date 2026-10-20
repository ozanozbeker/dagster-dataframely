# Collapsing the Schema-Level Rules

At `column` granularity, each schema-level rule, a `@dy.rule()` or the primary key, has its own `dy_rule__<rule>` check.
No setting collapses them into one check.
`schema_rules="collapsed"` did, into `dy_schema__rules`, and it was the default.

## Why this is out of scope

The maintainer agreed to drop the setting in a naming review on 2026-10-20.

### A setting inside a setting

`schema_rules` had an effect only when `check_granularity="column"`.
Every docstring had to say so, and the two value sets, `rule`/`column` and `collapsed`/`per_rule`, named the same idea two ways.

### The reason for `column` does not apply to a schema-level rule

`column` exists because Dataframely generates rules nobody wrote: `nullability`, `inf`, `nan`, and the `inner_` rules of a nested column.
A schema with no written constraints still has checks, and collapsing them per column keeps the list readable ([`schema-granularity.md`](schema-granularity.md)).
A `@dy.rule()` or a `primary_key` is always written by the user.
In dagster-dbt, each dbt data test has its own asset check, and [#8](https://github.com/ozanozbeker/dagster-dataframely/issues/8) called per-rule history "the entire reason to prefer asset checks over a log line".

### One check mixed severities

The maintainer agreed on 2026-10-19 to drop the `schema` granularity, in part because one check held `"warn"` and `"error"` rules.
When a `"warn"` rule and an `"error"` rule both failed, the check failed at `ERROR`, and nothing showed the `WARN`.
`dy_schema__rules` had the same flaw for the schema-level rules.

## If someone proposes this again

A request for fewer checks on a schema with many `@dy.rule()`s is a request to collapse written rules into checks, which would need rule sets the user names, not a flag.
No issue records a user asking for it.

## Prior requests

- [#8](https://github.com/ozanozbeker/dagster-dataframely/issues/8), "Check granularity: one check per rule, or per schema?"
