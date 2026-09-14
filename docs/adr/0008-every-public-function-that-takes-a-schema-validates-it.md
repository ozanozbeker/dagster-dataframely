# 8. Every public function that takes a schema validates it

Accepted, 2026-09-13. Extends [ADR-0007](0007-the-quarantines-key-is-checked-at-run-time-not-at-load.md), which named the three reserved namespaces and enforced the third. The decorator is `dd.asset` since 0.8.

## Context

`validate_namespace` guarded one of the eight public functions that take a schema (#123). `check_specs` called it. `dy_asset` inherited the refusal by resolving its specs through `check_specs`, and only once the returned decorator was applied. The other six accepted a schema this package cannot name.

Three of them were wrong rather than merely unguarded.

`_collapsed_metadata` builds each sample row as `{"dy_rule": rule_name, **row}`. A user column named `dy_rule` won that merge, so a collapsed check's sample lost the only thing saying which rule put a row in it. Both collapsed granularities were affected. At `rule` granularity there was no overwrite, but the check then carried `dy_rule: "amount|min"` in its own metadata and `dy_rule: "kept"` in its sample rows: one key, two meanings, on one check.

`quarantine_frame` renames each rule column to that rule's check name, so a user column already holding the name collided. It raised Polars' own duplicate-column error, which names nothing about this package.

`quarantine_spec` builds the quarantine's Columns tab from the same rename. It duplicated the column silently, at definition time, and added a doubled prefix for the rule derived from the offending column.

The hole predates #80, which only added a second function onto it. `validation_results` never called the guard either.

The two errors leak together. `check_results` at `rule` granularity yielded two results under one check name for two rules that rewrite alike, which is the `CheckNameCollisionError` half of the same gap.

## Decision

**`dy_` is reserved in the user's column space unconditionally, and every public function taking a schema refuses one that claims it.**

The guarantee is a property of the schema, not of what each function does with it. `table_schema` and `schema_metadata` project the user's own columns and collide with nothing, so they could have been exempt. They are not, because the rule that holds everywhere is one sentence and the rule with an exemption needs an `unless` clause. CONTEXT.md already states the reservation as absolute and never configurable, so a `dy_` column is outside the contract whether or not a given projection survives it.

**The guard runs first, before the settings resolve and before any frame is read.** `check_specs` already ordered it that way. A schema this package cannot name is broken whatever the settings say and whatever the frame holds.

**`dy_asset` validates when the factory is called, not when the decorator is applied.** The call is redundant with the `check_specs` below it. It buys uniformity: every public function then refuses on the call that takes the schema, so `maker = dy_asset(Reserved)` fails on that line rather than on first use.

**`check_results` and `validation_results` raise on first iteration.** Both are generators, so a guard in the body cannot fire at call time. `ColumnSchemaError` already behaves that way in `check_results`, and inside a Dagster step the two are indistinguishable.

**`validate_namespace` is not exported, unlike `validate_quarantine_key`.** ADR-0007 exported its check because no function in `wiring` can reach a run's asset graph, so a hand-wired asset has to make that call itself. This one needs only the class, so every public function makes it and nobody has to remember.

**Nothing is cached.** A `functools.cache` keyed on a class holds a strong reference for the process lifetime, and the measurements below say it would be paid to save microseconds.

**A test lists the set rather than reflecting it.** `tests/test_reserved_namespace.py` names each of the eight and calls it with whatever else it needs to reach the guard. Nothing else is touched when the guard runs first, so `ReservedColumnError` proves the ordering and a `TypeError` proves the guard sits too late. `test_public_surface.py` already makes every new public name a decision somebody takes in a test file, so a ninth schema-taking function costs one line beside the eight it joins.

> **Abandoned in `3c5438f`, 2026-09-13.** This first decided the opposite: a test reflecting the set off the package, finding every public callable in `dagster_dataframely` and `dagster_dataframely.wiring` whose first parameter was annotated `type[dy.Schema]` and calling each with `None` for every other argument, so a ninth such function was covered the day it was written. Two tests existed to hold the reflection up, one asserting that no public function takes a schema anywhere but first, one asserting the reflection had found the functions it knew about. Both went with it. The reflection bought coverage of a function nobody has written, and paid for it with two tests of its own scaffolding and a stub that only works while no guard reads an argument.

**`validate_namespace` refuses a third thing on the same terms.** A column spelled in anything but `A-Za-z0-9_` raises `UnnameableColumnError`. It is the same kind of property as the reserved one: no annotation expresses it, and only reading the schema's columns tells you. Dataframely's `alias=` is how a column comes by such a name, and Dagster validates every check name against `^[A-Za-z0-9_]+$`, so the two features meet at the name this package builds from the column.

A `|` in a column name is checked ahead of the rule walk rather than beside the others. It is Dataframely's own delimiter, so `described_rules` reads part of the column as a rule name and hands back a column the schema does not have. Before the guard, the first renderer to index by it died on `KeyError: 'a'`, which is the one unnameable column Dagster's own refusal would never have caught.

## Evidence

Measured on dagster 1.13.20, dataframely 3.0.0, polars 1.44.1.

The eight, found by first-parameter annotation:

```text
dd.dy_asset              dd.wiring.check_specs
dd.quarantine_spec       dd.wiring.check_results
                         dd.wiring.quarantine_frame
                         dd.wiring.schema_metadata
                         dd.wiring.table_schema
                         dd.wiring.validation_results
```

The sample overwrite, on a schema declaring `dy_rule` beside `amount = dy.Int64(min=1)`:

```text
check_results       schema  dy_schema__rules  [{'dy_rule': 'kept', 'amount': -1}]
check_results       column  dy_col__amount    [{'dy_rule': 'kept', 'amount': -1}]
validation_results  schema  dy_schema__rules  [{'dy_rule': 'kept', 'amount': -1}]
validation_results  column  dy_col__amount    [{'dy_rule': 'kept', 'amount': -1}]
```

Every one should read `amount|min`.

`quarantine_spec` on a schema declaring `dy_rule__amount__min`, which is the name the rename produces:

```text
['dy_rule__amount__min', 'amount',
 'dy_rule__dy_rule__amount__min__nullability',
 'dy_rule__amount__nullability',
 'dy_rule__amount__min']
```

`quarantine_frame` on the same schema raised `DuplicateError: column 'dy_rule__amount__min' is duplicate`.

The cost of the guard, against the cheapest call that now carries it:

```text
  5 columns,  15 rules     24 us
 40 columns, 120 rules    173 us
200 columns, 600 rules    853 us

check_results, 40 columns, 3 rows    8 ms
```

At 40 columns the guard is 2.2 percent of a `check_results` call on three rows, and the denominator only grows with the data.
