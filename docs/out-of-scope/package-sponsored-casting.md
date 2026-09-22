# Casting by the Package

`dd.asset` takes no `cast` argument, and nothing in this package coerces a dtype on a user's behalf.
When a frame's column schema does not match the schema, the blocking column-schema check fails the run.
A user who wants a cast calls `Schema.cast` in the decorated function, where they can see it.

## Why this is out of scope

Designed in full on 2026-09-10 while triaging [#88](https://github.com/ozanozbeker/dagster-dataframely/issues/88), and rejected.
A new proposal has to address the measurements below, and repeating them takes an afternoon.

### Projection needs no cast

The report assumed that extra columns forced a user to call `Schema.cast` in all 21 of their assets.
It did not.
With `cast=False`, `Schema.filter` runs `lf.select(target.column_names())`, eager or lazy, so it narrows a superset to the schema's columns, in the schema's order:

```text
frame  {"extra", "amount", "order_id"}      superset, scrambled order
cast=False -> ['order_id', 'amount']
cast=True  -> ['order_id', 'amount']
```

`_column_schema_problems` iterates the schema's columns, so the check never reads a column the schema does not declare.

So `cast=True` adds only dtype coercion, and projection does not justify a `cast` argument.

### One flag enables lossless and lossy casts together

Dataframely's `cast` is a `bool` that selects `casting="lenient"`, so it enables every cast or none.
Widening casts keep the value:

```text
Int32          -> Int64      1 -> 1
Decimal(18,3)  -> Float64    1.5 -> 1.5
```

The same flag enables narrowing casts:

```text
Float64 -> Int64      1.9 -> 1              truncated, no rule fails
Int64   -> Int32      2**40 -> row dropped  the valid frame is empty
Duration ns -> us     1500ns -> 1us         precision gone
```

A truncation or a loss of precision fails no rule.
A value the cast cannot convert becomes null, and the `<column>|dtype` rule that `cast=True` adds makes its row invalid.
That covers `2**40` above and `"abc"` cast to `Int64`, and the failure info keeps the original value.

Allowing only widening casts would require this package to keep its own lattice of Polars dtypes.
That is new public API, and it goes out of date each time Polars adds a dtype.
`CONTEXT.md` also has the package use Dagster's, Dataframely's and Polars' own terms, and the lattice would be its own.

### The blocking check does not work with a cast

The column-schema check runs before `Schema.filter` and fails the run on a dtype mismatch, which is what a cast would fix.
So adding a cast means changing what that check does, and every option failed when measured.

Running the check after the cast does not work.
`collect_schema()` on the cast plan reports the target dtypes by construction, so the check passes for every input:

```text
Int32   -> Int64     pre-cast problems: [order_id]   post-cast problems: []
Struct  -> Float64   pre-cast problems: [amount]     post-cast problems: []
```

For a cast that cannot succeed, the check passes a wrong frame.
Polars resolves a lenient `Struct` cast to the target dtype at plan time, and produces a struct at collect time:

```text
plan-time: {'amount': Float64}
runtime  : {'amount': Struct({'a': Float64})}
```

A check that reads the plan would pass that frame, and the IO manager would write the struct to storage under a schema that declares `Float64`.
The cast plan also has `__DATAFRAMELY_ORIGINAL__*` columns, so reading it needs two private Dataframely names: `_match_to_schema.match_to_schema` and `ORIGINAL_COLUMN_PREFIX`.

This follows from the design, not from a detail of the implementation: a column schema does not show whether a cast will succeed.
Only collecting the plan shows it, because Polars raises there, and the only other source is the lattice rejected above.

### What an impossible cast does today

For anyone measuring this again, `filter(cast=True)` end to end:

```text
Int32 -> Int64, Decimal -> Float64    cast cleanly
"abc" -> Float64                      invalid row, quarantined, |dtype rule reports it
List  -> Float64                      InvalidOperationError at collect
Struct -> Float64                     InvalidOperationError at collect
```

The last two fail the run with a raw Polars error from `collect_all`, after the plan has run.
Only the `List` error names the failing cast.

## What is in scope

`user_guide/the-failure-policy.qmd` documents the policy.
The policy is right, but its first stated reason was wrong: casting `Duration('ns')` to `Duration('us')` rescales the value, so the "thousandfold error" it warned about does not happen.
That correction, and the fact that projection needs no cast, are [#88](https://github.com/ozanozbeker/dagster-dataframely/issues/88).

## If someone proposes this again

Adding `cast` later would change no existing behaviour, so this decision is easy to reverse, and the maintainer wrote no ADR.
A user whose dtype mismatch does not come from a warehouse read would reopen it.
In the DuckDB case behind the original proposal, the read returns `DECIMAL` where the schema declares `Float64`.
That comes from the reader, not from the column, so the fix may belong in the IO manager.

## Prior requests

- [#88](https://github.com/ozanozbeker/dagster-dataframely/issues/88), "Every one of my 21 assets ends in `Schema.cast(...)`, which makes the deliberate cast a ritual".

The maintainer designed the `cast` argument in triage, and the report did not request it, so #88 covered only the README correction and closed with it.
