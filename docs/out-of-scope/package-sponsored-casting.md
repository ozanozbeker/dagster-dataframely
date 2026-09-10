# Package-Sponsored Casting

`dy_asset` takes no `cast` argument, and nothing in this package coerces a dtype on a user's behalf.
A frame whose column schema does not match the schema aborts the run through the blocking column-schema check.
A user who wants conformance writes `Schema.cast` in their own asset body, where they can see it.

## Why this is out of scope

Designed in full on 2026-09-10 while triaging [#88](https://github.com/ozanozbeker/dagster-dataframely/issues/88), and rejected.
The measurements below are the durable part.
They are what a fresh proposal has to answer, and re-deriving them costs an afternoon.

### The projection half is already free

The report hypothesised that the column-schema check's treatment of extra columns was what forced `Schema.cast` into all 21 of a user's assets.
It is not.
`Schema.filter` runs `lf.select(target.column_names())` on both of its paths, so it narrows a superset to the schema's columns, in the schema's order, eager and lazy, with `cast=False`:

```text
frame  {"extra", "amount", "order_id"}      superset, scrambled order
cast=False -> ['order_id', 'amount']
cast=True  -> ['order_id', 'amount']
```

`column_schema_problems` agrees from the other side: it iterates the schema's columns, so a column present in the frame and absent from the schema is never read.

What `cast=True` adds over `cast=False` is dtype coercion and nothing else.
Any proposal that justifies itself by projection is answered here.

### One flag buys the safe and the silent together

Dataframely's `cast` is a `bool` over `casting="lenient"`.
It is all or nothing.
The widenings are exact:

```text
Int32          -> Int64      1 -> 1
Decimal(18,3)  -> Float64    1.5 -> 1.5
```

The narrowings pass through the same flag, and none of them reports anything:

```text
Float64 -> Int64      1.9 -> 1              truncated, no rule fires
Int64   -> Int32      2**40 -> row dropped  valid frame comes back empty
Duration ns -> us     1500ns -> 1us         precision gone
```

Only an unparsable value is handled honestly.
`"abc"` into an `Int64` becomes an invalid row under a `<column>|dtype` rule that `cast=True` adds, carrying its original value into the failure info.

Restricting the flag to the widening direction means this package holding a dtype lattice over Polars.
That is real surface, it drifts every time Polars adds a type, and `CONTEXT.md` says borrow rather than invent.

### The blocking check cannot be made to agree with a cast

The column-schema check runs before `Schema.filter` and aborts on a dtype mismatch, which is the thing a cast exists to fix.
So casting cannot be added without deciding what that check means, and every answer measured badly.

Running the check after casting does not work.
`collect_schema()` on the cast plan reports the target dtypes by construction, so the comparison passes for every input:

```text
Int32   -> Int64     pre-cast problems: [order_id]   post-cast problems: []
Struct  -> Float64   pre-cast problems: [amount]     post-cast problems: []
```

On the uncastable case it is worse than vacuous.
Polars resolves a lenient `Struct` cast to the target dtype at plan time and produces a struct at collect time:

```text
plan-time: {'amount': Float64}
runtime  : {'amount': Struct({'a': Float64})}
```

A check reading the plan would pass that frame, and the struct would reach storage under a schema declaring `Float64`.
The cast plan also carries `__DATAFRAMELY_ORIGINAL__*` columns, so reading it needs two private upstream names, `_match_to_schema.match_to_schema` and `ORIGINAL_COLUMN_PREFIX`.

The reason is structural rather than a detail of the implementation.
Cast feasibility is not answerable from a column schema.
It is known at collect, where Polars raises, or from the dtype lattice rejected above.

### What an impossible cast does today

For anyone measuring this again, `filter(cast=True)` end to end:

```text
Int32 -> Int64, Decimal -> Float64    cast cleanly
"abc" -> Float64                      invalid row, quarantined, |dtype rule reports it
List  -> Float64                      InvalidOperationError at collect
Struct -> Float64                     InvalidOperationError at collect
```

The last two stop the run as a raw Polars error out of `collect_all`, after the plan ran, and only the `List` case names the offending cast in its message.

## What is in scope

Saying all of this in the README.
The doctrine is right and its stated justification was wrong: a `Duration('ns')` widened to `Duration('us')` rescales correctly, so the "thousandfold error" it warns about does not happen.
That correction, and the fact that projection is free, are [#88](https://github.com/ozanozbeker/dagster-dataframely/issues/88).

## If this is reconsidered

Adding `cast` later is purely additive, so nothing here is expensive to reverse and no ADR was written.
The evidence that would reopen it is a user whose dtype mismatch is not a warehouse read.
The DuckDB case that motivated the original proposal returns `DECIMAL` where a schema declares `Float64`, and that is a property of the reader rather than of the column, so it may belong to the IO manager instead.

## Prior requests

- [#88](https://github.com/ozanozbeker/dagster-dataframely/issues/88), "Every one of my 21 assets ends in `Schema.cast(...)`, which makes the deliberate cast a ritual".

The casting design came out of triage rather than the report, so #88 itself stays open for the README correction.
