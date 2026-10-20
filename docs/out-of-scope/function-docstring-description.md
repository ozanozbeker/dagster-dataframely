# The Function's Docstring as the Description

`dd.asset` takes the asset's description from `description=`, then the schema's docstring, then the decorated function's docstring.

## Why this is out of scope

Proposed as [#135](https://github.com/ozanozbeker/dagster-dataframely/issues/135) on 2026-10-18, and rejected by the maintainer the same day.
The proposal put the function's docstring before the schema's, because several assets often share one schema.
The current order comes from [#75](https://github.com/ozanozbeker/dagster-dataframely/pull/75), which closed [#71](https://github.com/ozanozbeker/dagster-dataframely/issues/71).

### The catalog shows what the table holds

The schema's docstring describes the rows: `OrdersSchema`'s is "A customer order line: one row per product on one order."
The function's docstring describes the code that writes them.
Its summary line is imperative under numpydoc, such as "Validate the storefront's order lines."
The maintainer wants the catalog to show what the table holds, not what the code does.

### A shared schema shares its description on purpose

In the demo, 10 of the 11 `dd.asset` assets use `OrdersSchema`, and all 10 show its docstring.
Each of them holds order lines, so the same sentence is correct for each.
An asset that needs its own sentence passes `description=`, which takes precedence over both docstrings.

## Prior requests

- [#135](https://github.com/ozanozbeker/dagster-dataframely/issues/135), "The schema's docstring replaces the decorated function's docstring in the catalog".
