# Expression-Based Metadata

This package writes the statistics it derives from the schema, and takes no expression from a user.
There is no way to ask for an aggregate over chosen columns, and no way to put a revenue total, a ratio or a predicate count on a materialization.

The `statistics` setting is on or off.
When it is on, the package writes one table per dtype group present, under `dataframely/valid_statistics/<group>`, and no setting changes the groups.

## Why this is out of scope

The maintainer deferred it from #15 while designing the statistics, and closed it on 2026-09-10.
No user asked for it in the month #47 was open.

### The two features report different things

The group tables show what the table looks like, which is why a data consumer opens an asset.
Polars defines every aggregate in them for every value of its column's dtype, so computing them cannot fail.

A named derived metric shows how the business is doing.
That is a question about the data, not about the table.
Dagster already has a place for it: a metric asset, with its own key, its own history, and its own checks.
A number added to a validated table's materialization metadata has none of those.

### The setting becomes a query language

Accepting an expression from a user requires three decisions, and nothing gives a basis for any of them:

- Where the package writes the result: beside `dataframely/valid_statistics/<group>`, or in a namespace of its own.
- What happens when a metric fails to evaluate.
  The statistics cannot fail, so there is no precedent, and both options are bad.
  Failing the materialization lets a typo in an optional number fail a valid table.
  Dropping the metric with a warning lets it stop appearing while every run succeeds.
- Whether Dagster can plot the result.
  For a numeric metric over time, the plot is most of the value, and it depends on the metadata type, not the expression.

None of those has a right answer without a user to ask.
Guessing the answers and fixing them in the public API is worse than not having the feature.

## If someone proposes this again

A user would reopen it by naming a metric they cannot get any other way, and explaining why a separate asset is the wrong place for it.
That request would also settle the failure-mode question, which the maintainer cannot settle alone.

Selector aggregates are the smaller of the two features, and the maintainer could add them first.
They need no failure mode, because a selector picks columns whose dtypes the column-schema check already verifies, so adding them needs no answer to the derived-metric questions.

## Prior requests

- [#47](https://github.com/ozanozbeker/dagster-dataframely/issues/47), "Expression-based metadata helpers: selector aggregates and derived metrics", deferred from #15.
