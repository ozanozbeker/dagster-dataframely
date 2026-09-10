# Expression-Based Metadata

This package writes the statistics it derives from the schema, and takes no expression from a user.
There is no way to say "profile these columns with this aggregate", and no way to put a revenue total, a ratio between two columns, or a count matching a predicate on a materialization.

The `statistics` setting is on or off.
What it writes is one table per dtype family present, under `dataframely/valid_stats/<family>`, and the families are fixed.

## Why this is out of scope

Deferred from #15 when the statistics pass was designed, and closed on 2026-09-10 with no user having asked in the year since.

### The two features answer different questions

The family tables answer "what does this table look like", which is why a data consumer opens an asset.
Every aggregate in them is typed and total, so the pass has no failure mode at all.

A named derived metric answers "how is the business doing", which is a question about the data rather than about the table.
Dagster already has a place for that: a metric asset, with its own key, its own history, and its own checks.
A number smuggled into a validated table's materialization metadata gets none of those.

### The knob turns into a query language

The moment a user supplies an expression, this package owns three decisions it has no basis for:

- Where the result lands, alongside `stats/*` or in a namespace of its own.
- What happens when a metric fails to evaluate.
  The statistics pass cannot fail, so there is no precedent to follow, and either answer is bad: taking the materialization down means a typo in a nice-to-have number fails a valid table, and dropping it with a warning means a metric can silently stop existing.
- Whether the result is plottable, which is most of the value for a numeric metric over time and is a property of the metadata type rather than of the expression.

None of those has a right answer without a user to ask.
Guessing them and freezing the guess into the surface is worse than not having the feature.

## If this is reconsidered

The evidence that reopens it is a user who names a metric they cannot get any other way, and says why a separate asset is the wrong home for it.
That request will also answer the failure-mode question, which is the one this package cannot decide alone.

Selector aggregates are the smaller half and may arrive first.
They need no failure mode, since a selector over columns that are already typed stays total, so they could land without settling the derived-metric questions at all.

## Prior requests

- [#47](https://github.com/ozanozbeker/dagster-dataframely/issues/47), "Expression-based metadata helpers: selector aggregates and derived metrics", deferred from #15.
