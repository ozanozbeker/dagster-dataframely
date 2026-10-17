# Collection Assets

This package builds no asset from a `dy.Collection`.
Each member of a Collection is an asset you declare, usually with `dd.asset` and the member's own schema.
`dd.collection_checks` reports each collection filter as an asset check on those assets, and no run removes a row because it fails a collection filter.

## Why this is out of scope

[#4](https://github.com/ozanozbeker/dagster-dataframely/issues/4) mapped a Collection onto Dagster in August 2026.
It recommended one `@dg.multi_asset` with an output per member, design A in [the pre-1.0 record](../pre-1.0.md#how-a-dycollection-maps-onto-the-dagster-asset-model).
It ruled out member assets plus a `@dg.multi_asset_check`, design B, because a check reports a failing row and does not remove it. [#13](https://github.com/ozanozbeker/dagster-dataframely/issues/13) deferred Collection support, and [#43](https://github.com/ozanozbeker/dagster-dataframely/issues/43) took it up for 1.0.
The maintainer chose design B on 2026-10-16, during the triage of #43.
The probes below ran on dagster 1.13.24, dataframely 3.1.2 and polars 1.44.2.

### A collection filter states a relation between tables

The maintainer's model of a Collection is referential integrity, as a SQL database enforces it between tables.
A collection filter is how Dataframely states that relation.
Dataframely compares members only on their common primary key, and it raises `ImplementationError` for a Collection with filters whose members share no primary-key column.
So a foreign key such as `orders.customer_id` to `customers.customer_id` is a collection filter on a Collection where `customers` has `ignored_in_filters=True`.
That filter reported one failure on `orders`, for an order whose customer did not exist, and `customers` had no check.

The tables already exist as assets, each validated against its own schema.
A Collection that produced them would put N assets in one step to state a relation between them.
A dbt project states the same relation as a `relationships` test.
In dagster-dbt 0.29.25, that test is an asset check on the referencing model, with `additional_deps` on every other model the test reads.
`dd.collection_checks` follows that precedent.

### Design A required the members to run together

`Collection.filter` takes every required member, and a row that fails in one member removes rows from another.
So design A had to materialize every member in one step, and `can_subset=True` was unsound.
Selecting one member would write it with the collection filters applied and leave the others as an earlier run wrote them.

The checks have no such constraint.
With `can_subset=True`, a run that selects `orders` evaluates the check on `orders` and reads `customers` as the last run wrote it.
Each member stays an asset that runs alone.

### A row that fails a collection filter stays in its table

[#4](https://github.com/ozanozbeker/dagster-dataframely/issues/4) named this, and it still holds.

An `"error"` check fails the run and blocks the member's downstream assets in that run.
The row stays in the member's table, so a later run that selects only a downstream asset reads it.
The check reports the failure and does not change the table.

### Failure propagation has no check

`propagate_row_failures=True` removes a row from every member when a row with the same common primary key fails its own member's rules.
That is a cascade, not a condition on the tables as written.
The failing row is in its member's error rows, not in its table, so a check over the tables always passes.
On the persisted members, `Collection.filter` with `skip_member_validation=True` returned no `orders|failure_propagation` failure for a customer whose only order had failed `amount|min`.
`dd.collection_checks` raises `FailurePropagationError` for a non-ignored member that sets it.

### The checks read whole tables

Collection filters are partition-local, as the pre-1.0 record states.
Specs with `partitions_def` reported a customer in partition `d1` whose only order was in `d2`, and reported that order in `d2`, although both rows were valid across the whole dataset.
Specs without `partitions_def` over partitioned members loaded every partition, as a dict from `FilesystemIOManager`, and raised `FileNotFoundError` until every partition existed.
So `dd.collection_checks` raises `InvalidMembersError` for a partitioned member.

## If someone proposes this again

Design A would return for a user who needs each table to hold only rows that pass every collection filter, which a check cannot guarantee.
The pre-1.0 record lists the four Dagster constraints #4 verified for it: `outs=` instead of `specs=`, the type check that compares a `LazyFrame` with `!=`, `is_required=False` for an optional member, and an unsound `can_subset=True`.

Failure propagation would need each propagating member's error rows as an input, so it would require `filter=True` on every one of those members.

Partitioned members would need a user whose entities never span partitions, so that a per-partition check reports no failure that the whole dataset does not have.

## Prior requests

- [#4](https://github.com/ozanozbeker/dagster-dataframely/issues/4), "How does a dataframely Collection map onto the Dagster asset model?"
- [#13](https://github.com/ozanozbeker/dagster-dataframely/issues/13), "The Collection shape in Dagster".
- [#43](https://github.com/ozanozbeker/dagster-dataframely/issues/43), "Collection support: a sibling door for `dy.Collection`", which builds `dd.collection_checks` instead.
