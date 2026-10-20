# dbt's Data Test Configs

This package copies `severity` from dbt's data test configs, and none of the others.
It has no counterpart to `warn_if`, `error_if` or `limit`.
The `DAGSTER_DATAFRAMELY_*` variables set a default for a code location, as the `+` configs in `dbt_project.yml` do for a project.
They set only the settings, and no variable sets `filter`, a rule's severity or the error-rows key.
`filter=True` is the nearest thing to `store_failures`: a run writes the rows that failed an `"error"` rule under `<name>__error_rows`, beside the asset.

## Why this is out of scope

The maintainer compared each pre-1.0 decision with dbt's data test configs in [#130](https://github.com/ozanozbeker/dagster-dataframely/issues/130) on 2026-10-18.
The maintainer then renamed the default error-rows key from `<name>_error_rows` to `<name>__error_rows`, and declined every config below.
The probes below ran on dagster 1.13.24, dagster-duckdb-polars 0.29.24, DuckDB 1.5.5 and dataframely 3.1.2.

### `store_failures` writes to `<schema>_dbt_test__audit`

In dbt, `store_failures` writes a test's failing rows to the schema `<schema>_dbt_test__audit` by default.
This package writes the error rows beside the asset instead.
An asset keyed `["sales", "orders"]` writes them under `["sales", "orders__error_rows"]`.

The suffix is on the last key part, because `DbIOManager` takes the table name from the last key part only.
It takes the schema from the asset's `schema` metadata, then from its own `schema` config, and only then from the key part before the last one.
`build_io_manager_writer` passes the asset's definition metadata to the IO manager unchanged.
So with either of the first two set, a leading part such as `["error_rows", "orders"]` writes to the asset's own table.
An asset with `metadata={"schema": "sales"}` and `error_rows_key=["error_rows", "orders"]` wrote its error rows to `sales.orders`.
The asset's own write to that table then raised a DuckDB `BinderException`.
`validate_error_rows_key` raised nothing, because it compares asset keys.
A trailing part such as `["orders", "error_rows"]` names every asset's error-rows table `error_rows`.
Two assets with `metadata={"schema": "sales"}` and trailing parts both wrote to `sales.error_rows`, and the second write replaced the first one's rows with no error.

With `error_rows_io_manager_key`, a run can already write the error rows to a separate schema, as dbt does.
The key names a second IO manager configured with that schema.
The asset's `schema` metadata still takes precedence over that config.

The suffix starts with `__`, because the package already uses `__` to separate the parts of a name it generates.
`dy_rule__amount__min` and `dy_schema__columns` are two such names.
So the default never collides with an asset a user names `orders_error_rows`.

`__` has a meaning in two other places.
In dlt, `__` forms the name of a nested table: dlt writes the items of each user's `pets` list to `users__pets`.
`AssetKey.to_python_identifier()` returns `orders__error_rows` for both `["orders__error_rows"]` and `["orders", "error_rows"]`.
Dagster names an op, a multi-asset output and a check output with it.
The error rows have none of the three.

### `store_failures` on a test with `severity: error`

In dbt, a test with `severity: error` and `store_failures` fails and stores its failing rows.
Here, a row that fails an `"error"` rule on an asset with `filter=False` fails the run.
That run writes no error rows.
With `filter=True`, a run where every row fails an `"error"` rule writes the error rows and then fails.

Writing the error rows needs a writer, and the writer writes on every run where `Schema.filter` runs.
On a run with no error rows, it writes an empty frame.
An IO manager has no delete, so only an empty write replaces the last run's rows.
So writing them is opt-in, as `store_failures` is in dbt.
`filter=True` is the opt-in.
A run that fails and still writes its error rows would need a second argument for that one combination.

### `store_failures` on a test with `severity: warn`

In dbt, a test with `severity: warn` and `store_failures` warns and stores its failing rows.
The model keeps them.
Here, a row that fails only `"warn"` rules is written to the asset's table and not to the error rows.
Writing it to the error rows as well would break the guarantee that the table and the error rows never share a row.
A second frame for those rows would need a second key, an empty write on every run and a second collision check.

To read the rows that failed only `"warn"` rules, run `Schema.filter` on the table.
Dataframely does not read severity, so those rows fail the `"warn"` rules again.
`FailureInfo.invalid()` returns them.

### `+severity`, `+store_failures` and `+schema` in `dbt_project.yml`

In dbt, a `+` config in `dbt_project.yml` sets a default for every test in a project, a package or a folder.
Here, `functools.partial(dd.asset, ...)` sets a default for the assets that use it.
A call can still override a bound argument.
[The settings page](https://ozanozbeker.com/dagster-dataframely/user-guide/settings.html) shows an example.

There is no `set_default_*()` function, because Dagster loads code locations lazily and a package-level default would depend on import order.
Dagster has no group object to hold a default per asset group, because `group_name` is a string on each asset.
An environment variable for `error_rows_io_manager_key` would be the nearest thing to a project-wide `+schema`.
The maintainer deferred one from [#111](https://github.com/ozanozbeker/dagster-dataframely/issues/111) to this comparison.
The maintainer then declined it, because it would change `required_resource_keys` per environment.

### `warn_if` and `error_if`

In dbt, `warn_if` and `error_if` compare the number of failures with a threshold, such as `>10`.
A threshold changes only whether the test passes, warns or errors.
Here, severity also sets whether a row is written to the table or to the error rows.
So a threshold would do one of two things.
Either whether a row is an error row would depend on how many other rows failed, or a run would leave rows out of the table while their rule's check passes.
Each reading needs a design first.

Adding a key beside `severity` later is an additive change.
The package raises on any other key in the `dagster_dataframely` metadata entry today, so no schema already sets one.

### `limit`

In dbt, `limit` caps the number of failures a test query returns.
The stored failures have the same cap, and the model still has every row.
Here, the error rows are the only copy of the rows that a run with `filter=True` leaves out of the table.
So a cap would drop rows that exist nowhere else.
The package has already collected every error row when the writer receives them, so a cap would make only the write smaller.

## If someone proposes this again

A user would reopen thresholds with a design for one of the two readings in [`warn_if` and `error_if`](#warn_if-and-error_if).
The design would add a key beside `severity` in the metadata entry.

A user would reopen error rows for a failed run by showing that an asset with `filter=False` needs them.
The change would be a second argument that has an effect only with `filter=False`.

A user would reopen a second frame for the rows that failed only `"warn"` rules by showing that they cannot read the table back to filter it.

A user would reopen a cap on the error rows with a measurement where writing the error rows, not collecting them, takes most of the run's time.
The user would also have to accept that the cap drops rows.

## Prior requests

- [#111](https://github.com/ozanozbeker/dagster-dataframely/issues/111), "Override the error-rows key and the IO manager that writes the error rows", which deferred an environment variable for either keyword to #130.
- [#130](https://github.com/ozanozbeker/dagster-dataframely/issues/130), "Compare the pre-1.0 decisions with dbt's data test configs".
