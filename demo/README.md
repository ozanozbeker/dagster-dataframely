# `dagster-dataframely` demo

The demo is an online retailer's pipeline, and Dataframely schemas validate its silver and gold assets.
The example in the repository's README, the two modules on the Get started page and the user guide's screenshots come from it.

The bronze assets load a customer export and four order feeds, unchanged.
The silver assets validate each order feed against `dd.data.OrdersSchema`, the schema the package ships for its examples.
Three of those feeds have the kinds of errors real feeds have: one has a column with the wrong dtype, one has some rows that fail a rule, and one has only rows that fail a rule.
`customers` is the smallest silver asset: three columns, validated against a schema declared in the same module.
The README and the Get started page open with it.
The gold assets read the validated tables.

## Getting started

The demo is part of the repository's uv workspace, so it runs the package from the working tree, not from a published wheel.
Edit `src/dagster_dataframely/`, and the next `dg dev` shows the change.

```bash
cd demo && uv run dg dev
```

Then open <http://localhost:3000>.

You do not need to set `DAGSTER_HOME`.
`dg dev` creates its instance in a temporary directory and deletes it when it stops, so every session starts empty.
The IO manager writes tables to `storage/`, and they remain after the session ends.
Run `rm -rf storage` to delete them too.

Syncing the demo installs the webserver into the workspace's shared `.venv`.
Running `uv sync` at the repository root removes it again.

## Materialize before you look

Much of what the UI shows appears only after an asset has run.
Paste each selection into the asset graph's search bar and click **Materialize**, in this order:

1. `group:bronze` materializes the five bronze assets.
2. `key:orders or key:customers or key:marketing_orders or key:warehouse_orders or key:high_value_orders or key:orders_snapshot` materializes six assets.
   The run still fails, because the blocking checks of `warehouse_orders` fail in their own step.
3. `key:daily_orders` opens the backfill dialog: select all five days.
4. `key:regional_orders` opens it too: select all fifteen partitions.
   APAC opened on 4 August, so the asset skips the APAC partitions of 1 to 3 August, and they stay unmaterialized.
5. `key:weekly_orders` combines the five days of `daily_orders`, so run it after them.
6. `key:finance_orders or key:partner_orders or key:legacy_orders` runs the three assets that fail on purpose.

`daily_orders` and `regional_orders` have different partitions definitions, so each has a run of its own.

## The pipeline

| group | asset | what it shows |
| --- | --- | --- |
| `bronze` | `raw_orders` | The storefront's order export has no invalid rows. It is a plain `@dg.asset`, so its Columns tab shows only what the IO manager infers. |
| | `raw_customers` | The storefront's customer export has no invalid rows. |
| | `raw_marketplace_orders` | The marketplace feed has twelve valid lines and eight invalid ones. |
| | `raw_partner_orders` | The B2B partner's export has `quantity` as `Int64`. |
| | `raw_legacy_orders` | This one-off export from the old platform has refunds as negative amounts. |
| `silver` | **`orders`** | **Start here.** `dd.asset` fills the Columns tab from the schema before the first run. The asset has 24 checks, all blocking: the column-schema check and one per rule. The materialization has statistics and a row sample. |
| | `customers` | It has three columns and eight checks, so it is the smallest use of the package. |
| | `marketing_orders` | The marketplace feed with `filter=True`: seven checks fail at `ERROR`, none of them blocking, and the run writes the eight failing lines under `marketing_orders__error_rows` and succeeds. |
| | `finance_orders` | The same feed with `filter=False`: the checks fail at `ERROR`, and the run raises `ValidationAbortError` and writes nothing. |
| | `partner_orders` | The blocking `dy_schema__columns` check fails on `quantity`, and the run raises `ColumnSchemaError` before `Schema.filter` runs. |
| | `legacy_orders` | Every row fails an `"error"` rule, so the run writes all three rows under `legacy_orders__error_rows` and raises `NoValidRowsError`. It writes no `legacy_orders` table, not even an empty one. |
| | `daily_orders` | It has five daily partitions and 16 checks, at `column` granularity. |
| | `regional_orders` | The partitions are day by region, fifteen in all. The decorated function returns `None` for APAC on the days before APAC opened. |
| | `warehouse_orders` | It is hand-wired: the IO manager writes the table, and then a `@dg.multi_asset_check` built from `dd.wiring` reports the rules. A failing `"error"` rule's check is blocking, so its check step fails. |
| `gold` | `weekly_orders` | It reads every partition of `daily_orders` as one `dict[str, pl.LazyFrame]`. |
| | `high_value_orders` | It reads `orders` as a `pl.LazyFrame` and returns a `pl.LazyFrame`, which the package validates in one pass. |
| | `orders_snapshot` | It returns a `dg.MaterializeResult`, and the package copies its metadata, tags and data version onto the materialization. |

The error rows of the three assets with `filter=True` appear beside them in `silver`.
The lineage view draws them with a dashed outline, because they are external assets: the step of the asset beside them writes them and logs their materialization.

## The code

| file | what it is |
| --- | --- |
| `defs/silver/customers.py` | The module defines `CustomersSchema` and its asset, and the README and the Get started page copy it. |
| `defs/bronze/`, `defs/silver/`, `defs/gold/` | The pipeline has one folder per layer. Each `defs.yaml` sets the group, so no asset repeats it. |
| `defs/resources.py` | It binds `dd.data.ParquetIOManager`, which writes every table, and the Get started page copies it. |
| `_data.py` | It defines the sample rows only the demo uses, and the helpers that select a day's or a region's orders. |

A comment that starts with `# demo:` explains the demo, not the pipeline.
The rest of the code is ordinary pipeline code.

## Screenshots

`../assets/images/` contains the screenshots that the user guide uses.
One command retakes all of them:

```bash
./screenshots.sh
```

It starts `dg dev` on a separate instance and materializes the pipeline in the order above.
It then takes a screenshot of each view with headless Chrome and stops everything.
It needs macOS and Google Chrome, and nothing else may listen on port 3000.

Take every screenshot in the light theme, as the script does.
Retake the screenshots after a Dagster upgrade, because they show Dagster's UI, which can change between releases.
The row samples and failure examples in the images show only the fake rows from `dd.data` and `_data.py`.
That is the only reason the images are safe to publish.
