# `dagster-dataframely` demo

An online retailer's pipeline, from bronze to silver to gold, validated by Dataframely schemas.
Every code block and screenshot in the repository's README comes from it.

The bronze assets load a customer export and four order feeds, unchanged.
The silver assets validate each order feed against [`Orders`](src/dagster_dataframely_demo/schema.py).
Three of those feeds have the kinds of errors real feeds have, so some of their rows fail validation.
`customers` is the smallest silver asset: three columns, validated against a schema declared in the same module.
It is the README's first example.
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

1. `group:bronze` materializes the five feeds.
2. `key:orders or key:customers or key:marketing_orders or key:warehouse_orders or key:high_value_orders or key:orders_snapshot` materializes the six assets that succeed.
3. `key:daily_orders` opens the backfill dialog: select all five days.
4. `key:regional_orders` opens it too: select all fifteen partitions.
   APAC opened on 4 August, so the asset skips its first three days and they stay unmaterialized.
5. `key:weekly_orders` combines the five days of `daily_orders`, so run it after them.
6. `key:finance_orders or key:partner_orders or key:legacy_orders` materializes the three assets that fail, so this run fails.

Dagster cannot run partitioned and unpartitioned assets together, so `daily_orders` and `regional_orders` each have a run of their own.

## The pipeline

| group | asset | what it shows |
| --- | --- | --- |
| `bronze` | `raw_orders` | The storefront's export, with no invalid rows. A plain `@dg.asset`, so its Columns tab shows only what the IO manager infers. |
| | `raw_customers` | The storefront's customer export, which has no invalid rows. |
| | `raw_marketplace_orders` | A marketplace feed that has twelve valid lines and eight invalid ones. |
| | `raw_partner_orders` | A B2B partner's export, which writes `quantity` as `Int64`. |
| | `raw_legacy_orders` | A one-off export from the old platform, which stored refunds as negative amounts. |
| `silver` | **`orders`** | **Start here.** The Columns tab, filled from the schema before the first run. 24 checks, including the blocking column-schema check. Statistics and a row sample on the materialization. |
| | `customers` | Three columns and eight checks make it the README's first example and the smallest use of the package. |
| | `marketing_orders` | The marketplace feed with `quarantine=True`: seven checks fail at `WARN`, and the run writes the eight invalid lines to `marketing_orders_quarantine` and succeeds. |
| | `finance_orders` | The same feed with no quarantine: the checks fail at `ERROR`, and the run raises `ValidationAbortError` and writes nothing. |
| | `partner_orders` | The blocking `dy_schema__columns` check fails on `quantity`, and the run raises `ColumnSchemaError` before `Schema.filter` runs. |
| | `legacy_orders` | Every row fails validation, so the run writes no table, not even an empty one, and raises `NoValidRowsError`. It uses `schema` granularity, so one check reports for every rule. |
| | `daily_orders` | It has five daily partitions, at `column` granularity. |
| | `regional_orders` | Day by region gives fifteen partitions. APAC returns `None` for the days before it opened. |
| | `warehouse_orders` | A plain asset that adds the checks through `dd.wiring`, so the IO manager writes the table first and the checks report afterwards. |
| `gold` | `weekly_orders` | A fan-in over `daily_orders`, read as `dict[str, pl.LazyFrame]`. |
| | `high_value_orders` | A lazy input and a lazy return, validated in one pass. |
| | `orders_snapshot` | A returned `dg.MaterializeResult`, whose metadata, tags and data version the package copies onto the materialization. |

The three quarantines appear beside their assets in `silver`.
The lineage view draws them with a dashed outline, because nothing materializes them.

## The code

| file | what it is |
| --- | --- |
| `schema.py` | `Orders`, the schema `dd.asset` validates every order table against. |
| `defs/silver/customers.py` | `Customers` and its asset sit in one module, which the README copies. |
| `defs/bronze/`, `defs/silver/`, `defs/gold/` | The pipeline has one folder per layer. Each `defs.yaml` sets the group, so no asset repeats it. |
| `defs/resources.py` | The IO manager that writes every table. |
| `_data.py` | The sample data the bronze assets read, in place of real sources. |

A comment that starts with `# demo:` explains the demo, not the pipeline.
The rest of the code is ordinary pipeline code.

## Screenshots

`../assets/images/` contains the images that the README and the user guide use.
One command retakes all of them:

```bash
./screenshots.sh
```

It starts `dg dev` on a separate instance and materializes the pipeline in the order above.
It then takes a screenshot of each view with headless Chrome and stops everything.
It needs Google Chrome, and nothing else may listen on port 3000.

Take every screenshot in the same theme, light or dark.
Record the Dagster version wherever you use the images, because they show Dagster's UI, which can change between releases.
The rows in the samples are the fake data from `_data.py`.
That is the only reason they are safe to publish.
