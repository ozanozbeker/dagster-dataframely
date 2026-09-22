# `dagster-dataframely` demo

An online retailer's pipeline, bronze to silver to gold, validated by dataframely schemas.
It is where every code block in the repository's README comes from, and every screenshot.

The bronze assets land a customer export and four order feeds as they arrive.
The silver assets hold each order feed to [`Orders`](src/dagster_dataframely_demo/schema.py), and three of them are dirty the way real feeds are, so the package has something to catch.
`customers` is the small one: three columns, held to a schema declared beside it, and the table the README opens on.
The gold assets build on the validated tables.

## Getting started

It is a member of the repository's uv workspace, so it runs the working tree rather than a published wheel.
Edit `src/dagster_dataframely/` and the next `dg dev` shows the change.

```bash
cd demo && uv run dg dev
```

Then open <http://localhost:3000>.

There is no `DAGSTER_HOME` to set.
`dg dev` builds its instance in a temp directory and deletes it on exit, so every session starts empty.
Tables land in `storage/` and outlive the session; `rm -rf storage` starts those over too.

Syncing the demo installs the webserver into the workspace's shared `.venv`, and `uv sync` back at the repository root takes it out again.

## Materialize before you look

Half the surfaces only exist once an asset has run.
Paste each selection into the asset graph's search bar and hit **Materialize**, in this order:

1. `group:bronze` lands the five feeds.
2. `key:orders or key:customers or key:marketing_orders or key:warehouse_orders or key:high_value_orders or key:orders_snapshot` is everything that goes green.
3. `key:daily_orders` opens the backfill dialog: take all five days.
4. `key:regional_orders` opens it too: take all fifteen cells.
   APAC opened on 4 August, so its first three days skip and stay unmaterialized.
5. `key:weekly_orders` combines the five days, so it runs after step 3.
6. `key:finance_orders or key:partner_orders or key:legacy_orders` runs the three loads that fail, so this run ends red.

Partitioned and unpartitioned assets cannot share a run, which is why steps 3 and 4 stand alone.

## The pipeline

| group | asset | what it shows |
| --- | --- | --- |
| `bronze` | `raw_orders` | The storefront's export, clean. A plain `@dg.asset`, so its Columns tab is what the IO manager could infer and no more. |
| | `raw_customers` | The storefront's customer export, clean. |
| | `raw_marketplace_orders` | A marketplace feed: twelve good lines and eight with defects. |
| | `raw_partner_orders` | A B2B partner's export, which writes `quantity` as `Int64`. |
| | `raw_legacy_orders` | A one-off export from the old platform, which stored refunds as negative amounts. |
| `silver` | **`orders`** | **Start here.** The Columns tab filled from the schema before the first run, 24 checks behind the blocking column-schema check, statistics and a row sample on the materialization. |
| | `customers` | Three columns and eight checks: the README's opening example, and the whole integration at its smallest. |
| | `marketing_orders` | The marketplace feed with `quarantine=True`: seven checks fail at `WARN`, the eight bad lines go to `marketing_orders_quarantine`, and the run stays green. |
| | `finance_orders` | The same feed with no quarantine: the checks fail at `ERROR`, the run raises `ValidationAbortError`, and nothing is written. |
| | `partner_orders` | The blocking `dy_schema__columns` check fails on `quantity` and the run raises `ColumnSchemaError` before a row is filtered. |
| | `legacy_orders` | Nothing survives, so the valid table is skipped rather than emptied and the run raises `NothingSurvivedError`. At `schema` granularity, so one check covers every rule. |
| | `daily_orders` | Five daily partitions, at `column` granularity. |
| | `regional_orders` | Day crossed with region, fifteen cells, with APAC returning `None` before it opened. |
| | `warehouse_orders` | A plain asset with the checks attached through `dd.wiring`, so the load writes first and the checks report after. |
| `gold` | `weekly_orders` | A fan-in over `daily_orders`, read as `dict[str, pl.LazyFrame]`. |
| | `high_value_orders` | A lazy input and a lazy return, validated in one pass. |
| | `orders_snapshot` | A returned `dg.MaterializeResult`, whose metadata, tags and data version land on the materialization. |

The three quarantines sit beside their assets in `silver`, dashed in the lineage view because nothing ever materializes them.

## The code

| file | what it is |
| --- | --- |
| `schema.py` | `Orders`, the declaration every order table is held to. |
| `defs/silver/customers.py` | `Customers` and its asset in one module, which is what the README copies. |
| `defs/bronze/`, `defs/silver/`, `defs/gold/` | One folder per layer. Each `defs.yaml` sets the group, so no asset repeats it. |
| `defs/resources.py` | The IO manager every table is written through. |
| `_data.py` | The sample extracts the bronze assets read, standing in for real sources. |

A comment starting `# demo:` is about the demo rather than the pipeline.
Nothing else in the code is.

## Screenshots

`../assets/images/` holds the images the README and the user guide use, and one command reshoots all of them:

```bash
./screenshots.sh
```

It starts `dg dev` on an instance of its own, materializes the pipeline in the order above, shoots each view with headless Chrome, and shuts it all down.
It needs Google Chrome and nothing already serving port 3000.

Shoot light or dark consistently, and note the Dagster version wherever the images land: they are assertions about someone else's UI.
The rows in the samples are the fake ones from `_data.py`, which is the only reason they are safe to publish.
