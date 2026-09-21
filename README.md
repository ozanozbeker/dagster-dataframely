# `dagster-dataframely`

[Dataframely](https://github.com/Quantco/dataframely) describes what a [Polars](https://pola.rs) frame should look like.
[Dagster](https://dagster.io) has first-class places to show that: the Columns tab and asset checks.
`dagster-dataframely` wires the two together, so you describe a table once and Dagster shows it everywhere.

```python
import dagster as dg
import dataframely as dy
import polars as pl

import dagster_dataframely as dd


class Orders(dy.Schema):
    order_id = dy.String(primary_key=True)
    amount = dy.Float64(nullable=False, min=0.0)


@dd.asset(Orders)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return raw_orders.select("order_id", "amount")
```

That is the whole integration.
From that one declaration you get:

- **The catalog's Columns tab**, filled in before the asset has ever run: dtypes, descriptions, nullability, uniqueness, the primary key stated once at table level, and every remaining constraint listed beside it.

  ![The catalog's Columns tab, filled from the schema: each column's dtype and description, `tracking_id` marked unique, the composite primary key stated once at table level, and the column tags `amount` declared through `metadata=`.](https://raw.githubusercontent.com/ozanozbeker/dagster-dataframely/main/assets/images/columns-tab.png)

- **One asset check per Dataframely rule**, each with its own pass/fail history.

  ![A clean run's Checks tab at the default granularity: one check per rule, each with its own history. The selected one is described by its rendered constraint, and carries the rule's name and expression as metadata.](https://raw.githubusercontent.com/ozanozbeker/dagster-dataframely/main/assets/images/check-list.png)

- **A blocking column-schema check** that compares the frame's columns and dtypes against the schema, before a single row is filtered.

  ![A run log where `quantity` arrived `Int64`. The failing `dy_schema__columns` check carries `dy_schema__errors` with the expected and actual dtype, and the step failure below it repeats the same column in the `ColumnSchemaError` message.](https://raw.githubusercontent.com/ozanozbeker/dagster-dataframely/main/assets/images/error-column-schema.png)

- **Somewhere for the rows that do not fit**, if you want it.
  Add `quarantine=True` and the rows that fail validation are written beside the table rather than failing the run, as long as something survives.

  ![The lineage view. The table materialized with 17 of its 24 checks passing, and its quarantine sits beside it as a node of its own, drawn dashed because nothing ever materializes it: the rejected rows were written inside the table's own step.](https://raw.githubusercontent.com/ozanozbeker/dagster-dataframely/main/assets/images/quarantine-lineage.png)

The decorated function is an ordinary Dagster asset body.
Upstream assets bind as parameters, you declare `context` if you want it, and you can return any of five things: a frame, or a `dg.MaterializeResult` carrying one, eager or lazy, or `None`.

`@dg.asset` is the mechanism underneath, and the vocabulary.
Anything `@dg.asset` lets you say about one asset, you can say here under the same name, bar six parameters the decorator owns or rules out.
A test asserts that in both directions, and [the user guide](https://ozanozbeker.com/dagster-dataframely/user-guide/declaring-an-asset.html) lists the six.

## Package philosophy

**Schema on write.**
Validation happens when a table is written, never when it is read.
The checks run before the IO manager sees the frame, so the rows that fail never reach the table.

That puts this package after ingestion, at bronze to silver to gold, where the data is already on your side and the question is whether it is fit to publish.
Land raw records with [`dlt`](https://dlthub.com), which has good reasons to be permissive, and declare a schema at the first table someone else would query.
Ingestion-scale and larger-than-memory work belongs elsewhere.

**The schema is the table's shape, and closing the gap is the asset's job.**
A dtype that disagrees aborts the run rather than being coerced, because coercing quietly is how a wrong number reaches a table nobody re-reads.
There is no lenient mode to turn on.
Narrowing is free, though: `Schema.filter` drops the columns the schema never declared and returns the rest in the schema's order, so dtypes are the only thing ever yours to fix.

**Consent to partial data is a declaration, not a setting.** `quarantine=True` is the only dial, and no environment variable reaches it.
Leave it off and one failing row stops the write, so your last-known-good table stays in place.

**The strictness belongs to the decorator, not to the package.**
It is assembled from parts the package also exports under `dd.wiring`, and each one plugs a single feature into an asset the decorator does not fit: the Columns tab onto an asset that writes its own storage, or the checks onto a table something else already wrote.

## Quick start

```bash
uv add dagster-dataframely
```

You will need Python 3.12 or newer.

`dagster`, `dataframely` and `polars` are the dependencies, plus `universal-pathlib`, which already arrives with `dagster`.

This package ships no IO manager, so bring one. [`dagster-polars`](https://docs.dagster.io/integrations/libraries/polars) writes Polars frames to a filesystem or object store, and [`dagster-duckdb-polars`](https://docs.dagster.io/integrations/libraries/duckdb) writes them to a warehouse.
Anything addressed by asset key works, because nothing here learns which manager you bound.

> **Pre-1.0.**
> The public surface is covered by a characterization test rather than held by convention, so it will not move quietly.
> It can still move: a `0.x` minor release is where a breaking change lands.
> Pin to one minor if that matters to you: `>=` the version you installed, `<` the next minor.
> Coming from 0.6 or 0.7, read [the changelog](https://ozanozbeker.com/dagster-dataframely/changelog.html) first.

Declare the schema and the asset as above, then tell the code location where to write:

```python
from dagster_polars import PolarsParquetIOManager


@dg.asset
def raw_orders() -> pl.DataFrame:
    return pl.DataFrame({"order_id": ["a", "b"], "amount": [1.0, 2.0]})


defs = dg.Definitions(
    assets=[raw_orders, orders],
    resources={"io_manager": PolarsParquetIOManager(base_dir="data/warehouse")},
)
```

`orders` binds `raw_orders`, so whatever produces that goes in the list too.
Point `dg dev` at that module and materialize `orders` from the UI, or call `dg.materialize([orders], resources=...)` from a script.

Four things now exist that did not before:

- The catalog's Columns tab, filled from `Orders`, before the first run.
- One asset check per rule, each with its own history, evaluated on every run.
- A materialization carrying the row count, a row sample and per-dtype-group statistics.
- A table wherever your IO manager puts one.

To keep the rows that fail rather than failing the run, add `quarantine=True`:

```python
@dd.asset(Orders, quarantine=True)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return raw_orders.select("order_id", "amount")
```

The invalid rows go through the same IO manager, under the asset's own key with `_quarantine` on the end, carrying one column per rule saying why.
The checks then fail at `WARN` and the run succeeds, so downstream proceeds on the data that is fine.

![The Checks tab for a quarantined asset. Seven of the twenty-four checks failed at `WARN`, and the selected one carries the rendered constraint, the rule's expression, and the two rows it rejected.](https://raw.githubusercontent.com/ozanozbeker/dagster-dataframely/main/assets/images/quarantine-checks.png)

## Documentation

[**https://ozanozbeker.com/dagster-dataframely**](https://ozanozbeker.com/dagster-dataframely/) is the guide, the API reference and the upgrade log.
The site publishes what a user needs; what a contributor needs stays in the repo: [`CONTEXT.md`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/CONTEXT.md) is the glossary, [`ARCHITECTURE.md`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/ARCHITECTURE.md) is how the parts fit, [`docs/adr/`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/adr/) holds the decisions and [`docs/research/`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/research/) the measurements behind them.

## License

Apache 2.0.
See [`LICENSE`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/LICENSE).
