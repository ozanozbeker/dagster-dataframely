# dagster-dataframely

[![PyPI](https://img.shields.io/pypi/v/dagster-dataframely)](https://pypi.org/project/dagster-dataframely/) [![CI](https://github.com/ozanozbeker/dagster-dataframely/actions/workflows/ci.yml/badge.svg)](https://github.com/ozanozbeker/dagster-dataframely/actions/workflows/ci.yml)

[Dataframely](https://github.com/Quantco/dataframely) validates [Polars](https://pola.rs) frames against a schema.
[Dagster](https://dagster.io) has two built-in places to show a schema: the Columns tab and asset checks.
`dagster-dataframely` attaches a Dataframely schema to a Dagster asset, so you declare a table once and Dagster shows it in both places.

<!-- snippet: demo/src/dagster_dataframely_demo/defs/silver/customers.py -->
```python
import dataframely as dy
import polars as pl

import dagster_dataframely as dd


class CustomersSchema(dy.Schema):
    """A storefront customer account."""

    customer_id = dy.String(primary_key=True, description="Account identifier.")
    email = dy.String(nullable=False, description="Where receipts are sent.")
    lifetime_value = dy.Float64(nullable=False, min=0.0, description="Spend to date.")


@dd.asset(CustomersSchema)
def customers(raw_customers: pl.DataFrame) -> pl.DataFrame:
    """Validate the storefront's customer accounts."""
    return raw_customers
```

That is the whole integration.
From one declaration you get the catalog's Columns tab, one asset check per rule, a code version derived from the schema, and a choice of what happens to a row that fails a rule: the run fails, or with `filter=True` the row is written to a table beside the asset's.
[Get started](https://ozanozbeker.com/dagster-dataframely/user-guide/dagster-dataframely.html) walks through each of those with screenshots of the demo pipeline.

## Installation

```bash
uv add dagster-dataframely
```

You need Python 3.12 or newer.
The dependencies are `dagster`, `dataframely` and `polars`, plus `universal-pathlib`, which `dagster` already installs.

This package does not include an IO manager.
[`dagster-polars`](https://docs.dagster.io/integrations/libraries/polars) writes Polars frames to a filesystem or object store, and [`dagster-duckdb-polars`](https://docs.dagster.io/integrations/libraries/duckdb) writes them to a warehouse.
Any IO manager that stores by asset key works.

> **Pre-1.0.**
> A characterization test covers the public surface, so it never changes by accident.
> It can still change: a `0.x` minor release can include breaking changes.
> If that matters to you, pin to one minor version: `>=` the version you installed, `<` the next minor.
> [Upgrading](https://ozanozbeker.com/dagster-dataframely/user-guide/upgrading.html) has the migration for each one.

## Documentation

[**https://ozanozbeker.com/dagster-dataframely**](https://ozanozbeker.com/dagster-dataframely/) has the guide, the reference and the changelog.
The site is for users.
Contributor documentation stays in the repository: [`CONTEXT.md`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/CONTEXT.md) is the glossary, and [`docs/pre-1.0.md`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/pre-1.0.md) records the decisions and the measurements the package was built on.

## License

Apache 2.0.
See [`LICENSE`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/LICENSE).
