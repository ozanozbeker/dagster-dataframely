"""Two assets that read the marketplace feed, one with `quarantine=True` and one without."""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders


@dd.asset(Orders, quarantine=True, owners=["team:marketing"])
def marketing_orders(raw_marketplace_orders: pl.DataFrame) -> pl.DataFrame:
    """Materialize the valid marketplace lines, and write the invalid lines to the quarantine."""
    return raw_marketplace_orders


marketing_orders_quarantine = dd.quarantine_spec(Orders, marketing_orders)


@dd.asset(Orders, owners=["team:finance"])
def finance_orders(raw_marketplace_orders: pl.DataFrame) -> pl.DataFrame:
    """Materialize the marketplace lines for revenue reporting, or fail the run on any invalid line."""
    return raw_marketplace_orders
