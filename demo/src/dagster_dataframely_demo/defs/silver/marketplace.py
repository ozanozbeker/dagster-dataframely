"""Two consumers of the marketplace feed, which differ on whether to accept partial data."""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders


@dd.asset(Orders, quarantine=True, owners=["team:marketing"])
def marketing_orders(raw_marketplace_orders: pl.DataFrame) -> pl.DataFrame:
    """Take the marketplace lines that pass, and set the rest aside for review."""
    return raw_marketplace_orders


marketing_orders_quarantine = dd.quarantine_spec(Orders, marketing_orders)


@dd.asset(Orders, owners=["team:finance"])
def finance_orders(raw_marketplace_orders: pl.DataFrame) -> pl.DataFrame:
    """Take the marketplace lines for revenue reporting, where one bad line stops the load."""
    return raw_marketplace_orders
