"""Order lines from the storefront, validated."""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely.data import OrdersSchema


@dd.asset(OrdersSchema)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """Validate the storefront's order lines."""
    return raw_orders
