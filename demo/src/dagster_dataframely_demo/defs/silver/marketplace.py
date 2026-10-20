"""Two assets that read the marketplace feed, one with `filter=True` and one without."""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely.data import OrdersSchema


@dd.asset(OrdersSchema, filter=True, owners=["team:marketing"])
def marketing_orders(raw_marketplace_orders: pl.DataFrame) -> pl.DataFrame:
    """Materialize the valid marketplace lines, and write the lines that fail an `"error"` rule to the error rows."""
    return raw_marketplace_orders


marketing_orders__error_rows = dd.build_error_rows_spec(OrdersSchema, marketing_orders)


@dd.asset(OrdersSchema, owners=["team:finance"])
def finance_orders(raw_marketplace_orders: pl.DataFrame) -> pl.DataFrame:
    """Materialize the marketplace lines for revenue reporting, or fail the run on any invalid line."""
    return raw_marketplace_orders
