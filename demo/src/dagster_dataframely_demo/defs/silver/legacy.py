"""`legacy_orders` validates the order lines migrated from the old platform."""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely.data import OrdersSchema


@dd.asset(OrdersSchema, filter=True)
def legacy_orders(raw_legacy_orders: pl.DataFrame) -> pl.DataFrame:
    """Migrate the old platform's order lines."""
    # demo: every legacy amount is negative, so no row is valid and the run fails.
    return raw_legacy_orders


legacy_orders__error_rows = dd.build_error_rows_spec(OrdersSchema, legacy_orders)
