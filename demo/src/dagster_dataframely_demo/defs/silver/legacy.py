"""Order lines migrated from the old platform."""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders


@dd.asset(Orders, filter=True)
def legacy_orders(raw_legacy_orders: pl.DataFrame) -> pl.DataFrame:
    """Migrate the old platform's order lines, and write the lines that fail an `"error"` rule to the error rows."""
    # demo: every legacy amount is negative, so no row is valid and the run fails.
    return raw_legacy_orders


legacy_orders__error_rows = dd.error_rows_spec(Orders, legacy_orders)
