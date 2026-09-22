"""Order lines migrated from the old platform."""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders


@dd.asset(Orders, quarantine=True, check_granularity="schema")
def legacy_orders(raw_legacy_orders: pl.DataFrame) -> pl.DataFrame:
    """Migrate the old platform's order lines, setting aside any that fail."""
    # demo: every legacy amount is negative, so nothing survives and the run fails.
    return raw_legacy_orders


legacy_orders_quarantine = dd.quarantine_spec(Orders, legacy_orders)
