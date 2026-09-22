"""Order lines from the B2B partner, validated."""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders


@dd.asset(Orders)
def partner_orders(raw_partner_orders: pl.DataFrame) -> pl.DataFrame:
    """Validate the partner's order lines."""
    # demo: the partner exports `quantity` as `Int64`, so the column-schema check fails every run.
    return raw_partner_orders
