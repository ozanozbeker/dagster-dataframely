"""A hand-wired asset and its checks, which run after the asset writes the table."""

from collections.abc import Iterator

import dagster as dg
import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders

WAREHOUSE_ORDERS = dg.AssetKey(["warehouse_orders"])


@dg.asset(metadata=dd.wiring.schema_metadata(Orders))
def warehouse_orders(raw_marketplace_orders: pl.LazyFrame) -> pl.LazyFrame:
    """Load the marketplace feed into the warehouse without validating it first."""
    return raw_marketplace_orders


@dg.multi_asset_check(specs=dd.wiring.check_specs(Orders, asset=WAREHOUSE_ORDERS))
def warehouse_orders_checks(
    warehouse_orders: pl.LazyFrame,
) -> Iterator[dg.AssetCheckResult]:
    """Check the written `warehouse_orders` table, and report failures as warnings."""
    yield from dd.wiring.check_results(
        Orders,
        warehouse_orders,
        asset_key=WAREHOUSE_ORDERS,
        severity=dg.AssetCheckSeverity.WARN,
    )
