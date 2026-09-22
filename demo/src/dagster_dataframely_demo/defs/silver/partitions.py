"""Order lines partitioned by day, and by day and region."""

import datetime as dt

import dagster as dg
import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo import _data
from dagster_dataframely_demo.schema import Orders

DAILY = dg.DailyPartitionsDefinition(start_date="2026-08-01", end_date="2026-08-06")

BY_REGION = dg.MultiPartitionsDefinition({
    "day": DAILY,
    "region": dg.StaticPartitionsDefinition(["apac", "eu", "us"]),
})


@dd.asset(Orders, partitions_def=DAILY, check_granularity="column")
def daily_orders(
    context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
) -> pl.DataFrame:
    """Return one day's order lines."""
    return _data.orders_on(raw_orders, dt.date.fromisoformat(context.partition_key))


@dd.asset(Orders, partitions_def=BY_REGION, quarantine=True)
def regional_orders(
    context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
) -> pl.DataFrame | None:
    """Return one region's order lines for one day, or skip APAC before its storefront opened."""
    cell = context.partition_key.keys_by_dimension
    day = dt.date.fromisoformat(cell["day"])
    if cell["region"] == "apac" and day < _data.APAC_LAUNCH:
        return None
    return _data.orders_in(raw_orders, day, cell["region"])


regional_orders_quarantine = dd.quarantine_spec(Orders, regional_orders)
