"""Tables built from the validated order lines."""

import dagster as dg
import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders

#: The priority fulfilment team handles every line with an amount above this.
HIGH_VALUE = 100


@dd.asset(Orders)
def weekly_orders(daily_orders: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    """Combine the week's daily partitions into one table."""
    return pl.concat(daily_orders.values())


@dd.asset(Orders)
def high_value_orders(orders: pl.LazyFrame) -> pl.LazyFrame:
    """Keep the lines the priority fulfilment team handles."""
    return orders.filter(pl.col("amount") > HIGH_VALUE)


@dd.asset(Orders)
def orders_snapshot(orders: pl.DataFrame) -> dg.MaterializeResult[pl.DataFrame]:
    """Snapshot `orders` for the BI dashboards, with its extract window in the metadata."""
    return dg.MaterializeResult(
        value=orders,
        metadata={
            "source": "storefront",
            "extract/rows": orders.height,
            "extract/window": dg.MetadataValue.md("`2026-08-01` to `2026-08-05`"),
            # demo: 999 is wrong on purpose; the package's count of the valid rows takes precedence.
            "dagster/row_count": 999,
        },
        data_version=dg.DataVersion("2026-08-05"),
        tags={"snapshot": "nightly"},
    )
