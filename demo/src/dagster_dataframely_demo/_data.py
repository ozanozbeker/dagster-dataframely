"""The sample data the bronze assets return in place of a real source, derived from `dd.data`."""

import datetime as dt

import polars as pl

from dagster_dataframely.data import storefront_orders

#: The day the APAC storefront opened.
APAC_LAUNCH = dt.date(2026, 8, 4)


def storefront_customers() -> pl.DataFrame:
    """Return the storefront's customer accounts."""
    return pl.DataFrame({
        "customer_id": ["CUS-001", "CUS-002", "CUS-003", "CUS-004", "CUS-005"],
        "email": [
            "ada@example.com",
            "bo@example.com",
            "cyd@example.com",
            "dev@example.com",
            "eli@example.com",
        ],
        "lifetime_value": [143.50, 340.00, 12.99, 89.95, 0.00],
    })


def partner_orders() -> pl.DataFrame:
    """Return the B2B partner's order lines, whose export writes `quantity` as `Int64`."""
    return storefront_orders.with_columns(pl.col("quantity").cast(pl.Int64))


def legacy_orders() -> pl.DataFrame:
    """Return the old platform's order lines, which stored refunds as negative amounts."""
    return storefront_orders.head(3).with_columns(
        order_id=pl.Series(["ORD-0021", "ORD-0022", "ORD-0023"]),
        line_no=pl.lit(1, dtype=pl.Int32),
        amount=(-pl.col("amount").abs() - 1).cast(pl.Decimal(10, 2)),
        ordered_at=pl.Series([dt.datetime(2026, 8, 7, 8 + i) for i in range(3)]),  # noqa: DTZ001 - the schema declares no time zone
    )


def orders_on(orders: pl.DataFrame, day: dt.date) -> pl.DataFrame:
    """Return the lines of `orders` placed on `day`."""
    return orders.filter(pl.col("ordered_at").dt.date() == day)


def orders_in(orders: pl.DataFrame, day: dt.date, region: str) -> pl.DataFrame:
    """Return the lines of `orders` placed on `day` in `region`.

    The source data has no region column, so the order number sets the region.
    """
    number = pl.col("order_id").str.slice(4).cast(pl.Int32)
    apac = (number % 3 == 0) & pl.lit(day >= APAC_LAUNCH)
    routed = {
        "apac": apac,
        "eu": ~apac & (number % 2 == 0),
        "us": ~apac & (number % 2 == 1),
    }
    return orders_on(orders, day).filter(routed[region])
