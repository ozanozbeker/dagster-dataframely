"""The sample data that the bronze assets return instead of reading a real source."""

# demo: the rows are hard-coded, not generated, so every run and screenshot shows the same values.

import datetime as dt
from decimal import Decimal

import polars as pl

#: The source systems export these dtypes.
SOURCE_SCHEMA: dict[str, pl.DataType] = {
    "order_id": pl.String(),
    "line_no": pl.Int32(),
    "email": pl.String(),
    "amount": pl.Decimal(10, 2),
    "quantity": pl.Int32(),
    "priority": pl.Int32(),
    "status": pl.Enum(["new", "paid", "shipped", "cancelled"]),
    "tracking_id": pl.String(),
    "is_gift": pl.Boolean(),
    "ordered_at": pl.Datetime("us"),
    "fulfilled_in": pl.Duration("us"),
    "tags": pl.List(pl.String()),
    "note": pl.String(),
}

#: The day the APAC storefront opened.
APAC_LAUNCH = dt.date(2026, 8, 4)


def _line(  # noqa: PLR0913 - one parameter per column of a thirteen-column table
    order_id: str,
    line_no: int,
    email: str,
    amount: str,
    *,
    quantity: int = 1,
    priority: int | None = 2,
    status: str = "new",
    is_gift: bool = False,
    ordered_at: dt.datetime = dt.datetime(2026, 8, 1, 9, 0),  # noqa: DTZ001 - the schema declares no time zone
    fulfilled_in: dt.timedelta | None = None,
    tags: list[str] | None = None,
    note: str | None = None,
) -> dict[str, object]:
    """Return one order line, with defaults for the columns that most lines share."""
    return {
        "order_id": order_id,
        "line_no": line_no,
        "email": email,
        "amount": Decimal(amount),
        "quantity": quantity,
        "priority": priority,
        "status": status,
        "tracking_id": f"TRK-{order_id}-{line_no}",
        "is_gift": is_gift,
        "ordered_at": ordered_at,
        "fulfilled_in": fulfilled_in,
        "tags": tags,
        "note": note,
    }


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=SOURCE_SCHEMA)


def _at(day: int, hour: int, minute: int) -> dt.datetime:
    return dt.datetime(2026, 8, day, hour, minute)  # noqa: DTZ001 - the schema declares no time zone


def storefront_orders() -> pl.DataFrame:
    """Return the storefront's order lines for the first week of August."""
    # fmt: off
    return _frame(
        [
            _line("ORD-0001", 1, "ada@example.com", "125.00", quantity=2, priority=1, status="shipped", ordered_at=_at(1, 9, 0), fulfilled_in=dt.timedelta(hours=26), tags=["priority"]),
            _line("ORD-0001", 2, "ada@example.com", "18.50", priority=1, status="shipped", ordered_at=_at(1, 9, 0), fulfilled_in=dt.timedelta(hours=26), tags=["priority"]),
            _line("ORD-0002", 1, "bo@example.com", "340.00", quantity=4, status="paid", is_gift=True, ordered_at=_at(1, 14, 30), fulfilled_in=dt.timedelta(hours=3), tags=["gift", "fragile"], note="Leave with the neighbour."),
            _line("ORD-0003", 1, "cyd@example.com", "12.99", priority=None, ordered_at=_at(2, 8, 15)),
            _line("ORD-0004", 1, "dev@example.com", "89.95", quantity=3, priority=3, status="shipped", ordered_at=_at(2, 11, 45), fulfilled_in=dt.timedelta(hours=50), tags=["bulk"]),
            _line("ORD-0005", 1, "eli@example.com", "0.00", status="cancelled", ordered_at=_at(3, 16, 20), tags=["cancelled"], note="Customer changed their mind."),
            _line("ORD-0006", 1, "fay@example.com", "1250.00", quantity=10, priority=1, status="paid", ordered_at=_at(3, 19, 5), fulfilled_in=dt.timedelta(hours=1, minutes=30), tags=["priority", "bulk"]),
            _line("ORD-0006", 2, "fay@example.com", "75.00", priority=1, status="paid", is_gift=True, ordered_at=_at(3, 19, 5), fulfilled_in=dt.timedelta(hours=1, minutes=30), tags=["gift"]),
            _line("ORD-0007", 1, "gus@example.com", "45.00", quantity=2, priority=None, ordered_at=_at(4, 7, 40)),
            _line("ORD-0008", 1, "hal@example.com", "199.00", priority=3, status="shipped", is_gift=True, ordered_at=_at(4, 12, 10), fulfilled_in=dt.timedelta(hours=72), tags=["gift"]),
            _line("ORD-0009", 1, "ivy@example.com", "6.75", status="paid", ordered_at=_at(5, 9, 55), fulfilled_in=dt.timedelta(hours=2)),
            _line("ORD-0010", 1, "jo@example.com", "512.40", quantity=6, priority=1, status="shipped", ordered_at=_at(5, 15, 30), fulfilled_in=dt.timedelta(hours=30), tags=["bulk", "priority"], note="Split across two pallets."),
        ]
    )
    # fmt: on


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


def marketplace_orders() -> pl.DataFrame:
    """Return the marketplace's order lines, eight of which fail at least one rule."""
    # fmt: off
    return _frame(
        [
            *storefront_orders().to_dicts(),
            _line("ORD-0011", 1, "kim@example.com", "-4.00", ordered_at=_at(6, 8, 0)),  # negative amount
            _line("ORD-0012", 1, "Lee@example.com", "22.00", ordered_at=_at(6, 9, 0)),  # uppercase email
            _line("ORD-0013", 1, "mo@example.com", "60.00", quantity=5000, ordered_at=_at(6, 10, 0)),  # quantity out of range
            _line("ORD-9", 1, "nia@example.com", "31.00", ordered_at=_at(6, 11, 0)),  # malformed order id
            _line("ORD-0014", 1, "ned@example.com", "77.00", priority=9, ordered_at=_at(6, 12, 0)),  # unknown priority
            _line("ORD-0015", 1, "Ora@example.com", "-1.00", status="paid", ordered_at=_at(6, 13, 0)),  # negative amount on a paid line, and uppercase email
            _line("ORD-0016", 1, "pat@example.com", "14.00", ordered_at=_at(6, 14, 0)),  # this order has lines 1 and 3 and no line 2
            _line("ORD-0016", 3, "pat@example.com", "16.00", ordered_at=_at(6, 14, 0)),
        ]
    )
    # fmt: on


def partner_orders() -> pl.DataFrame:
    """Return the B2B partner's order lines, whose export writes `quantity` as `Int64`."""
    return storefront_orders().with_columns(pl.col("quantity").cast(pl.Int64))


def legacy_orders() -> pl.DataFrame:
    """Return the old platform's order lines, which stored refunds as negative amounts."""
    # fmt: off
    return _frame(
        [
            _line("ORD-0021", 1, "quin@example.com", "-1.00", ordered_at=_at(7, 8, 0)),
            _line("ORD-0022", 1, "rae@example.com", "-2.00", ordered_at=_at(7, 9, 0)),
            _line("ORD-0023", 1, "sam@example.com", "-3.00", ordered_at=_at(7, 10, 0)),
        ]
    )
    # fmt: on


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
