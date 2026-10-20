"""The example schema and frames that the guide, the reference pages and the demo share.

A docstring example imports from here, so it runs for any user who has the package installed, as dplyr's examples use `starwars`.
The rows are hard-coded, not generated, so every run and screenshot shows the same values.
"""

import datetime as dt
from decimal import Decimal

import dataframely as dy
import polars as pl

__all__ = ["OrdersSchema", "marketplace_orders", "storefront_orders"]

# The fulfilment tool rejects an operator note of this many characters or more.
_NOTE_MAX_CHARS = 100


# One column per column constraint Dataframely offers, plus one per statistics dtype group, so no
# screenshot or guide sentence loses its example. The docstring is also the demo assets' description
# in Dagster, so it stays at one line and carries no example.
class OrdersSchema(dy.Schema):  # numpydoc ignore=EX01
    """A customer order line: one row per product on one order."""

    order_id = dy.String(
        primary_key=True,
        regex=r"^ORD-\d{4}$",
        description="Order identifier, unique together with `line_no`.",
    )
    line_no = dy.Int32(
        primary_key=True,
        min=1,
        description="Position of this line on the order, starting at 1.",
    )
    email = dy.String(
        nullable=False,
        max_length=254,
        check={"lowercase": lambda expr: expr.str.to_lowercase() == expr},
        description="Customer contact address at the time of the order.",
    )
    amount = dy.Decimal(
        10,
        2,
        nullable=False,
        min=Decimal("0.00"),
        description="Line total in the account's currency.",
        metadata={"owner": "finance", "pii": False},
    )
    quantity = dy.Int32(
        nullable=False, min=1, max=999, description="Units ordered on this line."
    )
    priority = dy.Int32(
        nullable=True, is_in=[1, 2, 3], description="Handling priority, 1 is highest."
    )
    status = dy.Enum(
        ["new", "paid", "shipped", "cancelled"],
        nullable=False,
        description="Fulfilment status of the line.",
    )
    tracking_id = dy.String(
        nullable=True,
        unique=True,
        description="Carrier tracking number, unique across every line.",
    )
    is_gift = dy.Bool(nullable=False, description="Whether the line ships as a gift.")
    ordered_at = dy.Datetime(nullable=False, description="When the order was placed.")
    fulfilled_in = dy.Duration(
        nullable=True, description="Time from order to dispatch, null until dispatched."
    )
    tags = dy.List(
        dy.String(),
        nullable=True,
        max_length=5,
        description="Free-form labels used for routing.",
    )
    note = dy.String(
        nullable=True,
        check=lambda expr: expr.str.len_chars() < _NOTE_MAX_CHARS,
        description="Operator note attached to the line.",
    )

    @dy.rule()
    def paid_orders_have_amount(cls) -> pl.Expr:
        """Require a positive amount on a paid line."""
        return (cls.status.col != "paid") | (cls.amount.col > 0)

    @dy.rule()
    def line_numbers_are_dense(cls) -> pl.Expr:
        """Require each order's line numbers to be 1 to n with no gaps."""
        return cls.line_no.col.max().over("order_id") == cls.line_no.col.count().over(
            "order_id"
        )


# The source systems export these dtypes, which match the schema's.
_SOURCE_SCHEMA: dict[str, pl.DataType] = {
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


def _line(  # noqa: PLR0913 - one parameter per column
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


def _at(day: int, hour: int, minute: int) -> dt.datetime:
    return dt.datetime(2026, 8, day, hour, minute)  # noqa: DTZ001 - the schema declares no time zone


# fmt: off
storefront_orders: pl.DataFrame = pl.DataFrame(
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
    ],
    schema=_SOURCE_SCHEMA,
)
"""The storefront's twelve order lines for the first week of August 2026, every one of which passes `OrdersSchema`."""

marketplace_orders: pl.DataFrame = pl.DataFrame(
    [
        *storefront_orders.to_dicts(),
        _line("ORD-0011", 1, "kim@example.com", "-4.00", ordered_at=_at(6, 8, 0)),  # negative amount
        _line("ORD-0012", 1, "Lee@example.com", "22.00", ordered_at=_at(6, 9, 0)),  # uppercase email
        _line("ORD-0013", 1, "mo@example.com", "60.00", quantity=5000, ordered_at=_at(6, 10, 0)),  # quantity out of range
        _line("ORD-9", 1, "nia@example.com", "31.00", ordered_at=_at(6, 11, 0)),  # malformed order id
        _line("ORD-0014", 1, "ned@example.com", "77.00", priority=9, ordered_at=_at(6, 12, 0)),  # unknown priority
        _line("ORD-0015", 1, "Ora@example.com", "-1.00", status="paid", ordered_at=_at(6, 13, 0)),  # negative amount on a paid line, and uppercase email
        _line("ORD-0016", 1, "pat@example.com", "14.00", ordered_at=_at(6, 14, 0)),  # this order has lines 1 and 3 and no line 2
        _line("ORD-0016", 3, "pat@example.com", "16.00", ordered_at=_at(6, 14, 0)),
    ],
    schema=_SOURCE_SCHEMA,
)
"""The storefront's lines plus eight marketplace lines, each of which fails at least one rule of `OrdersSchema`."""
# fmt: on
