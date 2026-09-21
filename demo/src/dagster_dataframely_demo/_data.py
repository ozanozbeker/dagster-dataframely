"""Plumbing: the five frames this demo feeds its assets.

Underscored because a reader is meant to skip it. It answers "where do the rows come from", which has nothing to do with this package. `defs/` is where the library is.

The rows are literal rather than generated. A demo you screenshot has to produce the same numbers every run, and someone chasing a red check has to be able to point at the row that caused it. Each frame's docstring says which rules it breaks and why that case is worth having.
"""

import datetime as dt
from decimal import Decimal

import polars as pl

#: The dtypes restated by hand rather than derived from `Orders`. A frame that drifts from the schema then trips the gate loudly, which is a case this demo wants, instead of being quietly rebuilt to match.
POLARS_SCHEMA: dict[str, pl.DataType] = {
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


def _line(  # noqa: PLR0913 - a row of a thirteen-column table, and naming each field is the readable spelling
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
    """Build one order line, defaulting the columns a given frame does not vary.

    `tracking_id` derives from the key rather than being passed, so the `unique` rule holds for free as long as the key does.
    """
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
    return pl.DataFrame(rows, schema=POLARS_SCHEMA)


def _at(day: int, hour: int, minute: int) -> dt.datetime:
    return dt.datetime(2026, 8, day, hour, minute)  # noqa: DTZ001 - the schema declares no time zone


def clean_orders() -> pl.DataFrame:
    """Twelve lines across ten orders, every one of them valid.

    Spread on purpose so the statistics tables have something to say: amounts from 0 to 1250, nulls in four columns, three gifts against nine, and five days of `ordered_at`.
    """
    # One call per line, because these rows are a table and reading them as one is the point. E501 is off in this repo, so the width costs nothing.
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


def defective_orders() -> pl.DataFrame:
    """Return the twelve clean lines plus eight that break a rule, one rule per row bar two.

    `ORD-0015` trips three rules at once, which is what makes the quarantine's `cooccurrence` table read as one broken row rather than three unrelated counts. `ORD-0016` skips line 2, so `line_numbers_are_dense` rejects both of that order's lines: the case where a rule takes down a group rather than a row.
    """
    # fmt: off
    return _frame(
        [
            *clean_orders().to_dicts(),
            _line("ORD-0011", 1, "kim@example.com", "-4.00", ordered_at=_at(6, 8, 0)),  # amount|min
            _line("ORD-0012", 1, "Lee@example.com", "22.00", ordered_at=_at(6, 9, 0)),  # email|check__lowercase
            _line("ORD-0013", 1, "mo@example.com", "60.00", quantity=5000, ordered_at=_at(6, 10, 0)),  # quantity|max
            _line("ORD-9", 1, "nia@example.com", "31.00", ordered_at=_at(6, 11, 0)),  # order_id|regex
            _line("ORD-0014", 1, "ned@example.com", "77.00", priority=9, ordered_at=_at(6, 12, 0)),  # priority|is_in
            _line("ORD-0015", 1, "Ora@example.com", "-1.00", status="paid", ordered_at=_at(6, 13, 0)),  # amount|min, email|check__lowercase and paid_orders_have_amount at once
            _line("ORD-0016", 1, "pat@example.com", "14.00", ordered_at=_at(6, 14, 0)),  # line_numbers_are_dense: line 2 is missing,
            _line("ORD-0016", 3, "pat@example.com", "16.00", ordered_at=_at(6, 14, 0)),  # so both of these go
        ]
    )
    # fmt: on


def hopeless_orders() -> pl.DataFrame:
    """Three lines, every one of them negative, so nothing survives the filter."""
    # fmt: off
    return _frame(
        [
            _line("ORD-0021", 1, "quin@example.com", "-1.00", ordered_at=_at(7, 8, 0)),
            _line("ORD-0022", 1, "rae@example.com", "-2.00", ordered_at=_at(7, 9, 0)),
            _line("ORD-0023", 1, "sam@example.com", "-3.00", ordered_at=_at(7, 10, 0)),
        ]
    )
    # fmt: on


def mistyped_orders() -> pl.DataFrame:
    """Return the clean lines with `quantity` arriving `Int64`: a pipeline defect, not a data one."""
    return clean_orders().with_columns(pl.col("quantity").cast(pl.Int64))


def orders_on(orders: pl.DataFrame, day: dt.date) -> pl.DataFrame:
    """Return the lines of `orders` placed on one day, for the partitioned asset.

    Takes the table rather than building it, because the partitioned assets read the whole of `raw_orders` and take their own slice out of it. That is what a partition mapping does in a real project, and it is why those assets have a parent at all.

    A slice rather than a restamp, so the five partitions hold disjoint orders and a fan-in over all of them is still a valid table. Restamping every line onto each day would duplicate the primary key the moment anything concatenated two partitions.

    Whole orders stay together, because `line_numbers_are_dense` looks across an order's lines: splitting `ORD-0001` across two days would reject both halves.
    """
    return orders.filter(pl.col("ordered_at").dt.date() == day)


def orders_for(orders: pl.DataFrame, day: dt.date, region: str) -> pl.DataFrame:
    """One cell of the day-by-region grid.

    The schema has no region column, so the split is by order number: `eu` takes the even ones and `us` the odd. Whole orders again, for the density rule, and every cell of the grid holds at least one line so no partition materializes empty.
    """
    wanted = 0 if region == "eu" else 1
    numbered = orders_on(orders, day).with_columns(
        pl.col("order_id").str.slice(4).cast(pl.Int32).alias("_number")
    )
    return numbered.filter(pl.col("_number") % 2 == wanted).drop("_number")
