"""The one `Orders` schema and the frames the whole effort runs against.

Lifted from `docs/probes/front-door/_scenario.py` and widened to the cases the rest of the effort needs: the dtypes a round trip or a metadata emission could get wrong, and the rule shapes the naming and description ladders have to distinguish.

Nothing here is a fixture. A schema is a class and a frame is a value, so both are cheaper to read as module constants than as fixture indirection, and `Orders` has to be importable at class-definition time to decorate an asset.

`POLARS_SCHEMA` restates the dtypes by hand rather than deriving them, so a frame that drifts from the schema fails the gate loudly in every runtime test instead of being silently rebuilt to match.
"""

import datetime as dt
from decimal import Decimal

import dataframely as dy
import polars as pl


class Orders(dy.Schema):
    """Customer orders, one row per order line.

    Every column and rule here earns its place by being awkward somewhere:

    - `Decimal` crashes `TableRecord` emission unless coerced (#23).
    - `Duration` has no readable polars string form (#23) and no naive CSV encoding (#22).
    - `Binary` and `List` have no CSV encoding at all (#22).
    - The composite primary key is the case where a per-column `unique` constraint would be false, and `tracking_id` is the case where it is true. dataframely keeps `primary_key` and `unique` independent, so both have to be exercised.
    - `paid_orders_have_amount` carries a docstring and `line_numbers_are_dense` does not, which is the description ladder's fork (#17).
    - `email` names its check and `note` leaves it anonymous, which is the check-name renderer's fork (#20).
    - `amount` carries free-form `metadata=` with a non-string value, which is the only dataframely attribute that reaches Dagster's column tags.
    """

    order_id = dy.String(
        primary_key=True,
        regex=r"^ORD-\d+$",
        description="Order identifier, unique together with `line_no`.",
    )
    line_no = dy.Int32(primary_key=True, min=1)
    email = dy.String(
        nullable=False,
        check={"lowercase": lambda expr: expr.str.to_lowercase() == expr},
        description="Customer contact address.",
    )
    amount = dy.Decimal(
        10,
        2,
        nullable=False,
        min=Decimal("0.00"),
        description="Line total in account currency.",
        # Mixed value types on purpose: Dagster's column tags are `Mapping[str, str]`.
        metadata={"owner": "finance", "pii": False},
    )
    tracking_id = dy.String(
        nullable=True,
        unique=True,
        description="Carrier tracking number, unique across every line.",
    )
    quantity = dy.Int32(nullable=False, min=1)
    status = dy.Enum(["new", "paid", "shipped"], nullable=False)
    ordered_at = dy.Datetime(nullable=False)
    fulfilled_in = dy.Duration(nullable=True)
    payload = dy.Binary(nullable=True)
    tags = dy.List(dy.String(), nullable=True)
    note = dy.String(nullable=True, check=lambda expr: expr.str.len_chars() < 100)

    @dy.rule()
    def paid_orders_have_amount(cls) -> pl.Expr:
        """Paid orders must carry a positive amount."""
        return (cls.status.col != "paid") | (cls.amount.col > 0)

    @dy.rule()
    def line_numbers_are_dense(cls) -> pl.Expr:
        return cls.line_no.col.max().over("order_id") == cls.line_no.col.count().over(
            "order_id"
        )


POLARS_SCHEMA: dict[str, pl.DataType] = {
    "order_id": pl.String(),
    "line_no": pl.Int32(),
    "email": pl.String(),
    "amount": pl.Decimal(10, 2),
    "tracking_id": pl.String(),
    "quantity": pl.Int32(),
    "status": pl.Enum(["new", "paid", "shipped"]),
    "ordered_at": pl.Datetime("us"),
    "fulfilled_in": pl.Duration("us"),
    "payload": pl.Binary(),
    "tags": pl.List(pl.String()),
    "note": pl.String(),
}

_ORDERED_AT = dt.datetime(2026, 8, 1, 12, 0, 0)  # noqa: DTZ001 - the schema declares no time zone


def _row(
    order_id: str,
    email: str,
    amount: str,
    quantity: int,
    status: str,
    *,
    line_no: int = 1,
) -> dict[str, object]:
    """Builds one row, defaulting the columns no test varies."""
    return {
        "order_id": order_id,
        "line_no": line_no,
        "email": email,
        "amount": Decimal(amount),
        "tracking_id": f"TRK-{order_id}-{line_no}",
        "quantity": quantity,
        "status": status,
        "ordered_at": _ORDERED_AT,
        "fulfilled_in": dt.timedelta(hours=26),
        "payload": b"\x00\x01",
        "tags": ["priority"],
        "note": None,
    }


def _frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=POLARS_SCHEMA)


def clean_orders() -> pl.DataFrame:
    """Every row valid."""
    return _frame(
        [
            _row("ORD-1", "a@example.com", "10.00", 1, "new"),
            _row("ORD-2", "b@example.com", "25.50", 2, "paid"),
            _row("ORD-3", "c@example.com", "99.00", 3, "shipped"),
        ]
    )


def mixed_orders() -> pl.DataFrame:
    """Three valid rows and three rejected, one per rule that can reject alone."""
    return _frame(
        [
            _row("ORD-1", "a@example.com", "10.00", 1, "new"),
            _row("ORD-2", "b@example.com", "25.50", 2, "paid"),
            _row("ORD-3", "c@example.com", "99.00", 3, "shipped"),
            _row("ORD-4", "d@example.com", "-4.00", 1, "new"),  # amount|min
            _row("ORD-5", "E@example.com", "12.00", 1, "new"),  # email|check__lowercase
            _row(
                "ORD-6", "f@example.com", "0.00", 1, "paid"
            ),  # paid_orders_have_amount
        ]
    )


def cooccurring_orders() -> pl.DataFrame:
    """Three valid rows and one that trips three rules at once.

    The fifth frame, added by #19. The other four reject at most one rule per row, so co-occurrence counts read as singletons on all of them and a broken emission would be indistinguishable from a working one.
    """
    return _frame(
        [
            _row("ORD-1", "a@example.com", "10.00", 1, "new"),
            _row("ORD-2", "b@example.com", "25.50", 2, "paid"),
            _row("ORD-3", "c@example.com", "99.00", 3, "shipped"),
            # amount|min, email|check__lowercase and paid_orders_have_amount together.
            _row("ORD-4", "D@example.com", "-1.00", 1, "paid"),
        ]
    )


def hopeless_orders() -> pl.DataFrame:
    """Every row violates `amount|min`, so nothing survives the filter."""
    return _frame(
        [
            _row("ORD-1", "a@example.com", "-1.00", 1, "new"),
            _row("ORD-2", "b@example.com", "-2.00", 1, "new"),
        ]
    )


def wrong_dtype_orders() -> pl.DataFrame:
    """`quantity` arrives `Int64`: a pipeline defect the gate catches before the filter."""
    return clean_orders().with_columns(pl.col("quantity").cast(pl.Int64))
