"""The schema every validated order table in the pipeline is held to."""

from decimal import Decimal

import dataframely as dy
import polars as pl

#: The longest operator note the fulfilment tool accepts.
NOTE_MAX_CHARS = 100


class Orders(dy.Schema):
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
        description="Where the line sits in fulfilment.",
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
        check=lambda expr: expr.str.len_chars() < NOTE_MAX_CHARS,
        description="Operator note attached to the line.",
    )

    @dy.rule()
    def paid_orders_have_amount(cls) -> pl.Expr:
        """Require a positive amount on a paid line."""
        return (cls.status.col != "paid") | (cls.amount.col > 0)

    @dy.rule()
    def line_numbers_are_dense(cls) -> pl.Expr:
        """Require every order's lines to run 1 to n with no gaps."""
        return cls.line_no.col.max().over("order_id") == cls.line_no.col.count().over(
            "order_id"
        )
