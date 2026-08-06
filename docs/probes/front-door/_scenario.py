"""PROTOTYPE - throwaway (wayfinder #11). Shared scenario for the front-door shape comparison.

One Orders schema, exercised identically by every candidate shape:
column rules (`amount|min`, `*|nullability`), a schema-level composite
primary key, and a multi-column `@dy.rule` - so the `|` -> `__` rewrite,
the schema-level bucket, and the gate all show up in the derived surface.
"""

import datetime as dt

import dataframely as dy
import polars as pl


class Orders(dy.Schema):
    """Customer orders, one row per order."""

    order_id = dy.String(primary_key=True, regex=r"^ORD-\d+$")
    customer_id = dy.String(nullable=False)
    #: Order total in account currency.
    amount = dy.Float64(nullable=False, min=0.0)
    quantity = dy.Int32(nullable=False, min=1)
    status = dy.Enum(["new", "paid", "shipped"], nullable=False)
    ordered_at = dy.Datetime(nullable=False)

    @dy.rule()
    def paid_orders_have_amount(cls) -> pl.Expr:
        """Paid orders must carry a positive amount."""
        return (cls.status.col != "paid") | (cls.amount.col > 0)


POLARS_SCHEMA = {
    "order_id": pl.String,
    "customer_id": pl.String,
    "amount": pl.Float64,
    "quantity": pl.Int32,
    "status": pl.Enum(["new", "paid", "shipped"]),
    "ordered_at": pl.Datetime("us"),
}

_T = dt.datetime(2026, 8, 1, 12, 0, 0)

Row = tuple[str, str, float, int, str, dt.datetime]


def _frame(rows: list[Row]) -> pl.DataFrame:
    return pl.DataFrame(
        [dict(zip(POLARS_SCHEMA, row, strict=True)) for row in rows],
        schema=POLARS_SCHEMA,
    )


def clean_orders() -> pl.DataFrame:
    """Every row valid."""
    return _frame(
        [
            ("ORD-1", "C-1", 10.0, 1, "new", _T),
            ("ORD-2", "C-2", 25.5, 2, "paid", _T),
            ("ORD-3", "C-1", 99.0, 3, "shipped", _T),
        ]
    )


def mixed_orders() -> pl.DataFrame:
    """Valid rows plus one `amount|min`, one `paid_orders_have_amount`, one PK dup pair."""
    return _frame(
        [
            ("ORD-1", "C-1", 10.0, 1, "new", _T),
            ("ORD-2", "C-2", 25.5, 2, "paid", _T),
            ("ORD-3", "C-3", -4.0, 1, "new", _T),  # amount|min
            ("ORD-4", "C-4", 0.0, 1, "paid", _T),  # paid_orders_have_amount
            ("ORD-5", "C-5", 12.0, 1, "new", _T),  # primary_key (dup with next)
            ("ORD-5", "C-6", 13.0, 1, "new", _T),  # primary_key
        ]
    )


def hopeless_orders() -> pl.DataFrame:
    """Every row violates `amount|min` - the nothing-survived case."""
    return _frame(
        [
            ("ORD-1", "C-1", -1.0, 1, "new", _T),
            ("ORD-2", "C-2", -2.0, 1, "new", _T),
        ]
    )


def wrong_dtype_orders() -> pl.DataFrame:
    """`quantity` arrives Int64 - a pipeline defect the gate must catch before filter."""
    return clean_orders().with_columns(pl.col("quantity").cast(pl.Int64))
