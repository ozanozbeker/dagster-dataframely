"""The one `Orders` schema and the frames the whole effort runs against.

It covers the cases the whole effort needs: the dtypes a round trip or a metadata emission could get wrong, and the rule shapes the naming and description fallbacks have to distinguish.

Nothing here is a fixture. A schema is a class and a frame is a value, so both read more cheaply as module constants than as fixture indirection. `Orders` also has to be importable at class-definition time to decorate an asset.

`POLARS_SCHEMA` restates the dtypes by hand rather than deriving them. A frame that drifts from the schema then fails the column-schema check loudly in every runtime test, instead of being silently rebuilt to match.

`storage` is here for the same reason the frames are: every run test needs somewhere to write, and only a handful care which manager writes it. It builds `dagster-polars`' parquet manager, which ADR-0004 makes this package's recommendation, so a test that merely needs storage exercises what a user will actually run.

`warehouse` is its opposite number, for the handful of tests that care that the manager is a database rather than a filesystem. Delegation is meant to place a quarantine on both without knowing which it is talking to (ADR-0006), and only two managers can show that.
"""

import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import NamedTuple

import dataframely as dy
import duckdb
import polars as pl
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_polars import PolarsParquetIOManager

#: The database schema `warehouse` writes into, and therefore the prefix the assets under it carry. `DbIOManager` addresses a table as `<schema>.<name>`, so the two have to agree.
WAREHOUSE_SCHEMA = "analytics"


def storage(tmp_path: Path) -> dict[str, PolarsParquetIOManager]:
    """Build the resources a run needs when all it needs is somewhere to write.

    Parameters
    ----------
    tmp_path
        The directory the run writes under.

    Returns
    -------
    The `resources` mapping to hand `dg.materialize`.
    """
    return {"io_manager": PolarsParquetIOManager(base_dir=str(tmp_path))}


def warehouse(tmp_path: Path) -> dict[str, DuckDBPolarsIOManager]:
    """Build the resources a run needs when it has to write into a database.

    The other of the two base classes Dagster ships. `PolarsParquetIOManager` is a `UPathIOManager` and this is a `DbIOManager`, and between them they cover nearly every first-party manager, so a quarantine that lands natively on both is not support for two integrations (ADR-0006).

    The schema is created here rather than by the manager, which assumes one exists.

    Parameters
    ----------
    tmp_path
        The directory the database file goes in.

    Returns
    -------
    The `resources` mapping to hand `dg.materialize`.
    """
    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.sql(f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_SCHEMA}")
    return {
        "io_manager": DuckDBPolarsIOManager(
            database=str(database), schema=WAREHOUSE_SCHEMA
        )
    }


class Table(NamedTuple):
    """What a warehouse test can ask about a table without reading its values back.

    Values stay in the database on purpose. `Orders` carries a `Duration`, DuckDB stores that as an INTERVAL, and Polars refuses to import one without an unstable environment variable set. The placement tests care that the table exists, holds the right rows and carries the rule columns, and the parquet tests are where the values themselves are compared.

    Attributes
    ----------
    columns
        The table's column names, in the order DuckDB reports them.
    height
        How many rows it holds.
    """

    columns: list[str]
    height: int


def tables(tmp_path: Path) -> dict[str, Table]:
    """Describe every table a `warehouse` run wrote, keyed by name.

    Parameters
    ----------
    tmp_path
        The same directory `warehouse` was given.

    Returns
    -------
    One description per table in the warehouse schema.
    """
    # Interpolated rather than parameterized: an identifier cannot be bound, and every
    # value here is this file's own literal or a name DuckDB itself reported.
    with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as connection:
        columns: dict[str, list[str]] = {}
        for name, column in connection.sql(
            "select table_name, column_name from information_schema.columns "  # noqa: S608
            f"where table_schema = '{WAREHOUSE_SCHEMA}' order by ordinal_position"
        ).fetchall():
            columns.setdefault(name, []).append(column)
        heights = {
            name: connection.sql(
                f"select count(*) from {WAREHOUSE_SCHEMA}.{name}"  # noqa: S608
            ).pl()["count_star()"][0]
            for name in columns
        }
        return {name: Table(names, heights[name]) for name, names in columns.items()}


class Orders(dy.Schema):
    """Customer orders, one row per order line.

    Every column and rule here earns its place by being awkward somewhere:

    - `Decimal` crashes `TableRecord` emission unless coerced (#23).
    - `Duration` has no readable Polars string form (#23).
    - `Binary` is the one member of the string statistics family with no string form to read, so it is the one the cast has to exempt (#23).
    - The composite primary key is the case where a per-column `unique` constraint would be false, and `tracking_id` is the case where it is true. Dataframely keeps `primary_key` and `unique` independent, so both have to be exercised.
    - `paid_orders_have_amount` carries a docstring and `line_numbers_are_dense` does not, so both paths of the description fallback run (#17).
    - `email` names its check and `note` leaves it anonymous, so both paths of the check-name renderer run (#20).
    - `email` and `tags` spell `max_length` identically and mean different things by it, bytes against elements, so both paths of the constraint renderer's length unit run (#20).
    - `amount` carries free-form `metadata=` with a non-string value, the only Dataframely attribute that reaches Dagster's column tags.
    """

    order_id = dy.String(
        primary_key=True,
        regex=r"^ORD-\d+$",
        description="Order identifier, unique together with `line_no`.",
    )
    line_no = dy.Int32(primary_key=True, min=1)
    email = dy.String(
        nullable=False,
        max_length=254,
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
    tags = dy.List(dy.String(), nullable=True, max_length=5)
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
    """Build one row, defaulting the columns no test varies."""
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
    """Three valid rows and three invalid, one per rule a row can fail alone."""
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

    The fifth frame, added by #19. The other four fail at most one rule per row, so co-occurrence counts read as singletons on all of them and a broken emission would look exactly like a working one.
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
    """`quantity` arrives `Int64`: a pipeline defect the column-schema check catches before the filter."""
    return clean_orders().with_columns(pl.col("quantity").cast(pl.Int64))
