"""The `Orders` schema, the frames the tests validate, and the helpers several test modules share."""

import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Any, NamedTuple

import dagster as dg
import dataframely as dy
import duckdb
import polars as pl
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_polars import PolarsParquetIOManager

WAREHOUSE_SCHEMA = "analytics"
"""The database schema `warehouse` writes to."""


def storage(tmp_path: Path) -> dict[str, PolarsParquetIOManager]:
    """Build the resources for a run that only needs somewhere to write."""
    return {"io_manager": PolarsParquetIOManager(base_dir=str(tmp_path))}


def warehouse(tmp_path: Path) -> dict[str, DuckDBPolarsIOManager]:
    """Build the resources for a run that writes into a database."""
    database = tmp_path / "warehouse.duckdb"
    with duckdb.connect(str(database)) as connection:
        connection.sql(f"CREATE SCHEMA IF NOT EXISTS {WAREHOUSE_SCHEMA}")
    return {
        "io_manager": DuckDBPolarsIOManager(
            database=str(database), schema=WAREHOUSE_SCHEMA
        )
    }


class Table(NamedTuple):
    """A warehouse table's column names and row count, not its values: Polars loads DuckDB's INTERVAL only with an unstable environment variable set."""

    columns: list[str]
    height: int


def tables(tmp_path: Path) -> dict[str, Table]:
    """Describe every table a `warehouse` run wrote, keyed by name."""
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


Yielded = list[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]
"""The events a called asset yields."""


def events(asset: dg.AssetsDefinition, *args: object) -> Yielded:
    """Call the asset and consume the events it yields."""
    return list(asset(*args))  # pyrefly: ignore[bad-argument-type]


def results(yielded: Yielded) -> dict[dg.AssetKey, dg.MaterializeResult[pl.DataFrame]]:
    """Index the materialization each output yielded by asset key."""
    return {
        event.asset_key: event
        for event in yielded
        if isinstance(event, dg.MaterializeResult) and event.asset_key is not None
    }


def materialize(
    tmp_path: Path,
    *assets: dg.AssetsDefinition,
    partition_key: str | None = None,
    instance: dg.DagsterInstance | None = None,
    raise_on_error: bool = True,
) -> dg.ExecuteInProcessResult:
    """Run the assets against `storage`."""
    return dg.materialize(
        list(assets),
        partition_key=partition_key,
        instance=instance,
        resources=storage(tmp_path),
        raise_on_error=raise_on_error,
    )


def materializations(
    result: dg.ExecuteInProcessResult,
) -> dict[dg.AssetKey, dg.AssetMaterialization]:
    """Index every materialization a run recorded by asset key."""
    return {
        event.asset_key: event.step_materialization_data.materialization
        for event in result.get_asset_materialization_events()
        if event.asset_key is not None
    }


def check_evaluations(
    result: dg.ExecuteInProcessResult,
) -> dict[str, dg.AssetCheckEvaluation]:
    """Index every check evaluation a run recorded by check name."""
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


def records(value: dg.MetadataValue[Any]) -> list[dict[str, Any]]:
    """Read a table metadata value back as a row per record."""
    assert isinstance(value, dg.TableMetadataValue)
    return [dict(record.data) for record in value.records]


class Orders(dy.Schema):
    """Customer orders, one row per order line.

    Each column or rule covers a case:

    - `amount` is a `Decimal`, which `TableRecord` raises on unless coerced.
    - `fulfilled_in` is a `Duration`, which has no readable string form.
    - `payload` is a `Binary`, the one dtype in the `string` dtype group with no string form.
    - The composite primary key is not unique per column, and `tracking_id` is.
    - `line_numbers_are_dense` has no docstring.
    - `email` names its check and `note` does not.
    - `email` and `tags` both set `max_length`, in bytes and in elements.
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


# Not derived from `Orders`, so a frame that differs from the schema fails the column-schema check.
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
    return _frame([
        _row("ORD-1", "a@example.com", "10.00", 1, "new"),
        _row("ORD-2", "b@example.com", "25.50", 2, "paid"),
        _row("ORD-3", "c@example.com", "99.00", 3, "shipped"),
    ])


def mixed_orders() -> pl.DataFrame:
    """Three valid rows and three invalid, one per rule a row can fail alone."""
    return _frame([
        _row("ORD-1", "a@example.com", "10.00", 1, "new"),
        _row("ORD-2", "b@example.com", "25.50", 2, "paid"),
        _row("ORD-3", "c@example.com", "99.00", 3, "shipped"),
        _row("ORD-4", "d@example.com", "-4.00", 1, "new"),  # amount|min
        _row("ORD-5", "E@example.com", "12.00", 1, "new"),  # email|check__lowercase
        _row("ORD-6", "f@example.com", "0.00", 1, "paid"),  # paid_orders_have_amount
    ])


def cooccurring_orders() -> pl.DataFrame:
    """Three valid rows and one that fails three rules, the only frame with more failures than invalid rows."""
    return _frame([
        _row("ORD-1", "a@example.com", "10.00", 1, "new"),
        _row("ORD-2", "b@example.com", "25.50", 2, "paid"),
        _row("ORD-3", "c@example.com", "99.00", 3, "shipped"),
        # amount|min, email|check__lowercase and paid_orders_have_amount
        _row("ORD-4", "D@example.com", "-1.00", 1, "paid"),
    ])


def no_valid_orders() -> pl.DataFrame:
    """Every row fails `amount|min`, so no row is valid."""
    return _frame([
        _row("ORD-1", "a@example.com", "-1.00", 1, "new"),
        _row("ORD-2", "b@example.com", "-2.00", 1, "new"),
    ])


def wrong_dtype_orders() -> pl.DataFrame:
    """`quantity` is `Int64`, so the column-schema check fails."""
    return clean_orders().with_columns(pl.col("quantity").cast(pl.Int64))
