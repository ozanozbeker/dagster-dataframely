"""A run under `dagster-polars`' `PolarsParquetIOManager`, the IO manager most users of this package bind.

The `integration` dependency group installs `dagster-polars`, and the module skips itself without it.
"""

from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest

import dagster_dataframely as dd
from dagster_dataframely.data import OrdersSchema, marketplace_orders, storefront_orders
from dagster_dataframely.wiring import get_table_schema
from tests.scenario import materializations

dagster_polars = pytest.importorskip("dagster_polars")


def _resources(tmp_path: Path) -> dict[str, Any]:
    """Build the resources for a run under `PolarsParquetIOManager`."""
    return {"io_manager": dagster_polars.PolarsParquetIOManager(base_dir=str(tmp_path))}


@dg.asset
def raw_orders() -> pl.DataFrame:
    return marketplace_orders


@dd.asset(OrdersSchema, filter=True)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return raw_orders


error_rows = dd.build_error_rows_spec(OrdersSchema, orders)


def test_a_run_writes_the_table_and_the_error_rows_beside_it(tmp_path: Path):
    result = dg.materialize(
        [raw_orders, orders, error_rows], resources=_resources(tmp_path)
    )

    assert result.success
    assert pl.read_parquet(tmp_path / "orders.parquet").height == 12
    assert pl.read_parquet(tmp_path / "orders__error_rows.parquet").height == 8


def test_a_downstream_asset_loads_the_table_lazily_and_the_error_rows_eagerly(
    tmp_path: Path,
):
    loaded: dict[str, pl.DataFrame | pl.LazyFrame] = {}

    @dg.asset
    def report(orders: pl.LazyFrame, orders__error_rows: pl.DataFrame) -> None:
        loaded.update(orders=orders, error_rows=orders__error_rows)

    dg.materialize(
        [raw_orders, orders, error_rows, report], resources=_resources(tmp_path)
    )

    assert isinstance(loaded["orders"], pl.LazyFrame)
    assert loaded["orders"].collect().height == 12
    assert isinstance(loaded["error_rows"], pl.DataFrame)
    assert loaded["error_rows"].height == 8


def test_the_package_writes_its_column_schema_over_the_io_managers(tmp_path: Path):
    result = dg.materialize(
        [raw_orders, orders, error_rows], resources=_resources(tmp_path)
    )

    column_schema = materializations(result)[orders.key].metadata[
        "dagster/column_schema"
    ]
    assert column_schema.value == get_table_schema(OrdersSchema)


def test_a_lazy_return_is_written(tmp_path: Path):
    @dd.asset(OrdersSchema, name="orders")
    def lazy() -> pl.LazyFrame:
        return storefront_orders.lazy()

    dg.materialize([lazy], resources=_resources(tmp_path))

    assert pl.read_parquet(tmp_path / "orders.parquet").height == 12


def test_an_unpartitioned_asset_loads_every_partition_as_a_dict(tmp_path: Path):
    days = dg.StaticPartitionsDefinition(["2026-08-01", "2026-08-02"])
    loaded: dict[str, pl.LazyFrame] = {}

    @dd.asset(OrdersSchema, partitions_def=days)
    def daily() -> pl.DataFrame:
        return storefront_orders

    @dg.asset
    def weekly(daily: dict[str, pl.LazyFrame]) -> None:
        loaded.update(daily)

    for day in days.get_partition_keys():
        dg.materialize([daily], partition_key=day, resources=_resources(tmp_path))
    dg.materialize([daily, weekly], selection=[weekly], resources=_resources(tmp_path))

    assert sorted(loaded) == ["2026-08-01", "2026-08-02"]
    assert all(frame.collect().height == 12 for frame in loaded.values())
