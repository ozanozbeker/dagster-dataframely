"""Tests for `DataframelyParquetIOManager`.

The seam is `dg.materialize` in-process against `tmp_path`. What Dagster ends up holding is the whole external behaviour of an IO manager: the materialization metadata, and the bytes on disk.
The manager is schema-agnostic, so every asset here is a plain `@dg.asset` returning a polars frame.
"""

import datetime as dt
import logging
from decimal import Decimal
from pathlib import Path

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dagster_dataframely import DataframelyParquetIOManager, UnwritableDtypeError

# The dtypes a round trip could plausibly get wrong: Decimal, Duration, Binary and a nested List. Nulls ride along in every column that admits them.
_ORDERS = pl.DataFrame(
    {
        "order_id": ["a", "b"],
        "amount": pl.Series(
            [Decimal("1.50"), Decimal("2.25")], dtype=pl.Decimal(10, 2)
        ),
        "placed_at": pl.Series(
            [dt.datetime(2026, 1, 1, 12, tzinfo=dt.UTC), None],
            dtype=pl.Datetime("us", "UTC"),
        ),
        "took": pl.Series([dt.timedelta(hours=2), None], dtype=pl.Duration("us")),
        "payload": pl.Series([b"\x00\x01", None], dtype=pl.Binary),
        "tags": pl.Series([["x", "y"], []], dtype=pl.List(pl.String)),
    }
)


@dg.asset(name="orders")
def _orders() -> pl.DataFrame:
    return _ORDERS


def _materialize(
    tmp_path: Path,
    *assets: dg.AssetsDefinition,
    instance: dg.DagsterInstance | None = None,
) -> dg.ExecuteInProcessResult:
    """Runs `assets` with the manager rooted at `tmp_path`."""
    return dg.materialize(
        list(assets),
        resources={"io_manager": DataframelyParquetIOManager(base_dir=str(tmp_path))},
        instance=instance,
    )


@pytest.fixture
def orders_metadata(tmp_path: Path) -> dict[str, dg.MetadataValue]:
    """Materializes `orders` once and returns the metadata on its materialization."""
    result = _materialize(tmp_path, _orders)
    (event,) = result.get_asset_materialization_events()
    return dict(event.step_materialization_data.materialization.metadata)


def test_a_written_frame_reads_back_equal(tmp_path: Path) -> None:
    """The round trip is the manager's whole job; every dtype in `_ORDERS` survives it."""
    read_back: list[pl.DataFrame] = []

    @dg.asset(name="orders_copy")
    def orders_copy(orders: pl.DataFrame) -> None:
        read_back.append(orders)

    assert _materialize(tmp_path, _orders, orders_copy).success
    assert_frame_equal(read_back[0], _ORDERS)


def test_a_lazy_frame_output_is_collected_and_says_so(tmp_path: Path) -> None:
    """`DataFrame` is the supported type, so a lazy output collects rather than sinking. Silently materializing a frame the user asked to keep lazy is the worse failure, so the warning is part of the contract (#27)."""

    @dg.asset(name="orders")
    def lazy_orders() -> pl.LazyFrame:
        return _ORDERS.lazy()

    # `caplog` never sees this. `context.log` writes to the event log, not the stdlib
    # logger tree, and the event log is where a user reads it anyway.
    with dg.DagsterInstance.ephemeral() as instance:
        result = _materialize(tmp_path, lazy_orders, instance=instance)
        warnings = [
            entry.user_message
            for entry in instance.all_logs(result.run_id)
            if entry.level == logging.WARNING
        ]

    assert result.success
    assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), _ORDERS)
    assert any("Collecting a LazyFrame" in message for message in warnings)


def test_the_manager_emits_path_bytes_written_and_storage_kind(
    tmp_path: Path, orders_metadata: dict[str, dg.MetadataValue]
) -> None:
    """These three are what varied this run. `bytes_written` comes off the disk, so it reports the compression actually achieved rather than an in-memory estimate."""
    written = tmp_path / "orders.parquet"

    assert orders_metadata["path"].value == str(written)
    assert orders_metadata["bytes_written"].value == written.stat().st_size
    assert orders_metadata["dagster/storage_kind"].value == "parquet"


def test_the_manager_emits_no_column_schema(
    orders_metadata: dict[str, dg.MetadataValue],
) -> None:
    """`dagster/column_schema` describes the data, not the write, so the asset definition emits it."""
    assert "dagster/column_schema" not in orders_metadata


def test_the_manager_writes_no_sample_and_runs_no_statistics_pass(
    orders_metadata: dict[str, dg.MetadataValue],
) -> None:
    """Nothing beyond what varied this run, so no data sample and no statistics pass."""
    assert set(orders_metadata) == {"path", "bytes_written", "dagster/storage_kind"}


def test_an_unwritable_dtype_raises_before_the_write(tmp_path: Path) -> None:
    """Left to polars, `pl.Object` fails as a `ComputeError` from inside the writer. The manager gets there first, names the column, and leaves nothing behind."""

    @dg.asset(name="orders")
    def unwritable() -> pl.DataFrame:
        return pl.DataFrame(
            {"order_id": ["a"], "payload": pl.Series([object()], dtype=pl.Object)}
        )

    with pytest.raises(UnwritableDtypeError) as raised:
        _materialize(tmp_path, unwritable)

    assert "Column 'payload' (Object)" in str(raised.value)
    assert "drop it" in str(raised.value)
    assert "order_id" not in str(raised.value)
    assert not list(tmp_path.rglob("*.parquet"))


def test_the_error_reads_as_plural_for_several_columns(tmp_path: Path) -> None:
    """The message is the only thing a user sees of this error, so it agrees in number."""

    @dg.asset(name="orders")
    def unwritable() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "payload": pl.Series([object()], dtype=pl.Object),
                "sidecar": pl.Series([object()], dtype=pl.Object),
            }
        )

    with pytest.raises(UnwritableDtypeError) as raised:
        _materialize(tmp_path, unwritable)

    assert "Columns 'payload' (Object), 'sidecar' (Object)" in str(raised.value)
    assert "drop them" in str(raised.value)


def test_an_output_that_is_not_a_frame_says_so(tmp_path: Path) -> None:
    """An asset that forgot its `-> None` annotation reaches the manager; it should not die on an `AttributeError` two frames down."""

    @dg.asset(name="orders")
    def not_a_frame():
        return None

    with pytest.raises(dg.DagsterInvariantViolationError, match="polars frames"):
        _materialize(tmp_path, not_a_frame)


@pytest.mark.parametrize(
    "base_dir", ["s3://bucket/prefix", "gs://bucket/prefix", "az://container/prefix"]
)
def test_a_cloud_uri_is_accepted_as_base_dir(base_dir: str) -> None:
    """Cloud support rests entirely on universal-pathlib, so the scheme has to survive path resolution."""
    manager = DataframelyParquetIOManager(base_dir=base_dir).create_io_manager(
        dg.build_init_resource_context()
    )

    # `_get_path` is private upstream API, and the only seam that answers "where would
    # this land" without reaching the network, which no test may do. Pin-and-assert
    # suite lands with #16.
    path = manager._get_path(dg.build_output_context(asset_key=dg.AssetKey(["orders"])))

    assert str(path) == f"{base_dir}/orders.parquet"
