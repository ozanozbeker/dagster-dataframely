"""Direct invocation of an asset built with `dd.asset`.

Each test runs a failure policy twice, once by a call and once by `dg.materialize`, because a call must yield the same keys, row counts and check results as a run.
A call has no IO manager, so an asset with `quarantine=True` writes its invalid rows with `file_writer` under `quarantine_dir` (ADR-0006).
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import dagster_dataframely as dd
from dagster_dataframely.errors import ColumnSchemaError, QuarantineDirError
from tests.scenario import (
    Orders,
    Yielded,
    clean_orders,
    cooccurring_orders,
    events,
    materialize,
    mixed_orders,
    results,
    wrong_dtype_orders,
)

_DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])
_QUARANTINE_DIR_ENV = "DAGSTER_DATAFRAMELY_QUARANTINE_DIR"


def _orders(frame: Callable[[], pl.DataFrame], **settings: Any) -> dg.AssetsDefinition:
    """Declare the `orders` asset with the given `dd.asset` settings."""

    @dd.asset(Orders, name="orders", **settings)
    def orders() -> pl.DataFrame:
        return frame()

    return orders


def _called(asset: dg.AssetsDefinition, *, quarantine: bool) -> Yielded:
    """Call the asset, passing a context when `quarantine=True` adds a `context` parameter."""
    return events(asset, *((dg.build_asset_context(),) if quarantine else ()))


# The two failure policies; `cooccurring_orders` has 3 valid rows and 1 invalid, so a row count shows which rows a call returned.
_SHAPES = [
    pytest.param(clean_orders, False, id="no quarantine"),
    pytest.param(cooccurring_orders, True, id="quarantine"),
]


@pytest.fixture
def quarantine_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Set `quarantine_dir` to a directory under `tmp_path`."""
    directory = tmp_path / "quarantine"
    monkeypatch.setenv(_QUARANTINE_DIR_ENV, str(directory))
    return directory


def _tables(events: Yielded) -> dict[dg.AssetKey, pl.DataFrame]:
    """The frame each output produced, keyed by asset."""
    return {key: result.value for key, result in results(events).items()}


def _checks(events: Yielded) -> dict[dg.AssetCheckKey, bool]:
    """Every check result's `passed`, keyed by `dg.AssetCheckKey`."""
    return {
        dg.AssetCheckKey(event.asset_key, event.check_name): event.passed
        for event in events
        if isinstance(event, dg.AssetCheckResult)
        if event.asset_key is not None and event.check_name is not None
    }


@pytest.mark.parametrize(("frame", "quarantine"), _SHAPES)
def test_calling_the_asset_returns_its_frame(
    quarantine_dir: Path, frame: Callable[[], pl.DataFrame], quarantine: bool
):
    asset = _orders(frame, quarantine=quarantine)

    tables = _tables(_called(asset, quarantine=quarantine))

    assert tables
    assert_frame_equal(next(iter(tables.values())), clean_orders())


@pytest.mark.parametrize(("frame", "quarantine"), _SHAPES)
def test_calling_the_asset_reports_every_check_it_declares(
    quarantine_dir: Path, frame: Callable[[], pl.DataFrame], quarantine: bool
):
    asset = _orders(frame, quarantine=quarantine)

    checks = _checks(_called(asset, quarantine=quarantine))

    assert set(checks) == {spec.key for spec in asset.check_specs}


@pytest.mark.parametrize(("frame", "quarantine"), _SHAPES)
def test_the_key_a_call_yields_is_the_key_a_run_writes_under(
    tmp_path: Path,
    quarantine_dir: Path,
    frame: Callable[[], pl.DataFrame],
    quarantine: bool,
):
    asset = _orders(frame, quarantine=quarantine)
    result = materialize(tmp_path, asset)

    assert result.success
    assert {
        key: len(table)
        for key, table in _tables(_called(asset, quarantine=quarantine)).items()
    } == {
        event.asset_key: event.step_materialization_data.materialization.metadata[
            "dagster/row_count"
        ].value
        for event in result.get_asset_materialization_events()
    }


@pytest.mark.parametrize(("frame", "quarantine"), _SHAPES)
def test_the_checks_a_call_yields_are_the_checks_a_run_evaluates(
    tmp_path: Path,
    quarantine_dir: Path,
    frame: Callable[[], pl.DataFrame],
    quarantine: bool,
):
    asset = _orders(frame, quarantine=quarantine)
    result = materialize(tmp_path, asset)

    assert _checks(_called(asset, quarantine=quarantine)) == {
        evaluation.asset_check_key: evaluation.passed
        for evaluation in result.get_asset_check_evaluations()
    }


def test_a_column_schema_drift_raises_out_of_the_call():
    asset = _orders(wrong_dtype_orders)

    with pytest.raises(ColumnSchemaError) as raised:
        events(asset)

    assert "Column 'quantity' (expected Int32, got Int64)" in str(raised.value)


def test_a_called_quarantine_writes_a_real_file_under_the_configured_root(
    quarantine_dir: Path,
):
    asset = _orders(mixed_orders, quarantine=True)

    tables = _tables(_called(asset, quarantine=True))
    written = pl.read_parquet(quarantine_dir / "orders_quarantine.parquet")

    assert set(tables) == {dg.AssetKey(["orders"])}
    assert_frame_equal(tables[dg.AssetKey(["orders"])], clean_orders())
    assert written.height == 3
    assert "dy_rule__amount__min" in written.columns


def test_a_called_quarantine_with_no_root_says_so_rather_than_choosing_one():
    asset = _orders(mixed_orders, quarantine=True)

    with pytest.raises(QuarantineDirError) as raised:
        _called(asset, quarantine=True)

    assert _QUARANTINE_DIR_ENV in str(raised.value)
    assert "orders" in str(raised.value)


def test_a_call_with_every_row_valid_needs_no_root_at_all():
    asset = _orders(clean_orders, quarantine=True)

    tables = _tables(_called(asset, quarantine=True))

    assert_frame_equal(tables[dg.AssetKey(["orders"])], clean_orders())


def test_a_root_set_after_the_asset_is_declared_is_the_one_the_rows_go_to(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    asset = _orders(mixed_orders, quarantine=True)
    directory = tmp_path / "named-late"
    monkeypatch.setenv(_QUARANTINE_DIR_ENV, str(directory))

    _called(asset, quarantine=True)

    assert pl.read_parquet(directory / "orders_quarantine.parquet").height == 3


def test_a_called_partitioned_quarantine_lands_under_its_partition(
    quarantine_dir: Path,
):
    @dd.asset(Orders, name="orders", quarantine=True, partitions_def=_DAYS)
    def orders() -> pl.DataFrame:
        return mixed_orders()

    events(orders, dg.build_asset_context(partition_key="2026-01-02"))

    assert (quarantine_dir / "orders_quarantine" / "2026-01-02.parquet").exists()


def test_a_decorated_function_taking_context_reads_its_partition_key_from_a_built_one():
    """`dg.build_asset_context` does not set the ContextVar that `AssetExecutionContext.get()` reads."""
    seen: dict[str, str] = {}

    @dd.asset(Orders, name="orders", partitions_def=_DAYS)
    def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
        seen["partition"] = context.partition_key
        return clean_orders()

    tables = _tables(events(orders, dg.build_asset_context(partition_key="2026-01-02")))

    assert seen == {"partition": "2026-01-02"}
    assert set(tables) == {dg.AssetKey(["orders"])}


def test_a_decorated_function_taking_context_alongside_an_input_is_invocable_too():
    """The context comes first and the upstream frames follow, as in Dagster."""

    @dd.asset(Orders, name="orders")
    def orders(
        context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
    ) -> pl.DataFrame:
        assert isinstance(context, dg.AssetExecutionContext)
        return raw_orders

    tables = _tables(events(orders, dg.build_asset_context(), clean_orders()))

    assert_frame_equal(tables[dg.AssetKey(["orders"])], clean_orders())
