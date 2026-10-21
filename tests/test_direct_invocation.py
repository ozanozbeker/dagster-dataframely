"""Direct invocation of an asset built with `dd.asset`.

A call must yield the same keys, row counts and check results as a run, so some tests compare the two.
A call has no step, so an asset with `filter=True` writes its error rows with `build_file_writer` under `error_rows_dir` (ADR-0006).
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import dagster_dataframely as dd
from dagster_dataframely.errors import ColumnSchemaError, ErrorRowsDirError
from dagster_dataframely.testing import DirectInvocationResult
from tests.scenario import (
    OrdersSchema,
    clean_orders,
    cooccurring_orders,
    materialize,
    mixed_orders,
    wrong_dtype_orders,
)

_DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])
_ERROR_ROWS_DIR_ENV = "DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR"


def _orders(frame: Callable[[], pl.DataFrame], **settings: Any) -> dg.AssetsDefinition:
    """Declare the `orders` asset with the given `dd.asset` settings."""

    @dd.asset(OrdersSchema, name="orders", **settings)
    def orders() -> pl.DataFrame:
        return frame()

    return orders


def _call(asset: dg.AssetsDefinition) -> DirectInvocationResult:
    """Call the asset with a built context."""
    return dd.testing.DirectInvocationResult.from_invocation(
        asset(dg.build_asset_context())
    )


# `cooccurring_orders` has 3 valid rows and 1 invalid, so a row count shows which rows a call returned.
_FILTER_SETTINGS = [
    pytest.param(clean_orders, False, id="no filtering"),
    pytest.param(cooccurring_orders, True, id="filtering"),
]


@pytest.fixture
def error_rows_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Set `error_rows_dir` to a directory under `tmp_path`."""
    directory = tmp_path / "filtering"
    monkeypatch.setenv(_ERROR_ROWS_DIR_ENV, str(directory))
    return directory


def _materialization(
    result: DirectInvocationResult,
) -> dg.MaterializeResult[pl.DataFrame]:
    """Return the materialization the call yielded."""
    assert result.materialization is not None
    return result.materialization


def _checks(result: DirectInvocationResult) -> dict[dg.AssetCheckKey, bool]:
    """Return every check result's `passed`, keyed by `dg.AssetCheckKey` like a run's evaluations."""
    return {
        dg.AssetCheckKey(check.asset_key, name): check.passed
        for name, check in result.checks.items()
        if check.asset_key is not None
    }


@pytest.mark.parametrize(("frame", "filtering"), _FILTER_SETTINGS)
def test_calling_the_asset_returns_its_frame(
    error_rows_dir: Path, frame: Callable[[], pl.DataFrame], filtering: bool
):
    asset = _orders(frame, filter=filtering)

    materialization = _materialization(_call(asset))

    assert_frame_equal(materialization.value, clean_orders())


@pytest.mark.parametrize(("frame", "filtering"), _FILTER_SETTINGS)
def test_calling_the_asset_reports_every_check_it_declares(
    error_rows_dir: Path, frame: Callable[[], pl.DataFrame], filtering: bool
):
    asset = _orders(frame, filter=filtering)

    checks = _checks(_call(asset))

    assert set(checks) == {spec.key for spec in asset.check_specs}


@pytest.mark.parametrize(("frame", "filtering"), _FILTER_SETTINGS)
def test_the_key_a_call_yields_is_the_key_a_run_writes_under(
    tmp_path: Path,
    error_rows_dir: Path,
    frame: Callable[[], pl.DataFrame],
    filtering: bool,
):
    asset = _orders(frame, filter=filtering)
    result = materialize(tmp_path, asset)
    called = _materialization(_call(asset))

    assert result.success
    assert {called.asset_key: len(called.value)} == {
        event.asset_key: event.step_materialization_data.materialization.metadata[
            "dagster/row_count"
        ].value
        for event in result.get_asset_materialization_events()
        # A call has no step, so it records no materialization for the error rows.
        if event.asset_key == asset.key
    }


@pytest.mark.parametrize(("frame", "filtering"), _FILTER_SETTINGS)
def test_the_checks_a_call_yields_are_the_checks_a_run_evaluates(
    tmp_path: Path,
    error_rows_dir: Path,
    frame: Callable[[], pl.DataFrame],
    filtering: bool,
):
    asset = _orders(frame, filter=filtering)
    result = materialize(tmp_path, asset)

    assert _checks(_call(asset)) == {
        evaluation.asset_check_key: evaluation.passed
        for evaluation in result.get_asset_check_evaluations()
    }


def test_a_column_schema_drift_raises_out_of_the_call():
    asset = _orders(wrong_dtype_orders)

    with pytest.raises(ColumnSchemaError) as raised:
        _call(asset)

    assert "Column 'quantity' (expected Int32, got Int64)" in str(raised.value)


def test_a_call_writes_the_error_rows_to_a_file_under_error_rows_dir(
    error_rows_dir: Path,
):
    asset = _orders(mixed_orders, filter=True)

    materialization = _materialization(_call(asset))
    written = pl.read_parquet(error_rows_dir / "orders__error_rows.parquet")

    assert materialization.asset_key == dg.AssetKey(["orders"])
    assert_frame_equal(materialization.value, clean_orders())
    assert written.height == 3
    assert "dy_rule__amount__min" in written.columns


def test_a_call_writes_the_error_rows_under_the_error_rows_key_it_set(
    error_rows_dir: Path,
):
    """A call writes under `error_rows_key` and does not use `error_rows_io_manager_key`."""
    asset = _orders(
        mixed_orders,
        filter=True,
        error_rows_key=["audit", "orders__error_rows"],
        error_rows_io_manager_key="audit",
    )

    called = dd.testing.DirectInvocationResult.from_invocation(
        asset(dg.build_asset_context(resources={"audit": None}))
    )
    path = error_rows_dir / "audit" / "orders__error_rows.parquet"

    assert pl.read_parquet(path).height == 3
    assert (_materialization(called).metadata or {})[
        "dataframely/error_rows_address"
    ] == str(path)


def test_a_call_with_error_rows_and_no_error_rows_dir_raises():
    asset = _orders(mixed_orders, filter=True)

    with pytest.raises(ErrorRowsDirError) as raised:
        _call(asset)

    assert _ERROR_ROWS_DIR_ENV in str(raised.value)
    assert "orders" in str(raised.value)


def test_a_call_with_every_row_valid_needs_no_error_rows_dir():
    asset = _orders(clean_orders, filter=True)

    materialization = _materialization(_call(asset))

    assert_frame_equal(materialization.value, clean_orders())


def test_a_call_with_every_row_valid_writes_nothing_under_error_rows_dir(
    error_rows_dir: Path,
):
    asset = _orders(clean_orders, filter=True)

    _call(asset)

    assert not error_rows_dir.exists()


def test_an_error_rows_dir_set_after_the_asset_is_declared_is_the_one_written_to(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    asset = _orders(mixed_orders, filter=True)
    directory = tmp_path / "named-late"
    monkeypatch.setenv(_ERROR_ROWS_DIR_ENV, str(directory))

    _call(asset)

    assert pl.read_parquet(directory / "orders__error_rows.parquet").height == 3


def test_a_call_writes_partitioned_error_rows_under_their_partition(
    error_rows_dir: Path,
):
    @dd.asset(OrdersSchema, name="orders", filter=True, partitions_def=_DAYS)
    def orders() -> pl.DataFrame:
        return mixed_orders()

    dd.testing.DirectInvocationResult.from_invocation(
        orders(dg.build_asset_context(partition_key="2026-01-02"))
    )

    assert (error_rows_dir / "orders__error_rows" / "2026-01-02.parquet").exists()


def test_a_call_with_a_partition_range_and_error_rows_raises(error_rows_dir: Path):
    @dd.asset(OrdersSchema, name="orders", filter=True, partitions_def=_DAYS)
    def orders() -> pl.DataFrame:
        return mixed_orders()

    days = dg.PartitionKeyRange("2026-01-02", "2026-01-03")
    with pytest.raises(dg.DagsterInvariantViolationError, match="partition range"):
        dd.testing.DirectInvocationResult.from_invocation(
            orders(dg.build_asset_context(partition_key_range=days))
        )

    assert not error_rows_dir.exists()


def test_a_decorated_function_taking_context_reads_its_partition_key_from_a_built_one():
    """`dg.build_asset_context` does not set the ContextVar that `AssetExecutionContext.get()` reads."""
    seen: dict[str, str] = {}

    @dd.asset(OrdersSchema, name="orders", partitions_def=_DAYS)
    def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
        seen["partition"] = context.partition_key
        return clean_orders()

    materialization = _materialization(
        dd.testing.DirectInvocationResult.from_invocation(
            orders(dg.build_asset_context(partition_key="2026-01-02"))
        )
    )

    assert seen == {"partition": "2026-01-02"}
    assert materialization.asset_key == dg.AssetKey(["orders"])


def test_a_decorated_function_taking_context_alongside_an_input_is_invocable_too():
    """The context comes first and the upstream frames follow, as in Dagster."""

    @dd.asset(OrdersSchema, name="orders")
    def orders(
        context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
    ) -> pl.DataFrame:
        assert isinstance(context, dg.AssetExecutionContext)
        return raw_orders

    materialization = _materialization(
        dd.testing.DirectInvocationResult.from_invocation(
            orders(dg.build_asset_context(), clean_orders())
        )
    )

    assert_frame_equal(materialization.value, clean_orders())
