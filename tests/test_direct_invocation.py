"""Direct invocation of an asset built with `dd.asset`.

Each test runs an outcome twice, once by a call and once by `dg.materialize`, because a call must yield the same keys, row counts and check results as a run.
A call has no IO manager, so an asset with `filter=True` writes its error rows with `file_writer` under `error_rows_dir` (ADR-0006).
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
    Orders,
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

    @dd.asset(Orders, name="orders", **settings)
    def orders() -> pl.DataFrame:
        return frame()

    return orders


def _called(asset: dg.AssetsDefinition, *, filtering: bool) -> DirectInvocationResult:
    """Call the asset, passing a context when `filter=True` adds a `context` parameter."""
    return dd.testing.direct_invocation_result(
        asset(*((dg.build_asset_context(),) if filtering else ()))
    )


# The two failure policies; `cooccurring_orders` has 3 valid rows and 1 invalid, so a row count shows which rows a call returned.
_SHAPES = [
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


@pytest.mark.parametrize(("frame", "filtering"), _SHAPES)
def test_calling_the_asset_returns_its_frame(
    error_rows_dir: Path, frame: Callable[[], pl.DataFrame], filtering: bool
):
    asset = _orders(frame, filter=filtering)

    materialization = _materialization(_called(asset, filtering=filtering))

    assert_frame_equal(materialization.value, clean_orders())


@pytest.mark.parametrize(("frame", "filtering"), _SHAPES)
def test_calling_the_asset_reports_every_check_it_declares(
    error_rows_dir: Path, frame: Callable[[], pl.DataFrame], filtering: bool
):
    asset = _orders(frame, filter=filtering)

    checks = _checks(_called(asset, filtering=filtering))

    assert set(checks) == {spec.key for spec in asset.check_specs}


@pytest.mark.parametrize(("frame", "filtering"), _SHAPES)
def test_the_key_a_call_yields_is_the_key_a_run_writes_under(
    tmp_path: Path,
    error_rows_dir: Path,
    frame: Callable[[], pl.DataFrame],
    filtering: bool,
):
    asset = _orders(frame, filter=filtering)
    result = materialize(tmp_path, asset)
    called = _materialization(_called(asset, filtering=filtering))

    assert result.success
    assert {called.asset_key: len(called.value)} == {
        event.asset_key: event.step_materialization_data.materialization.metadata[
            "dagster/row_count"
        ].value
        for event in result.get_asset_materialization_events()
    }


@pytest.mark.parametrize(("frame", "filtering"), _SHAPES)
def test_the_checks_a_call_yields_are_the_checks_a_run_evaluates(
    tmp_path: Path,
    error_rows_dir: Path,
    frame: Callable[[], pl.DataFrame],
    filtering: bool,
):
    asset = _orders(frame, filter=filtering)
    result = materialize(tmp_path, asset)

    assert _checks(_called(asset, filtering=filtering)) == {
        evaluation.asset_check_key: evaluation.passed
        for evaluation in result.get_asset_check_evaluations()
    }


def test_a_column_schema_drift_raises_out_of_the_call():
    asset = _orders(wrong_dtype_orders)

    with pytest.raises(ColumnSchemaError) as raised:
        dd.testing.direct_invocation_result(asset())

    assert "Column 'quantity' (expected Int32, got Int64)" in str(raised.value)


def test_a_called_error_rows_writes_a_real_file_under_the_configured_root(
    error_rows_dir: Path,
):
    asset = _orders(mixed_orders, filter=True)

    materialization = _materialization(_called(asset, filtering=True))
    written = pl.read_parquet(error_rows_dir / "orders_error_rows.parquet")

    assert materialization.asset_key == dg.AssetKey(["orders"])
    assert_frame_equal(materialization.value, clean_orders())
    assert written.height == 3
    assert "dy_rule__amount__min" in written.columns


def test_a_called_error_rows_with_no_root_says_so_rather_than_choosing_one():
    asset = _orders(mixed_orders, filter=True)

    with pytest.raises(ErrorRowsDirError) as raised:
        _called(asset, filtering=True)

    assert _ERROR_ROWS_DIR_ENV in str(raised.value)
    assert "orders" in str(raised.value)


def test_a_call_with_every_row_valid_needs_no_root_at_all():
    asset = _orders(clean_orders, filter=True)

    materialization = _materialization(_called(asset, filtering=True))

    assert_frame_equal(materialization.value, clean_orders())


def test_a_call_with_every_row_valid_writes_nothing_under_error_rows_dir(
    error_rows_dir: Path,
):
    asset = _orders(clean_orders, filter=True)

    _called(asset, filtering=True)

    assert not error_rows_dir.exists()


def test_a_root_set_after_the_asset_is_declared_is_the_one_the_rows_go_to(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    asset = _orders(mixed_orders, filter=True)
    directory = tmp_path / "named-late"
    monkeypatch.setenv(_ERROR_ROWS_DIR_ENV, str(directory))

    _called(asset, filtering=True)

    assert pl.read_parquet(directory / "orders_error_rows.parquet").height == 3


def test_a_called_partitioned_error_rows_lands_under_its_partition(
    error_rows_dir: Path,
):
    @dd.asset(Orders, name="orders", filter=True, partitions_def=_DAYS)
    def orders() -> pl.DataFrame:
        return mixed_orders()

    dd.testing.direct_invocation_result(
        orders(dg.build_asset_context(partition_key="2026-01-02"))
    )

    assert (error_rows_dir / "orders_error_rows" / "2026-01-02.parquet").exists()


def test_a_decorated_function_taking_context_reads_its_partition_key_from_a_built_one():
    """`dg.build_asset_context` does not set the ContextVar that `AssetExecutionContext.get()` reads."""
    seen: dict[str, str] = {}

    @dd.asset(Orders, name="orders", partitions_def=_DAYS)
    def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
        seen["partition"] = context.partition_key
        return clean_orders()

    materialization = _materialization(
        dd.testing.direct_invocation_result(
            orders(dg.build_asset_context(partition_key="2026-01-02"))
        )
    )

    assert seen == {"partition": "2026-01-02"}
    assert materialization.asset_key == dg.AssetKey(["orders"])


def test_a_decorated_function_taking_context_alongside_an_input_is_invocable_too():
    """The context comes first and the upstream frames follow, as in Dagster."""

    @dd.asset(Orders, name="orders")
    def orders(
        context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
    ) -> pl.DataFrame:
        assert isinstance(context, dg.AssetExecutionContext)
        return raw_orders

    materialization = _materialization(
        dd.testing.direct_invocation_result(
            orders(dg.build_asset_context(), clean_orders())
        )
    )

    assert_frame_equal(materialization.value, clean_orders())
