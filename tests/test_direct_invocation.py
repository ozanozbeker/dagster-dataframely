"""Calling a `@dy_asset` instead of running it.

Direct invocation is Dagster's documented unit-testing path, and it costs nothing: no run, no IO manager, no instance. `MaterializeResult` carries the frame on `value`, so a call hands back the validated rows and every check outcome as ordinary Python objects.

Both shapes here are asserted twice, once by calling and once through `dg.materialize`. The property worth having is not that a call yields a key, but that it yields *the same* key a run writes under.

A call reaches no IO manager, so a quarantined asset falls back to the file writer under `DAGSTER_DATAFRAMELY_QUARANTINE_DIR` (ADR-0006). That is what makes the held-back rows assertable here at all: a run puts them wherever the manager puts things, and a call puts them somewhere the test named.
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dagster_dataframely import dy_asset
from dagster_dataframely.errors import ColumnSchemaError, QuarantineDirError
from tests.scenario import (
    Orders,
    clean_orders,
    cooccurring_orders,
    mixed_orders,
    storage,
    wrong_dtype_orders,
)

_DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])
_Yielded = list[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]


def _orders(frame: Callable[[], pl.DataFrame], **settings: Any) -> dg.AssetsDefinition:
    """The same decorated function under whichever declaration a shape asks for."""

    @dy_asset(Orders, name="orders", **settings)
    def orders() -> pl.DataFrame:
        return frame()

    return orders


def _called(asset: dg.AssetsDefinition, *, quarantine: bool) -> _Yielded:
    """Call the asset, supplying the context a quarantined one declares.

    `quarantine=True` adds a `context` parameter whether or not the decorated function asked for one, because the writer is built from it and nothing else can reach it.
    """
    return _events(asset, *((dg.build_asset_context(),) if quarantine else ()))


# The two failure policies, each with the frame that reaches its middle exit.
# `cooccurring_orders` rather than `mixed_orders` for the quarantined shape: it splits 3 valid against 1 invalid, where `mixed_orders` splits 3 against 3, and an even split leaves the rows that were written and the rows that were held back indistinguishable by count.
_SHAPES = [
    pytest.param(clean_orders, False, id="no quarantine"),
    pytest.param(cooccurring_orders, True, id="quarantine"),
]


@pytest.fixture
def quarantine_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point `file_writer` at a directory this test owns.

    Set before the asset is declared, because the decorator resolves every setting where the asset is declared rather than where it runs.
    """
    directory = tmp_path / "quarantine"
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_QUARANTINE_DIR", str(directory))
    return directory


def _events(asset: dg.AssetsDefinition, *args: object) -> _Yielded:
    """Call the asset and drain what comes back.

    `AssetsDefinition.__call__` is annotated `-> object` upstream, because a direct call hands back whatever the body returns. Here it is always the wrapper's generator, which is what the ignore asserts and what `tests/test_upstream_characterization.py` pins.
    """
    return list(asset(*args))  # pyrefly: ignore[bad-argument-type]


def _tables(events: _Yielded) -> dict[dg.AssetKey, pl.DataFrame]:
    """The frame each output produced, keyed by asset."""
    return {
        event.asset_key: event.value
        for event in events
        if isinstance(event, dg.MaterializeResult) and event.asset_key is not None
    }


def _checks(events: _Yielded) -> dict[dg.AssetCheckKey, bool]:
    """Every check outcome, keyed the way Dagster keys a check."""
    return {
        dg.AssetCheckKey(event.asset_key, event.check_name): event.passed
        for event in events
        if isinstance(event, dg.AssetCheckResult)
        if event.asset_key is not None and event.check_name is not None
    }


def _run(tmp_path: Path, asset: dg.AssetsDefinition) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [asset],
        resources=storage(tmp_path),
    )


@pytest.mark.parametrize(("frame", "quarantine"), _SHAPES)
def test_calling_the_asset_hands_back_its_frame(
    quarantine_dir: Path, frame: Callable[[], pl.DataFrame], quarantine: bool
):
    """No run, no IO manager, no instance: the validated frame comes off `MaterializeResult.value`."""
    asset = _orders(frame, quarantine=quarantine)

    tables = _tables(_called(asset, quarantine=quarantine))

    assert tables
    assert_frame_equal(next(iter(tables.values())), clean_orders())


@pytest.mark.parametrize(("frame", "quarantine"), _SHAPES)
def test_calling_the_asset_reports_every_check_it_declares(
    quarantine_dir: Path, frame: Callable[[], pl.DataFrame], quarantine: bool
):
    """Standalone results, one per spec. Bundling them onto the materialization is what direct invocation refuses."""
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
    """The property the whole change rests on. Resolving the key at definition time is only safe while it agrees with what Dagster derives at run time.

    Compared by row count as well as by key, which is why the frames above split unevenly: a call that reported the held-back rows as the written ones would still yield the right key.
    """
    asset = _orders(frame, quarantine=quarantine)
    result = _run(tmp_path, asset)

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
    """Same names, same outcomes, same asset. A check whose key differed between the two would report against a spec no run declared."""
    asset = _orders(frame, quarantine=quarantine)
    result = _run(tmp_path, asset)

    assert _checks(_called(asset, quarantine=quarantine)) == {
        evaluation.asset_check_key: evaluation.passed
        for evaluation in result.get_asset_check_evaluations()
    }


def test_a_column_schema_drift_raises_out_of_the_call():
    """The error a user is meant to read, rather than a step failure they have to open a run to find."""
    asset = _orders(wrong_dtype_orders)

    with pytest.raises(ColumnSchemaError) as raised:
        _events(asset)

    assert "Column 'quantity' (expected Int32, got Int64)" in str(raised.value)


def test_a_called_quarantine_writes_a_real_file_under_the_configured_root(
    quarantine_dir: Path,
):
    """The middle exit, reached by calling: three rows handed back and three on disk where the setting says.

    A test that wants the placement a run would give runs the asset. This is the other half: the rows a call held back, in a file the test named, so a unit test can open them.
    """
    asset = _orders(mixed_orders, quarantine=True)

    tables = _tables(_called(asset, quarantine=True))
    written = pl.read_parquet(quarantine_dir / "orders_quarantine.parquet")

    assert set(tables) == {dg.AssetKey(["orders"])}
    assert_frame_equal(tables[dg.AssetKey(["orders"])], clean_orders())
    assert written.height == 3
    assert "dy_rule__amount__min" in written.columns


def test_a_called_quarantine_with_no_root_says_so_rather_than_choosing_one():
    """The rows are evidence, and writing them somewhere nobody named is how evidence gets lost."""
    asset = _orders(mixed_orders, quarantine=True)

    with pytest.raises(QuarantineDirError) as raised:
        _called(asset, quarantine=True)

    assert "DAGSTER_DATAFRAMELY_QUARANTINE_DIR" in str(raised.value)
    assert "orders" in str(raised.value)


def test_a_called_partitioned_quarantine_lands_under_its_partition(
    quarantine_dir: Path,
):
    """`build_asset_context(partition_key=...)` is the whole of what the fallback reads off the context, and one file per partition is what a backfill of one partition rewrites."""

    @dy_asset(Orders, name="orders", quarantine=True, partitions_def=_DAYS)
    def orders() -> pl.DataFrame:
        return mixed_orders()

    _events(orders, dg.build_asset_context(partition_key="2026-01-02"))

    assert (quarantine_dir / "orders_quarantine" / "2026-01-02.parquet").exists()


def test_a_decorated_function_taking_context_reads_its_partition_key_from_a_built_one():
    """`dg.build_asset_context` is what makes a partitioned asset testable by calling it. It cannot set the ContextVar `AssetExecutionContext.get()` reads, which is why the wrapper no longer reads one."""
    seen: dict[str, str] = {}

    @dy_asset(Orders, name="orders", partitions_def=_DAYS)
    def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
        seen["partition"] = context.partition_key
        return clean_orders()

    tables = _tables(
        _events(orders, dg.build_asset_context(partition_key="2026-01-02"))
    )

    assert seen == {"partition": "2026-01-02"}
    assert set(tables) == {dg.AssetKey(["orders"])}


def test_a_decorated_function_taking_context_alongside_an_input_is_invocable_too():
    """The context comes first and the frames follow, exactly as Dagster orders them."""

    @dy_asset(Orders, name="orders")
    def orders(
        context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
    ) -> pl.DataFrame:
        assert isinstance(context, dg.AssetExecutionContext)
        return raw_orders

    tables = _tables(_events(orders, dg.build_asset_context(), clean_orders()))

    assert_frame_equal(tables[dg.AssetKey(["orders"])], clean_orders())
