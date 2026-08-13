"""Calling a `@dataframely_asset` instead of running it.

Direct invocation is Dagster's documented unit-testing path, and it costs nothing: no run, no IO manager, no instance. `MaterializeResult` carries the frame on `value`, so a call hands back the validated rows, the quarantined rows, and every check outcome as ordinary Python objects.

Every shape here is asserted twice, once by calling and once through `dg.materialize`, because the property worth having is not that a call yields keys but that it yields *the same* keys a run writes under. The four shapes are the four ways the quarantine's key can be decided: absent, derived, derived under the quarantine's own prefix, and named outright.
"""

from collections.abc import Callable
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dagster_dataframely import DataframelyParquetIOManager, dataframely_asset
from dagster_dataframely.errors import SchemaShapeError
from tests.scenario import (
    Orders,
    clean_orders,
    cooccurring_orders,
    mixed_orders,
    wrong_dtype_orders,
)

_DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])
_Yielded = list[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]


def _orders(frame: Callable[[], pl.DataFrame], **settings: Any) -> dg.AssetsDefinition:
    """The same transform under whichever declaration a shape asks for."""

    @dataframely_asset(schema=Orders, name="orders", **settings)
    def orders() -> pl.DataFrame:
        return frame()

    return orders


# The four ways a quarantine key is decided, each with the frame that reaches both outs.
# `cooccurring_orders` rather than `mixed_orders` for the three that have a quarantine: it splits 3 valid against 1 invalid, where `mixed_orders` splits 3 against 3, and an even split leaves the two outs indistinguishable by row count.
_SHAPES = [
    pytest.param(clean_orders, {}, id="no quarantine"),
    pytest.param(cooccurring_orders, {"quarantine": dg.AssetOut()}, id="derived key"),
    pytest.param(
        cooccurring_orders,
        {"quarantine": dg.AssetOut(key_prefix="vault")},
        id="quarantine names a prefix",
    ),
    pytest.param(
        cooccurring_orders,
        {
            "key_prefix": "sales",
            "quarantine": dg.AssetOut(key=dg.AssetKey(["restricted", "orders"])),
        },
        id="quarantine names a key",
    ),
]


def _events(asset: dg.AssetsDefinition, *args: object) -> _Yielded:
    """Calls the asset and drains what comes back.

    `AssetsDefinition.__call__` is annotated `-> object` upstream, because a direct call hands back whatever the body returns. Here it is always the wrapper's generator, which is what the ignore asserts and what `tests/test_upstream_characterization.py` pins.
    """
    return list(asset(*args))  # pyrefly: ignore[bad-argument-type]


def _tables(events: _Yielded) -> dict[dg.AssetKey, pl.DataFrame]:
    """The frame each out produced, keyed by asset."""
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
        resources={"io_manager": DataframelyParquetIOManager(base_dir=str(tmp_path))},
    )


@pytest.mark.parametrize(("frame", "settings"), _SHAPES)
def test_calling_the_asset_hands_back_its_frames(
    frame: Callable[[], pl.DataFrame], settings: dict[str, Any]
):
    """No run, no IO manager, no instance: the validated frame comes off `MaterializeResult.value`."""
    tables = _tables(_events(_orders(frame, **settings)))

    assert tables
    assert_frame_equal(next(iter(tables.values())), clean_orders())


@pytest.mark.parametrize(("frame", "settings"), _SHAPES)
def test_calling_the_asset_reports_every_check_it_declares(
    frame: Callable[[], pl.DataFrame], settings: dict[str, Any]
):
    """Standalone results, one per spec. Bundling them onto the materialization is what direct invocation refuses."""
    asset = _orders(frame, **settings)
    checks = _checks(_events(asset))

    assert set(checks) == {spec.key for spec in asset.check_specs}


@pytest.mark.parametrize(("frame", "settings"), _SHAPES)
def test_the_keys_a_call_yields_are_the_keys_a_run_writes_under(
    tmp_path: Path, frame: Callable[[], pl.DataFrame], settings: dict[str, Any]
):
    """The property the whole change rests on. Resolving the keys at definition time is only safe while it agrees with what Dagster derives at run time, for every shape the quarantine's key can take.

    Compared by row count per key rather than by the set of keys, which is why the frames above split unevenly: a call that swapped the two outs would still yield the right pair of keys.
    """
    asset = _orders(frame, **settings)
    result = _run(tmp_path, asset)

    assert result.success
    assert {key: len(table) for key, table in _tables(_events(asset)).items()} == {
        event.asset_key: event.step_materialization_data.materialization.metadata[
            "dagster/row_count"
        ].value
        for event in result.get_asset_materialization_events()
    }


@pytest.mark.parametrize(("frame", "settings"), _SHAPES)
def test_the_checks_a_call_yields_are_the_checks_a_run_evaluates(
    tmp_path: Path, frame: Callable[[], pl.DataFrame], settings: dict[str, Any]
):
    """Same names, same outcomes, same asset. A check whose key differed between the two would report against a spec no run declared."""
    asset = _orders(frame, **settings)
    result = _run(tmp_path, asset)

    assert _checks(_events(asset)) == {
        evaluation.asset_check_key: evaluation.passed
        for evaluation in result.get_asset_check_evaluations()
    }


def test_a_shape_drift_raises_out_of_the_call():
    """The error a user is meant to read, rather than a step failure they have to open a run to find."""
    asset = _orders(wrong_dtype_orders)

    with pytest.raises(SchemaShapeError) as raised:
        _events(asset)

    assert "Column 'quantity' (expected Int32, got Int64)" in str(raised.value)


def test_the_quarantined_rows_come_back_on_the_second_result():
    """The middle exit, reached by calling: three rows written and three inspectable, with no storage anywhere."""
    asset = _orders(mixed_orders, quarantine=dg.AssetOut())
    tables = _tables(_events(asset))

    assert set(tables) == {
        dg.AssetKey(["orders"]),
        dg.AssetKey(["orders_quarantine"]),
    }
    assert_frame_equal(tables[dg.AssetKey(["orders"])], clean_orders())
    assert tables[dg.AssetKey(["orders_quarantine"])].height == 3


def test_a_transform_taking_context_reads_its_partition_key_from_a_built_one():
    """`dg.build_asset_context` is what makes a partitioned transform testable by calling it. It cannot set the ContextVar `AssetExecutionContext.get()` reads, which is why the wrapper no longer reads one."""
    seen: dict[str, str] = {}

    @dataframely_asset(schema=Orders, name="orders", partitions_def=_DAYS)
    def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
        seen["partition"] = context.partition_key
        return clean_orders()

    tables = _tables(
        _events(orders, dg.build_asset_context(partition_key="2026-01-02"))
    )

    assert seen == {"partition": "2026-01-02"}
    assert set(tables) == {dg.AssetKey(["orders"])}


def test_a_transform_taking_context_alongside_an_input_is_invocable_too():
    """The context comes first and the frames follow, exactly as Dagster orders them."""

    @dataframely_asset(schema=Orders, name="orders")
    def orders(
        context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
    ) -> pl.DataFrame:
        assert isinstance(context, dg.AssetExecutionContext)
        return raw_orders

    tables = _tables(_events(orders, dg.build_asset_context(), clean_orders()))

    assert_frame_equal(tables[dg.AssetKey(["orders"])], clean_orders())
