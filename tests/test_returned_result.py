"""A decorated function that returns a `dg.MaterializeResult` instead of a frame.

`dd.asset` merges the result's metadata, data version and tags into the asset's materialization. A run turns the data version and tags into event tags, so these tests assert them on a call and in a run.
"""

import re
from collections.abc import Callable
from pathlib import Path

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import dagster_dataframely as dd
from dagster_dataframely.errors import (
    MaterializeResultFieldError,
    MaterializeResultValueError,
    NoValidRowsError,
    ValidationAbortError,
)
from tests.scenario import (
    Orders,
    clean_orders,
    cooccurring_orders,
    events,
    materializations,
    materialize,
    mixed_orders,
    no_valid_orders,
    results,
)

_VALID = dg.AssetKey(["orders"])


def test_the_returned_metadata_is_merged_into_the_materialization():
    @dd.asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=clean_orders(), metadata={"source": "stripe", "batch": 7}
        )

    metadata = results(events(orders))[_VALID].metadata or {}

    assert metadata["source"] == "stripe"
    assert metadata["batch"] == 7


def test_a_run_records_the_returned_metadata_beside_the_packages_own(tmp_path: Path):
    @dd.asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(value=clean_orders(), metadata={"source": "stripe"})

    metadata = materializations(materialize(tmp_path, orders))[_VALID].metadata

    assert metadata["source"].value == "stripe"
    assert metadata["dagster/row_count"].value == 3


def test_the_packages_own_key_takes_precedence():
    @dd.asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=clean_orders(), metadata={"dagster/row_count": 999}
        )

    # A call, not a run: in a run the IO manager writes `dagster/row_count` last.
    called = results(events(orders))[_VALID].metadata or {}

    assert called["dagster/row_count"] == 3


def test_a_call_yields_the_returned_data_version_and_tags():
    @dd.asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=clean_orders(),
            data_version=dg.DataVersion("v1"),
            tags={"run/flavour": "backfill"},
        )

    result = results(events(orders))[_VALID]

    assert result.data_version == dg.DataVersion("v1")
    assert result.tags == {"run/flavour": "backfill"}


def test_a_run_turns_the_returned_data_version_and_tags_into_event_tags(tmp_path: Path):
    @dd.asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=clean_orders(),
            data_version=dg.DataVersion("v1"),
            tags={"run/flavour": "backfill"},
        )

    tags = materializations(materialize(tmp_path, orders))[_VALID].tags or {}

    assert tags["dagster/data_version"] == "v1"
    assert tags["run/flavour"] == "backfill"


def test_a_lazy_return_folds_the_same_way():
    """`dd.asset` merges a result with a `pl.LazyFrame` value like one with a `pl.DataFrame`."""

    @dd.asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.LazyFrame]:
        return dg.MaterializeResult(value=clean_orders().lazy(), metadata={"m": 1})

    result = results(events(orders))[_VALID]

    assert (result.metadata or {})["m"] == 1
    assert_frame_equal(result.value, clean_orders())


def test_with_quarantine_the_result_is_merged_into_the_one_materialization(
    tmp_path: Path,
):
    """The run writes the quarantine and does not materialize it, so the asset's materialization is the only one."""

    @dd.asset(Orders, name="orders", quarantine=True)
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=cooccurring_orders(),
            metadata={"source": "stripe", "dataframely/invalid_count": 99},
            data_version=dg.DataVersion("v1"),
            tags={"run/flavour": "backfill"},
        )

    recorded = materializations(materialize(tmp_path, orders))
    (event,) = recorded.values()
    tags = event.tags or {}

    assert set(recorded) == {_VALID}
    assert event.metadata["source"] == dg.MetadataValue.text("stripe")
    assert event.metadata["dataframely/invalid_count"] == dg.MetadataValue.int(1)
    assert tags["dagster/data_version"] == "v1"
    assert tags["run/flavour"] == "backfill"


# Asserted on a call and in a run, so a run never writes a table that a call rejects.
_REJECTED_RETURNS = [
    pytest.param(
        lambda: dg.MaterializeResult(metadata={"source": "stripe"}),
        MaterializeResultValueError,
        "context.add_asset_metadata",
        id="no value",
    ),
    pytest.param(
        lambda: dg.MaterializeResult(value=[1, 2, 3]),
        MaterializeResultValueError,
        "has no frame",
        id="a value that is not a frame",
    ),
    pytest.param(
        # Not a skip: a skipped run has no materialization for the metadata (#95).
        lambda: dg.MaterializeResult(value=None, metadata={"delivered": False}),
        MaterializeResultValueError,
        "has no frame",
        id="a value of None",
    ),
    pytest.param(
        lambda: dg.MaterializeResult(
            asset_key=dg.AssetKey(["elsewhere"]), value=clean_orders()
        ),
        MaterializeResultFieldError,
        "`asset_key`",
        id="asset_key",
    ),
    pytest.param(
        lambda: dg.MaterializeResult(
            value=clean_orders(),
            check_results=[dg.AssetCheckResult(check_name="mine", passed=True)],
        ),
        MaterializeResultFieldError,
        "`check_results`",
        id="check_results",
    ),
    pytest.param(
        lambda: dg.Output(clean_orders(), metadata={"source": "stripe"}),
        dg.DagsterInvariantViolationError,
        "'orders' returned a Output",
        id="dg.Output",
    ),
    pytest.param(
        # Not `None`, which is a skip (#95).
        lambda: "orders",
        dg.DagsterInvariantViolationError,
        "'orders' returned a str",
        id="something that is not a frame",
    ),
]


def _orders_asset(fn: Callable[[], object]) -> dg.AssetsDefinition:
    """Declare `fn` as the `orders` asset, the name every message above expects."""
    return dd.asset(Orders, name="orders")(fn)  # pyrefly: ignore[bad-argument-type]


@pytest.mark.parametrize(("fn", "error", "message"), _REJECTED_RETURNS)
def test_a_rejected_return_names_what_is_wrong(
    fn: Callable[[], object], error: type[Exception], message: str
):
    with pytest.raises(error, match=re.escape(message)):
        events(_orders_asset(fn))


@pytest.mark.parametrize(("fn", "error", "message"), _REJECTED_RETURNS)
def test_a_rejected_return_fails_the_run_and_writes_nothing(
    tmp_path: Path,
    fn: Callable[[], object],
    error: type[Exception],
    message: str,
):
    with pytest.raises(error, match=re.escape(message)):
        materialize(tmp_path, _orders_asset(fn))

    assert not list(tmp_path.rglob("*.parquet"))


def test_a_run_that_writes_no_table_still_raises_its_own_error(tmp_path: Path):
    """With no valid rows, `with_returned_fields` receives no materialization and lets `NoValidRowsError` propagate."""

    @dd.asset(Orders, name="orders", quarantine=True)
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=no_valid_orders(), metadata={"source": "stripe"}
        )

    with pytest.raises(NoValidRowsError):
        materialize(tmp_path, orders)


def test_an_abort_with_no_quarantine_still_raises_its_own_error():
    """With no quarantine, failing rows produce no materialization, and `with_returned_fields` lets `ValidationAbortError` propagate."""

    @dd.asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(value=mixed_orders(), metadata={"source": "stripe"})

    with pytest.raises(ValidationAbortError):
        events(orders)


def test_the_frame_guard_names_every_route_out():
    """The error for a return that is not a frame names every accepted return and the hand-wired alternative."""
    with pytest.raises(dg.DagsterInvariantViolationError) as raised:
        events(_orders_asset(lambda: "orders"))
    message = str(raised.value)

    assert "Polars DataFrame or LazyFrame" in message
    assert "dg.MaterializeResult" in message
    assert "`None` to skip" in message
    assert "plain `@dg.asset`" in message
    assert "schema_metadata" in message


def test_a_bare_frame_has_no_data_version_and_no_tags():
    @dd.asset(Orders, name="orders")
    def orders() -> pl.DataFrame:
        return clean_orders()

    result = results(events(orders))[_VALID]

    assert result.data_version is None
    assert result.tags is None


def test_a_bare_frame_and_a_returned_result_match_on_everything_else(tmp_path: Path):
    @dd.asset(Orders, name="orders")
    def bare() -> pl.DataFrame:
        return clean_orders()

    @dd.asset(Orders, name="orders")
    def returning_a_result() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(value=clean_orders())

    from_bare = materializations(materialize(tmp_path / "bare", bare))[_VALID].metadata
    from_result = materializations(
        materialize(tmp_path / "result", returning_a_result)
    )[_VALID].metadata

    assert set(from_bare) == set(from_result)
    assert_frame_equal(
        pl.read_parquet(tmp_path / "bare" / "orders.parquet"),
        pl.read_parquet(tmp_path / "result" / "orders.parquet"),
    )
