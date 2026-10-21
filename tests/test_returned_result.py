"""`stream_results` receives a `dg.MaterializeResult` in place of a frame.

It merges the result's metadata, data version and tags into the asset's materialization. A run turns the data version and tags into event tags, so these tests assert them on a direct call and in a run of `dd.asset`.
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
from dagster_dataframely.wiring import stream_results
from tests.scenario import (
    OrdersSchema,
    clean_orders,
    cooccurring_orders,
    materializations,
    materialize,
    mixed_orders,
    no_valid_orders,
    results,
)

_ORDERS = dg.AssetKey(["orders"])
_ERROR_ROWS_KEY = dg.AssetKey(["orders__error_rows"])


def _stream(
    returned: object,
) -> list[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]:
    """Return what `stream_results` yields for `returned` as the `orders` asset."""
    return list(
        stream_results(OrdersSchema, returned, asset_key=_ORDERS)  # pyrefly: ignore[bad-argument-type]
    )


def _materialization(returned: object) -> dg.MaterializeResult[pl.DataFrame]:
    """Return the materialization `stream_results` yields for `returned`."""
    return results(_stream(returned))[_ORDERS]


class TestMaterialization:
    """The materialization `stream_results` builds from a returned `dg.MaterializeResult` or a bare frame."""

    def test_the_returned_metadata_is_merged_into_the_materialization(self):
        returned = dg.MaterializeResult(
            value=clean_orders(), metadata={"source": "stripe", "batch": 7}
        )

        metadata = _materialization(returned).metadata or {}

        assert metadata["source"] == "stripe"
        assert metadata["batch"] == 7

    def test_a_run_records_the_returned_metadata_beside_the_packages_own(
        self, tmp_path: Path
    ):
        @dd.asset(OrdersSchema, name="orders")
        def orders() -> dg.MaterializeResult[pl.DataFrame]:
            return dg.MaterializeResult(
                value=clean_orders(), metadata={"source": "stripe"}
            )

        metadata = materializations(materialize(tmp_path, orders))[_ORDERS].metadata

        assert metadata["source"].value == "stripe"
        assert metadata["dagster/row_count"].value == 3

    def test_the_packages_own_key_takes_precedence(self):
        returned = dg.MaterializeResult(
            value=clean_orders(), metadata={"dagster/row_count": 999}
        )

        # A call, not a run: in a run the IO manager writes `dagster/row_count` last.
        called = _materialization(returned).metadata or {}

        assert called["dagster/row_count"] == 3

    def test_a_call_yields_the_returned_data_version_and_tags(self):
        returned = dg.MaterializeResult(
            value=clean_orders(),
            data_version=dg.DataVersion("v1"),
            tags={"run/flavour": "backfill"},
        )

        result = _materialization(returned)

        assert result.data_version == dg.DataVersion("v1")
        assert result.tags == {"run/flavour": "backfill"}

    def test_a_run_turns_the_returned_data_version_and_tags_into_event_tags(
        self, tmp_path: Path
    ):
        @dd.asset(OrdersSchema, name="orders")
        def orders() -> dg.MaterializeResult[pl.DataFrame]:
            return dg.MaterializeResult(
                value=clean_orders(),
                data_version=dg.DataVersion("v1"),
                tags={"run/flavour": "backfill"},
            )

        tags = materializations(materialize(tmp_path, orders))[_ORDERS].tags or {}

        assert tags["dagster/data_version"] == "v1"
        assert tags["run/flavour"] == "backfill"

    def test_a_lazy_return_folds_the_same_way(self):
        """`stream_results` merges a result with a `pl.LazyFrame` value like one with a `pl.DataFrame`."""
        returned = dg.MaterializeResult(value=clean_orders().lazy(), metadata={"m": 1})

        result = _materialization(returned)

        assert (result.metadata or {})["m"] == 1
        assert_frame_equal(result.value, clean_orders())

    def test_with_error_rows_the_result_is_merged_into_the_tables_materialization_only(
        self,
        tmp_path: Path,
    ):
        @dd.asset(OrdersSchema, name="orders", filter=True)
        def orders() -> dg.MaterializeResult[pl.DataFrame]:
            return dg.MaterializeResult(
                value=cooccurring_orders(),
                metadata={"source": "stripe", "dataframely/error_rows_count": 99},
                data_version=dg.DataVersion("v1"),
                tags={"run/flavour": "backfill"},
            )

        recorded = materializations(materialize(tmp_path, orders))
        event = recorded[_ORDERS]
        tags = event.tags or {}

        assert event.metadata["source"] == dg.MetadataValue.text("stripe")
        assert event.metadata["dataframely/error_rows_count"] == dg.MetadataValue.int(1)
        assert tags["dagster/data_version"] == "v1"
        assert tags["run/flavour"] == "backfill"
        assert "source" not in recorded[_ERROR_ROWS_KEY].metadata
        assert "run/flavour" not in (recorded[_ERROR_ROWS_KEY].tags or {})

    def test_a_bare_frame_has_no_data_version_and_no_tags(self):
        result = _materialization(clean_orders())

        assert result.data_version is None
        assert result.tags is None

    def test_a_bare_frame_and_a_returned_result_match_on_everything_else(
        self, tmp_path: Path
    ):
        @dd.asset(OrdersSchema, name="orders")
        def bare() -> pl.DataFrame:
            return clean_orders()

        @dd.asset(OrdersSchema, name="orders")
        def returning_a_result() -> dg.MaterializeResult[pl.DataFrame]:
            return dg.MaterializeResult(value=clean_orders())

        from_bare = materializations(materialize(tmp_path / "bare", bare))[
            _ORDERS
        ].metadata
        from_result = materializations(
            materialize(tmp_path / "result", returning_a_result)
        )[_ORDERS].metadata

        assert set(from_bare) == set(from_result)
        assert_frame_equal(
            pl.read_parquet(tmp_path / "bare" / "orders.parquet"),
            pl.read_parquet(tmp_path / "result" / "orders.parquet"),
        )


# Asserted on a direct call and in a run, so a run never writes a table that a call rejects.
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
        # `value=None` is not a skip, because a skip has no materialization for the metadata (#95).
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
    """Declare `fn` as the `orders` asset, because the `'orders' returned a` messages above contain its name."""
    return dd.asset(OrdersSchema, name="orders")(fn)  # pyrefly: ignore[bad-argument-type]


class TestErrors:
    """The errors `stream_results` raises for a rejected return, and for a returned result whose rows fail."""

    @pytest.mark.parametrize(("fn", "error", "message"), _REJECTED_RETURNS)
    def test_a_rejected_return_names_what_is_wrong(
        self, fn: Callable[[], object], error: type[Exception], message: str
    ):
        with pytest.raises(error, match=re.escape(message)):
            _stream(fn())

    @pytest.mark.parametrize(("fn", "error", "message"), _REJECTED_RETURNS)
    def test_a_rejected_return_fails_the_run_and_writes_nothing(
        self,
        tmp_path: Path,
        fn: Callable[[], object],
        error: type[Exception],
        message: str,
    ):
        with pytest.raises(error, match=re.escape(message)):
            materialize(tmp_path, _orders_asset(fn))

        assert not list(tmp_path.rglob("*.parquet"))

    def test_a_run_that_writes_no_table_still_raises_its_own_error(
        self, tmp_path: Path
    ):
        """With no valid rows, the returned result has no materialization to merge into, and `NoValidRowsError` propagates."""

        @dd.asset(OrdersSchema, name="orders", filter=True)
        def orders() -> dg.MaterializeResult[pl.DataFrame]:
            return dg.MaterializeResult(
                value=no_valid_orders(), metadata={"source": "stripe"}
            )

        with pytest.raises(NoValidRowsError):
            materialize(tmp_path, orders)

    def test_an_abort_with_no_error_rows_still_raises_its_own_error(self):
        """With no writer, `stream_results` raises `ValidationAbortError` before it builds a materialization to merge into."""
        returned = dg.MaterializeResult(
            value=mixed_orders(), metadata={"source": "stripe"}
        )

        with pytest.raises(ValidationAbortError):
            _stream(returned)

    def test_the_frame_guard_names_every_route_out(self):
        """The error for a return that is not a frame names every accepted return and the hand-wired alternative."""
        with pytest.raises(dg.DagsterInvariantViolationError) as raised:
            _stream("orders")
        message = str(raised.value)

        assert "Polars DataFrame or LazyFrame" in message
        assert "dg.MaterializeResult" in message
        assert "`None` to skip" in message
        assert "plain `@dg.asset`" in message
        assert "get_column_schema_metadata" in message
