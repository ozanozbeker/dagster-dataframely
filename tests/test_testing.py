"""These tests call an asset built with `dd.asset` and pass what it returns to `dd.testing.DirectInvocationResult.from_invocation`.

`TestNotACall` passes other arguments, and `from_invocation` raises `TypeError` on each.
"""

from pathlib import Path
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import dagster_dataframely as dd
from tests.scenario import OrdersSchema, clean_orders, mixed_orders


class AmountsSchema(dy.Schema):
    """One `"warn"` rule, so a failing row is still written to the table."""

    amount = dy.Int64(
        min=0, metadata={"dagster_dataframely": {"severity": {"min": "warn"}}}
    )


class InvoicesSchema(dy.Schema):
    invoice_id = dy.Int64(primary_key=True)


class InvoiceLinesSchema(dy.Schema):
    invoice_id = dy.Int64(primary_key=True)
    line = dy.Int64(primary_key=True)


class InvoiceBook(dy.Collection):
    """One collection filter, which has a check on each of the two members."""

    invoices: dy.LazyFrame[InvoicesSchema]
    lines: dy.LazyFrame[InvoiceLinesSchema]

    @dy.filter()
    def invoice_has_lines(self) -> pl.LazyFrame:
        return self.invoices.join(self.lines, on="invoice_id", how="semi")


@pytest.fixture
def error_rows_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR", str(tmp_path))
    return tmp_path


def _orders(frame: pl.DataFrame, **kwargs: Any) -> dg.AssetsDefinition:
    """Return the `orders` asset that `dd.asset` builds with the given keyword arguments."""

    @dd.asset(OrdersSchema, name="orders", **kwargs)
    def orders() -> pl.DataFrame:
        return frame

    return orders


class TestDirectInvocationResult:
    """The record `DirectInvocationResult.from_invocation` returns for a call of an asset built with `dd.asset`."""

    def test_a_clean_call_has_the_frame_and_every_check_passing(self):
        asset = _orders(clean_orders())

        result = dd.testing.DirectInvocationResult.from_invocation(asset())

        assert result.materialization is not None
        assert_frame_equal(result.materialization.value, clean_orders())
        assert set(result.checks) == {spec.name for spec in asset.check_specs}
        assert all(check.passed for check in result.checks.values())

    @pytest.mark.usefixtures("error_rows_dir")
    def test_a_filtering_call_has_the_valid_rows_and_the_failing_check(self):
        asset = _orders(mixed_orders(), filter=True)

        result = dd.testing.DirectInvocationResult.from_invocation(
            asset(dg.build_asset_context())
        )

        assert result.materialization is not None
        assert_frame_equal(result.materialization.value, clean_orders())
        failed = result.checks["dy_rule__amount__min"]
        assert not failed.passed
        assert "dataframely/failed_examples" in failed.metadata

    def test_a_warn_only_failure_keeps_every_row_and_fails_at_warn(self):
        @dd.asset(AmountsSchema)
        def amounts() -> pl.DataFrame:
            return pl.DataFrame({"amount": [1, -1]})

        result = dd.testing.DirectInvocationResult.from_invocation(amounts())

        assert result.materialization is not None
        assert result.materialization.value.height == 2
        failed = result.checks["dy_rule__amount__min"]
        assert not failed.passed
        assert failed.severity == dg.AssetCheckSeverity.WARN

    def test_a_skip_has_no_materialization_and_every_check_passing(self):
        @dd.asset(OrdersSchema)
        def orders() -> pl.DataFrame | None:
            return None

        result = dd.testing.DirectInvocationResult.from_invocation(orders())

        assert result.materialization is None
        assert set(result.checks) == {spec.name for spec in orders.check_specs}
        assert all(check.passed for check in result.checks.values())

    def test_an_abort_raises_out_of_the_helper(self):
        """`from_invocation` raises `ValidationAbortError` while it reads the events, and calling the asset does not raise."""
        asset = _orders(mixed_orders())

        with pytest.raises(dd.errors.ValidationAbortError):
            dd.testing.DirectInvocationResult.from_invocation(asset())


class TestNotACall:
    """An argument that is not what calling an asset built with `dd.asset` returns."""

    def test_an_uncalled_asset_raises(self):
        with pytest.raises(
            TypeError, match="type `AssetsDefinition` that is not iterable"
        ):
            dd.testing.DirectInvocationResult.from_invocation(_orders(clean_orders()))

    def test_a_plain_asset_returning_a_frame_raises(self):
        """`from_invocation` raises on a plain asset's frame instead of reading it as a skip."""

        @dg.asset
        def plain() -> pl.DataFrame:
            return clean_orders()

        with pytest.raises(
            TypeError, match="type `DataFrame` that yielded a value of type `Series`"
        ):
            dd.testing.DirectInvocationResult.from_invocation(plain())

    def test_a_plain_asset_returning_a_materialize_result_raises(self):
        @dg.asset
        def plain() -> dg.MaterializeResult[pl.DataFrame]:
            return dg.MaterializeResult(value=clean_orders())

        with pytest.raises(TypeError, match="type `MaterializeResult` that yielded"):
            dd.testing.DirectInvocationResult.from_invocation(plain())

    def test_a_second_materialization_raises(self):
        events = list(_orders(clean_orders())())  # pyrefly: ignore[bad-argument-type]

        with pytest.raises(TypeError, match="a second `MaterializeResult`"):
            dd.testing.DirectInvocationResult.from_invocation([*events, events[0]])

    def test_a_collection_checks_call_raises_on_the_repeated_check_name(self):
        checks = dd.build_collection_checks(
            InvoiceBook,
            members={
                "invoices": dg.AssetSpec("invoices"),
                "lines": dg.AssetSpec("lines"),
            },
        )
        called = checks(
            dg.build_asset_check_context(),
            invoices=pl.LazyFrame({"invoice_id": [1, 2]}),
            lines=pl.LazyFrame({"invoice_id": [1], "line": [1]}),
        )

        with pytest.raises(TypeError, match="'dy_filter__invoice_has_lines' twice"):
            dd.testing.DirectInvocationResult.from_invocation(called)
