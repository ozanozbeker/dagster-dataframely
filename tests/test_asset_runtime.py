"""Runtime behaviour of `dd.asset`, asserted on a run's events, check evaluations, metadata and written files.

`TestOutcomeSelection` calls `stream_results` directly, with the scenario frames and with `None`, to assert what it yields, writes and raises without a run.
"""

from collections.abc import Callable, Iterator, Mapping
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import dagster_dataframely as dd
from dagster_dataframely._settings import Granularity
from dagster_dataframely.errors import (
    ColumnSchemaError,
    DagsterDataframelyError,
    NoValidRowsError,
    ValidationAbortError,
)
from dagster_dataframely.wiring import (
    build_asset_check_specs,
    evaluate_checks,
    get_column_schema_metadata,
    get_table_schema,
    stream_results,
)
from tests.scenario import (
    OrdersSchema,
    Yielded,
    check_evaluations,
    clean_orders,
    cooccurring_orders,
    materializations,
    materialize,
    mixed_orders,
    no_valid_orders,
    results,
    wrong_dtype_orders,
)


@dg.asset(name="raw_orders")
def _raw_orders() -> pl.DataFrame:
    return clean_orders()


@dd.asset(OrdersSchema)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return raw_orders


class TestCleanFrame:
    """A run whose frame is valid."""

    def test_a_clean_frame_materializes_what_was_returned(self, tmp_path: Path):
        result = materialize(tmp_path, _raw_orders, orders)

        assert result.success
        assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())

    def test_a_clean_run_emits_row_count(self):
        # Called directly, because in a run `dagster-polars` writes the same key.
        materialization = dd.testing.DirectInvocationResult.from_invocation(
            orders(clean_orders())
        ).materialization
        assert materialization is not None
        metadata = materialization.metadata or {}

        assert metadata["dagster/row_count"] == 3
        assert "dagster/partition_row_count" not in metadata

    def test_a_clean_run_reports_every_rule_as_passing_at_warn(self, tmp_path: Path):
        evaluations = check_evaluations(materialize(tmp_path, _raw_orders, orders))
        rules = {
            name: e for name, e in evaluations.items() if name.startswith("dy_rule__")
        }

        assert len(evaluations) == len(list(orders.check_specs))
        # the rules are every check but the column-schema check
        assert len(rules) == len(evaluations) - 1
        assert all(e.passed for e in evaluations.values())
        assert all(e.severity == dg.AssetCheckSeverity.WARN for e in rules.values())

    def test_a_check_has_its_rule_and_the_live_expression(self, tmp_path: Path):
        evaluation = check_evaluations(materialize(tmp_path, _raw_orders, orders))[
            "dy_rule__amount__min"
        ]
        metadata = dict(evaluation.metadata)

        assert metadata["dataframely/rule"].value == "amount|min"
        assert 'col("amount")' in str(metadata["dataframely/rule_expr"].value)


# `_REJECTED_RETURNS` in `tests/test_returned_result.py` covers the frame guard, in a call and in a run.


class TestReturnAnnotation:
    """A run whose return does not match its annotation."""

    # Unlike on a plain `@dg.asset`, the annotation sets no `dagster_type`, because the output is always a `DataFrame`.
    @pytest.mark.parametrize(
        ("lazy", "annotation"),
        [
            pytest.param(True, pl.DataFrame, id="lazy under an eager annotation"),
            pytest.param(False, pl.LazyFrame, id="eager under a lazy annotation"),
        ],
    )
    def test_an_annotation_that_differs_from_the_return_changes_nothing(
        self, tmp_path: Path, lazy: bool, annotation: type
    ):
        def fn():
            return clean_orders().lazy() if lazy else clean_orders()

        # Set at runtime, because pyrefly reports `bad-return` on either mismatch written in the source.
        fn.__annotations__ = {"return": annotation}
        asset = dd.asset(OrdersSchema, name="orders")(fn)

        assert materialize(tmp_path, asset).success
        assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())


_DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])


# Without `filter=True`, `functools.wraps` exposes the decorated function's signature to Dagster, so Dagster passes `context` itself.
class TestContextParameter:
    """A run of a decorated function that takes the context."""

    def test_a_decorated_function_can_take_a_bare_context(self, tmp_path: Path):
        seen: dict[str, str] = {}

        @dd.asset(OrdersSchema, name="orders", partitions_def=_DAYS)
        # Unannotated, the only form that works with `from __future__ import annotations` in the user's module.
        def orders(context) -> pl.DataFrame:  # pyrefly: ignore[implicit-any-parameter]
            seen["partition"] = context.partition_key
            return clean_orders()

        result = materialize(tmp_path, orders, partition_key="2026-01-02")

        assert result.success
        assert seen == {"partition": "2026-01-02"}

    def test_a_decorated_function_can_take_the_context_alongside_an_upstream_frame(
        self,
        tmp_path: Path,
    ):
        seen: dict[str, object] = {}

        @dd.asset(OrdersSchema, name="orders")
        def orders(
            context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
        ) -> pl.DataFrame:
            seen["asset"] = context.asset_key.to_user_string()
            seen["rows"] = len(raw_orders)
            return raw_orders

        result = materialize(tmp_path, _raw_orders, orders)

        assert result.success
        assert seen == {"asset": "orders", "rows": 3}

    def test_a_filtered_function_keeps_the_context_it_declared(self, tmp_path: Path):
        """With `filter=True`, a function that declares `context` receives the step's context, not a second one."""
        seen: dict[str, str] = {}

        @dd.asset(OrdersSchema, name="orders", filter=True, partitions_def=_DAYS)
        def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
            seen["partition"] = context.partition_key
            return mixed_orders()

        result = materialize(tmp_path, orders, partition_key="2026-01-02")
        error_rows = pl.read_parquet(
            tmp_path / "orders__error_rows" / "2026-01-02.parquet"
        )

        assert result.success
        assert seen == {"partition": "2026-01-02"}
        assert len(error_rows) == 3


class TestSharedName:
    """A run of two assets that share a name."""

    def test_two_assets_sharing_a_name_under_different_prefixes_both_write(
        self, tmp_path: Path
    ):
        """The two steps have different keys, because each op takes its name from the whole asset key and the op name is the step key (#70)."""

        def shipments(prefix: str) -> dg.AssetsDefinition:
            @dd.asset(OrdersSchema, key_prefix=prefix, name="shipments")
            def _shipments() -> pl.DataFrame:
                return clean_orders()

            return _shipments

        result = materialize(tmp_path, shipments("alpha"), shipments("beta"))

        assert result.success
        assert_frame_equal(
            pl.read_parquet(tmp_path / "alpha" / "shipments.parquet"), clean_orders()
        )
        assert_frame_equal(
            pl.read_parquet(tmp_path / "beta" / "shipments.parquet"), clean_orders()
        )


@dd.asset(OrdersSchema, name="orders")
def _wrong_dtype() -> pl.DataFrame:
    return wrong_dtype_orders()


class TestColumnSchemaMismatch:
    """A run whose frame's columns do not match the schema."""

    def test_a_wrong_dtype_aborts_and_names_the_column(self, tmp_path: Path):
        with pytest.raises(ColumnSchemaError) as raised:
            materialize(tmp_path, _wrong_dtype)

        message = str(raised.value)

        assert (
            "Column 'quantity' (expected Int32, got Int64) does not match OrdersSchema"
            in message
        )

    def test_a_missing_column_aborts_too(self, tmp_path: Path):
        @dd.asset(OrdersSchema, name="orders")
        def missing_column() -> pl.DataFrame:
            return clean_orders().drop("quantity")

        with pytest.raises(ColumnSchemaError) as raised:
            materialize(tmp_path, missing_column)

        assert "'quantity' (expected Int32, got <missing>)" in str(raised.value)

    def test_the_column_schema_error_reads_as_plural_for_several_columns(
        self, tmp_path: Path
    ):
        @dd.asset(OrdersSchema, name="orders")
        def several() -> pl.DataFrame:
            return (
                clean_orders()
                .drop("email")
                .with_columns(pl.col("quantity").cast(pl.Int64))
            )

        with pytest.raises(ColumnSchemaError) as raised:
            materialize(tmp_path, several)

        assert "Columns 'email' (expected String, got <missing>), 'quantity' " in str(
            raised.value
        )
        assert "do not match OrdersSchema" in str(raised.value)

    def test_the_column_schema_check_runs_before_row_filtering(self, tmp_path: Path):
        result = materialize(tmp_path, _wrong_dtype, raise_on_error=False)
        evaluations = check_evaluations(result)
        column_schema = evaluations["dy_schema__columns"]

        assert not result.success
        assert set(evaluations) == {"dy_schema__columns"}
        assert not column_schema.passed
        assert column_schema.severity == dg.AssetCheckSeverity.ERROR
        assert not list(tmp_path.rglob("*.parquet"))

    def test_the_column_schema_check_tabulates_every_failing_column(
        self, tmp_path: Path
    ):
        result = materialize(tmp_path, _wrong_dtype, raise_on_error=False)
        metadata = dict(check_evaluations(result)["dy_schema__columns"].metadata)
        errors = metadata["dataframely/column_schema_errors"]

        assert isinstance(errors, dg.TableMetadataValue)
        assert [dict(record.data) for record in errors.records] == [
            {"column": "quantity", "expected": "Int32", "actual": "Int64"}
        ]


@dd.asset(OrdersSchema, name="orders")
def _mixed() -> pl.DataFrame:
    return mixed_orders()


class TestErrorRowsWithoutFilter:
    """A run where a row fails an `"error"` rule, with `filter=False`."""

    def test_invalid_rows_with_no_error_rows_fail_the_run(self, tmp_path: Path):
        with pytest.raises(ValidationAbortError) as raised:
            materialize(tmp_path, _mixed)

        assert "3 rows failed an 'error' rule of OrdersSchema" in str(raised.value)
        assert "1 row failed 'amount|min'" in str(raised.value)
        assert "never drops rows for you" in str(raised.value)
        assert "filter=True" in str(raised.value)

    def test_the_abort_writes_nothing(self, tmp_path: Path):
        result = materialize(tmp_path, _mixed, raise_on_error=False)

        assert not result.success
        assert not result.get_asset_materialization_events()
        assert not list(tmp_path.rglob("*.parquet"))

    def test_the_abort_still_reports_every_rule(self, tmp_path: Path):
        evaluations = check_evaluations(
            materialize(tmp_path, _mixed, raise_on_error=False)
        )
        failed = {name for name, e in evaluations.items() if not e.passed}

        assert evaluations["dy_schema__columns"].passed
        assert failed == {
            "dy_rule__amount__min",
            "dy_rule__email__check__lowercase",
            "dy_rule__paid_orders_have_amount",
        }
        assert (
            dict(evaluations["dy_rule__amount__min"].metadata)[
                "dataframely/failed_row_count"
            ].value
            == 1
        )

    def test_a_frame_with_no_valid_rows_aborts_the_same_way(self, tmp_path: Path):
        @dd.asset(OrdersSchema, name="orders")
        def all_invalid() -> pl.DataFrame:
            return no_valid_orders()

        with pytest.raises(ValidationAbortError) as raised:
            materialize(tmp_path, all_invalid)

        assert (
            "2 rows failed an 'error' rule of OrdersSchema. Per rule, 2 rows failed 'amount|min'"
            in str(raised.value)
        )
        assert not list(tmp_path.rglob("*.parquet"))

    def test_every_failing_rule_check_is_an_error(self, tmp_path: Path):
        evaluations = check_evaluations(
            materialize(tmp_path, _mixed, raise_on_error=False)
        )
        failed = [
            e
            for name, e in evaluations.items()
            if name.startswith("dy_rule__") and not e.passed
        ]

        assert failed
        assert all(e.severity == dg.AssetCheckSeverity.ERROR for e in failed)


_ORDERS_KEY = dg.AssetKey(["orders"])
_ERROR_ROWS_KEY = dg.AssetKey(["orders__error_rows"])


@dd.asset(OrdersSchema, name="orders", filter=True)
def _filtered() -> pl.DataFrame:
    return mixed_orders()


class TestFilterWithValidRows:
    """A run with `filter=True` where some rows pass every `"error"` rule."""

    def test_the_valid_rows_are_written_and_the_rest_are_filtered(self, tmp_path: Path):
        """An in-process run also materializes the error rows, because it has no code location to show that their spec is missing."""
        result = materialize(tmp_path, _filtered)

        assert result.success
        assert set(materializations(result)) == {_ORDERS_KEY, _ERROR_ROWS_KEY}
        assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())
        assert pl.read_parquet(tmp_path / "orders__error_rows.parquet").height == 3

    def test_the_materialization_reports_the_error_rows_address(self, tmp_path: Path):
        """The package computes the error-rows address, because the IO manager's metadata for the error rows appears only in their own materialization."""
        metadata = materializations(materialize(tmp_path, _filtered))[
            _ORDERS_KEY
        ].metadata

        assert metadata["dataframely/error_rows_address"] == dg.MetadataValue.text(
            "orders__error_rows"
        )
        assert metadata["dataframely/error_rows_count"] == dg.MetadataValue.int(3)
        assert isinstance(
            metadata["dataframely/error_rows_sample"], dg.TableMetadataValue
        )

    def test_an_upstream_and_a_error_rows_leave_the_graph_alone(self, tmp_path: Path):
        """Error rows add no step: the asset's own step writes them and logs their materialization."""

        @dd.asset(OrdersSchema, name="orders", filter=True)
        def downstream(raw_orders: pl.DataFrame) -> pl.DataFrame:
            return mixed_orders()

        result = materialize(tmp_path, _raw_orders, downstream)

        assert result.success
        assert set(materializations(result)) == {
            dg.AssetKey(["raw_orders"]),
            _ORDERS_KEY,
            _ERROR_ROWS_KEY,
        }
        assert len(result.get_step_success_events()) == 2

    def test_a_filtered_run_succeeds_with_every_failing_check_at_error(
        self, tmp_path: Path
    ):
        evaluations = check_evaluations(materialize(tmp_path, _filtered))
        failed = {name for name, e in evaluations.items() if not e.passed}
        rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

        assert failed == {
            "dy_rule__amount__min",
            "dy_rule__email__check__lowercase",
            "dy_rule__paid_orders_have_amount",
        }
        assert all(
            e.severity == dg.AssetCheckSeverity.ERROR for e in rules if not e.passed
        )

    def test_the_check_of_every_rule_has_its_rule_and_expression_whether_it_passed_or_failed(
        self,
        tmp_path: Path,
    ):
        evaluations = check_evaluations(materialize(tmp_path, _filtered))
        rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

        assert any(e.passed for e in rules)
        assert any(not e.passed for e in rules)
        assert all(
            {"dataframely/rule", "dataframely/rule_expr"} <= set(e.metadata)
            for e in rules
        )

    def test_every_evaluation_records_the_description_a_direct_call_returns(
        self, tmp_path: Path
    ):
        evaluations = check_evaluations(materialize(tmp_path, _filtered))
        direct = evaluate_checks(OrdersSchema, mixed_orders(), asset_key=_ORDERS_KEY)

        assert {name: e.description for name, e in evaluations.items()} == {
            result.check_name: result.description for result in direct
        }

    def test_downstream_proceeds_on_the_data_that_is_fine(self, tmp_path: Path):
        """The rules' checks are not blocking, so the downstream asset runs on the valid rows."""
        seen: dict[str, int] = {}

        @dg.asset(name="reconciled")
        def reconciled(orders: pl.DataFrame) -> None:
            seen["rows"] = len(orders)

        result = materialize(tmp_path, _filtered, reconciled)

        assert result.success
        assert seen == {"rows": 3}

    def test_the_error_rows_holds_the_original_columns_plus_a_rule_column_each(
        self,
        tmp_path: Path,
    ):
        materialize(tmp_path, _filtered)
        error_rows = pl.read_parquet(tmp_path / "orders__error_rows.parquet")
        get_rule_columns = [
            name for name in error_rows.columns if name.startswith("dy_rule__")
        ]
        checks = {spec.name for spec in _filtered.check_specs} - {"dy_schema__columns"}

        assert error_rows.columns[: len(OrdersSchema.columns())] == list(
            OrdersSchema.columns()
        )
        assert set(error_rows.columns) == set(OrdersSchema.columns()) | set(
            get_rule_columns
        )
        # A rule column has the name of its rule's check.
        assert set(get_rule_columns) == checks

    def test_the_rule_columns_are_cast_from_enum_to_string(self, tmp_path: Path):
        """Rule columns are strings, because the Delta writer panics on an `Enum` column."""
        materialize(tmp_path, _filtered)
        error_rows = pl.read_parquet(tmp_path / "orders__error_rows.parquet")
        get_rule_columns = [
            name for name in error_rows.columns if name.startswith("dy_rule__")
        ]

        assert all(error_rows.schema[name] == pl.String for name in get_rule_columns)

    def test_a_rule_column_shows_which_rows_failed_its_rule(self, tmp_path: Path):
        materialize(tmp_path, _filtered)
        error_rows = pl.read_parquet(tmp_path / "orders__error_rows.parquet").sort(
            "order_id"
        )
        invalid_by_min = error_rows.filter(pl.col("dy_rule__amount__min") == "invalid")

        assert error_rows["dy_rule__amount__min"].to_list() == [
            "invalid",
            "valid",
            "valid",
        ]
        assert invalid_by_min["order_id"].to_list() == ["ORD-4"]

    def test_the_materialization_tabulates_the_rules_that_failed_together(
        self, tmp_path: Path
    ):
        @dd.asset(OrdersSchema, name="orders", filter=True)
        def cooccurring() -> pl.DataFrame:
            return cooccurring_orders()

        metadata = materializations(materialize(tmp_path, cooccurring))[
            _ORDERS_KEY
        ].metadata
        cooccurrence = metadata["dataframely/error_rows_by_rules"]

        assert isinstance(cooccurrence, dg.TableMetadataValue)
        assert [dict(record.data) for record in cooccurrence.records] == [
            {
                "rules": (
                    "dy_rule__amount__min, dy_rule__email__check__lowercase, "
                    "dy_rule__paid_orders_have_amount"
                ),
                "count": 1,
            }
        ]

    def test_the_error_rows_by_rules_table_lists_the_set_most_rows_failed_first(
        self,
        tmp_path: Path,
    ):
        """Rows sort by count, then by rule names."""
        duplicate_break = (
            mixed_orders()
            .filter(pl.col("order_id") == "ORD-4")  # amount|min alone
            .with_columns(
                pl.lit("ORD-7").alias("order_id"),
                pl.lit("TRK-ORD-7-1").alias("tracking_id"),
            )
        )

        @dd.asset(OrdersSchema, name="orders", filter=True)
        def repeated() -> pl.DataFrame:
            return pl.concat([mixed_orders(), duplicate_break])

        metadata = materializations(materialize(tmp_path, repeated))[
            _ORDERS_KEY
        ].metadata
        cooccurrence = metadata["dataframely/error_rows_by_rules"]

        assert isinstance(cooccurrence, dg.TableMetadataValue)
        assert [dict(record.data) for record in cooccurrence.records] == [
            {"rules": "dy_rule__amount__min", "count": 2},
            {"rules": "dy_rule__email__check__lowercase", "count": 1},
            {"rules": "dy_rule__paid_orders_have_amount", "count": 1},
        ]


@dd.asset(OrdersSchema, name="orders", filter=True)
def _no_valid_rows() -> pl.DataFrame:
    return no_valid_orders()


class TestFilterWithNoValidRows:
    """A run with `filter=True` where every row fails an `"error"` rule."""

    def test_no_valid_rows_writes_the_error_rows_and_materializes_only_them(
        self,
        tmp_path: Path,
    ):
        result = materialize(tmp_path, _no_valid_rows, raise_on_error=False)

        assert not result.success
        assert set(materializations(result)) == {_ERROR_ROWS_KEY}
        assert pl.read_parquet(tmp_path / "orders__error_rows.parquet").height == 2
        assert not (tmp_path / "orders.parquet").exists()

    def test_no_valid_rows_leaves_the_last_known_good_table_intact(
        self, tmp_path: Path
    ):
        @dd.asset(OrdersSchema, name="orders", filter=True)
        def spotless() -> pl.DataFrame:
            return clean_orders()

        materialize(tmp_path, spotless)
        materialize(tmp_path, _no_valid_rows, raise_on_error=False)

        assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())

    def test_no_valid_rows_fails_the_run_and_names_the_damage(self, tmp_path: Path):
        with pytest.raises(NoValidRowsError) as raised:
            materialize(tmp_path, _no_valid_rows)

        assert "2 of 2 rows failed an 'error' rule of OrdersSchema" in str(raised.value)
        assert "2 rows failed 'amount|min'" in str(raised.value)
        assert "orders__error_rows" in str(raised.value)

    def test_no_valid_rows_fails_every_failing_rule_check_at_error(
        self, tmp_path: Path
    ):
        result = materialize(tmp_path, _no_valid_rows, raise_on_error=False)
        evaluations = check_evaluations(result)
        rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

        assert evaluations["dy_schema__columns"].passed
        assert rules
        assert all(
            e.severity == dg.AssetCheckSeverity.ERROR for e in rules if not e.passed
        )
        assert (
            dict(evaluations["dy_rule__amount__min"].metadata)[
                "dataframely/failed_row_count"
            ].value
            == 2
        )


@dd.asset(OrdersSchema, name="orders")
def _skipping() -> pl.DataFrame | None:
    return None


@dd.asset(OrdersSchema, name="orders", filter=True)
def _skipping_with_error_rows() -> pl.DataFrame | None:
    return None


class TestSkip:
    """A run whose decorated function returns `None` (#95)."""

    @pytest.mark.parametrize(
        "asset",
        [_skipping, _skipping_with_error_rows],
        ids=["bare", "check_and_filter"],
    )
    def test_a_skipped_run_succeeds_and_materializes_nothing(
        self, tmp_path: Path, asset: dg.AssetsDefinition
    ):
        result = materialize(tmp_path, asset)

        assert result.success
        assert materializations(result) == {}
        assert not list(tmp_path.rglob("*.parquet"))

    def test_a_skipped_runs_checks_claim_no_materialization(self, tmp_path: Path):
        """A skip's check results target no materialization, so a pass does not apply to an earlier run's data."""
        evaluations = check_evaluations(materialize(tmp_path, _skipping))

        assert all(e.target_materialization_data is None for e in evaluations.values())

    @pytest.mark.parametrize("granularity", ["rule", "column"])
    def test_a_skip_yields_a_result_for_every_check_the_asset_declares(
        self, tmp_path: Path, granularity: Granularity
    ):
        """A skip yields a result for every check the asset declares, at every granularity."""

        @dd.asset(OrdersSchema, name="orders", check_granularity=granularity)
        def skipping() -> pl.DataFrame | None:
            return None

        evaluations = check_evaluations(materialize(tmp_path, skipping))

        assert set(evaluations) == {spec.name for spec in skipping.check_specs}
        assert all(e.passed for e in evaluations.values())

    def test_a_hand_wired_asset_with_output_required_false_skips(
        self,
        tmp_path: Path,
    ):
        """A hand-wired asset with `output_required=False` skips when it passes `None` to `stream_results`."""

        @dg.asset(
            name="orders",
            output_required=False,
            metadata=get_column_schema_metadata(OrdersSchema),
            check_specs=build_asset_check_specs(OrdersSchema, asset="orders"),
        )
        def orders_by_hand(
            context: dg.AssetExecutionContext,
        ) -> Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]:
            yield from stream_results(OrdersSchema, None, asset_key=context.asset_key)

        result = materialize(tmp_path, orders_by_hand)

        assert result.success
        assert materializations(result) == {}
        assert len(check_evaluations(result)) == len(list(orders_by_hand.check_specs))


def _both_ways(
    frame: Callable[[], pl.DataFrame], filtering: bool
) -> tuple[dg.AssetsDefinition, dg.AssetsDefinition]:
    """Declare the same asset twice, with an eager and a lazy return."""

    @dd.asset(OrdersSchema, name="orders", filter=filtering)
    def eager() -> pl.DataFrame:
        return frame()

    @dd.asset(OrdersSchema, name="orders", filter=filtering)
    def lazy() -> pl.LazyFrame:
        return frame().lazy()

    return eager, lazy


def _pluralize(frame: pl.DataFrame) -> tuple[pl.LazyFrame, list[int]]:
    """Return a plan over `frame` and the heights of the batches it executes."""
    seen: list[int] = []

    def count(batch: pl.DataFrame) -> pl.DataFrame:
        seen.append(batch.height)
        return batch

    return frame.lazy().map_batches(count, streamable=True), seen


class TestLazyReturn:
    """A run whose decorated function returns a `LazyFrame`."""

    # No test compares an eager and a lazy return for each scenario frame, because `stream_results` calls `frame.lazy()` on both.

    @pytest.mark.parametrize("lazily", [False, True], ids=["eager", "lazy"])
    def test_the_filter_runs_on_the_streaming_engine(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lazily: bool
    ):
        """The engine does not change the output, so this asserts the `collect_all` call."""
        engines: list[object] = []
        collect_all = pl.collect_all

        def spy(frames: Any, **kwargs: Any) -> Any:
            engines.append(kwargs.get("engine"))
            return collect_all(frames, **kwargs)

        monkeypatch.setattr(pl, "collect_all", spy)
        eager, lazy = _both_ways(clean_orders, filtering=False)

        assert materialize(tmp_path, lazy if lazily else eager).success
        assert engines == ["streaming"]

    def test_a_lazy_return_executes_its_plan_once(self, tmp_path: Path):
        plan, seen = _pluralize(clean_orders())

        @dd.asset(OrdersSchema, name="orders")
        def lazy() -> pl.LazyFrame:
            return plan

        assert materialize(tmp_path, lazy).success
        assert sum(seen) == clean_orders().height

    def test_a_plan_that_fails_the_column_schema_check_never_executes(
        self, tmp_path: Path
    ):
        plan, seen = _pluralize(wrong_dtype_orders())

        @dd.asset(OrdersSchema, name="orders")
        def lazy() -> pl.LazyFrame:
            return plan

        with pytest.raises(ColumnSchemaError):
            materialize(tmp_path, lazy)
        assert seen == []


@dd.asset(OrdersSchema, name="orders", filter=True, check_granularity="column")
def _by_column() -> pl.DataFrame:
    return mixed_orders()


def _failure_counts_by_rule(
    evaluation: dg.AssetCheckEvaluation,
) -> dict[object, object]:
    """Return the failure count of each rule in a collapsed check."""
    table = dict(evaluation.metadata)["dataframely/rules"]
    assert isinstance(table, dg.TableMetadataValue)
    return {record.data["rule"]: record.data["failed"] for record in table.records}


class TestCollapsedChecks:
    """A run whose `check_granularity` collapses the rules into fewer checks."""

    def test_a_collapsed_check_fails_when_any_rule_it_reports_for_failed(
        self, tmp_path: Path
    ):
        evaluations = check_evaluations(materialize(tmp_path, _by_column))
        failed = {name for name, e in evaluations.items() if not e.passed}

        assert failed == {
            "dy_col__amount",
            "dy_col__email",
            "dy_rule__paid_orders_have_amount",
        }
        assert all(
            evaluations[name].severity == dg.AssetCheckSeverity.ERROR for name in failed
        )

    def test_a_collapsed_check_reports_the_failure_count_of_every_rule(
        self,
        tmp_path: Path,
    ):
        """The counts are per rule, because one row can fail several rules."""
        evaluations = check_evaluations(materialize(tmp_path, _by_column))

        assert _failure_counts_by_rule(evaluations["dy_col__email"]) == {
            "email|nullability": 0,
            "email|check__lowercase": 1,
            "email|max_length": 0,
        }
        assert _failure_counts_by_rule(evaluations["dy_col__amount"]) == {
            "amount|nullability": 0,
            "amount|min": 1,
        }

    def test_a_collapsed_check_has_the_live_expression_of_every_rule(
        self, tmp_path: Path
    ):
        evaluation = check_evaluations(materialize(tmp_path, _by_column))[
            "dy_col__amount"
        ]
        table = dict(evaluation.metadata)["dataframely/rules"]

        assert isinstance(table, dg.TableMetadataValue)
        assert all(record.data["expr"] for record in table.records)
        assert any(
            'col("amount")' in str(record.data["expr"]) for record in table.records
        )

    def test_a_clean_run_passes_every_collapsed_check(self, tmp_path: Path):
        """A collapsed check reports 0 for each rule on a clean run, instead of omitting the count."""

        @dd.asset(OrdersSchema, name="orders", check_granularity="column")
        def spotless() -> pl.DataFrame:
            return clean_orders()

        evaluations = check_evaluations(materialize(tmp_path, spotless))

        assert all(e.passed for e in evaluations.values())
        assert set(_failure_counts_by_rule(evaluations["dy_col__email"]).values()) == {
            0
        }

    def test_collapsing_the_checks_does_not_collapse_the_error_rows(
        self, tmp_path: Path
    ):
        """The error rows have one rule column per rule at every granularity."""
        materialize(tmp_path, _by_column)
        error_rows = pl.read_parquet(tmp_path / "orders__error_rows.parquet")

        assert "dy_rule__amount__min" in error_rows.columns
        assert not [name for name in error_rows.columns if name.startswith("dy_col__")]


class TestMetadataKeys:
    """The metadata keys of a failing check."""

    @pytest.mark.parametrize(
        ("asset", "check"),
        [(_filtered, "dy_rule__amount__min"), (_by_column, "dy_col__amount")],
        ids=["rule", "column"],
    )
    def test_a_failing_check_names_every_key_under_dataframely(
        self, tmp_path: Path, asset: dg.AssetsDefinition, check: str
    ):
        evaluation = check_evaluations(materialize(tmp_path, asset))[check]

        assert not evaluation.passed
        assert all(key.startswith("dataframely/") for key in evaluation.metadata)


def _column_schema(result: dg.ExecuteInProcessResult) -> object:
    """Return the column schema on the table's materialization."""
    return materializations(result)[_ORDERS_KEY].metadata["dagster/column_schema"].value


class TestColumnSchema:
    """The `dagster/column_schema` entry on the table's materialization in a run."""

    @pytest.mark.parametrize(
        "filtering", [False, True], ids=["bare", "check_and_filter"]
    )
    def test_the_schemas_column_schema_replaces_the_io_managers(
        self, tmp_path: Path, filtering: bool
    ):
        """`PolarsParquetIOManager` writes a column schema without constraints, and the package writes the schema's over it."""

        @dd.asset(OrdersSchema, name="orders", filter=filtering)
        def orders() -> pl.DataFrame:
            return clean_orders()

        assert _column_schema(materialize(tmp_path, orders)) == get_table_schema(
            OrdersSchema
        )

    def test_the_schemas_column_schema_replaces_one_the_function_adds(
        self, tmp_path: Path
    ):
        @dd.asset(OrdersSchema, name="orders")
        def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
            context.add_asset_metadata({
                "dagster/column_schema": dg.TableSchema(columns=[])
            })
            return clean_orders()

        assert _column_schema(materialize(tmp_path, orders)) == get_table_schema(
            OrdersSchema
        )


_ADDRESS = "orders__error_rows"


def _drained(
    frame: pl.DataFrame | None, *, filtering: bool
) -> tuple[Yielded, list[pl.DataFrame], DagsterDataframelyError | None]:
    """Return what `stream_results` yielded and wrote, and the error it raised, if any.

    `stream_results` raises `ColumnSchemaError`, `ValidationAbortError` or `NoValidRowsError` after it yields check results, so `list()` alone would raise before it returns them.
    """
    written: list[pl.DataFrame] = []

    # A stub, because `tests/test_error_rows.py` tests the real writers.
    def writer(invalid: pl.DataFrame) -> str:
        written.append(invalid)
        return _ADDRESS

    yielded: Yielded = []
    results = stream_results(
        OrdersSchema,
        frame,
        asset_key=_ORDERS_KEY,
        error_rows_writer=writer if filtering else None,
    )
    try:
        for result in results:
            yielded.append(result)
    except DagsterDataframelyError as error:
        return yielded, written, error
    return yielded, written, None


def _metadata(yielded: Yielded) -> Mapping[str, Any]:
    materializations = [r for r in yielded if isinstance(r, dg.MaterializeResult)]
    if not materializations:
        return {}
    (materialization,) = materializations
    return materialization.metadata or {}


def _checks(yielded: Yielded) -> list[dg.AssetCheckResult]:
    return [result for result in yielded if isinstance(result, dg.AssetCheckResult)]


def _rules(yielded: Yielded) -> list[dg.AssetCheckResult]:
    """Return the check results except the column-schema check's, which reports no rule."""
    return [c for c in _checks(yielded) if c.check_name != "dy_schema__columns"]


class TestOutcomeSelection:
    """What `stream_results` yields, writes and raises for the scenario frames and for `None`, with no run."""

    def test_a_clean_frame_writes_an_empty_error_rows_with_every_column(self):
        yielded, written, error = _drained(clean_orders(), filtering=True)
        _, dirty, _ = _drained(mixed_orders(), filtering=True)

        assert error is None
        assert list(results(yielded)) == [_ORDERS_KEY]
        (empty,) = written
        assert empty.is_empty()
        assert empty.schema == dirty[0].schema

    def test_a_clean_frame_omits_the_error_rows_keys(self):
        """A clean run omits the four `dataframely/error_rows_*` keys instead of writing empty values."""
        clean, _, _ = _drained(clean_orders(), filtering=True)
        partial, _, _ = _drained(mixed_orders(), filtering=True)

        # A difference, not a prefix, because the valid rows' sample is under `dataframely/` too.
        assert set(_metadata(partial)) - set(_metadata(clean)) == {
            "dataframely/error_rows_address",
            "dataframely/error_rows_count",
            "dataframely/error_rows_by_rules",
            "dataframely/error_rows_sample",
        }

    def test_a_wrong_column_schema_reports_the_check_and_writes_nothing(self):
        yielded, written, error = _drained(wrong_dtype_orders(), filtering=True)

        assert isinstance(error, ColumnSchemaError)
        assert list(results(yielded)) == []
        assert written == []
        assert [c.check_name for c in _checks(yielded)] == ["dy_schema__columns"]

    def test_invalid_rows_with_no_writer_write_nothing_but_still_report(self):
        yielded, written, error = _drained(mixed_orders(), filtering=False)

        assert isinstance(error, ValidationAbortError)
        assert list(results(yielded)) == []
        assert written == []
        assert any(not c.passed for c in _checks(yielded))

    def test_invalid_rows_with_a_writer_write_the_table_and_the_error_rows(self):
        yielded, written, error = _drained(mixed_orders(), filtering=True)
        (invalid,) = written

        assert error is None
        assert list(results(yielded)) == [_ORDERS_KEY]
        assert len(invalid) == 3
        assert "dy_rule__amount__min" in invalid.columns

    def test_a_partial_run_reports_the_invalid_rows_on_the_materialization(self):
        yielded, _, _ = _drained(mixed_orders(), filtering=True)
        metadata = _metadata(yielded)

        assert metadata["dataframely/error_rows_address"] == _ADDRESS
        assert metadata["dataframely/error_rows_count"] == 3
        assert isinstance(
            metadata["dataframely/error_rows_by_rules"], dg.TableMetadataValue
        )
        assert isinstance(
            metadata["dataframely/error_rows_sample"], dg.TableMetadataValue
        )

    def test_the_error_rows_sample_is_bounded_by_the_row_sample_setting(self):
        yielded, _, _ = _drained(mixed_orders(), filtering=True)
        sample = _metadata(yielded)["dataframely/error_rows_sample"]
        assert isinstance(sample, dg.TableMetadataValue)

        bounded = stream_results(
            OrdersSchema,
            mixed_orders(),
            asset_key=_ORDERS_KEY,
            error_rows_writer=lambda _: _ADDRESS,
            row_sample=1,
        )
        (materialization,) = [r for r in bounded if isinstance(r, dg.MaterializeResult)]
        narrowed = (materialization.metadata or {})["dataframely/error_rows_sample"]
        assert isinstance(narrowed, dg.TableMetadataValue)

        assert len(sample.records) == 3
        assert len(narrowed.records) == 1

    def test_no_valid_rows_writes_the_rows_and_yields_no_table(self):
        yielded, written, error = _drained(no_valid_orders(), filtering=True)
        (invalid,) = written

        assert isinstance(error, NoValidRowsError)
        assert list(results(yielded)) == []
        assert len(invalid) == 2

    def test_an_abort_puts_the_address_on_every_check_instead(self):
        """When every row fails an `"error"` rule, every check result has the error-rows address (ADR-0004)."""
        yielded, _, _ = _drained(no_valid_orders(), filtering=True)
        checks = _checks(yielded)

        assert checks
        # `_add_address` wraps the address, because `_replace` skips normalization.
        assert all(
            (c.metadata or {})["dataframely/error_rows_address"]
            == dg.MetadataValue.text(_ADDRESS)
            for c in checks
        )

    def test_the_abort_names_the_address_the_writer_returned(self):
        _, _, error = _drained(no_valid_orders(), filtering=True)

        assert _ADDRESS in str(error)

    def test_no_valid_rows_without_a_writer_aborts_instead(self):
        yielded, written, error = _drained(no_valid_orders(), filtering=False)

        assert isinstance(error, ValidationAbortError)
        assert list(results(yielded)) == []
        assert written == []

    def test_check_results_are_yielded_standalone(self):
        """Direct invocation needs standalone check results (ADR-0002)."""
        yielded, _, _ = _drained(mixed_orders(), filtering=True)

        assert not [
            result
            for result in yielded
            if isinstance(result, dg.MaterializeResult) and result.check_results
        ]
        assert _checks(yielded)

    def test_severity_follows_the_rule_with_and_without_filter(self):
        """The same failing checks are `ERROR` with and without `filter`, because every rule of `OrdersSchema` is `"error"`."""
        check_and_filter, _, _ = _drained(mixed_orders(), filtering=True)
        aborted, _, _ = _drained(mixed_orders(), filtering=False)

        def failing(yielded: Yielded) -> set[dg.AssetCheckSeverity]:
            return {c.severity for c in _rules(yielded) if not c.passed}

        assert (
            failing(check_and_filter)
            == failing(aborted)
            == {dg.AssetCheckSeverity.ERROR}
        )

    @pytest.mark.parametrize(
        "filtering", [False, True], ids=["bare", "check_and_filter"]
    )
    def test_a_skip_writes_no_table_and_raises_nothing(self, filtering: bool):
        yielded, written, error = _drained(None, filtering=filtering)

        assert error is None
        assert list(results(yielded)) == []
        assert written == []

    def test_a_skip_still_yields_a_result_for_every_check(self):
        """A skip yields a result for every check, because each check spec is a non-optional op output."""
        skipped, _, _ = _drained(None, filtering=False)
        clean, _, _ = _drained(clean_orders(), filtering=False)

        assert {c.check_name for c in _checks(skipped)} == {
            c.check_name for c in _checks(clean)
        }

    def test_a_skips_checks_pass_because_the_rules_ran_over_an_empty_frame(self):
        yielded, _, _ = _drained(None, filtering=False)

        assert all(c.passed for c in _checks(yielded))
        assert {c.severity for c in _rules(yielded)} == {dg.AssetCheckSeverity.WARN}
