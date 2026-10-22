"""These characterization tests check the upstream behaviour that this package depends on.

Each docstring names the upstream behaviour and the code here that uses it, so a failure shows what upstream changed.
"""

import copy
import datetime as dt
import inspect
import warnings
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Annotated, Any, override

import dagster as dg
import dataframely as dy
import polars as pl
import pytest
from dagster._check import CheckError
from dagster._core.definitions.assets.definition.asset_dep import CoercibleToAssetDep
from dagster._core.definitions.decorators.op_decorator import is_context_provided
from dagster._core.errors import DagsterInvalidPropertyError
from dagster._core.execution.plan.outputs import StepOutputHandle
from dagster._core.storage.upath_io_manager import (
    coerce_to_relative_parts,
    escape_dotdot_segments,
    escape_leading_slash,
)
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_shared.utils.warnings import PreviewWarning
from dataframely._rule import Rule, RuleFactory
from upath import UPath

from dagster_dataframely._rules import DAGSTER_NAME
from dagster_dataframely.data import ParquetIOManager
from dagster_dataframely.wiring import get_error_rows_path
from tests.scenario import events


class OrdersSchema(dy.Schema):
    """A schema with a column rule, a primary key and a `@dy.rule()`."""

    order_id = dy.String(primary_key=True)
    amount = dy.Float64(nullable=False, min=0.0)
    status = dy.Enum(["new", "paid"], nullable=False)

    @dy.rule()
    def paid_orders_have_amount(cls) -> pl.Expr:
        """Require a positive amount on a paid order."""
        return (cls.status.col != "paid") | (cls.amount.col > 0)


class RefundsSchema(dy.Schema):
    """A schema with the same primary key as `OrdersSchema`."""

    order_id = dy.String(primary_key=True)


class OrderBook(dy.Collection):
    """A Collection with one collection filter, an eager member and an ignored optional member."""

    orders: dy.LazyFrame[OrdersSchema]
    refunds: dy.DataFrame[RefundsSchema]
    notes: Annotated[
        dy.LazyFrame[RefundsSchema] | None,
        dy.CollectionMember(ignored_in_filters=True, propagate_row_failures=True),
    ]

    @dy.filter()
    def refund_has_order(self) -> pl.LazyFrame:
        """Every refund's order exists."""
        return self.refunds.lazy().join(self.orders, on="order_id", how="semi")


# One valid row, then rows that fail `amount|min`, `paid_orders_have_amount` and `primary_key`.
_MIXED_ORDERS = pl.DataFrame({
    "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-3"],
    "amount": [10.0, -1.0, 0.0, 5.0],
    "status": pl.Series(["new", "new", "paid", "new"], dtype=pl.Enum(["new", "paid"])),
})


class TestDataframely:
    """The Dataframely behaviour this package depends on."""

    def test_rules_are_keyed_by_pipe_delimited_rule_name(self):
        """`Schema._validation_rules` keys a column rule as `<column>|<rule>`, and `_rules.describe_rules` reads every rule from it."""
        rules = OrdersSchema._validation_rules(with_cast=False)

        assert "amount|min" in rules
        assert "primary_key" in rules
        assert "paid_orders_have_amount" in rules
        assert all(
            name.split("|")[0] in OrdersSchema.columns()
            for name in rules
            if "|" in name
        )

    def test_rule_values_are_rule_instances_with_a_polars_expr(self):
        """Each `_validation_rules` value is a `Rule` with a `pl.Expr` on `expr`, which `_rules.DescribedRule` stores and `_checks` writes to check metadata."""
        rules = OrdersSchema._validation_rules(with_cast=False)

        assert all(isinstance(rule, Rule) for rule in rules.values())
        assert all(isinstance(rule.expr, pl.Expr) for rule in rules.values())

    def test_a_dy_rule_stays_reachable_by_name_and_keeps_its_docstring(self):
        """A `@dy.rule()` leaves a `RuleFactory` on the class, and `DescribedRule.from_rule` reads the rule's description from its `validation_fn`."""
        factory = OrdersSchema.paid_orders_have_amount

        assert isinstance(factory, RuleFactory)
        assert (
            factory.validation_fn.__doc__
            == "Require a positive amount on a paid order."
        )

    def test_a_column_rule_is_not_reachable_by_name(self):
        """No class attribute has a column rule's name, and `primary_key` names a method, so `DescribedRule.from_rule` checks for a `RuleFactory`."""
        assert getattr(OrdersSchema, "amount|min", None) is None

        assert not isinstance(getattr(OrdersSchema, "primary_key", None), RuleFactory)

    def test_a_constraint_rule_is_named_after_the_parameter_that_declared_it(self):
        """Each constraint's rule has its parameter's name, and the column stores the value under that name, which `_rendering` reads with `getattr`."""
        declared: dict[str, dy.Column] = {
            "min": dy.Int64(min=1),
            "max": dy.Int64(max=9),
            "min_exclusive": dy.Float64(min_exclusive=0.0),
            "max_exclusive": dy.Float64(max_exclusive=1.0),
            "is_in": dy.Int64(is_in=[1, 2]),
            "regex": dy.String(regex="^a$"),
            "min_length": dy.String(min_length=1),
            "max_length": dy.List(dy.String(), max_length=2),
            "resolution": dy.Datetime(resolution="1h"),
        }

        for parameter, column in declared.items():
            assert parameter in column.validation_rules(pl.col("a"))
            assert getattr(column, parameter) is not None

    def test_length_bounds_are_declared_on_exactly_string_and_list(self):
        """Only `dy.String` (bytes) and `dy.List` (elements) take length bounds, so `_rendering._get_length_unit` checks for `dy.List` alone."""
        columns = [
            column
            for name in dy.__all__
            if isinstance(column := getattr(dy, name), type)
            and issubclass(column, dy.Column)
        ]
        with_length = {
            column.__name__
            for column in columns
            if "max_length" in inspect.signature(column.__init__).parameters
        }

        assert with_length == {"String", "List"}
        assert "len_bytes" in str(
            dy.String(max_length=3).validation_rules(pl.col("a"))["max_length"]
        )
        assert ".list.length()" in str(
            dy.List(dy.String(), max_length=3).validation_rules(pl.col("a"))[
                "max_length"
            ]
        )

    def test_a_struct_emits_one_inner_rule_per_constrained_field(self):
        """A `dy.Struct` names its fields' rules `inner_<field>_<rule>` under its own column, so `column` granularity collapses them into that column's check."""
        address = dy.Struct({
            "city": dy.String(nullable=False),
            "postcode": dy.String(nullable=True),
            "number": dy.Int32(nullable=False, min=1),
        })

        assert set(address.validation_rules(pl.col("address"))) == {
            "nullability",
            "inner_city_nullability",
            "inner_number_nullability",
            "inner_number_min",
        }

    def test_a_list_and_an_array_still_reach_their_element_column_through_inner(self):
        """`dy.List` and `dy.Array` keep the element column on `inner` and name its rules `inner_<rule>`, so `_rendering._render_column_constraint` renders `inner_min` as `elements >= 1`."""
        tags = dy.List(dy.Int32(nullable=False, min=1), min_length=1)
        point = dy.Array(dy.Int32(nullable=False, min=1), shape=3)

        assert isinstance(tags.inner, dy.Int32)
        assert isinstance(point.inner, dy.Int32)
        assert set(tags.validation_rules(pl.col("tags"))) == {
            "nullability",
            "min_length",
            "inner_nullability",
            "inner_min",
        }
        assert set(point.validation_rules(pl.col("point"))) == {
            "nullability",
            "inner_nullability",
            "inner_min",
        }

    def test_a_float_column_forbids_inf_and_nan_by_default_and_drops_the_rules_when_allowed(
        self,
    ):
        """A float column has `inf` and `nan` rules until `allow_inf=True` and `allow_nan=True` remove them, so `_rendering.render_column_constraints` leaves both out of the Columns tab."""
        assert not dy.Float64().allow_inf
        assert not dy.Float64().allow_nan
        assert {"inf", "nan"} <= set(dy.Float64().validation_rules(pl.col("a")))
        assert not {"inf", "nan"} & set(
            dy.Float64(allow_inf=True, allow_nan=True).validation_rules(pl.col("a"))
        )

    def test_validate_dtype_still_compares_the_dtype_alone_down_to_the_time_unit(
        self,
    ):
        """The undocumented `dy.Column.validate_dtype` rejects another `Datetime` time unit or time zone, and `_checks._find_column_schema_problems` calls it."""
        assert dy.Int64().validate_dtype(pl.Int64)
        assert not dy.Int64().validate_dtype(pl.Int32)

        assert dy.Datetime().dtype == pl.Datetime("us")
        assert dy.Datetime().validate_dtype(pl.Datetime("us"))
        assert not dy.Datetime().validate_dtype(pl.Datetime("ns"))
        assert not dy.Datetime().validate_dtype(pl.Datetime("us", "UTC"))

        assert dy.Any().validate_dtype(pl.Int64)
        assert dy.Any().validate_dtype(pl.String)

    def test_filter_projects_a_superset_to_the_schemas_columns_in_order(self):
        """`Schema.filter` keeps only the schema's columns, in the schema's order, as `user_guide/validation.qmd` states and Dataframely does not document."""
        superset = _MIXED_ORDERS.select(
            "status", "amount", pl.lit("note").alias("shipping"), "order_id"
        )
        expected = list(OrdersSchema.columns())

        valid, _ = OrdersSchema.filter(superset, cast=False)
        assert valid.columns == expected

        lazy_valid, _ = OrdersSchema.filter(superset.lazy(), cast=False)
        assert lazy_valid.collect_schema().names() == expected

    def test_a_lazy_filter_still_defers_to_collect_all_and_forwards_the_engine(self):
        """On a `LazyFrame`, `Schema.filter` returns a result whose `collect_all` passes `engine=` to Polars, which `_checks.check_and_filter` calls with `engine="streaming"`."""
        lazy_result = OrdersSchema.filter(_MIXED_ORDERS.lazy(), cast=False)

        assert not hasattr(
            OrdersSchema.filter(_MIXED_ORDERS, cast=False), "collect_all"
        )

        valid, failure = lazy_result.collect_all(engine="streaming")

        assert isinstance(valid, pl.DataFrame)
        assert isinstance(failure, dy.FailureInfo)
        assert valid.height == 1
        assert len(failure) == 3

        # Polars raises this error, so `collect_all` passes `engine=` through to Polars.
        with pytest.raises(ValueError, match="Invalid engine argument"):
            lazy_result.collect_all(engine="nonsense")

    def test_details_returns_invalid_rows_plus_one_column_per_rule(self):
        """`FailureInfo.details()` returns the invalid rows plus one rule column per rule, which `_error_rows.get_error_rows` and `_checks._read_failure_examples` both read."""
        _, failure = OrdersSchema.filter(_MIXED_ORDERS)
        details = failure.details()

        assert details.height == failure.invalid().height
        assert set(details.columns) == set(OrdersSchema.columns()) | set(
            OrdersSchema._validation_rules(with_cast=False)
        )

        # An `Enum`, which `get_error_rows` casts to `String` because the Delta writer panics on it.
        assert details.schema["amount|min"] == pl.Enum(["valid", "invalid", "unknown"])
        assert (
            details.filter(pl.col("order_id") == "ORD-2")["amount|min"].item()
            == "invalid"
        )

    def test_the_failure_example_limit_does_not_truncate_the_filter_path(self):
        """`dy.Config(max_failure_examples=)` limits only the message `validate` builds, so only `max_failure_examples` limits the rows in check metadata."""
        with dy.Config(max_failure_examples=1):
            _, failure = OrdersSchema.filter(_MIXED_ORDERS)

        assert len(failure) == 3
        assert failure.details().height == 3
        assert failure.counts()["primary_key"] == 2

    def test_cooccurrence_counts_are_keyed_by_a_frozenset_of_rule_names(self):
        """`FailureInfo.cooccurrence_counts()` keys each count by a `frozenset` of rule names, which `_runtime._tabulate_cooccurrence` sorts before rendering."""
        _, failure = OrdersSchema.filter(_MIXED_ORDERS)
        cooccurrence = failure.cooccurrence_counts()

        assert all(isinstance(rules, frozenset) for rules in cooccurrence)
        assert sum(cooccurrence.values()) == len(failure)
        assert cooccurrence[frozenset({"amount|min"})] == 1

    def test_a_collection_keeps_each_filter_by_name_with_its_function(self):
        """`Collection._filters()` maps each `@dy.filter` name to a `Filter` whose `logic` is the decorated function, which `_rules.get_filter_severities` and `_collection` read."""
        filters = OrderBook._filters()

        assert list(filters) == ["refund_has_order"]
        assert (
            inspect.getdoc(filters["refund_has_order"].logic)
            == "Every refund's order exists."
        )

    def test_a_collection_member_records_how_it_takes_part_in_the_filters(self):
        """`_collection` reads `is_lazy`, `ignored_in_filters` and `propagate_row_failures` from each member in `Collection.members()`, and the required members from `Collection.required_members()`."""
        members = OrderBook.members()

        assert members["orders"].is_lazy
        assert not members["refunds"].is_lazy
        assert members["notes"].is_optional
        assert members["notes"].ignored_in_filters
        assert members["notes"].propagate_row_failures
        assert OrderBook.required_members() == {"orders", "refunds"}

    def test_skipping_member_validation_counts_only_the_collection_filters(self):
        """With `skip_member_validation=True`, `Collection.filter` runs no member rule and reports each collection filter on every non-ignored member, which `_collection` relies on."""
        orders = pl.DataFrame({
            "order_id": ["ORD-1", "ORD-2"],
            "amount": [-1.0, 5.0],
            "status": pl.Series(["new", "new"], dtype=pl.Enum(["new", "paid"])),
        })
        refunds = pl.DataFrame({"order_id": ["ORD-1", "ORD-9"]})

        _, failure = OrderBook.filter(
            {"orders": orders.lazy(), "refunds": refunds.lazy()},
            skip_member_validation=True,
        )

        assert {member: info.counts() for member, info in failure.items()} == {
            "orders": {"refund_has_order": 1},
            "refunds": {"refund_has_order": 1},
        }


class TestPolars:
    """The Polars behaviour this package depends on."""

    def test_polars_renders_a_duration_in_its_own_friendly_style(self):
        """`dt.to_string("polars")` formats a duration as the frame repr does, and `_statistics` formats every duration in the temporal statistics table with it."""
        spans = pl.Series(
            "spans",
            [
                dt.timedelta(days=8),
                dt.timedelta(minutes=1, seconds=30),
                dt.timedelta(hours=2, minutes=5),
                dt.timedelta(seconds=-90),
                dt.timedelta(0),
                None,
            ],
            dtype=pl.Duration("us"),
        )

        assert spans.dt.to_string("polars").to_list() == [
            "8d",
            "1m 30s",
            "2h 5m",
            "-1m -30s",
            "0µs",
            None,
        ]

        # Why `_statistics` names the format: the default is ISO-8601, and a cast raises.
        assert spans.dt.to_string().to_list()[0] == "P8D"
        with pytest.raises(pl.exceptions.InvalidOperationError):
            spans.cast(pl.String)


class TestDagster:
    """The Dagster behaviour this package depends on that ADR-0006 and ADR-0007 do not list."""

    def test_a_partitioned_asset_check_spec_warns_preview(self):
        """`AssetCheckSpec(partitions_def=)` warns `PreviewWarning` and nothing else, as `user_guide/partitioning.qmd` states."""
        # The whole list, not `pytest.warns`, so a failure distinguishes `[]` (GA) from `[BetaWarning]`.
        with warnings.catch_warnings(record=True) as recorded:
            warnings.simplefilter("always")
            dg.AssetCheckSpec(
                "row_count",
                asset=dg.AssetKey(["orders"]),
                partitions_def=dg.StaticPartitionsDefinition(["a"]),
            )

        assert [warning.category for warning in recorded] == [PreviewWarning]

    def test_dagster_does_not_enforce_a_matching_asset_check_partitions_def(self):
        """Dagster does not check that a spec's `partitions_def` matches its asset's, so `build_asset_check_specs` passes its `partitions_def` through without checking it either."""
        days = dg.StaticPartitionsDefinition(["mon", "tue"])
        elsewhere = dg.StaticPartitionsDefinition(["x", "y"])
        key = dg.AssetKey(["orders"])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", PreviewWarning)
            spec = dg.AssetCheckSpec("row_count", asset=key, partitions_def=elsewhere)

        @dg.asset(name="orders", partitions_def=days, check_specs=[spec])
        def orders():
            return dg.MaterializeResult(
                check_results=[dg.AssetCheckResult(check_name="row_count", passed=True)]
            )

        graph = dg.Definitions(assets=[orders]).resolve_asset_graph()

        assert graph.get(dg.AssetCheckKey(key, "row_count")).partitions_def == elsewhere
        assert graph.get(key).partitions_def == days

        evaluation = dg.materialize(
            [orders], partition_key="mon"
        ).get_asset_check_evaluations()[0]

        assert evaluation.partition == "mon"

    def test_a_blocking_asset_check_takes_a_partitions_def_and_still_stops_the_run(
        self,
    ):
        """A failing blocking check with a `partitions_def` still fails the run, which `build_asset_check_specs` needs because it gives one to the column-schema check."""
        days = dg.StaticPartitionsDefinition(["mon", "tue"])
        key = dg.AssetKey(["orders"])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", PreviewWarning)
            spec = dg.AssetCheckSpec(
                "dy_schema__columns", asset=key, blocking=True, partitions_def=days
            )

        @dg.asset(name="orders", partitions_def=days, check_specs=[spec])
        def orders():
            return dg.MaterializeResult(
                check_results=[
                    dg.AssetCheckResult(check_name="dy_schema__columns", passed=False)
                ]
            )

        result = dg.materialize([orders], partition_key="mon", raise_on_error=False)
        evaluation = result.get_asset_check_evaluations()[0]

        assert not result.success
        assert not evaluation.passed
        assert evaluation.partition == "mon"

    def test_dagster_still_rejects_a_check_name_outside_the_regex_this_package_restates(
        self,
    ):
        """Dagster quotes `^[A-Za-z0-9_]+$` when it rejects a check name, and `_rules.DAGSTER_NAME` copies that regex for `validate_namespace`."""
        key = dg.AssetKey(["orders"])

        with pytest.raises(dg.DagsterInvalidDefinitionError) as raised:

            @dg.asset(
                name="orders", check_specs=[dg.AssetCheckSpec("unit-price", asset=key)]
            )
            def orders():
                yield dg.MaterializeResult(asset_key=key)

        assert f"regex ^{DAGSTER_NAME.pattern}$" in str(raised.value)
        # Dagster names the op output, not the column, so `validate_namespace` raises first.
        assert '"orders_unit-price"' in str(raised.value)

    def test_the_deps_union_is_still_the_one_the_asset_decorator_annotates(self):
        """`dg.asset` annotates `deps` with the `CoercibleToAssetDep` union, which `_asset.py` imports to annotate `dd.asset`'s own `deps`."""
        assert (
            inspect.signature(dg.asset).parameters["deps"].annotation
            == Iterable[CoercibleToAssetDep] | None
        )

    def test_direct_invocation_is_satisfied_only_by_a_standalone_check_result(self):
        """A direct invocation raises on a check result inside a `MaterializeResult` but accepts one yielded separately, as `stream_results` yields them (ADR-0002)."""

        def spec(asset: dg.AssetKey) -> dg.AssetCheckSpec:
            return dg.AssetCheckSpec("dy_schema__columns", asset=asset)

        def result(asset: dg.AssetKey) -> dg.AssetCheckResult:
            return dg.AssetCheckResult(
                check_name="dy_schema__columns", asset_key=asset, passed=True
            )

        bundled_key = dg.AssetKey(["bundled_orders"])
        standalone_key = dg.AssetKey(["standalone_orders"])

        @dg.asset(name="bundled_orders", check_specs=[spec(bundled_key)])
        def bundled():
            yield dg.MaterializeResult(
                asset_key=bundled_key, check_results=[result(bundled_key)]
            )

        @dg.asset(name="standalone_orders", check_specs=[spec(standalone_key)])
        def standalone():
            yield dg.MaterializeResult(asset_key=standalone_key)
            yield result(standalone_key)

        with pytest.raises(dg.DagsterInvariantViolationError) as raised:
            events(bundled)

        assert "did not return an output" in str(raised.value)
        assert [type(event) for event in events(standalone)] == [
            dg.MaterializeResult,
            dg.AssetCheckResult,
        ]

    def test_a_plain_asset_still_fails_the_run_when_its_return_differs_from_its_annotation(
        self,
        tmp_path: Path,
    ):
        """`@dg.asset` fails the run on a return that does not match its annotation, the contrast `user_guide/declaring-an-asset.qmd` draws with `dd.asset`."""

        @dg.asset(name="mismatch")
        def mismatch() -> pl.DataFrame:
            return pl.LazyFrame({"order_id": ["ORD-1"]})  # pyrefly: ignore[bad-return]

        with pytest.raises(dg.DagsterTypeCheckDidNotPass) as raised:
            dg.materialize(
                [mismatch],
                resources={
                    "io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path))
                },
            )

        assert "failed type check for Dagster type DataFrame" in str(raised.value)

    def test_a_multi_asset_check_reads_its_inputs_off_an_explicit_signature(
        self, tmp_path: Path
    ):
        """`@dg.multi_asset_check` reads each input's name and type off a `__signature__` set on the function, which `_collection.build_collection_checks` sets because it receives the members only when it is called."""
        loaded: dict[str, type] = {}

        @dg.asset
        def refunds() -> pl.DataFrame:
            return pl.DataFrame({"order_id": ["ORD-1"]})

        def checks(
            context: dg.AssetCheckExecutionContext, **frames: object
        ) -> Iterator[dg.AssetCheckResult]:
            loaded.update({name: type(frame) for name, frame in frames.items()})
            yield dg.AssetCheckResult(asset_key="refunds", check_name="c", passed=True)

        checks.__signature__ = inspect.Signature([  # pyrefly: ignore[missing-attribute]
            inspect.Parameter(
                "context",
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=dg.AssetCheckExecutionContext,
            ),
            inspect.Parameter(
                "book_refunds",
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                annotation=pl.LazyFrame,
            ),
        ])
        definition = dg.multi_asset_check(
            specs=[dg.AssetCheckSpec("c", asset="refunds")],
            ins={"book_refunds": dg.AssetIn(key="refunds")},
        )(checks)

        dg.materialize(
            [refunds, definition],
            resources={"io_manager": ParquetIOManager(base_dir=str(tmp_path))},
        )

        assert loaded == {"book_refunds": pl.LazyFrame}

    def test_a_check_input_is_still_type_checked_against_its_annotation(self):
        """Dagster type-checks a `@dg.multi_asset_check` input, so `evaluate_checks` does not (`docs/out-of-scope/wiring-argument-type-guards.md`)."""
        key = dg.AssetKey(["orders"])
        ran = False

        class Lying(dg.IOManager):
            @override
            def handle_output(self, context: dg.OutputContext, obj: object) -> None: ...

            @override
            def load_input(self, context: dg.InputContext) -> object:
                return {"order_id": ["ORD-1"]}

        @dg.asset(name="orders")
        def orders() -> pl.DataFrame:
            return pl.DataFrame({"order_id": ["ORD-1"]})

        @dg.multi_asset_check(
            specs=[dg.AssetCheckSpec("dy_schema__columns", asset=key)]
        )
        def orders_checks(orders: pl.DataFrame) -> Iterator[dg.AssetCheckResult]:
            nonlocal ran
            ran = True
            yield dg.AssetCheckResult(
                check_name="dy_schema__columns", asset_key=key, passed=True
            )

        with pytest.raises(dg.DagsterTypeCheckDidNotPass) as raised:
            dg.materialize([orders, orders_checks], resources={"io_manager": Lying()})

        assert 'Type check failed for step input "orders"' in str(raised.value)
        assert not ran

    def test_materialize_result_value_still_defaults_to_a_sentinel(self):
        """`dg.MaterializeResult().value` defaults to a sentinel, not `None`, so one `isinstance` check in `_runtime._unpack_return` covers a missing and a non-frame `value`."""
        assert dg.MaterializeResult().value is not None

    def test_add_asset_metadata_takes_precedence_over_the_io_managers_column_schema(
        self, tmp_path: Path
    ):
        """`dd.asset` writes its column schema with `add_asset_metadata`, because only that takes precedence over the one `ParquetIOManager` writes."""
        declared = dg.TableSchema(
            columns=[
                dg.TableColumn(
                    "a",
                    "Int64",
                    constraints=dg.TableColumnConstraints(
                        nullable=False, other=[">= 0"]
                    ),
                )
            ]
        )

        @dg.asset
        def returned() -> dg.MaterializeResult[pl.DataFrame]:
            return dg.MaterializeResult(
                value=pl.DataFrame({"a": [1, 2, 3]}),
                metadata={"dagster/column_schema": declared},
            )

        @dg.asset
        def added(context: dg.AssetExecutionContext) -> pl.DataFrame:
            context.add_asset_metadata({"dagster/column_schema": declared})
            return pl.DataFrame({"a": [1, 2, 3]})

        result = dg.materialize(
            [returned, added],
            resources={"io_manager": ParquetIOManager(base_dir=str(tmp_path))},
        )
        recorded = {
            event.asset_key: event.step_materialization_data.materialization.metadata[
                "dagster/column_schema"
            ].value
            for event in result.get_asset_materialization_events()
        }

        assert recorded[returned.key] != declared
        assert recorded[added.key] == declared

    def test_the_current_context_is_unset_under_direct_invocation(self):
        """`dg.AssetExecutionContext.get()` raises `DagsterInvariantViolationError` in any direct invocation, which `dd.asset` catches to skip its column schema."""

        @dg.asset(name="orders")
        def with_context(context: dg.AssetExecutionContext) -> str:
            with pytest.raises(dg.DagsterInvariantViolationError):
                dg.AssetExecutionContext.get()
            return "called"

        @dg.asset(name="orders")
        def without_context() -> str:
            with pytest.raises(dg.DagsterInvariantViolationError):
                dg.AssetExecutionContext.get()
            return "called"

        assert with_context(dg.build_asset_context()) == "called"
        assert without_context() == "called"

    def test_a_built_context_still_raises_on_add_asset_metadata(self):
        """A built context's `add_asset_metadata` raises `AttributeError`, as `user_guide/hand-wiring.qmd` states."""

        @dg.asset(name="orders")
        def orders(context: dg.AssetExecutionContext) -> str:
            with pytest.raises(AttributeError):
                context.add_asset_metadata({"source": "stripe"})
            return "called"

        assert orders(dg.build_asset_context()) == "called"

    def test_a_materialization_logged_for_another_key_is_recorded_before_a_raise(
        self,
    ):
        """`log_event` records a materialization for another asset's key before a later raise, which `_error_rows.build_io_manager_writer` needs when a run raises `NoValidRowsError`."""
        key = dg.AssetKey(["orders"])

        @dg.asset(
            name="orders", check_specs=[dg.AssetCheckSpec(name="probe", asset=key)]
        )
        def orders(
            context: dg.AssetExecutionContext,
        ) -> Iterator[dg.AssetCheckResult]:
            context.log_event(dg.AssetMaterialization(asset_key="orders__error_rows"))
            yield dg.AssetCheckResult(check_name="probe", asset_key=key, passed=False)
            no_valid_rows = "every row failed"
            raise ValueError(no_valid_rows)

        result = dg.materialize([orders], raise_on_error=False)

        assert not result.success
        assert [e.asset_key for e in result.get_asset_materialization_events()] == [
            dg.AssetKey(["orders__error_rows"])
        ]

    def test_the_three_path_escapes_dagster_applies_are_still_importable(self):
        """`UPathIOManager` escapes a leading `/` but not `..`, and `_error_rows.get_error_rows_path` calls these three functions to escape both."""
        assert escape_leading_slash("/etc") == "%2Fetc"
        assert escape_leading_slash("etc") == "etc"
        assert escape_dotdot_segments("../etc") == "%2E%2E/etc"
        assert escape_dotdot_segments("my..backup") == "my..backup"
        assert coerce_to_relative_parts(UPath("/sales/orders")) == (
            "%2Fsales",
            "orders",
        )
        assert coerce_to_relative_parts(UPath("sales/orders")) == ("sales", "orders")

    def test_upath_io_manager_still_formats_a_multi_partition_key_by_dimension_name(
        self,
        tmp_path: Path,
    ):
        """`UPathIOManager` formats a multi-partition key in a closure, so `_error_rows._format_partition_key` copies it and this test compares the two paths."""
        written: dict[str, UPath] = {}

        class Probe(dg.UPathIOManager):
            extension = ".txt"

            @override
            def dump_to_path(
                self, context: dg.OutputContext, obj: str, path: UPath
            ) -> None:
                written["path"] = path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(obj)

            @override
            def load_from_path(self, context: dg.InputContext, path: UPath) -> str:
                return path.read_text()

        grid = dg.MultiPartitionsDefinition({
            "region": dg.StaticPartitionsDefinition(["eu", "us"]),
            "day": dg.StaticPartitionsDefinition(["2026-01-02"]),
        })
        key = dg.MultiPartitionKey({"region": "eu", "day": "2026-01-02"})

        @dg.asset(name="orders", partitions_def=grid)
        def orders() -> str:
            return "written"

        result = dg.materialize(
            [orders],
            partition_key=key,
            resources={"io_manager": Probe(base_path=UPath(tmp_path))},
        )
        check_and_filter = get_error_rows_path(
            dg.AssetKey(["orders__error_rows"]), tmp_path, key
        )

        assert result.success
        base = UPath(tmp_path)
        assert check_and_filter.relative_to(base / "orders__error_rows") == (
            written["path"].relative_to(base / "orders").with_suffix(".parquet")
        )


class TestPrivateDagsterApis:
    """The private Dagster APIs that `build_io_manager_writer`, `build_error_rows_writer`, `build_error_rows_spec` and `dd.asset` read."""

    def test_a_step_still_hands_over_the_output_context_and_manager_it_was_going_to_use(
        self,
        tmp_path: Path,
    ):
        """A step's `get_output_context` and `get_io_manager` return the context and IO manager of its output, which `_error_rows.build_io_manager_writer` uses."""
        copied: dict[str, Any] = {}

        @dg.asset(
            name="orders", io_manager_key="warehouse", metadata={"owner": "finance"}
        )
        def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
            step = context.get_step_execution_context()
            handle = StepOutputHandle(
                step.step.key, context.output_for_asset_key(context.asset_key)
            )
            original = step.get_output_context(handle)
            copied["metadata"] = dict(original.definition_metadata or {})
            copied["resource_config"] = original.resource_config
            copied["manager"] = type(step.get_io_manager(handle)).__name__
            return pl.DataFrame({"order_id": ["ORD-1"]})

        result = dg.materialize(
            [orders],
            resources={"warehouse": ParquetIOManager(base_dir=str(tmp_path))},
        )

        assert result.success
        assert copied["metadata"]["owner"] == "finance"
        assert copied["resource_config"] is not None
        assert copied["manager"] == "ParquetIOManager"

    def test_a_steps_run_config_still_holds_each_resources_config(self, tmp_path: Path):
        """A step's `resolved_run_config.resources` holds the config of each resource its op requires, which `_error_rows.build_io_manager_writer` reads for `error_rows_io_manager_key`."""
        database = str(tmp_path / "errors.duckdb")
        seen: dict[str, Any] = {}

        @dg.asset(name="orders", required_resource_keys={"audit"})
        def orders(context: dg.AssetExecutionContext) -> None:
            step = context.get_step_execution_context()
            seen["config"] = step.resolved_run_config.resources["audit"].config

        result = dg.materialize(
            [orders], resources={"audit": DuckDBPolarsIOManager(database=database)}
        )

        assert result.success
        assert seen["config"]["database"] == database

    def test_an_output_context_still_reads_its_resource_config_off_one_attribute(self):
        """`resource_config` returns `_resource_config`, which `_error_rows.build_io_manager_writer` sets on its copy for `error_rows_io_manager_key`."""
        original = dg.build_output_context(resource_config={"base_dir": "/warehouse"})

        repointed = copy.copy(original)
        repointed._resource_config = {"database": "errors.duckdb"}

        assert repointed.resource_config == {"database": "errors.duckdb"}
        assert original.resource_config == {"base_dir": "/warehouse"}

    def test_a_definition_still_names_the_io_manager_key_of_its_output(self):
        """`get_io_manager_key_for_asset_key` returns an output's IO manager key, which `_error_rows.build_error_rows_spec` reads off a hand-wired definition."""

        @dg.asset(name="orders", io_manager_key="warehouse")
        def orders() -> None: ...

        @dg.asset(name="refunds")
        def refunds() -> None: ...

        assert orders.get_io_manager_key_for_asset_key(orders.key) == "warehouse"
        assert refunds.get_io_manager_key_for_asset_key(refunds.key) == "io_manager"

    def test_a_directly_invoked_asset_still_has_no_step(self):
        """`get_step_execution_context` raises `DagsterInvalidPropertyError` in a direct invocation, which `_error_rows.build_error_rows_writer` catches to use `build_file_writer`."""

        @dg.asset(name="orders")
        def orders(context: dg.AssetExecutionContext) -> str:
            with pytest.raises(DagsterInvalidPropertyError):
                context.get_step_execution_context()
            return "called"

        assert orders(dg.build_asset_context()) == "called"

    def test_dagster_still_reads_the_context_parameter_off_the_first_name_alone(self):
        """`is_context_provided` reads only the first parameter's name, and `dd.asset` calls it to check whether the decorated function declares `context`."""

        def declared(context: dg.AssetExecutionContext, raw: str) -> str: ...

        def bare(raw: str) -> str: ...

        assert is_context_provided(
            list(inspect.signature(declared).parameters.values())
        )
        assert not is_context_provided(
            list(inspect.signature(bare).parameters.values())
        )
        assert not is_context_provided([])

    def test_an_output_context_still_clones_and_re_points_by_attribute(self):
        """A `copy.copy` of an `OutputContext` accepts a new `_asset_key` and keeps its own output metadata, which `_error_rows.build_io_manager_writer` reads back with `get_logged_metadata`."""
        original = dg.build_output_context(
            asset_key=dg.AssetKey(["analytics", "orders"]),
            definition_metadata={"partition_expr": "ordered_at"},
        )

        repointed = copy.copy(original)
        repointed._asset_key = dg.AssetKey(["analytics", "orders__error_rows"])
        repointed.add_output_metadata({"path": "somewhere"})

        assert repointed is not original
        assert repointed.asset_key.path == ["analytics", "orders__error_rows"]
        assert original.asset_key.path == ["analytics", "orders"]
        assert repointed.definition_metadata == original.definition_metadata
        assert "path" in repointed.get_logged_metadata()
        assert original.get_logged_metadata() == {}


class TestExecutableKeys:
    """The Dagster behaviour that `_get_executable_keys` and `build_io_manager_writer` read off the run's asset graph (ADR-0007)."""

    def test_an_in_process_run_still_has_no_repository_definition(self):
        """`repository_def` raises `CheckError` in an in-process run, which `_error_rows._get_executable_keys` catches to read the job's asset graph and `_error_rows.build_io_manager_writer` catches to log the error rows' materialization."""

        @dg.asset(name="orders")
        def orders(context: dg.AssetExecutionContext) -> str:
            with pytest.raises(CheckError):
                _ = context.repository_def
            return "ran"

        assert dg.materialize([orders]).success

    def test_a_directly_invoked_asset_still_has_no_repository_definition(self):
        """`repository_def` raises `DagsterInvalidPropertyError` in a direct invocation, which `_error_rows._get_executable_keys` catches to return no keys."""

        @dg.asset(name="orders")
        def orders(context: dg.AssetExecutionContext) -> str:
            with pytest.raises(DagsterInvalidPropertyError):
                _ = context.repository_def
            return "called"

        assert orders(dg.build_asset_context()) == "called"

    def test_the_job_graph_still_narrows_to_what_the_run_selected(self):
        """The job's asset graph holds only the selected assets, so `_error_rows._get_executable_keys` reads it only when `repository_def` raises."""
        seen: dict[str, set[str]] = {}

        @dg.asset(name="orders")
        def orders(context: dg.AssetExecutionContext) -> str:
            graph = context.job_def.asset_layer.asset_graph
            seen[context.run.run_id] = {
                key.to_user_string() for key in graph.executable_asset_keys
            }
            return "ran"

        @dg.asset(name="orders__error_rows")
        def orders_error_rows() -> str:
            return "ran"

        whole = dg.materialize([orders, orders_error_rows])
        subset = dg.materialize([orders, orders_error_rows], selection=[orders])

        assert seen[whole.run_id] == {"orders", "orders__error_rows"}
        assert seen[subset.run_id] == {"orders"}

    def test_a_bare_spec_is_still_unexecutable_beside_its_asset(self):
        """Dagster marks a bare `dg.AssetSpec` unexecutable, which `validate_error_rows_key` and `_error_rows.build_io_manager_writer` both read."""
        key = dg.AssetKey(["orders"])

        @dg.asset(name="orders")
        def orders() -> str:
            return "ran"

        graph = (
            dg
            .Definitions(
                assets=[
                    orders,
                    dg.AssetSpec(key=dg.AssetKey(["orders__error_rows"]), deps=[key]),
                ]
            )
            .get_repository_def()
            .asset_graph
        )

        assert graph.executable_asset_keys == {key}
        assert graph.unexecutable_asset_keys == {dg.AssetKey(["orders__error_rows"])}
