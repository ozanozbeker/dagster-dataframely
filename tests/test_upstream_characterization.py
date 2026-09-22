"""Characterization tests for the upstream behaviour this package depends on.

Each docstring names the upstream behaviour and the code here that uses it, so a failure shows what upstream changed.
"""

import copy
import datetime as dt
import inspect
import warnings
from collections.abc import Iterable, Iterator
from pathlib import Path
from typing import Any, override

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
from dagster_polars import PolarsParquetIOManager
from dagster_shared.utils.warnings import PreviewWarning
from dataframely._rule import Rule, RuleFactory
from upath import UPath

from dagster_dataframely._rules import DAGSTER_NAME
from dagster_dataframely.wiring import quarantine_path
from tests.scenario import events


class Orders(dy.Schema):
    """A schema with a column rule, a primary key and a `@dy.rule()`."""

    order_id = dy.String(primary_key=True)
    amount = dy.Float64(nullable=False, min=0.0)
    status = dy.Enum(["new", "paid"], nullable=False)

    @dy.rule()
    def paid_orders_have_amount(cls) -> pl.Expr:
        """Paid orders must carry a positive amount."""
        return (cls.status.col != "paid") | (cls.amount.col > 0)


# One valid row, then rows that fail `amount|min`, `paid_orders_have_amount` and `primary_key`.
_MIXED_ORDERS = pl.DataFrame({
    "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-3"],
    "amount": [10.0, -1.0, 0.0, 5.0],
    "status": pl.Series(["new", "new", "paid", "new"], dtype=pl.Enum(["new", "paid"])),
})


def test_rules_are_keyed_by_pipe_delimited_rule_name():
    """`Schema._validation_rules` keys a column rule as `<column>|<rule>`, and `_rules.described_rules` reads every rule from it."""
    rules = Orders._validation_rules(with_cast=False)

    assert "amount|min" in rules
    assert "primary_key" in rules
    assert "paid_orders_have_amount" in rules
    assert all(name.split("|")[0] in Orders.columns() for name in rules if "|" in name)


def test_rule_values_are_rule_instances_carrying_a_polars_expr():
    """Each `_validation_rules` value is a `Rule` with a `pl.Expr` on `expr`, which `_rules.DescribedRule` stores and `_checks` writes to check metadata."""
    rules = Orders._validation_rules(with_cast=False)

    assert all(isinstance(rule, Rule) for rule in rules.values())
    assert all(isinstance(rule.expr, pl.Expr) for rule in rules.values())


def test_a_dy_rule_stays_reachable_by_name_and_keeps_its_docstring():
    """A `@dy.rule()` leaves a `RuleFactory` on the class, and `_rules._described_rule` reads the rule's description from its `validation_fn`."""
    factory = Orders.paid_orders_have_amount

    assert isinstance(factory, RuleFactory)
    assert factory.validation_fn.__doc__ == "Paid orders must carry a positive amount."


def test_a_column_rule_is_not_reachable_by_name():
    """No class attribute has a column rule's name, and `primary_key` names a method, so `_rules._described_rule` checks for a `RuleFactory`."""
    assert getattr(Orders, "amount|min", None) is None

    assert not isinstance(getattr(Orders, "primary_key", None), RuleFactory)


def test_a_constraint_rule_is_named_after_the_parameter_that_declared_it():
    """Each constraint's rule has its parameter's name, and the column stores the value under that name, which `_rendering._value` reads with `getattr`."""
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


def test_length_bounds_are_declared_on_exactly_string_and_list():
    """Only `dy.String` (bytes) and `dy.List` (elements) take length bounds, so `_rendering._length_unit` checks for `dy.List` alone."""
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
        dy.List(dy.String(), max_length=3).validation_rules(pl.col("a"))["max_length"]
    )


def test_a_struct_emits_one_inner_rule_per_constrained_field():
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


def test_a_list_and_an_array_still_reach_their_element_column_through_inner():
    """`dy.List` and `dy.Array` keep the element column on `inner` and name its rules `inner_<rule>`, so `_rendering._column_constraint` renders `inner_min` as `elements >= 1`."""
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


def test_a_float_column_forbids_inf_and_nan_by_default_and_drops_the_rules_when_allowed():
    """A float column has `inf` and `nan` rules until `allow_inf=True` and `allow_nan=True` remove them, so `_rendering.column_constraints` leaves both out of the Columns tab."""
    assert not dy.Float64().allow_inf
    assert not dy.Float64().allow_nan
    assert {"inf", "nan"} <= set(dy.Float64().validation_rules(pl.col("a")))
    assert not {"inf", "nan"} & set(
        dy.Float64(allow_inf=True, allow_nan=True).validation_rules(pl.col("a"))
    )


def test_validate_dtype_still_decides_on_the_dtype_alone_down_to_the_time_unit():
    """The undocumented `dy.Column.validate_dtype` rejects another `Datetime` time unit or time zone, and `_checks._column_schema_problems` calls it."""
    assert dy.Int64().validate_dtype(pl.Int64)
    assert not dy.Int64().validate_dtype(pl.Int32)

    assert dy.Datetime().dtype == pl.Datetime("us")
    assert dy.Datetime().validate_dtype(pl.Datetime("us"))
    assert not dy.Datetime().validate_dtype(pl.Datetime("ns"))
    assert not dy.Datetime().validate_dtype(pl.Datetime("us", "UTC"))

    assert dy.Any().validate_dtype(pl.Int64)
    assert dy.Any().validate_dtype(pl.String)


def test_filter_projects_a_superset_to_the_schemas_columns_in_order():
    """`Schema.filter` keeps only the schema's columns, in the schema's order, as `user_guide/the-failure-policy.qmd` states and Dataframely does not document."""
    superset = _MIXED_ORDERS.select(
        "status", "amount", pl.lit("note").alias("shipping"), "order_id"
    )
    expected = list(Orders.columns())

    valid, _ = Orders.filter(superset, cast=False)
    assert valid.columns == expected

    lazy_valid, _ = Orders.filter(superset.lazy(), cast=False)
    assert lazy_valid.collect_schema().names() == expected


def test_a_lazy_filter_still_defers_to_collect_all_and_forwards_the_engine():
    """On a `LazyFrame`, `Schema.filter` returns a result whose `collect_all` passes `engine=` to Polars, which `_checks.filtered` calls with `engine="streaming"`."""
    lazy_result = Orders.filter(_MIXED_ORDERS.lazy(), cast=False)

    assert not hasattr(Orders.filter(_MIXED_ORDERS, cast=False), "collect_all")

    valid, failure = lazy_result.collect_all(engine="streaming")

    assert isinstance(valid, pl.DataFrame)
    assert isinstance(failure, dy.FailureInfo)
    assert valid.height == 1
    assert len(failure) == 3

    # Polars raises this error, so `collect_all` passes `engine=` through to Polars.
    with pytest.raises(ValueError, match="Invalid engine argument"):
        lazy_result.collect_all(engine="nonsense")


def test_details_returns_invalid_rows_plus_one_column_per_rule():
    """`FailureInfo.details()` returns the invalid rows plus one rule column per rule, which `_runtime.quarantine_frame` and `_checks._failed_rows` both read."""
    _, failure = Orders.filter(_MIXED_ORDERS)
    details = failure.details()

    assert details.height == failure.invalid().height
    assert set(details.columns) == set(Orders.columns()) | set(
        Orders._validation_rules(with_cast=False)
    )

    # An `Enum`, which `quarantine_frame` casts to `String` because the Delta writer panics on it.
    assert details.schema["amount|min"] == pl.Enum(["valid", "invalid", "unknown"])
    assert (
        details.filter(pl.col("order_id") == "ORD-2")["amount|min"].item() == "invalid"
    )


def test_the_failure_example_limit_does_not_truncate_the_filter_path():
    """`dy.Config(max_failure_examples=)` limits only the message `validate` builds, so only `max_failure_samples` limits the rows in check metadata."""
    with dy.Config(max_failure_examples=1):
        _, failure = Orders.filter(_MIXED_ORDERS)

    assert len(failure) == 3
    assert failure.details().height == 3
    assert failure.counts()["primary_key"] == 2


def test_cooccurrence_counts_are_keyed_by_a_frozenset_of_rule_names():
    """`FailureInfo.cooccurrence_counts()` keys each count by a `frozenset` of rule names, which `_runtime._cooccurrence` sorts before rendering."""
    _, failure = Orders.filter(_MIXED_ORDERS)
    cooccurrence = failure.cooccurrence_counts()

    assert all(isinstance(rules, frozenset) for rules in cooccurrence)
    assert sum(cooccurrence.values()) == len(failure)
    assert cooccurrence[frozenset({"amount|min"})] == 1


def test_polars_renders_a_duration_in_its_own_friendly_style():
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


def test_a_partitioned_asset_check_spec_is_still_in_preview():
    """`AssetCheckSpec(partitions_def=)` warns `PreviewWarning` and nothing else, and #31 waits until Dagster removes that warning."""
    # The whole list, not `pytest.warns`, so a failure distinguishes `[]` (GA) from `[BetaWarning]`.
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        dg.AssetCheckSpec(
            "row_count",
            asset=dg.AssetKey(["orders"]),
            partitions_def=dg.StaticPartitionsDefinition(["a"]),
        )

    assert [warning.category for warning in recorded] == [PreviewWarning]


def test_dagster_does_not_enforce_a_matching_asset_check_partitions_def():
    """Dagster does not check that a spec's `partitions_def` matches its asset's, so #31 can pass one through `check_specs` without checking it either."""
    days = dg.StaticPartitionsDefinition(["mon", "tue"])
    elsewhere = dg.StaticPartitionsDefinition(["x", "y"])
    key = dg.AssetKey(["orders"])

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", PreviewWarning)
        spec = dg.AssetCheckSpec("row_count", asset=key, partitions_def=elsewhere)

    # Unannotated: with `check_specs`, `-> None` raises `Expected Tuple annotation for multiple outputs`.
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


def test_a_blocking_asset_check_takes_a_partitions_def_and_still_stops_the_run():
    """A failing blocking check with a `partitions_def` still fails the run, which #31 needs because it gives one to the column-schema check."""
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


def test_dagster_still_rejects_a_check_name_outside_the_regex_this_package_restates():
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


def test_the_deps_union_is_still_the_one_the_asset_decorator_annotates():
    """`dg.asset` annotates `deps` with the `CoercibleToAssetDep` union, which `_asset.py` imports to annotate `dd.asset`'s own `deps`."""
    assert (
        inspect.signature(dg.asset).parameters["deps"].annotation
        == Iterable[CoercibleToAssetDep] | None
    )


def test_direct_invocation_is_satisfied_only_by_a_standalone_check_result():
    """A direct invocation raises on a check result inside a `MaterializeResult` but accepts one yielded separately, as `validation_results` yields them (ADR-0002)."""

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


def test_a_plain_asset_still_fails_the_run_when_its_return_annotation_disagrees(
    tmp_path: Path,
):
    """`@dg.asset` fails the run on a return that does not match its annotation, the contrast `user_guide/declaring-an-asset.qmd` draws with `dd.asset`."""

    @dg.asset(name="mismatch")
    def mismatch() -> pl.DataFrame:
        return pl.LazyFrame({"order_id": ["ORD-1"]})  # pyrefly: ignore[bad-return]

    with pytest.raises(dg.DagsterTypeCheckDidNotPass) as raised:
        dg.materialize(
            [mismatch],
            resources={"io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path))},
        )

    assert "failed type check for Dagster type DataFrame" in str(raised.value)


def test_a_check_input_is_still_type_checked_against_its_annotation():
    """Dagster type-checks a `@dg.multi_asset_check` input, so `check_results` does not (`docs/out-of-scope/wiring-argument-type-guards.md`)."""
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

    @dg.multi_asset_check(specs=[dg.AssetCheckSpec("dy_schema__columns", asset=key)])
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


def test_materialize_result_value_still_defaults_to_a_sentinel():
    """`dg.MaterializeResult().value` defaults to a sentinel, not `None`, so one `isinstance` check in `_returns.frame_and_result` covers a missing and a non-frame `value`."""
    assert dg.MaterializeResult().value is not None


def test_replace_still_skips_the_constructors_normalization_and_keeps_every_other_field():
    """`_replace` skips the constructor's normalization and keeps every field the caller does not pass, and `_runtime._addressed` and `_returns.with_returned_fields` call it."""
    check = dg.AssetCheckResult(
        check_name="dy_schema__columns",
        asset_key=dg.AssetKey(["orders"]),
        passed=False,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={"rows": 3},
    )
    result = dg.MaterializeResult(
        asset_key=dg.AssetKey(["orders"]), metadata={"rows": 3}, tags={"team": "sales"}
    )

    assert isinstance(check, tuple)
    assert isinstance(result, tuple)
    assert check.metadata == {"rows": dg.MetadataValue.int(3)}

    # A raw string on purpose: the assertion below shows `_replace` does not wrap it.
    readdressed = check._replace(
        metadata={"dataframely/quarantine_address": "here"}  # pyrefly: ignore[bad-assignment]
    )
    rebuilt = result._replace(metadata={"dagster/row_count": 1})

    assert readdressed.metadata == {"dataframely/quarantine_address": "here"}
    assert readdressed.severity == dg.AssetCheckSeverity.ERROR
    assert not readdressed.passed
    assert readdressed.check_name == "dy_schema__columns"
    assert rebuilt.tags == {"team": "sales"}
    assert rebuilt.asset_key == result.asset_key
    assert rebuilt.value is result.value


def test_dagster_polars_writes_its_own_row_count_over_the_steps(tmp_path: Path):
    """`PolarsParquetIOManager` writes its own `dagster/row_count` over the step's, so tests assert this package's row count on what the step yields."""

    @dg.asset
    def counted() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=pl.DataFrame({"a": [1, 2, 3]}), metadata={"dagster/row_count": 999}
        )

    result = dg.materialize(
        [counted],
        resources={"io_manager": PolarsParquetIOManager(base_dir=str(tmp_path))},
    )
    recorded = next(
        event.step_materialization_data.materialization.metadata
        for event in result.get_asset_materialization_events()
    )

    assert result.success
    assert recorded["dagster/row_count"].value == 3


def test_the_three_path_escapes_dagster_applies_are_still_importable():
    """`UPathIOManager` escapes a leading `/` but not `..`, and `_quarantine.quarantine_path` calls these three functions to escape both."""
    assert escape_leading_slash("/etc") == "%2Fetc"
    assert escape_leading_slash("etc") == "etc"
    assert escape_dotdot_segments("../etc") == "%2E%2E/etc"
    assert escape_dotdot_segments("my..backup") == "my..backup"
    assert coerce_to_relative_parts(UPath("/sales/orders")) == ("%2Fsales", "orders")
    assert coerce_to_relative_parts(UPath("sales/orders")) == ("sales", "orders")


def test_upath_io_manager_still_formats_a_multi_partition_key_by_dimension_name(
    tmp_path: Path,
):
    """`UPathIOManager` formats a multi-partition key in a closure, so `_quarantine._formatted_partition_key` copies it and this test compares the two paths."""
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
    quarantined = quarantine_path(dg.AssetKey(["orders"]), tmp_path, key)

    assert result.success
    base = UPath(tmp_path)
    assert quarantined.relative_to(base / "orders_quarantine") == (
        written["path"].relative_to(base / "orders").with_suffix(".parquet")
    )


# --- The private Dagster APIs ADR-0006 lists ---
def test_a_step_still_hands_over_the_output_context_and_manager_it_was_going_to_use(
    tmp_path: Path,
):
    """A step's `get_output_context` and `get_io_manager` return the context and IO manager of its output, which `_quarantine.delegating_writer` uses."""
    borrowed: dict[str, Any] = {}

    @dg.asset(name="orders", io_manager_key="warehouse", metadata={"owner": "finance"})
    def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
        step = context.get_step_execution_context()
        (output_name,) = [
            name
            for name, key in context.assets_def.keys_by_output_name.items()
            if key == context.asset_key
        ]
        handle = StepOutputHandle(step.step.key, output_name)
        original = step.get_output_context(handle)
        borrowed["metadata"] = dict(original.definition_metadata or {})
        borrowed["resource_config"] = original.resource_config
        borrowed["manager"] = type(step.get_io_manager(handle)).__name__
        return pl.DataFrame({"order_id": ["ORD-1"]})

    result = dg.materialize(
        [orders],
        resources={"warehouse": PolarsParquetIOManager(base_dir=str(tmp_path))},
    )

    assert result.success
    assert borrowed["metadata"]["owner"] == "finance"
    assert borrowed["resource_config"] is not None
    assert borrowed["manager"] == "PolarsParquetIOManager"


def test_a_directly_invoked_asset_still_has_no_step():
    """`get_step_execution_context` raises `DagsterInvalidPropertyError` in a direct invocation, which `_quarantine.quarantine_writer` catches to use `file_writer`."""

    @dg.asset(name="orders")
    def orders(context: dg.AssetExecutionContext) -> str:
        with pytest.raises(DagsterInvalidPropertyError):
            context.get_step_execution_context()
        return "called"

    assert orders(dg.build_asset_context()) == "called"


def test_keys_by_output_name_still_omits_the_check_outputs():
    """`keys_by_output_name` omits check outputs, so `_quarantine.delegating_writer` unpacks the asset's one output name from it."""
    key = dg.AssetKey(["orders"])

    @dg.asset(name="orders", check_specs=[dg.AssetCheckSpec(name="probe", asset=key)])
    def orders():
        yield dg.MaterializeResult(asset_key=key)
        yield dg.AssetCheckResult(check_name="probe", asset_key=key, passed=True)

    assert orders.keys_by_output_name == {"result": key}
    assert len(orders.op.output_defs) == 2


def test_dagster_still_reads_the_context_parameter_off_the_first_name_alone():
    """`is_context_provided` reads only the first parameter's name, and `dd.asset` calls it to check whether the decorated function declares `context`."""

    def declared(context: dg.AssetExecutionContext, raw: str) -> str: ...

    def bare(raw: str) -> str: ...

    assert is_context_provided(list(inspect.signature(declared).parameters.values()))
    assert not is_context_provided(list(inspect.signature(bare).parameters.values()))
    assert not is_context_provided([])


def test_an_output_context_still_clones_and_re_points_by_attribute():
    """A `copy.copy` of an `OutputContext` accepts a new `_asset_key` and has its own output metadata, which `_quarantine.delegating_writer` depends on."""
    original = dg.build_output_context(
        asset_key=dg.AssetKey(["analytics", "orders"]),
        definition_metadata={"partition_expr": "ordered_at"},
    )

    repointed = copy.copy(original)
    repointed._asset_key = dg.AssetKey(["analytics", "orders_quarantine"])
    repointed.add_output_metadata({"path": "somewhere"})

    assert repointed is not original
    assert repointed.asset_key.path == ["analytics", "orders_quarantine"]
    assert original.asset_key.path == ["analytics", "orders"]
    assert repointed.definition_metadata == original.definition_metadata
    assert "path" in repointed.get_logged_metadata()
    assert original.get_logged_metadata() == {}


# --- What validate_quarantine_key reads (ADR-0007) ---
def test_an_in_process_run_still_has_no_repository_definition():
    """`repository_def` raises `CheckError` in an in-process run, which `_quarantine._executable_keys` catches to read the job's asset graph instead."""

    @dg.asset(name="orders")
    def orders(context: dg.AssetExecutionContext) -> str:
        with pytest.raises(CheckError):
            _ = context.repository_def
        return "ran"

    assert dg.materialize([orders]).success


def test_a_directly_invoked_asset_still_has_no_repository_definition():
    """`repository_def` raises `DagsterInvalidPropertyError` in a direct invocation, which `_quarantine._executable_keys` catches to return no keys."""

    @dg.asset(name="orders")
    def orders(context: dg.AssetExecutionContext) -> str:
        with pytest.raises(DagsterInvalidPropertyError):
            _ = context.repository_def
        return "called"

    assert orders(dg.build_asset_context()) == "called"


def test_the_job_graph_still_narrows_to_what_the_run_selected():
    """The job's asset graph holds only the selected assets, so `_quarantine._executable_keys` reads it only when `repository_def` raises."""
    seen: dict[str, set[str]] = {}

    @dg.asset(name="orders")
    def orders(context: dg.AssetExecutionContext) -> str:
        graph = context.job_def.asset_layer.asset_graph
        seen[context.run.run_id] = {
            key.to_user_string() for key in graph.executable_asset_keys
        }
        return "ran"

    @dg.asset(name="orders_quarantine")
    def sibling() -> str:
        return "ran"

    whole = dg.materialize([orders, sibling])
    subset = dg.materialize([orders, sibling], selection=[orders])

    assert seen[whole.run_id] == {"orders", "orders_quarantine"}
    assert seen[subset.run_id] == {"orders"}


def test_a_bare_spec_is_still_unexecutable_beside_its_asset():
    """Dagster marks a bare `dg.AssetSpec` unexecutable, so the spec from `quarantine_spec` is not among the keys `validate_quarantine_key` checks."""
    key = dg.AssetKey(["orders"])

    @dg.asset(name="orders")
    def orders() -> str:
        return "ran"

    graph = (
        dg
        .Definitions(
            assets=[
                orders,
                dg.AssetSpec(key=dg.AssetKey(["orders_quarantine"]), deps=[key]),
            ]
        )
        .get_repository_def()
        .asset_graph
    )

    assert graph.executable_asset_keys == {key}
    assert graph.unexecutable_asset_keys == {dg.AssetKey(["orders_quarantine"])}
