"""Characterization tests for the upstream APIs this package takes a hard dependency on.

These test upstream, not this package. Each one covers a shape that is private, unexported, or undocumented. Each carries a comment naming the decision that took the dependency, so a failure reads as "Dataframely changed" rather than "something broke".
"""

import copy
import datetime as dt
import inspect
import warnings
from pathlib import Path
from typing import Any, override

import dagster as dg
import dataframely as dy
import polars as pl
import pytest
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

from dagster_dataframely.wiring import quarantine_path


class Orders(dy.Schema):
    """The smallest schema carrying all three rule shapes: a column rule, a schema-level primary key, and a `@dy.rule`."""

    order_id = dy.String(primary_key=True)
    amount = dy.Float64(nullable=False, min=0.0)
    status = dy.Enum(["new", "paid"], nullable=False)

    @dy.rule()
    def paid_orders_have_amount(cls) -> pl.Expr:
        """Paid orders must carry a positive amount."""
        return (cls.status.col != "paid") | (cls.amount.col > 0)


# One clean row, then one row per rule shape: `amount|min`, `paid_orders_have_amount`, and a `primary_key` duplicate pair.
_MIXED_ORDERS = pl.DataFrame(
    {
        "order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-3"],
        "amount": [10.0, -1.0, 0.0, 5.0],
        "status": pl.Series(
            ["new", "new", "paid", "new"], dtype=pl.Enum(["new", "paid"])
        ),
    }
)


def test_rules_are_keyed_by_pipe_delimited_rule_name():
    """`Schema._validation_rules(with_cast=False)` still returns rule objects keyed by rule name."""
    # #17 derives one asset check per rule from this dict, statically from the schema, and rewrites `|` to `__` to get the check name.
    # `_validation_rules` is private and has no public equivalent: `validate()` flattens per-rule detail into one error string, which nothing can derive a check from.
    rules = Orders._validation_rules(with_cast=False)

    assert "amount|min" in rules  # column rule: `<column>|<rule>`
    assert "primary_key" in rules  # schema-level rule: a bare name
    assert "paid_orders_have_amount" in rules  # `@dy.rule`: the method name
    assert all(name.split("|")[0] in Orders.columns() for name in rules if "|" in name)


def test_rule_values_are_rule_instances_carrying_a_polars_expr():
    """The values of `_validation_rules` are still `dataframely._rule.Rule` objects with a `pl.Expr` on `expr`."""
    # `naming.py` types against `Rule`, and every check's `dy_rule__expr` metadata is `str(rule.expr)`, so the value side of the dict matters as much as the key side.
    rules = Orders._validation_rules(with_cast=False)

    assert all(isinstance(rule, Rule) for rule in rules.values())
    assert all(isinstance(rule.expr, pl.Expr) for rule in rules.values())


def test_a_dy_rule_stays_reachable_by_name_and_keeps_its_docstring():
    """A `@dy.rule()` still leaves a `RuleFactory` on the class whose `validation_fn` carries the decorated function's docstring."""
    # `naming.rule_description` reads a check's description off this. The metaclass builds a `Rule` for validation, but the `RuleFactory` is what survives on the class with the docstring attached, and there is no public route to it.
    factory = Orders.paid_orders_have_amount

    assert isinstance(factory, RuleFactory)
    assert factory.validation_fn.__doc__ == "Paid orders must carry a positive amount."


def test_a_column_rule_is_not_reachable_by_name():
    """A `|`-delimited column rule still misses the `getattr` lookup, so descriptions fall through to the rule-name fallback."""
    # `naming.rule_description` looks every rule up by name. Column rules are generated from column arguments and have no function to document, and the `|` is what makes the lookup miss without needing a branch.
    assert getattr(Orders, "amount|min", None) is None

    # `primary_key` is the reason that function tests `isinstance` rather than truthiness: it collides with `Schema.primary_key`, so the lookup hits a bound method whose docstring belongs to Dataframely rather than to any rule.
    assert not isinstance(getattr(Orders, "primary_key", None), RuleFactory)


def test_a_constraint_rule_is_named_after_the_parameter_that_declared_it():
    """Every value-carrying constraint still generates a rule named after its column parameter, and still keeps the value on an attribute of the same name."""
    # #20 renders the constraint from that value, so it reads the attribute by the rule's own name rather than carrying a second table mapping one to the other. The mixins holding these attributes are private, which is why the regularity is covered by a test rather than typed against.
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
    """`min_length` / `max_length` are still parameters of `dy.String` and `dy.List` and of no other column type, and each still counts something different."""
    # #20 dispatches the constraint's unit on the column type, because the parameter name cannot distinguish them: `String` measures bytes and `List` measures elements. A third column type growing the parameter would render silently in whichever unit it is not.
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
    """A `dy.Struct` still generates its fields' rules as `inner_<field>_<rule>`, under the struct column's own name."""
    # #21 collapses a schema's checks by column, and a struct is where that matters most: every field's rules land on one column, so a ten-field struct is ten checks at `rule` granularity and one at `column`. The `<column>|inner_...` spelling is what puts them there, and a flatter naming upstream would scatter them across columns that do not exist.
    address = dy.Struct(
        {
            "city": dy.String(nullable=False),
            "postcode": dy.String(nullable=True),
            "number": dy.Int32(nullable=False, min=1),
        }
    )

    assert set(address.validation_rules(pl.col("address"))) == {
        # The struct's own rule sits beside its fields', which is the whole reason one check per column can hold them all.
        "nullability",
        "inner_city_nullability",
        "inner_number_nullability",
        "inner_number_min",
    }


def test_a_float_column_forbids_inf_and_nan_by_default_and_drops_the_rules_when_allowed():
    """`allow_inf` and `allow_nan` still default to `False`, and still generate their rules only at that default."""
    # #20 draws the line for defaulted constraints here: an `inf` constraint on every float column would state something no author asked for, and it could never state anything else, because allowing the value removes the rule instead of inverting it.
    assert not dy.Float64().allow_inf
    assert not dy.Float64().allow_nan
    assert {"inf", "nan"} <= set(dy.Float64().validation_rules(pl.col("a")))
    assert not {"inf", "nan"} & set(
        dy.Float64(allow_inf=True, allow_nan=True).validation_rules(pl.col("a"))
    )


def test_details_returns_invalid_rows_plus_one_column_per_rule():
    """`FailureInfo.details()` still returns the invalid rows plus one column for each rule."""
    # #19 builds the quarantine frame straight off `details()`: original columns untouched, rule columns renamed into the reserved namespace.
    # #24 filters the same frame on the same vocabulary, one rule at a time, to sample the rows each rule rejected.
    # Guide-documented and upstream-tested, but absent from Dataframely's API reference.
    _, failure = Orders.filter(_MIXED_ORDERS)
    details = failure.details()

    assert details.height == failure.invalid().height
    assert set(details.columns) == set(Orders.columns()) | set(
        Orders._validation_rules(with_cast=False)
    )

    # #19 casts the rule columns to String because a raw Enum panics the Delta writer.
    # This dtype is the whole reason that cast is mandatory rather than defensive, so it is covered alongside the vocabulary it carries.
    assert details.schema["amount|min"] == pl.Enum(["valid", "invalid", "unknown"])
    assert (
        details.filter(pl.col("order_id") == "ORD-2")["amount|min"].item() == "invalid"
    )


def test_the_failure_example_limit_does_not_truncate_the_filter_path():
    """`dy.Config`'s `max_failure_examples` still governs only the message `validate` builds, and nothing `filter` returns."""
    # #24 bounds the sample of failing rows in check metadata with the package's own setting. That is only a bound worth having if Dataframely is not already applying one, and only package-owned if a project that tightened this one still gets the sample it asked this package for.
    # Documented as "examples to include in failure messages", which says where it applies but not where it does not.
    with dy.Config(max_failure_examples=1):
        _, failure = Orders.filter(_MIXED_ORDERS)

    assert len(failure) == 3
    assert failure.details().height == 3
    assert failure.counts()["primary_key"] == 2


def test_cooccurrence_counts_are_keyed_by_a_frozenset_of_rule_names():
    """`FailureInfo.cooccurrence_counts()` still returns counts keyed by the set of rules a row broke together."""
    # #19 emits this as the quarantine's `cooccurrence` metadata, which is what makes one broken upstream field tripping three rules at once visible as one row rather than as three unrelated counts.
    # Documented upstream, but the key type is the part that matters: a `frozenset` is unordered, so the package sorts it before rendering and a tuple here would silently change that rendering.
    _, failure = Orders.filter(_MIXED_ORDERS)
    cooccurrence = failure.cooccurrence_counts()

    assert all(isinstance(rules, frozenset) for rules in cooccurrence)
    assert sum(cooccurrence.values()) == len(failure)
    assert cooccurrence[frozenset({"amount|min"})] == 1


def test_polars_renders_a_duration_in_its_own_friendly_style():
    """`dt.to_string("polars")` still renders a duration the way a Polars frame repr does: `8d`, `1m 30s`, `2h 5m`, a sign on every part of a negative one, and a null left null."""
    # #23 renders every duration cell of the temporal statistics table through it. The ticket specifies Polars' own style and calls it unreachable, which was true before Polars 1.14 added `format="polars"`; the package's floor is well past that, so the rendering is upstream's rather than fifteen lines of this package's.
    # Documented as "the same form seen in the frame repr", which is a repr and therefore restyleable without anybody upstream calling it a break. That is what makes it worth characterizing: the four decisions below are the ones the tables state.
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

    # The two routes the ticket ruled out, covered so a change to either is visible: the default is ISO-8601, and the obvious cast raises rather than falling back to one.
    assert spans.dt.to_string().to_list()[0] == "P8D"
    with pytest.raises(pl.exceptions.InvalidOperationError):
        spans.cast(pl.String)


def test_a_partitioned_asset_check_spec_is_still_in_preview():
    """`AssetCheckSpec(partitions_def=)` still warns `PreviewWarning`, and still warns nothing else."""
    # #31 waits on this warning disappearing entirely. It is the ticket's only trigger: `tests/test_partitions.py` pins the symptoms of not using the parameter, and those hold whether it is preview or GA, so nothing else in the suite notices upstream promoting it.
    # The category is asserted exactly rather than through `pytest.warns`, because the three outcomes have to be told apart and `pytest.warns` reports two of them identically as "DID NOT WARN". An empty list is GA and the parameter can be adopted; a `BetaWarning` is the stage in between and cannot, because beta still permits "behavior changes in patch releases", which is the risk the deferral rests on.
    # `dagster_shared` is Dagster's own vendored utilities rather than public API, so the category has no exported spelling.
    with warnings.catch_warnings(record=True) as recorded:
        warnings.simplefilter("always")
        dg.AssetCheckSpec(
            "row_count",
            asset=dg.AssetKey(["orders"]),
            partitions_def=dg.StaticPartitionsDefinition(["a"]),
        )

    assert [warning.category for warning in recorded] == [PreviewWarning]


def test_dagster_does_not_enforce_a_matching_asset_check_partitions_def():
    """A spec whose `partitions_def` differs from its asset's is still accepted, through a resolved asset graph and through a run."""
    # Upstream states the constraint in the parameter's own docstring: "Must be either None or the same as the PartitionsDefinition of the asset specified by `asset`." Nothing checks it, at construction, at attach, at definition load, or at run.
    # #31 would forward a caller's `partitions_def` through `check_specs` without validating it, because that function takes an asset key and a key carries no partitioning. That is only safe while this holds: a release that starts enforcing the constraint would raise on hand-wired call sites written against it.
    days = dg.StaticPartitionsDefinition(["mon", "tue"])
    elsewhere = dg.StaticPartitionsDefinition(["x", "y"])
    key = dg.AssetKey(["orders"])

    with warnings.catch_warnings():
        # The warning is the subject of the test above, not of this one, and letting it through would put a line in every run's warnings summary.
        warnings.simplefilter("ignore", PreviewWarning)
        spec = dg.AssetCheckSpec("row_count", asset=key, partitions_def=elsewhere)

    # No return annotation, unlike the assets above: one carrying `check_specs` infers its outputs from the annotation, so `-> None` fails with `Expected Tuple annotation for multiple outputs`.
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

    # The stamp is the step's partition key and never the spec's own definition, which is what makes a mismatch inert in storage: `mon` is not a member of `elsewhere`.
    assert evaluation.partition == "mon"


def test_a_blocking_asset_check_takes_a_partitions_def_and_still_stops_the_run():
    """A blocking check carrying a `partitions_def` still stamps its partition, and a failing one still ends the run."""
    # #31 passes the partitions_def to every spec `check_specs` builds, the column-schema check included. That one is worth proving rather than assuming: it is the only spec this package marks `blocking=True`, and a preview parameter that quietly disarmed the gate would let a frame whose columns do not match reach the table.
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


def _invoked(asset: dg.AssetsDefinition) -> list[object]:
    """Call an asset directly and drain what comes back.

    `AssetsDefinition.__call__` is annotated `-> object`, because a direct call hands back whatever the body returns. Both assets below are generators, which is what the ignore asserts.
    """
    return list(asset())  # pyrefly: ignore[bad-argument-type]


def test_direct_invocation_is_satisfied_only_by_a_standalone_check_result():
    """Calling an asset directly still refuses a check result bundled onto a `MaterializeResult`, and still accepts the same result yielded standalone."""

    # #72 unbundles every check result for exactly this reason (ADR-0002). A run flattens the two forms into one event stream, so nothing else in the suite can tell them apart, and the bundled form is what made a decorated asset untestable by calling it.
    # Undocumented: direct invocation is Dagster's own documented unit-testing path, but nothing says a bundled check leaves its output unsatisfied. The error names an output name no user wrote.
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
        _invoked(bundled)

    assert "did not return an output" in str(raised.value)
    assert [type(event) for event in _invoked(standalone)] == [
        dg.MaterializeResult,
        dg.AssetCheckResult,
    ]


def test_a_plain_asset_still_fails_the_run_when_its_return_annotation_disagrees(
    tmp_path: Path,
):
    """`@dg.asset` still infers the output's `dagster_type` from the return annotation and still fails the run when the returned object does not match it."""

    # #77 documents that `dy_asset` does the opposite, and the claim is only worth making while this half of the contrast holds. The decorator cannot follow: `dagster_type` describes what the asset stores, and validation is eager, so the asset holds a `DataFrame` however the decorated function arrived at it.
    # Undocumented as a contrast, though each half is documented alone. What a reader carries over from `@dg.asset` is exactly the expectation this breaks.
    @dg.asset(name="mismatch")
    def mismatch() -> pl.DataFrame:
        return pl.LazyFrame({"order_id": ["ORD-1"]})  # pyrefly: ignore[bad-return]

    with pytest.raises(dg.DagsterTypeCheckDidNotPass) as raised:
        dg.materialize(
            [mismatch],
            resources={"io_manager": dg.FilesystemIOManager(base_dir=str(tmp_path))},
        )

    assert "failed type check for Dagster type DataFrame" in str(raised.value)


def test_materialize_result_still_takes_exactly_the_six_fields_the_fold_names():
    """`dg.MaterializeResult`'s constructor still takes exactly six fields, and `value` still defaults to a sentinel rather than to `None`."""
    # #77 folds a returned result into the materialization `process` built, rebuilding it because it is immutable and naming every field. A seventh added upstream would be dropped silently, so the field set is pinned here rather than left to be noticed.
    # The sentinel is what lets one `isinstance` check cover a result carrying nothing and one carrying something that is not a frame, so it is asserted too: a default of `None` would make the two indistinguishable from a decorated function that returned `value=None` on purpose.
    fields = set(inspect.signature(dg.MaterializeResult.__new__).parameters) - {"cls"}

    assert fields == {
        "asset_key",
        "metadata",
        "check_results",
        "data_version",
        "tags",
        "value",
    }
    assert dg.MaterializeResult().value is not None


def test_dagster_polars_writes_its_own_row_count_over_the_steps(tmp_path: Path):
    """An IO manager's metadata lands on the materialization after the step's, so a key both of them write reads as the manager's.

    `dagster-polars` counts the rows it writes and files them under `dagster/row_count`, the same key this package yields. The two agree on the number, so nothing is wrong, but a test asserting that count through a run would pass with the package's emission deleted. The row-count assertions therefore read the step's own yield instead, and this records why.
    """

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
    """`quarantine_path` calls all three rather than restating them, so a quarantine and the table it came from are escaped by one implementation (#104).

    None is exported from `dagster` and none is marked `@public`, which is why they are pinned here. What each protects against is a write outside the root: `pathlib` drops the left side of a join when the right side is absolute, and the OS walks a `..` segment upward at write time.

    `escape_dotdot_segments` is the pair `FilesystemIOManager.make_safe_partition_path` applies. The `UPathIOManager` base class applies only the first and documents overriding for the second, which is what this package does.
    """
    assert escape_leading_slash("/etc") == "%2Fetc"
    assert escape_leading_slash("etc") == "etc"
    assert escape_dotdot_segments("../etc") == "%2E%2E/etc"
    assert escape_dotdot_segments("my..backup") == "my..backup"
    assert coerce_to_relative_parts(UPath("/sales/orders")) == ("%2Fsales", "orders")
    assert coerce_to_relative_parts(UPath("sales/orders")) == ("sales", "orders")


def test_upath_io_manager_still_spells_a_multi_partition_key_by_dimension_name(
    tmp_path: Path,
):
    """The one path rule `quarantine_path` restates instead of importing, because upstream keeps it in a closure inside `_get_paths_for_partitions` (#104).

    Asserted as a derivation rather than as a literal. The quarantine's tail under its own leaf has to be the tail the manager writes under the asset's leaf, so upstream reordering the dimensions fails here rather than leaving a quarantine nobody can find.
    """
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

    grid = dg.MultiPartitionsDefinition(
        {
            "region": dg.StaticPartitionsDefinition(["eu", "us"]),
            "day": dg.StaticPartitionsDefinition(["2026-01-02"]),
        }
    )
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


# --- what the delegating writer borrows (ADR-0006) ---
def test_a_step_still_hands_over_the_output_context_and_manager_it_was_going_to_use(
    tmp_path: Path,
):
    """The four private APIs the quarantine's placement rests on, asserted through one asset.

    `get_step_execution_context` reaches the step, `StepOutputHandle` names its output, `get_output_context` hands back the context that output was going to be written under, and `get_io_manager` hands back the manager that was going to write it. Nothing else can reach a `DbIOManager`'s `resource_config`, which it reads its database and connection settings off at write time, and reconstructing that by hand is what delegation exists to avoid.

    `get_io_manager` is one ADR-0006 did not name until it was amended, and it replaces reading `context.resources.io_manager`. That reading needed the asset to declare `required_resource_keys`, which Dagster validates at bind time, which would make every direct invocation supply a manager it has no use for.
    """
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


def test_a_directly_invoked_asset_still_refuses_to_hand_over_a_step():
    """How the decorator tells a run from a call, which is what decides whether the quarantine is delegated or written to a file.

    A call has no step, so `DagsterInvalidPropertyError` is the whole of the signal. Asked rather than tested for, because there is no predicate that answers it.
    """

    @dg.asset(name="orders")
    def orders(context: dg.AssetExecutionContext) -> str:
        with pytest.raises(DagsterInvalidPropertyError):
            context.get_step_execution_context()
        return "called"

    assert orders(dg.build_asset_context()) == "called"


def test_keys_by_output_name_still_omits_the_check_outputs():
    """A check spec is an op output too, so the step has one more output per declared check and the asset's own has to be picked out by key. `keys_by_output_name` is the mapping that does it, and it carries no `@public`."""
    key = dg.AssetKey(["orders"])

    @dg.asset(name="orders", check_specs=[dg.AssetCheckSpec(name="probe", asset=key)])
    def orders():
        yield dg.MaterializeResult(asset_key=key)
        yield dg.AssetCheckResult(check_name="probe", asset_key=key, passed=True)

    assert orders.keys_by_output_name == {"result": key}
    assert len(orders.op.output_defs) == 2


def test_dagster_still_reads_the_context_parameter_off_the_first_name_alone():
    """The decorator prepends a `context` parameter to a quarantined asset, and has to agree with Dagster about whether the decorated function already declared one. `is_context_provided` is upstream's own rule, imported rather than restated so the two cannot disagree."""

    def declared(context: dg.AssetExecutionContext, raw: str) -> str: ...

    def bare(raw: str) -> str: ...

    assert is_context_provided(list(inspect.signature(declared).parameters.values()))
    assert not is_context_provided(list(inspect.signature(bare).parameters.values()))
    assert not is_context_provided([])


def test_a_check_result_still_takes_exactly_the_six_fields_the_address_names():
    """The abort exits rebuild every check result to carry the quarantine's address, naming each field, so a seventh added upstream would silently drop.

    It also normalises what it is handed, where `dg.MaterializeResult` does not, which is why the two surfaces are read differently in the tests.
    """
    result = dg.AssetCheckResult(
        passed=True, check_name="probe", asset_key=dg.AssetKey(["orders"])
    )

    assert set(result._fields) == {
        "passed",
        "asset_key",
        "check_name",
        "metadata",
        "severity",
        "description",
    }
    assert dg.AssetCheckResult(
        passed=True,
        check_name="probe",
        asset_key=dg.AssetKey(["a"]),
        metadata={"k": "v"},
    ).metadata == {"k": dg.MetadataValue.text("v")}


def test_an_output_context_still_clones_and_re_points_by_attribute():
    """The fifth private API the delegating writer rests on: `OutputContext` keeps its asset key on `_asset_key` and offers no setter, so re-pointing a clone is an attribute write.

    A shallow copy is what carries `resource_config`, `definition_metadata`, the partition and everything else no code here understands. Two properties make that safe, and both are asserted: the copy is a separate object, and `add_output_metadata` rebinds the mapping rather than mutating the one the original shares, so a manager writing onto the clone cannot reach the step's own output.
    """
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
