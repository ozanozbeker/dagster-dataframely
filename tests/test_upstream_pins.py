"""Pin-and-assert tests for the upstream APIs this package takes a hard dependency on.

These test upstream, not this package. Each one pins a shape that is private, unexported, or undocumented, and carries a comment naming the decision that took the dependency, so a failure reads as "dataframely changed" rather than "something broke".
"""

import datetime as dt
import inspect
from pathlib import Path
from typing import override

import dagster as dg
import dataframely as dy
import polars as pl
import pytest
from dagster._core.definitions.metadata.metadata_value import ObjectMetadataValue
from dagster._serdes import deserialize_value, serialize_value
from dataframely._rule import Rule, RuleFactory
from upath import UPath


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
    # `naming.py` types against `Rule`, and every check's `dy_rule__expr` metadata is `str(rule.expr)`, so the value side of the dict is as load-bearing as the key side.
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

    # `primary_key` is the reason that function tests `isinstance` rather than truthiness: it collides with `Schema.primary_key`, so the lookup hits a bound method whose docstring belongs to dataframely rather than to any rule.
    assert not isinstance(getattr(Orders, "primary_key", None), RuleFactory)


def test_a_constraint_rule_is_named_after_the_parameter_that_declared_it():
    """Every value-carrying constraint still generates a rule named after its column parameter, and still keeps the value on an attribute of the same name."""
    # #20 renders the pill from that value, so it reads the attribute by the rule's own name rather than carrying a second table mapping one to the other. The mixins holding these attributes are private, which is why the regularity is pinned rather than typed against.
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
    # #20 dispatches the pill's unit on the column type, because the parameter name cannot distinguish them: `String` measures bytes and `List` measures elements. A third column type growing the parameter would render silently in whichever unit it is not.
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
    # #20 draws the line for defaulted constraints here: an `inf` pill on every float column would state something no author asked for, and it could never state anything else, because allowing the value removes the rule instead of inverting it.
    assert not dy.Float64().allow_inf
    assert not dy.Float64().allow_nan
    assert {"inf", "nan"} <= set(dy.Float64().validation_rules(pl.col("a")))
    assert not {"inf", "nan"} & set(
        dy.Float64(allow_inf=True, allow_nan=True).validation_rules(pl.col("a"))
    )


def test_details_returns_invalid_rows_plus_one_column_per_rule():
    """`FailureInfo.details()` still returns the invalid rows plus one outcome column per rule."""
    # #19 builds the quarantine frame straight off `details()`: original columns untouched, outcome columns renamed into the reserved namespace.
    # #24 filters the same frame on the same vocabulary, one rule at a time, to sample the rows each rule rejected.
    # Guide-documented and upstream-tested, but absent from dataframely's API reference.
    _, failure = Orders.filter(_MIXED_ORDERS)
    details = failure.details()

    assert details.height == failure.invalid().height
    assert set(details.columns) == set(Orders.columns()) | set(
        Orders._validation_rules(with_cast=False)
    )

    # #19 casts the outcome columns to String because a raw Enum panics the Delta writer.
    # This dtype is the whole reason that cast is mandatory rather than defensive, so it is pinned alongside the vocabulary it carries.
    assert details.schema["amount|min"] == pl.Enum(["valid", "invalid", "unknown"])
    assert (
        details.filter(pl.col("order_id") == "ORD-2")["amount|min"].item() == "invalid"
    )


def test_the_failure_example_limit_does_not_truncate_the_filter_path():
    """`dy.Config`'s `max_failure_examples` still governs only the message `validate` builds, and nothing `filter` returns."""
    # #24 bounds the sample of failing rows in check metadata with the package's own setting. That is only a bound worth having if dataframely is not already applying one, and only package-owned if a project that tightened this one still gets the sample it asked this package for.
    # Documented as "examples to include in failure messages", which says where it applies but not where it does not.
    with dy.Config(max_failure_examples=1):
        _, failure = Orders.filter(_MIXED_ORDERS)

    assert len(failure) == 3
    assert failure.details().height == 3
    assert failure.counts()["primary_key"] == 2


def test_cooccurrence_counts_are_keyed_by_a_frozenset_of_rule_names():
    """`FailureInfo.cooccurrence_counts()` still returns counts keyed by the set of rules a row broke together."""
    # #19 emits this as the quarantine's `cooccurrence` metadata, which is what makes one broken upstream field tripping three rules at once visible as one row rather than as three unrelated counts.
    # Documented upstream, but the key type is the load-bearing part: a `frozenset` is unordered, so the package sorts it before rendering and a tuple here would silently change that rendering.
    _, failure = Orders.filter(_MIXED_ORDERS)
    cooccurrence = failure.cooccurrence_counts()

    assert all(isinstance(rules, frozenset) for rules in cooccurrence)
    assert sum(cooccurrence.values()) == len(failure)
    assert cooccurrence[frozenset({"amount|min"})] == 1


def test_polars_renders_a_duration_in_its_own_friendly_style():
    """`dt.to_string("polars")` still renders a duration the way a polars frame repr does: `8d`, `1m 30s`, `2h 5m`, a sign on every part of a negative one, and a null left null."""
    # #23 renders every duration cell of the temporal statistics table through it. The ticket specifies polars' own style and calls it unreachable, which was true before polars 1.14 added `format="polars"`; the package's floor is well past that, so the rendering is upstream's rather than fifteen lines of this package's.
    # Documented as "the same form seen in the frame repr", which is a repr and therefore restyleable without anybody upstream calling it a break. That is what makes it worth pinning: the four decisions below are the ones the tables state.
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

    # The two routes the ticket ruled out, pinned so a change to either is visible: the default is ISO-8601, and the obvious cast raises rather than falling back to one.
    assert spans.dt.to_string().to_list()[0] == "P8D"
    with pytest.raises(pl.exceptions.InvalidOperationError):
        spans.cast(pl.String)


def test_every_asset_out_parameter_has_a_readable_attribute_of_the_same_name():
    """`dg.AssetOut` is still readable back out of every one of its constructor parameters."""
    # #19 rebuilds the quarantine's `AssetOut` from its attributes, because it is immutable and has no `_replace`. A parameter Dagster adds whose attribute is named differently would be dropped silently, taking the user's value with it, so the property the rebuild rests on is asserted rather than assumed.
    # `kwargs` is the constructor's own catch-all, not a setting.
    parameters = set(inspect.signature(dg.AssetOut.__init__).parameters) - {
        "self",
        "kwargs",
    }
    out = dg.AssetOut()

    assert parameters
    assert {name for name in parameters if not hasattr(out, name)} == set()


def test_object_metadata_value_carries_a_live_python_object():
    """`ObjectMetadataValue` is still importable from its path and still carries a live object through `instance=`."""
    # #17 hangs the schema class itself off the definition metadata, so the runtime recovers it from the asset instead of re-importing it.
    # Marked `@public` upstream but absent from the `dagster` top level, and there is no `MetadataValue.object()` factory, so the import has to name a private module path.
    carrier = ObjectMetadataValue(Orders.__name__, instance=Orders)

    assert carrier.instance is Orders
    assert carrier.value == Orders.__name__


def test_definition_metadata_reaches_an_io_manager_on_both_paths(tmp_path: Path):
    """Definition metadata still arrives at `OutputContext.definition_metadata` on the write and at `InputContext.upstream_output.definition_metadata` on the read, with a live object still the same object at both ends."""
    # #22's CSV read path recovers the schema from there and from nowhere else: not from a sidecar file, and not from the data. Both ends are pinned because the write is what a schema-shaped asset declares and the read is what makes the decode possible at all.
    # `InputContext.definition_metadata` is the trap this pins the way around: it holds the `dg.AssetIn`'s own metadata, never the upstream asset's, so it is empty here.
    carriers: dict[str, ObjectMetadataValue] = {}
    on_the_input: dict[str, object] = {}

    class Probe(dg.UPathIOManager):
        extension = ".txt"

        @override
        def dump_to_path(
            self, context: dg.OutputContext, obj: str, path: UPath
        ) -> None:
            carriers["write"] = context.definition_metadata["carrier"]
            path.write_text(obj)

        @override
        def load_from_path(self, context: dg.InputContext, path: UPath) -> str:
            upstream = context.upstream_output
            assert upstream is not None
            carriers["read"] = upstream.definition_metadata["carrier"]
            on_the_input.update(context.definition_metadata)
            return path.read_text()

    @dg.asset(
        metadata={"carrier": ObjectMetadataValue(Orders.__name__, instance=Orders)}
    )
    def upstream() -> str:
        return "written"

    @dg.asset
    def downstream(upstream: str) -> None:
        pass

    result = dg.materialize(
        [upstream, downstream],
        resources={"io_manager": Probe(base_path=UPath(tmp_path))},
    )

    assert result.success
    assert carriers["write"].instance is Orders
    assert carriers["read"].instance is Orders
    assert on_the_input == {}


def test_an_object_metadata_value_degrades_to_its_label_across_process():
    """Serializing an `ObjectMetadataValue` still keeps the label and drops the instance, rather than raising on an object that cannot be serialized."""
    # #22 falls back to an inferred CSV read when the schema does not survive the trip, which is only a fallback because this degrades. Raising here would instead fail every run whose IO manager sits behind a process boundary.
    restored = deserialize_value(
        serialize_value(ObjectMetadataValue(Orders.__name__, instance=Orders)),
        ObjectMetadataValue,
    )

    assert restored.instance is None
    assert restored.value == Orders.__name__
