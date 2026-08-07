"""Pin-and-assert tests for the upstream APIs this package takes a hard dependency on.

These test upstream, not this package. Each one pins a shape that is private, unexported, or undocumented, and carries a comment naming the decision that took the dependency, so a failure reads as "dataframely changed" rather than "something broke".
"""

import dataframely as dy
import polars as pl
from dagster._core.definitions.metadata.metadata_value import ObjectMetadataValue
from dataframely._rule import Rule, RuleFactory


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


def test_details_returns_invalid_rows_plus_one_column_per_rule():
    """`FailureInfo.details()` still returns the invalid rows plus one outcome column per rule."""
    # #19 builds the quarantine frame straight off `details()`: original columns untouched, outcome columns renamed into the reserved namespace.
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


def test_object_metadata_value_carries_a_live_python_object():
    """`ObjectMetadataValue` is still importable from its path and still carries a live object through `instance=`."""
    # #17 hangs the schema class itself off the definition metadata, so the runtime recovers it from the asset instead of re-importing it.
    # Marked `@public` upstream but absent from the `dagster` top level, and there is no `MetadataValue.object()` factory, so the import has to name a private module path.
    carrier = ObjectMetadataValue(Orders.__name__, instance=Orders)

    assert carrier.instance is Orders
    assert carrier.value == Orders.__name__
