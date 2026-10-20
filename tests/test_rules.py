"""These tests check each field of `DescribedRule` directly, because a wrong field can render the same as a correct one."""

import dataframely as dy
import polars as pl
import pytest

from dagster_dataframely._rules import describe_rules
from tests.scenario import OrdersSchema


def test_one_record_per_rule_in_the_schemas_own_order():
    rules = describe_rules(OrdersSchema)

    # Dataframely sets the order; `tests/test_asset_definition.py` has the literal list of names.
    assert list(rules) == list(OrdersSchema._validation_rules(with_cast=False))


def test_only_a_delimited_rule_owns_a_column():
    """`describe_rules` sets `column` and `rule_name` from the `|`, or leaves both `None`."""
    rules = describe_rules(OrdersSchema)

    assert (rules["amount|min"].column, rules["amount|min"].rule_name) == (
        "amount",
        "min",
    )
    assert rules["primary_key"].column is None
    assert rules["primary_key"].rule_name is None
    assert all(
        (rule.column is None) == (rule.rule_name is None) for rule in rules.values()
    )
    assert {rule.column for rule in rules.values() if rule.column} <= set(
        OrdersSchema.columns()
    )


def test_a_description_appears_only_where_a_rule_wrote_one():
    rules = describe_rules(OrdersSchema)

    assert rules["paid_orders_have_amount"].description == (
        "Require a positive amount on a paid order."
    )
    assert rules["line_numbers_are_dense"].description is None
    assert rules["amount|min"].description is None


def test_the_check_name_puts_the_rule_name_in_the_dy_rule_namespace():
    """The check and the error rows' rule column share this name."""
    rules = describe_rules(OrdersSchema)

    assert rules["amount|min"].check_name == "dy_rule__amount__min"
    assert rules["primary_key"].check_name == "dy_rule__primary_key"


def test_the_expression_is_not_resolved_where_the_record_is_built():
    """`describe_rules` does not read a rule's expression, because reading every expression makes the records slow to build (`docs/pre-1.0.md`)."""

    class ExplodingSchema(dy.Schema):
        amount = dy.Int64()

        @dy.rule()
        def never_evaluated(cls) -> pl.Expr:
            message = "the expression was resolved"
            raise RuntimeError(message)

    rules = describe_rules(ExplodingSchema)

    assert rules["never_evaluated"].name == "never_evaluated"
    with pytest.raises(RuntimeError, match="the expression was resolved"):
        _ = rules["never_evaluated"].expr
