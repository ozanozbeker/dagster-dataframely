"""The fields of `DescribedRule`, tested directly because a wrong field can render the same as a correct one."""

import dataframely as dy
import polars as pl
import pytest

from dagster_dataframely._rules import described_rules
from tests.scenario import Orders


def test_one_record_per_rule_in_the_schemas_own_order():
    rules = described_rules(Orders)

    # Dataframely sets the order; `tests/test_asset_definition.py` has the literal list of names.
    assert list(rules) == list(Orders._validation_rules(with_cast=False))


def test_only_a_delimited_rule_owns_a_column():
    """`described_rules` sets `column` and `rule_name` from the `|`, or leaves both `None`."""
    rules = described_rules(Orders)

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
        Orders.columns()
    )


def test_a_description_appears_only_where_a_rule_wrote_one():
    rules = described_rules(Orders)

    assert rules["paid_orders_have_amount"].description == (
        "Paid orders must carry a positive amount."
    )
    assert rules["line_numbers_are_dense"].description is None
    assert rules["amount|min"].description is None


def test_the_check_name_is_the_rewrite():
    """The check and the quarantine's rule column share this name."""
    rules = described_rules(Orders)

    assert rules["amount|min"].check_name == "dy_rule__amount__min"
    assert rules["primary_key"].check_name == "dy_rule__primary_key"


def test_the_expression_is_not_resolved_where_the_record_is_built():
    """Only a run reads the expression, because reading it at definition time is slow (`docs/pre-1.0.md`)."""

    class Exploding(dy.Schema):
        amount = dy.Int64()

        @dy.rule()
        def never_evaluated(cls) -> pl.Expr:
            message = "the expression was resolved"
            raise RuntimeError(message)

    rules = described_rules(Exploding)

    assert rules["never_evaluated"].name == "never_evaluated"
    with pytest.raises(RuntimeError, match="the expression was resolved"):
        _ = rules["never_evaluated"].expr
