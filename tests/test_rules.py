"""What a `DescribedRule` promises, which no single surface shows whole.

Four places read these records and each reads different fields, so a wrong one can hide. A rule name this package has not met renders as the rule's name on every surface, which is also what a correct `kind` with no renderer does, so nothing downstream would report the difference.

`Orders` carries a rule of every awkward kind on purpose, which is why the assertions below name it rather than declaring schemas of their own. `tests/scenario.py` says which rule covers what.
"""

import dataframely as dy
import polars as pl
import pytest

from dagster_dataframely._rules import described_rules
from tests.scenario import Orders


def test_one_record_per_rule_in_the_schemas_own_order():
    """Keyed by the name Dataframely gives it, so a caller indexes one rule or iterates them all.

    Compared against Dataframely's own dict rather than a literal: order is the promise, and upstream is what defines it. `test_asset_definition.py` holds the literal that catches a rule disappearing.
    """
    rules = described_rules(Orders)

    assert list(rules) == list(Orders._validation_rules(with_cast=False))
    assert all(name == rule.name for name, rule in rules.items())


def test_only_a_delimited_rule_owns_a_column():
    """The `|` decides, and it sets both fields or neither. Every renderer leans on that pairing."""
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
    """A `@dy.rule()` keeps its docstring on the `RuleFactory`. A column rule is generated from a column argument and has no function to document."""
    rules = described_rules(Orders)

    assert rules["paid_orders_have_amount"].description == (
        "Paid orders must carry a positive amount."
    )
    assert rules["line_numbers_are_dense"].description is None
    assert rules["amount|min"].description is None


def test_the_check_name_is_the_rewrite():
    """One string for the check, the quarantine's rule column and the metadata key, so the record carries it rather than each reader rebuilding it."""
    rules = described_rules(Orders)

    assert rules["amount|min"].check_name == "dy_rule__amount__min"
    assert rules["primary_key"].check_name == "dy_rule__primary_key"


def test_the_expression_is_not_resolved_where_the_record_is_built():
    """Dataframely builds a `@dy.rule()` body as `Rule(expr=lambda: ...)`, and nothing at definition time reads it: only a run's check metadata does.

    Resolving every expression eagerly costs a schema of forty rule bodies nine times what leaving it alone costs. This asserts the laziness rather than the timing, because a body that raises can only run if something read it.
    """

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
