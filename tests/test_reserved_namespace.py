"""Every public function that takes a schema rejects one whose column or rule names this package cannot use.

The other arguments are placeholders, so a `TypeError` means the check runs too late (ADR-0008).
"""

from collections.abc import Callable

import dagster as dg
import dataframely as dy
import polars as pl
import pytest

import dagster_dataframely as dd
from dagster_dataframely.errors import (
    CheckNameCollisionError,
    InvalidColumnNameError,
    ReservedColumnError,
)
from tests.scenario import Orders, clean_orders


class Reserved(dy.Schema):
    dy_rule = dy.String(nullable=False)
    amount = dy.Int64(min=1)


class Colliding(dy.Schema):
    """Two rules that rewrite to `dy_rule__order_id__nullability`."""

    order_id = dy.String(nullable=False)

    @dy.rule()
    def order_id__nullability(cls) -> pl.Expr:
        return cls.order_id.col.is_not_null()


class InvalidNames(dy.Schema):
    total = dy.Int64(min=1, alias="Order Total")


class Delimited(dy.Schema):
    total = dy.Int64(min=1, alias="a|b")


_KEY = dg.AssetKey(["orders"])
_, _FAILURE = Orders.filter(clean_orders())

_TAKERS: dict[str, Callable[[type[dy.Schema]], object]] = {
    "asset": dd.asset,  # raises before it returns a decorator (ADR-0008)
    "quarantine_spec": lambda schema: dd.quarantine_spec(schema, _KEY),
    "wiring.check_results": lambda schema: list(
        dd.wiring.check_results(
            schema, pl.DataFrame(), asset_key=_KEY, severity=dg.AssetCheckSeverity.WARN
        )
    ),
    "wiring.check_specs": lambda schema: dd.wiring.check_specs(schema, asset=_KEY),
    "wiring.quarantine_frame": lambda schema: dd.wiring.quarantine_frame(
        schema, _FAILURE
    ),
    "wiring.schema_metadata": dd.wiring.schema_metadata,
    "wiring.table_schema": dd.wiring.table_schema,
    "wiring.validation_results": lambda schema: list(
        dd.wiring.validation_results(schema, pl.DataFrame(), valid_key=_KEY)
    ),
}


@pytest.mark.parametrize("taker", list(_TAKERS.values()), ids=list(_TAKERS))
def test_a_public_function_rejects_a_reserved_column(
    taker: Callable[[type[dy.Schema]], object],
) -> None:
    with pytest.raises(ReservedColumnError) as raised:
        taker(Reserved)

    message = str(raised.value)

    assert "Column 'dy_rule' of Reserved uses" in message
    assert "Rename it." in message
    assert "amount" not in message


@pytest.mark.parametrize("taker", list(_TAKERS.values()), ids=list(_TAKERS))
def test_a_public_function_rejects_two_rules_that_rewrite_to_one_check_name(
    taker: Callable[[type[dy.Schema]], object],
) -> None:
    with pytest.raises(CheckNameCollisionError) as raised:
        taker(Colliding)

    message = str(raised.value)

    assert "order_id__nullability" in message
    assert "order_id|nullability" in message
    assert "dy_rule__order_id__nullability" in message


@pytest.mark.parametrize("taker", list(_TAKERS.values()), ids=list(_TAKERS))
def test_a_public_function_rejects_a_column_dagster_cannot_name(
    taker: Callable[[type[dy.Schema]], object],
) -> None:
    with pytest.raises(InvalidColumnNameError) as raised:
        taker(InvalidNames)

    message = str(raised.value)

    assert "Column 'Order Total' of InvalidNames contains characters outside" in message
    assert "A-Za-z0-9_" in message
    assert "`alias=`" in message


@pytest.mark.parametrize("taker", list(_TAKERS.values()), ids=list(_TAKERS))
def test_a_public_function_rejects_a_column_name_with_the_rule_delimiter(
    taker: Callable[[type[dy.Schema]], object],
) -> None:
    """`|` is Dataframely's rule delimiter, so `described_rules` would read `a|b|min` as column `a` and rule `b|min`."""
    with pytest.raises(InvalidColumnNameError):
        taker(Delimited)
