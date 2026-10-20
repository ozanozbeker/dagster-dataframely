"""Every public function that takes a schema rejects one whose column or rule names this package cannot use."""

from collections.abc import Callable

import dataframely as dy
import polars as pl
import pytest

from dagster_dataframely.errors import (
    CheckNameCollisionError,
    InvalidColumnNameError,
    ReservedColumnError,
)
from tests.scenario import SCHEMA_TAKERS


class ReservedSchema(dy.Schema):
    dy_rule = dy.String(nullable=False)
    amount = dy.Int64(min=1)


class CollidingSchema(dy.Schema):
    """Two rules whose check names are both `dy_rule__order_id__nullability`."""

    order_id = dy.String(nullable=False)

    @dy.rule()
    def order_id__nullability(cls) -> pl.Expr:
        return cls.order_id.col.is_not_null()


class InvalidNamesSchema(dy.Schema):
    total = dy.Int64(min=1, alias="Order Total")


class DelimitedSchema(dy.Schema):
    total = dy.Int64(min=1, alias="a|b")


@pytest.mark.parametrize("taker", list(SCHEMA_TAKERS.values()), ids=list(SCHEMA_TAKERS))
def test_a_public_function_rejects_a_reserved_column(
    taker: Callable[[type[dy.Schema]], object],
) -> None:
    with pytest.raises(ReservedColumnError) as raised:
        taker(ReservedSchema)

    message = str(raised.value)

    assert "Column 'dy_rule' of ReservedSchema uses" in message
    assert "Rename it." in message
    assert "amount" not in message


@pytest.mark.parametrize("taker", list(SCHEMA_TAKERS.values()), ids=list(SCHEMA_TAKERS))
def test_a_public_function_rejects_two_rules_with_one_check_name(
    taker: Callable[[type[dy.Schema]], object],
) -> None:
    with pytest.raises(CheckNameCollisionError) as raised:
        taker(CollidingSchema)

    message = str(raised.value)

    assert "order_id__nullability" in message
    assert "order_id|nullability" in message
    assert "dy_rule__order_id__nullability" in message


@pytest.mark.parametrize("taker", list(SCHEMA_TAKERS.values()), ids=list(SCHEMA_TAKERS))
def test_a_public_function_rejects_a_column_dagster_cannot_name(
    taker: Callable[[type[dy.Schema]], object],
) -> None:
    with pytest.raises(InvalidColumnNameError) as raised:
        taker(InvalidNamesSchema)

    message = str(raised.value)

    assert (
        "Column 'Order Total' of InvalidNamesSchema contains characters outside"
        in message
    )
    assert "A-Za-z0-9_" in message
    assert "`alias=`" in message


@pytest.mark.parametrize("taker", list(SCHEMA_TAKERS.values()), ids=list(SCHEMA_TAKERS))
def test_a_public_function_rejects_a_column_name_with_the_rule_delimiter(
    taker: Callable[[type[dy.Schema]], object],
) -> None:
    """`|` is Dataframely's rule delimiter, so `describe_rules` would read `a|b|min` as column `a` and rule name `b|min`."""
    with pytest.raises(InvalidColumnNameError):
        taker(DelimitedSchema)
