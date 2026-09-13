"""Every public function that takes a schema refuses one this package cannot name.

Every argument but the schema is a stand-in nothing reads. The guard runs first, so `ReservedColumnError` proves the ordering and a `TypeError` proves the guard sits too late.

Listed, not reflected. `test_public_surface.py` already makes every new public name a decision taken in a test file, so a ninth schema-taking function costs one line here beside the eight it joins.
"""

from collections.abc import Callable

import dagster as dg
import dataframely as dy
import polars as pl
import pytest

import dagster_dataframely as dd
from dagster_dataframely.errors import CheckNameCollisionError, ReservedColumnError
from tests.scenario import Orders, clean_orders


class Reserved(dy.Schema):
    """A schema claiming a column inside `dy_`."""

    dy_rule = dy.String(nullable=False)
    amount = dy.Int64(min=1)


class Colliding(dy.Schema):
    """Two rules that rewrite to `dy_rule__order_id__nullability`."""

    order_id = dy.String(nullable=False)

    @dy.rule()
    def order_id__nullability(cls) -> pl.Expr:
        return cls.order_id.col.is_not_null()


_KEY = dg.AssetKey(["orders"])
_, _FAILURE = Orders.filter(clean_orders())
"""A failure nothing reads. `quarantine_frame` takes one, and its guard raises before it looks."""

# Every public function that takes a schema, called with whatever else it needs to reach its guard. The two generators are drained, because theirs raises on first iteration rather than at call time. `dy_asset` is the factory itself, never applied: `maker = dy_asset(Reserved)` has to raise rather than hand back a decorator that will (ADR-0008).
_TAKERS: dict[str, Callable[[type[dy.Schema]], object]] = {
    "dy_asset": dd.dy_asset,
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
def test_a_public_function_refuses_a_reserved_column(
    taker: Callable[[type[dy.Schema]], object],
) -> None:
    with pytest.raises(ReservedColumnError) as raised:
        taker(Reserved)

    message = str(raised.value)

    assert "Column 'dy_rule' of Reserved uses" in message
    assert "Rename it." in message
    # The other column is fine, so the message never mentions it.
    assert "amount" not in message


@pytest.mark.parametrize("taker", list(_TAKERS.values()), ids=list(_TAKERS))
def test_a_public_function_refuses_two_rules_that_rewrite_to_one_check_name(
    taker: Callable[[type[dy.Schema]], object],
) -> None:
    with pytest.raises(CheckNameCollisionError) as raised:
        taker(Colliding)

    message = str(raised.value)

    # Both rules by their Dataframely names, and the one check name they collide on.
    assert "order_id__nullability" in message
    assert "order_id|nullability" in message
    assert "dy_rule__order_id__nullability" in message
