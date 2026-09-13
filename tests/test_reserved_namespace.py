"""Every public function that takes a schema refuses one this package cannot name.

The set is reflected off the package rather than listed, so a ninth schema-taking function is covered the day it is written. This is not the self-agreement `test_public_surface.py` refuses: a reflected set generates assertions, so a new name adds a test that can fail and never relaxes one.

Every argument but the schema is `None`. Nothing else is touched when the guard runs first, so `ReservedColumnError` proves the ordering and a `TypeError` proves the guard sits too late.
"""

import inspect
from collections.abc import Callable, Iterator
from typing import Any

import dataframely as dy
import polars as pl
import pytest

import dagster_dataframely as dd
from dagster_dataframely.errors import CheckNameCollisionError, ReservedColumnError


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


# The two namespaces that hold public names. `errors` holds no callable that takes a schema.
_NAMESPACES = (("dagster_dataframely", dd), ("dagster_dataframely.wiring", dd.wiring))

_KNOWN = {
    "dagster_dataframely.dy_asset",
    "dagster_dataframely.quarantine_spec",
    "dagster_dataframely.wiring.check_results",
    "dagster_dataframely.wiring.check_specs",
    "dagster_dataframely.wiring.quarantine_frame",
    "dagster_dataframely.wiring.schema_metadata",
    "dagster_dataframely.wiring.table_schema",
    "dagster_dataframely.wiring.validation_results",
}
"""The functions as of ADR-0008, to stop a broken reflection passing on no cases at all."""


def _schema_parameters(member: object) -> list[int]:
    """Answer at which positions a public name takes a schema.

    A class is excluded before the signature is read: `Granularity` and the error types are callable, and a schema reaches these functions as an argument rather than as a base.

    `eval_str` so a stringized annotation still resolves. Nothing here writes one today, and a module that later does would otherwise empty this list and take every case below with it.
    """
    if not callable(member) or inspect.isclass(member):
        return []
    parameters = inspect.signature(member, eval_str=True).parameters.values()
    return [
        index
        for index, parameter in enumerate(parameters)
        if parameter.annotation == type[dy.Schema]
    ]


def _takes_a_schema(member: object) -> bool:
    """Answer whether a public name takes exactly one schema, first."""
    return _schema_parameters(member) == [0]


def _schema_takers() -> list[tuple[str, Callable[..., Any]]]:
    """Every public callable that takes a schema, under its qualified name."""
    return [
        (f"{label}.{name}", member)
        for label, module in _NAMESPACES
        for name in sorted(module.__all__)
        if _takes_a_schema(member := getattr(module, name))
    ]


def _stubbed(taker: Callable[..., Any]) -> tuple[list[None], dict[str, None]]:
    """Build every argument but the schema as `None`.

    Only the parameters with no default, because a function that reaches its own optional arguments has already gone too far to matter here.
    """
    parameters = list(inspect.signature(taker).parameters.values())[1:]
    required = [
        parameter
        for parameter in parameters
        if parameter.default is inspect.Parameter.empty
        and parameter.kind
        not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]
    return (
        [
            None
            for parameter in required
            if parameter.kind is not parameter.KEYWORD_ONLY
        ],
        {
            parameter.name: None
            for parameter in required
            if parameter.kind is parameter.KEYWORD_ONLY
        },
    )


def _call(taker: Callable[..., Any], schema: type[dy.Schema]) -> None:
    """Push a schema through one of them, consuming whatever it hands back.

    `check_results` and `validation_results` are generators, so their guard raises on first iteration rather than at call time. Consuming is what makes the two shapes answer alike here.
    """
    args, kwargs = _stubbed(taker)
    result: object = taker(schema, *args, **kwargs)
    if isinstance(result, Iterator):
        list(result)


_TAKERS = _schema_takers()
_CASES = [taker for _, taker in _TAKERS]
_IDS = [name for name, _ in _TAKERS]


def test_the_reflection_finds_the_functions_it_knew_about():
    """A broken reflection would collect no cases and pass on every one of them.

    A subset rather than an equality, so a ninth schema-taking function adds cases without editing this line. Whether that function should be public at all is `test_public_surface.py`'s question.
    """
    assert set(_IDS) >= _KNOWN


def test_no_public_function_takes_a_schema_anywhere_but_first():
    """The convention the reflection rests on, asserted rather than assumed.

    Every case above stubs the other arguments as `None`, which only works while the schema is the first parameter. A function taking one second would otherwise contribute no cases and leave the suite green, which is the one way the guarantee ADR-0008 states can lapse without anybody noticing.
    """
    misplaced: dict[str, list[int]] = {
        f"{label}.{name}": positions
        for label, module in _NAMESPACES
        for name in sorted(module.__all__)
        if (positions := _schema_parameters(getattr(module, name))) not in ([], [0])
    }

    assert not misplaced


@pytest.mark.parametrize("taker", _CASES, ids=_IDS)
def test_a_public_function_refuses_a_reserved_column(
    taker: Callable[..., Any],
) -> None:
    with pytest.raises(ReservedColumnError) as raised:
        _call(taker, Reserved)

    assert "dy_rule" in str(raised.value)


@pytest.mark.parametrize("taker", _CASES, ids=_IDS)
def test_a_public_function_refuses_two_rules_that_rewrite_to_one_check_name(
    taker: Callable[..., Any],
) -> None:
    with pytest.raises(CheckNameCollisionError) as raised:
        _call(taker, Colliding)

    assert "dy_rule__order_id__nullability" in str(raised.value)


def test_the_decorator_refuses_before_it_is_applied():
    """`dy_asset(Reserved)` raises rather than handing back a decorator that will.

    Stated on its own because the reflected cases never apply what a factory returns, so nothing above would notice the guard slipping down into the wrapper.
    """
    with pytest.raises(ReservedColumnError):
        dd.dy_asset(Reserved)
