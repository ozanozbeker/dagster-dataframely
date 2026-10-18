"""The record a test reads after it calls an asset built with `dd.asset`.

A test imports these names, and a definition does not.
So they are in their own public module, like `pl.testing` and `dy.testing`.
"""

from collections.abc import Iterable
from dataclasses import dataclass

import dagster as dg
import polars as pl

__all__ = ["DirectInvocationResult", "direct_invocation_result"]


@dataclass(frozen=True)
class DirectInvocationResult:
    """What one direct invocation of an asset built with `dd.asset` yielded.

    Attributes
    ----------
    materialization
        The asset's materialization, or `None` when the decorated function returned `None`.
    checks
        Every check result, keyed by check name, in the order the call yielded them.
    """

    materialization: dg.MaterializeResult[pl.DataFrame] | None
    checks: dict[str, dg.AssetCheckResult]


def direct_invocation_result(called: object) -> DirectInvocationResult:
    """Return the materialization and the check results a direct invocation yields.

    Parameters
    ----------
    called
        What calling the asset returns.
        Dagster annotates `AssetsDefinition.__call__` as returning `object`, so a narrower annotation would make every test cast.
    """
    if not isinstance(called, Iterable):
        raise TypeError(_wrong_argument_message(called, "that is not iterable"))
    materialization: dg.MaterializeResult[pl.DataFrame] | None = None
    checks: dict[str, dg.AssetCheckResult] = {}
    for value in called:
        if isinstance(value, dg.MaterializeResult):
            if materialization is not None:
                problem = "that yielded a second `MaterializeResult`"
                raise TypeError(_wrong_argument_message(called, problem))
            materialization = value
        elif isinstance(value, dg.AssetCheckResult) and value.check_name is not None:
            if value.check_name in checks:
                problem = f"that yielded the check name '{value.check_name}' twice"
                raise TypeError(_wrong_argument_message(called, problem))
            checks[value.check_name] = value
        else:
            problem = f"that yielded a value of type `{type(value).__name__}`"
            raise TypeError(_wrong_argument_message(called, problem))
    return DirectInvocationResult(materialization, checks)


def _wrong_argument_message(called: object, problem: str) -> str:
    """Return the message for an argument that no call of an asset built with `dd.asset` returns."""
    return (
        f"direct_invocation_result received a value of type `{type(called).__name__}` {problem}. "
        "Pass what calling an asset built with `dd.asset` returns, such as `orders(dg.build_asset_context(), frame)`."
    )
