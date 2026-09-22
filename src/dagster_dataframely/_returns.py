"""What a decorated function may return, and how a returned `dg.MaterializeResult` changes the materialization.

Both functions wrap `validation_results`, which takes only a frame.
Returning `dg.MaterializeResult(value=None)` raises instead of skipping, because a skipped run writes no materialization to copy the result's fields onto.
"""

import dagster as dg
import polars as pl

from dagster_dataframely._runtime import AssetYield
from dagster_dataframely.errors import (
    MaterializeResultFieldError,
    MaterializeResultValueError,
)

ReturnedResult = dg.MaterializeResult[pl.DataFrame] | dg.MaterializeResult[pl.LazyFrame]
"""This union names both frame types, because `dg.MaterializeResult` is invariant in its value type."""

DecoratedReturn = pl.DataFrame | pl.LazyFrame | ReturnedResult | None


def frame_and_result(
    returned: DecoratedReturn, *, asset: str
) -> tuple[pl.DataFrame | pl.LazyFrame | None, ReturnedResult | None]:
    """Separate the frame to validate from the returned result."""
    if not isinstance(returned, dg.MaterializeResult):
        return returned, None
    if returned.asset_key is not None:
        raise MaterializeResultFieldError(asset, "asset_key")
    if returned.check_results:
        raise MaterializeResultFieldError(asset, "check_results")
    # The default `value` is a sentinel, not `None`; `tests/test_upstream_characterization.py` pins it.
    if not isinstance(returned.value, (pl.DataFrame, pl.LazyFrame)):
        raise MaterializeResultValueError(asset)
    return returned.value, returned


def with_returned_fields(
    results: AssetYield,
    returned_result: ReturnedResult | None,
    *,
    valid_key: dg.AssetKey,
) -> AssetYield:
    """Copy the returned result's `metadata`, `data_version` and `tags` onto the asset's materialization."""
    if returned_result is None:
        yield from results
        return
    for result in results:
        if (
            not isinstance(result, dg.MaterializeResult)
            or result.asset_key != valid_key
        ):
            yield result
            continue
        # `_replace` keeps every field the call omits; `tests/test_upstream_characterization.py` pins it.
        yield result._replace(
            metadata={**(returned_result.metadata or {}), **(result.metadata or {})},
            data_version=returned_result.data_version,
            tags=returned_result.tags,
        )
