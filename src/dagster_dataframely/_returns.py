"""What a decorated function may hand back, and what a returned `dg.MaterializeResult` does to the materialization.

A decorated function returns the frame to validate, or a `dg.MaterializeResult` carrying it. The second spelling is what Dagster's own docs teach for attaching metadata, and it is the only route there is to a materialization's tags and data version, so refusing it cost parity with the `@dg.asset` this decorator is modelled on (#77).

Two halves, and neither reads without the other. The unwrap runs between calling the decorated function and handing the frame to `process`; the fold runs over what `process` yields. `process` itself is untouched by both, which is what keeps a hand-wired asset handing it a frame and keeps its own guard the thing that says so.

The unwrap takes `value`, and the fold takes `metadata`, `data_version` and `tags`. That is four of the six fields; the other two are refused here, because the decorator decides the asset keys from the outs it declares and the check results from the schema's rules.
"""

import dagster as dg
import polars as pl

from dagster_dataframely._runtime import AssetYield
from dagster_dataframely.errors import (
    MaterializeResultFieldError,
    MaterializeResultValueError,
)

#: A `dg.MaterializeResult` a decorated function returned. Both frame types are spelled out because `MaterializeResult` is generic and invariant in its value, so one parameterized on the union would not accept either.
ReturnedResult = dg.MaterializeResult[pl.DataFrame] | dg.MaterializeResult[pl.LazyFrame]

#: Everything a decorated function is allowed to return. A static promise only: `unwrap` and the staging decision both read the object that arrives, never the annotation it was declared under, so a decorated function annotated wrongly still behaves as what it returned. `@dg.asset` holds its annotation by inferring the output's `dagster_type` from it, which this decorator cannot: `dagster_type` describes what the out stores, and validation is eager, so the out holds a `DataFrame` however the decorated function arrived at it.
DecoratedReturn = pl.DataFrame | pl.LazyFrame | ReturnedResult


def unwrap(
    returned: DecoratedReturn, *, asset: str
) -> tuple[pl.DataFrame | pl.LazyFrame, ReturnedResult | None]:
    """Separates the frame to validate from the fields to fold in.

    A bare frame passes straight through with nothing to fold, which is what makes returning one byte-for-byte what it was: no metadata, tags or data version can appear on that path that did not appear before.

    Every refusal is raised here rather than inside `process`, and all three are pipeline defects that no run should reach twice. The frame guard in `process` is deliberately left alone and still refuses a `dg.MaterializeResult` handed to it directly, which is what hand-wiring gets.

    Args:
        returned: Whatever the decorated function returned, frame or result alike.
        asset: The valid out's asset key, rendered, for the messages.

    Returns:
        The frame `process` validates, and the result to fold over its yields, or `None` when a bare frame was returned.

    Raises:
        MaterializeResultFieldError: The result sets `asset_key` or `check_results`.
        MaterializeResultValueError: The result carries no frame on `value`.
    """
    if not isinstance(returned, dg.MaterializeResult):
        return returned, None
    # Spelled out rather than looped, because the two fields default differently: `asset_key` to `None` and `check_results` to an empty sequence.
    if returned.asset_key is not None:
        raise MaterializeResultFieldError(asset, "asset_key")
    if returned.check_results:
        raise MaterializeResultFieldError(asset, "check_results")
    # `value` defaults to a sentinel rather than to `None`, so one check covers both a result carrying nothing and one carrying something that is not a frame.
    if not isinstance(returned.value, (pl.DataFrame, pl.LazyFrame)):
        raise MaterializeResultValueError(asset)
    return returned.value, returned


def fold(
    results: AssetYield,
    returned_result: ReturnedResult | None,
    *,
    valid_key: dg.AssetKey,
) -> AssetYield:
    """Carries the returned result's remaining three fields onto the valid out's materialization.

    Three, because the unwrap already took `value`: whatever it still holds is the frame `process` has since validated, so nothing here reads it.

    The valid out only. The quarantine keeps the tags and the data version Dagster gives it, on the same reasoning that keeps `automation_condition` and `freshness_policy` off the quarantine out: one returned result describes the table the decorated function produced, not the rows the schema rejected.

    The two metadata mappings combine with the package's own keys last, so a returned `dagster/row_count` loses to the one this package counted. That is the precedence the decorator already uses for definition metadata, and for the same reason: those keys are this package's surface and a collision is a mistake.

    The materialization is rebuilt rather than mutated, because `dg.MaterializeResult` is immutable. Every field is named, which is what a seventh field added upstream would silently drop; `tests/test_upstream_characterization.py` pins the six so that it fails there instead.

    Args:
        results: What `process` yielded, materializations and check results alike.
        returned_result: What the decorated function returned, or `None` when it returned a bare frame, in which case everything passes through untouched.
        valid_key: The asset key whose materialization the fields land on.

    Yields:
        The same results in the same order, with the valid out's materialization rebuilt.
    """
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
        yield dg.MaterializeResult(
            asset_key=result.asset_key,
            metadata={**(returned_result.metadata or {}), **(result.metadata or {})},
            check_results=result.check_results,
            data_version=returned_result.data_version,
            tags=returned_result.tags,
            value=result.value,
        )
