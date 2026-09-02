"""What a decorated function may hand back, and what a returned `dg.MaterializeResult` does to the materialization.

A decorated function returns the frame to validate, or a `dg.MaterializeResult` carrying it, or `None` to skip the asset entirely. Dagster's own docs teach the second spelling for attaching metadata, and it is the only route to a materialization's tags and data version. Refusing it would cost parity with the `@dg.asset` this decorator is modelled on (#77).

`None` carries nothing, so the unwrap has nothing to take off it and the fold has nothing to land on. That asymmetry is the reason `dg.MaterializeResult(value=None)` stays refused: a skipped run writes no materialization, so there is nowhere for its `metadata`, `tags` and `data_version` to go (#95).

Two halves, and neither reads without the other. The unwrap runs between calling the decorated function and handing the frame to `process`. The fold runs over what `process` yields. `process` itself is untouched by both, so a hand-wired asset still hands it a frame and its own guard still says so.

The unwrap takes `value`. The fold takes `metadata`, `data_version` and `tags`. That is four of the six fields. The other two are refused here, because the decorator decides the asset key from what it was declared with and the check results from the schema's rules.
"""

import dagster as dg
import polars as pl

from dagster_dataframely._runtime import AssetYield
from dagster_dataframely.errors import (
    MaterializeResultFieldError,
    MaterializeResultValueError,
)

#: A `dg.MaterializeResult` a decorated function returned. Both frame types are spelled out because `MaterializeResult` is generic and invariant in its value, so one parameterized on the union would accept neither.
ReturnedResult = dg.MaterializeResult[pl.DataFrame] | dg.MaterializeResult[pl.LazyFrame]

#: Everything a decorated function is allowed to return. A static promise only. `unwrap` and the staging decision both read the object that arrives, never the annotation it was declared under, so a wrongly annotated function still behaves as whatever it returned. `@dg.asset` holds its annotation by inferring the output's `dagster_type` from it. This decorator cannot: `dagster_type` describes what the asset stores, validation is eager, and the asset holds a `DataFrame` however the decorated function arrived at it.
#:
#: `None` is the skip, and it is a value rather than an exception on purpose. The decorator cannot tell a source file that is legitimately absent from a path that is misconfigured, so it never catches one to decide. The author writes the test that returns `None` (#95).
DecoratedReturn = pl.DataFrame | pl.LazyFrame | ReturnedResult | None


def unwrap(
    returned: DecoratedReturn, *, asset: str
) -> tuple[pl.DataFrame | pl.LazyFrame | None, ReturnedResult | None]:
    """Separate the frame to validate from the fields to fold in.

    A bare frame passes straight through with nothing to fold. Returning one is therefore byte-for-byte what it was: no metadata, tags or data version can appear on that path that did not appear before. `None` passes through the same way, and `process` reads it as the skip.

    Every refusal is raised here rather than inside `process`, and all three are pipeline defects that no run should reach twice. The frame guard in `process` is deliberately left alone and still refuses a `dg.MaterializeResult` handed to it directly. That is what hand-wiring gets.

    Parameters
    ----------
    returned
        Whatever the decorated function returned, frame or result or `None` alike.
    asset
        The asset key, rendered, for the messages.

    Returns
    -------
    The frame `process` validates, and the result to fold over its yields. The frame is `None` when the decorated function skipped. The result is `None` when a bare frame was returned.

    Raises
    ------
    MaterializeResultFieldError
        The result sets `asset_key` or `check_results`.
    MaterializeResultValueError
        The result carries no frame on `value`. A result is how metadata reaches a materialization, and a skipped run has none, so `value=None` is refused rather than read as the skip.
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
    """Carry the returned result's remaining three fields onto the asset's materialization.

    Three, because the unwrap already took `value`. Whatever it still holds is the frame `process` has since validated, so nothing here reads it.

    The table only. Nothing reaches the quarantine, which materializes no event to carry a tag or a data version: it is evidence of a run rather than an asset (ADR-0004).

    The two metadata mappings combine with the package's own keys last, so a returned `dagster/row_count` loses to the one this package counted. The decorator already uses that precedence for definition metadata, and for the same reason: those keys are this package's surface, and a collision is a mistake.

    The materialization is rebuilt rather than mutated, because `dg.MaterializeResult` is immutable. Every field is named, so a seventh field added upstream would silently drop. `tests/test_upstream_characterization.py` pins the six and fails there instead.

    Parameters
    ----------
    results
        What `process` yielded, materializations and check results alike.
    returned_result
        What the decorated function returned, or `None` when it returned a bare frame. `None` passes everything through untouched.
    valid_key
        The asset key whose materialization the fields land on.

    Yields
    ------
    The same results in the same order, with the materialization rebuilt.
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
