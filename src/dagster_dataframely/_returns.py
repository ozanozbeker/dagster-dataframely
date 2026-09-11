"""What a decorated function may return, and what a returned `dg.MaterializeResult` does to the materialization.

A decorated function returns the frame to validate, a `dg.MaterializeResult` carrying it, or `None` to skip the asset. Dagster's docs teach the second spelling for attaching metadata, and it is the only route to a materialization's tags and data version. Refusing it would cost parity with `@dg.asset` (#77).

`None` carries nothing, so `separated_return` has nothing to take off it and `with_returned_fields` has nothing to land on. So `dg.MaterializeResult(value=None)` stays refused: a skipped run writes no materialization, so its `metadata`, `tags` and `data_version` would have nowhere to go (#95).

Two functions. `separated_return` runs between calling the decorated function and handing the frame to `validation_results`. `with_returned_fields` runs over what `validation_results` yields. `validation_results` itself is untouched by both, so a hand-wired asset still hands it a frame and its own guard still says so.

`separated_return` takes `value`. `with_returned_fields` takes `metadata`, `data_version` and `tags`. The other two fields are refused here: the decorator decides the asset key from its declaration and the check results from the schema's rules.
"""

from typing import NamedTuple

import dagster as dg
import polars as pl

from dagster_dataframely._runtime import AssetYield
from dagster_dataframely.errors import (
    MaterializeResultFieldError,
    MaterializeResultValueError,
)

ReturnedResult = dg.MaterializeResult[pl.DataFrame] | dg.MaterializeResult[pl.LazyFrame]
"""A `dg.MaterializeResult` a decorated function returned. Both frame types are spelled out because `MaterializeResult` is generic and invariant in its value, so one parameterized on the union would accept neither."""

DecoratedReturn = pl.DataFrame | pl.LazyFrame | ReturnedResult | None
"""Everything a decorated function may return. A static promise only: `separated_return` reads the object that arrives, never the annotation, so a wrongly annotated function still behaves as whatever it returned.

`None` is the skip, and it is a value rather than an exception. The decorator cannot tell a source file that is legitimately absent from a misconfigured path, so it never catches an exception to decide. The author writes the condition that returns `None` (#95).
"""


class SeparatedReturn(NamedTuple):
    """What a decorated function returned, with the frame taken off the result.

    Attributes
    ----------
    frame
        The frame `validation_results` validates. `None` when the decorated function skipped.
    result
        The result whose fields land on the materialization. `None` when a bare frame was returned.
    """

    frame: pl.DataFrame | pl.LazyFrame | None
    result: ReturnedResult | None


def separated_return(returned: DecoratedReturn, *, asset: str) -> SeparatedReturn:
    """Separate the frame to validate from the fields to carry onto the materialization.

    A bare frame passes straight through with nothing to carry, so that path is unchanged: no metadata, tags or data version appear on it. `None` passes through the same way, and `validation_results` reads it as the skip.

    Every refusal is raised here, not inside `validation_results`. All three are pipeline defects no run should reach twice. The frame guard in `validation_results` stays as it is and still refuses a `dg.MaterializeResult` handed to it directly. Hand-wiring gets that guard and nothing more.

    Parameters
    ----------
    returned
        Whatever the decorated function returned: frame, result or `None`.
    asset
        The asset key, rendered, for the messages.

    Returns
    -------
    The frame `validation_results` validates, and the result whose fields land on the materialization.

    Raises
    ------
    MaterializeResultFieldError
        The result sets `asset_key` or `check_results`.
    MaterializeResultValueError
        The result carries no frame on `value`. A result exists to carry metadata onto a materialization, and a skipped run has none, so `value=None` is refused rather than read as the skip.
    """
    if not isinstance(returned, dg.MaterializeResult):
        return SeparatedReturn(returned, None)
    # Spelled out rather than looped, because the two fields default differently: `asset_key` to `None` and `check_results` to an empty sequence.
    if returned.asset_key is not None:
        raise MaterializeResultFieldError(asset, "asset_key")
    if returned.check_results:
        raise MaterializeResultFieldError(asset, "check_results")
    # `value` defaults to a sentinel, not `None`, so one check covers a result carrying nothing and one carrying something that is not a frame.
    if not isinstance(returned.value, (pl.DataFrame, pl.LazyFrame)):
        raise MaterializeResultValueError(asset)
    return SeparatedReturn(returned.value, returned)


def with_returned_fields(
    results: AssetYield,
    returned_result: ReturnedResult | None,
    *,
    valid_key: dg.AssetKey,
) -> AssetYield:
    """Carry the returned result's remaining three fields onto the asset's materialization.

    Three, because `separated_return` already took `value`. Whatever `value` still holds is the frame `validation_results` has since validated, so nothing here reads it.

    Not a participle, which is what this package names a transformer by: only the materialization changes here, and every check result passes through untouched.

    The table only. The quarantine materializes no event to carry a tag or a data version: it is evidence of a run, not an asset (ADR-0004).

    The two metadata mappings combine with the package's own keys last, so a returned `dagster/row_count` loses to the one this package counted. The decorator uses the same precedence for definition metadata: those keys belong to this package, and a collision is a mistake.

    The materialization is rebuilt, not mutated, because `dg.MaterializeResult` is immutable. Every field is named, so a seventh field added upstream would silently drop. `tests/test_upstream_characterization.py` pins the six and fails there instead.

    Parameters
    ----------
    results
        What `validation_results` yielded, materializations and check results alike.
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
