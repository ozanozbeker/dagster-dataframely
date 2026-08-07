"""The state machine a schema-backed asset runs: gate, filter, then one of three outcomes.

The asset's declared shape is the failure policy. There is no lenient/strict flag anywhere, so the failure behaviour is visible in the definition rather than in an argument's value, and it cannot disagree with what the asset actually declares.

The two outcomes that need somewhere to put rejected rows arrive with the quarantine (#19).
"""

from collections.abc import Iterator

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely.checks import _rule_results
from dagster_dataframely.errors import SchemaGateError, ValidationAbortError
from dagster_dataframely.naming import GATE_CHECK

AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]


def _require_frame(frame: object, out_name: str) -> None:
    """Rejects a transform output the gate cannot read.

    The parameter's annotation is a promise Dagster cannot enforce, because it calls the transform dynamically. Left alone, a forgotten return annotation surfaces two frames down as `'NoneType' object has no attribute 'collect_schema'`.

    Dagster's own error rather than the package's: this is a wiring mistake, not a data one, which is the line `_ParquetIOManager` already draws.
    """
    if isinstance(frame, (pl.DataFrame, pl.LazyFrame)):
        return
    wrong_type = (
        f"'{out_name}' returned a {type(frame).__name__}. A schema-backed asset must "
        f"return a polars DataFrame or LazyFrame, because the gate reads its columns "
        f"and dtypes before anything is written. An asset that manages its own storage "
        f"has no schema to validate, so write it as a plain `@dg.asset`."
    )
    raise dg.DagsterInvariantViolationError(wrong_type)


def _gate_problems(
    schema: type[dy.Schema], frame: pl.DataFrame | pl.LazyFrame
) -> list[dict[str, str]]:
    """Compares the frame's shape against the schema, naming every mismatch.

    An explicit pre-check rather than a `try`/`except` around `filter`, which would behave differently depending on what the transform returned: `filter(cast=False)` raises at call time on a `DataFrame`, but on a `LazyFrame` it returns cleanly and the same error surfaces only on the eventual collect. The door promises either return type works, so the gate cannot be built on a difference between them.

    Only public API, and none of it executes: `collect_schema()` resolves a `LazyFrame`'s shape without running it.

    Returns:
        One mapping of `column`, `expected` and `actual` per offending column, empty when the frame matches. The same list feeds the failing check's metadata and `SchemaGateError`, so the two cannot disagree.
    """
    actual = frame.collect_schema()
    return [
        {
            "column": name,
            "expected": str(column.dtype),
            "actual": str(actual[name]) if name in actual else "<missing>",
        }
        for name, column in schema.columns().items()
        if name not in actual or not column.validate_dtype(actual[name])
    ]


def _gate_failure(
    problems: list[dict[str, str]], *, asset_key: dg.AssetKey
) -> dg.AssetCheckResult:
    """Builds the failing gate check, tabulating every offending column."""
    return dg.AssetCheckResult(
        check_name=GATE_CHECK,
        asset_key=asset_key,
        passed=False,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "dy_schema__errors": dg.MetadataValue.table(
                [dg.TableRecord(problem) for problem in problems]
            )
        },
    )


def _check_results(
    schema: type[dy.Schema], failure: dy.FailureInfo, *, asset_key: dg.AssetKey
) -> list[dg.AssetCheckResult]:
    """Builds every check result for a run that made it past the gate.

    Severity is derived here, once, from whether the run rejected anything. That is what makes it a property of the run's outcome rather than of any one rule: no code path can hand two sibling checks different severities.
    """
    severity = (
        dg.AssetCheckSeverity.ERROR if len(failure) else dg.AssetCheckSeverity.WARN
    )
    return [
        dg.AssetCheckResult(check_name=GATE_CHECK, asset_key=asset_key, passed=True),
        *_rule_results(
            schema, failure.counts(), asset_key=asset_key, severity=severity
        ),
    ]


def process(
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    context: dg.AssetExecutionContext,
    good_out: str,
) -> AssetYield:
    """Validates a transform's output and reports it to Dagster.

    Two stages and three exits. The gate runs first, so a wrong-shaped frame never pays to be filtered. Then `Schema.filter(frame, cast=False)` splits the rows: it is the only validation call, because `validate()` carries per-rule detail as a string and this package needs structured counts.

    Args:
        schema: The schema the frame must satisfy.
        frame: Whatever the transform returned.
        context: The executing asset's context, for resolving the out to its key.
        good_out: The output name the validated frame materializes under.

    Yields:
        A `MaterializeResult` carrying the good frame and every check result, or, on either failure path, the check results on their own.

    Raises:
        DagsterInvariantViolationError: The transform returned something that is not a polars frame.
        SchemaGateError: The frame's columns or dtypes do not match the schema.
        ValidationAbortError: Rows were rejected and no quarantine is declared.
    """
    _require_frame(frame, good_out)
    good_key = context.asset_key_for_output(good_out)

    # --- Stage 1: the schema gate ---
    problems = _gate_problems(schema, frame)
    if problems:
        # Exit: pipeline defect. Nothing is filtered and nothing is written, so a wrong-shaped frame cannot corrupt the table.
        yield _gate_failure(problems, asset_key=good_key)
        raise SchemaGateError(schema.__name__, problems)

    # --- Stage 2: the row filter ---
    # Eager, not lazy: `filter` already collected, and `row_count` needs the length.
    result, failure = schema.filter(frame, cast=False)
    good = result.collect() if isinstance(result, pl.LazyFrame) else result
    checks = _check_results(schema, failure, asset_key=good_key)
    rejected = len(failure)

    if rejected:
        # Exit: data defect with no quarantine declared, so consent to partial data was never given.
        # Both halves are discarded and the last-known-good table survives, but every rule still reports, so the failed run says what failed and by how much.
        # The two quarantine exits land here with #19.
        yield from checks
        raise ValidationAbortError(schema.__name__, rejected, failure.counts())

    # Exit: everything survived. The only path that materializes.
    yield dg.MaterializeResult(
        asset_key=good_key,
        value=good,
        metadata={"dagster/row_count": len(good)},
        check_results=checks,
    )
