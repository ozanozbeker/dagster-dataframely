"""The state machine a schema-backed asset runs: gate, filter, then one of five outcomes.

The asset's declared shape is the failure policy. There is no lenient/strict flag anywhere, so the failure behaviour is visible in the definition rather than in an argument's value, and it cannot disagree with what the asset actually declares. Declaring a quarantine out is what splits three outcomes into five: it is the consent to partial data, and its absence is the refusal.
"""

from collections.abc import Iterator, Mapping

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely.checks import _rule_results
from dagster_dataframely.errors import (
    NothingSurvivedError,
    SchemaGateError,
    ValidationAbortError,
)
from dagster_dataframely.naming import GATE_CHECK, check_name, validation_rules

AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]


def _require_frame(frame: object, out_name: str) -> None:
    """Rejects a transform output the gate cannot read.

    The parameter's annotation is a promise Dagster cannot enforce, because it calls the transform dynamically. Left alone, a forgotten return annotation surfaces two frames down as `'NoneType' object has no attribute 'collect_schema'`.

    Dagster's own error rather than the package's: this is a wiring mistake, not a data one, which is the line `_ParquetIOManager` already draws.
    """
    if isinstance(frame, (pl.DataFrame, pl.LazyFrame)):
        return
    wrong_type: str = f"'{out_name}' returned a {type(frame).__name__}. A schema-backed asset must return a polars DataFrame or LazyFrame, because the gate reads its columns and dtypes before anything is written. An asset that manages its own storage has no schema to validate, so write it as a plain `@dg.asset`."
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
    actual: pl.Schema = frame.collect_schema()
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
    schema: type[dy.Schema],
    failure: dy.FailureInfo,
    *,
    asset_key: dg.AssetKey,
    aborting: bool,
) -> list[dg.AssetCheckResult]:
    """Builds every check result for a run that made it past the gate.

    Severity is derived here, once, from whether the good table landed. That is what makes it a property of the run's outcome rather than of any one rule: no code path can hand two sibling checks different severities. A rejected row with a quarantine to go to is a warning; the same row with nowhere to go, or with nothing left beside it, is an error.
    """
    severity = dg.AssetCheckSeverity.ERROR if aborting else dg.AssetCheckSeverity.WARN
    return [
        dg.AssetCheckResult(check_name=GATE_CHECK, asset_key=asset_key, passed=True),
        *_rule_results(
            schema, failure.counts(), asset_key=asset_key, severity=severity
        ),
    ]


def quarantine_frame(schema: type[dy.Schema], failure: dy.FailureInfo) -> pl.DataFrame:
    """Builds the frame the quarantine out materializes.

    `FailureInfo.details()` rather than `invalid()`: the rejected rows plus one outcome column per rule reading `valid` / `invalid` / `unknown`. Attribution has to be here because check-metadata samples are bounded, so without it the per-row detail exists nowhere at volume.

    Two changes to what dataframely hands over. The outcome columns are renamed into the reserved namespace, so a column of this table and the asset check for the same rule are the same string. And they are cast from `Enum` to `String`, which is mandatory rather than defensive: a raw `Enum` panics the Delta writer with a Rust `unreachable!()`. It is the one cast this package makes, and it touches only columns the package itself generated.

    Args:
        schema: The schema that rejected the rows.
        failure: What `Schema.filter` reported.

    Returns:
        The rejected rows: the original columns in their own order, then one `String` outcome column per rule.
    """
    # Bound once: `details()` rebuilds the frame on every call.
    details: pl.DataFrame = failure.details()
    renames: dict[str, str] = {
        rule: check_name(rule)
        for rule in validation_rules(schema)
        if rule in details.collect_schema()
    }
    return details.rename(renames).with_columns(
        pl.col(name).cast(pl.String) for name in renames.values()
    )


def _cooccurrence(counts: Mapping[frozenset[str], int]) -> dg.TableMetadataValue:
    """Tabulates which rules a row broke together.

    One broken upstream field tripping three rules at once then reads as one row rather than as three unrelated counts.

    Rules are named as their asset checks, not as dataframely names them. Both places this table sends a reader spell them that way: the check list, and the quarantine's own columns. The original name lives on `dy_rule` in each check's metadata.

    Args:
        counts: How many rows broke each set of rules together. The key is a `frozenset` and therefore unordered, so it is sorted before rendering.

    Returns:
        One record per co-occurring set, ready for the quarantine's materialization metadata.
    """
    return dg.MetadataValue.table(
        [
            dg.TableRecord(
                {"rules": ", ".join(sorted(check_name(r) for r in rules)), "count": n}
            )
            for rules, n in counts.items()
        ]
    )


def process(
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    context: dg.AssetExecutionContext,
    good_out: str,
    quarantine_out: str | None = None,
) -> AssetYield:
    """Validates a transform's output and reports it to Dagster.

    Two stages and five exits. The gate runs first, so a wrong-shaped frame never pays to be filtered. Then `Schema.filter(frame, cast=False)` splits the rows: it is the only validation call, because `validate()` carries per-rule detail as a string and this package needs structured counts.

    Which of the five a run reaches is decided by the asset's shape, never by an argument's value. `quarantine_out` is the whole policy: with it, rejected rows land next door and the run stays green; without it, the same rows fail the run. The one case it does not rescue is nothing surviving, where the good out is skipped rather than materialized empty.

    Args:
        schema: The schema the frame must satisfy.
        frame: Whatever the transform returned.
        context: The executing asset's context, for resolving each out to its key.
        good_out: The output name the validated frame materializes under.
        quarantine_out: The output name the rejected rows materialize under, or `None` when the asset declares no quarantine.

    Yields:
        A `MaterializeResult` per out that survived its outcome, and every check result: bundled onto the good materialization where there is one, standalone where the good out is skipped.

    Raises:
        DagsterInvariantViolationError: The transform returned something that is not a polars frame.
        SchemaGateError: The frame's columns or dtypes do not match the schema.
        ValidationAbortError: Rows were rejected and no quarantine is declared.
        NothingSurvivedError: Rows were rejected and none survived.
    """
    _require_frame(frame, good_out)
    good_key = context.asset_key_for_output(good_out)

    # --- Stage 1: the schema gate ---
    problems: list[dict[str, str]] = _gate_problems(schema, frame)
    if problems:
        # Exit: pipeline defect. Nothing is filtered and neither out is written, so a wrong-shaped frame cannot corrupt either table.
        yield _gate_failure(problems, asset_key=good_key)
        raise SchemaGateError(schema.__name__, problems)

    # --- Stage 2: the row filter ---
    # Eager, not lazy: `filter` already collected, and `row_count` needs the length.
    result, failure = schema.filter(frame, cast=False)
    # Annotated because `filter` returns dataframely's phantom `dy.DataFrame[Schema]`, and the out is declared as a plain polars frame.
    good: pl.DataFrame = (
        result.collect() if isinstance(result, pl.LazyFrame) else result
    )
    rejected: int = len(failure)
    # A quarantine is consent to partial data, not to no data, so nothing surviving aborts even with one declared.
    aborting = bool(rejected) and (quarantine_out is None or not len(good))
    checks = _check_results(schema, failure, asset_key=good_key, aborting=aborting)

    good_result = dg.MaterializeResult(
        asset_key=good_key,
        value=good,
        metadata={"dagster/row_count": len(good)},
        check_results=checks,
    )

    if not rejected:
        # Exit: everything survived. The quarantine out is skipped rather than written empty, so an empty quarantine partition means something.
        yield good_result
        return

    if quarantine_out is None:
        # Exit: data defect with nowhere to route it, so consent to partial data was never given.
        # Both halves are discarded and the last-known-good table survives, but every rule still reports, so the failed run says what failed and by how much.
        yield from checks
        raise ValidationAbortError(schema.__name__, rejected, failure.counts())

    quarantine_key = context.asset_key_for_output(quarantine_out)
    quarantine_result = dg.MaterializeResult(
        asset_key=quarantine_key,
        value=quarantine_frame(schema, failure),
        metadata={
            "dagster/row_count": rejected,
            "cooccurrence": _cooccurrence(failure.cooccurrence_counts()),
        },
    )

    if not len(good):
        # Exit: nothing survived. The rows are all inspectable next door, but the good out is skipped so an empty table cannot replace a last-known-good snapshot.
        # The checks are yielded standalone: there is no good materialization to bundle them onto.
        yield quarantine_result
        yield from checks
        raise NothingSurvivedError(
            schema.__name__, rejected, failure.counts(), quarantine_key.to_user_string()
        )

    # Exit: the middle case. The survivors land, the rest are inspectable next door, and downstream proceeds on the data that is fine.
    yield good_result
    yield quarantine_result
