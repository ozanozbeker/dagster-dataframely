"""What a schema-backed asset runs after its transform: check the shape, stage, filter, then one of five exits.

The asset's declared shape is the failure policy. There is no lenient/strict flag anywhere, so the failure behaviour is visible in the definition rather than in an argument's value, and it cannot disagree with what the asset actually declares. Declaring a quarantine out is what splits three exits into five: it is the consent to partial data, and its absence is the refusal.

The middle phase is the only one a transform can skip, and its return type is what skips it: see `_staged_frame` for what a plan buys by staging. Validation itself is eager and stays that way, because this package does not promise to write a file, it promises to write a file and report on it: `dy.FailureInfo` is eager by construction, the statistics pass runs two global aggregates, and no exit can be chosen without counting both halves of the split. `docs/research/lazyframe-end-to-end.md` has the measurements.
"""

import tempfile
from collections.abc import Iterator, Mapping
from pathlib import Path

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._checks import _rule_results
from dagster_dataframely._errors import (
    NothingSurvivedError,
    SchemaShapeError,
    ValidationAbortError,
)
from dagster_dataframely._naming import SHAPE_CHECK, check_name, validation_rules
from dagster_dataframely._samples import SAMPLE_KEY, sample_metadata, sample_rows
from dagster_dataframely._settings import (
    MAX_FAILURE_SAMPLES,
    ROW_SAMPLE,
    STATISTICS,
    TEMP_DIR,
    Granularity,
    MultiColumnRules,
)
from dagster_dataframely._statistics import statistics_metadata

AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]

#: What both staging files name their temp directory. Shared with the IO manager, which sinks a lazy output through one of its own, so an operator sweeping a filled disk finds every staging file this package makes with one glob.
STAGING_PREFIX = "dagster_dataframely_"


def _require_frame(frame: object, out_name: str) -> None:
    """Rejects a transform output the shape check cannot read.

    The parameter's annotation is a promise Dagster cannot enforce, because it calls the transform dynamically. Left alone, a forgotten return annotation surfaces two frames down as `'NoneType' object has no attribute 'collect_schema'`.

    Dagster's own error rather than the package's: this is a wiring mistake, not a data one, which is the line `_ParquetIOManager` already draws.
    """
    if isinstance(frame, (pl.DataFrame, pl.LazyFrame)):
        return
    wrong_type: str = f"'{out_name}' returned a {type(frame).__name__}. A schema-backed asset must return a polars DataFrame or LazyFrame, because the shape check reads its columns and dtypes before anything is written. An asset that manages its own storage has no schema to validate, so write it as a plain `@dg.asset`."
    raise dg.DagsterInvariantViolationError(wrong_type)


def shape_problems(
    schema: type[dy.Schema], frame: pl.DataFrame | pl.LazyFrame
) -> list[dict[str, str]]:
    """Compares the frame's shape against the schema, naming every mismatch.

    An explicit pre-check rather than a `try`/`except` around `filter`, which would behave differently depending on what the transform returned: `filter(cast=False)` raises at call time on a `DataFrame`, but on a `LazyFrame` it returns cleanly and the same error surfaces only on the eventual collect. The decorator promises either return type works, so the shape check cannot be built on a difference between them.

    Package-internal rather than private, because the CSV IO manager asks the same question of an asset that reached it without the decorator. Sharing the comparison is what stops two shape checks from drawing the line differently.

    Only public API, and none of it executes: `collect_schema()` resolves a `LazyFrame`'s shape without running it.

    Args:
        schema: The schema the frame claims to match.
        frame: The frame to compare, eager or lazy.

    Returns:
        One mapping of `column`, `expected` and `actual` per offending column, empty when the frame matches. The same list feeds the failing check's metadata and `SchemaShapeError`, so the two cannot disagree.
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


def _staged_frame(frame: pl.LazyFrame, *, temp_dir: str | None) -> pl.DataFrame:
    """Streams a plan to a local parquet, reads it back whole, and removes the file.

    What this buys is the peak. The plan's high-water mark becomes the size of the frame it produced, which is the saving for a transform with a large intermediate: a join that fans out before filtering back down otherwise pays for the fan-out in memory. What it costs is one local write and one local read of that frame, which is why an eager return never comes here. A frame the user already materialized has nothing left to stream, so staging it would be pure cost.

    The file is gone before this returns, so no exit can leave one behind, including the two whose whole purpose is that nothing is written.

    A configured directory is not created. The setting exists to move the staging file off a container's ephemeral disk, so a mistyped path silently created there is exactly the failure somebody set it to avoid.

    Args:
        frame: The plan to stage.
        temp_dir: Where the staging file goes, or `None` for wherever `tempfile` puts things. That absence is why the package default is not `tempfile.gettempdir()`: the decorator resolves every setting where the asset is *declared*, so an unset setting has to mean the temp directory of whichever process stages the frame.

    Returns:
        The frame the plan produced, read back whole.

    Raises:
        FileNotFoundError: `temp_dir` names a directory that does not exist.
    """
    with tempfile.TemporaryDirectory(dir=temp_dir, prefix=STAGING_PREFIX) as staging:
        path = Path(staging) / "staged.parquet"
        # Named rather than left to `auto`, because the streaming engine is the whole reason to stage: an engine that chose to collect would pay the write and keep the peak.
        frame.sink_parquet(path, engine="streaming")
        return pl.read_parquet(path)


def _shape_failure(
    problems: list[dict[str, str]], *, asset_key: dg.AssetKey
) -> dg.AssetCheckResult:
    """Builds the failing shape check, tabulating every offending column."""
    return dg.AssetCheckResult(
        check_name=SHAPE_CHECK,
        asset_key=asset_key,
        passed=False,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "dy_schema__errors": dg.MetadataValue.table(
                [dg.TableRecord(problem) for problem in problems]
            )
        },
    )


def _check_results(  # noqa: PLR0913 - every setting the specs were derived with has to reach the results, or the two disagree
    schema: type[dy.Schema],
    failure: dy.FailureInfo,
    *,
    asset_key: dg.AssetKey,
    aborting: bool,
    check_granularity: Granularity | None,
    multi_column_rules: MultiColumnRules | None,
    max_failure_samples: int | None,
) -> list[dg.AssetCheckResult]:
    """Builds every check result for a run that made it past the shape check.

    Severity is derived here, once, from whether the valid table was written. That is what makes it a property of the run's outcome rather than of any one rule: no code path can hand two sibling checks different severities. A invalid row with a quarantine to go to is a warning; the same row with nowhere to go, or with nothing left beside it, is an error.

    The shape check is not a rule, so it reports on its own at every granularity and never joins a rule set.
    """
    severity = dg.AssetCheckSeverity.ERROR if aborting else dg.AssetCheckSeverity.WARN
    return [
        dg.AssetCheckResult(check_name=SHAPE_CHECK, asset_key=asset_key, passed=True),
        *_rule_results(
            schema,
            failure,
            asset_key=asset_key,
            severity=severity,
            check_granularity=check_granularity,
            multi_column_rules=multi_column_rules,
            max_failure_samples=max_failure_samples,
        ),
    ]


def quarantine_frame(schema: type[dy.Schema], failure: dy.FailureInfo) -> pl.DataFrame:
    """Builds the frame the quarantine out materializes.

    `FailureInfo.details()` rather than `invalid()`: the invalid rows plus a rule column for every rule, reading `valid` / `invalid` / `unknown`. Attribution has to be here because check-metadata samples are bounded, so without it the per-row detail exists nowhere at volume.

    Two changes to what dataframely hands over. The rule columns are renamed into the reserved namespace, so a column of this table and the asset check for the same rule are the same string. And they are cast from `Enum` to `String`, which is mandatory rather than defensive: a raw `Enum` panics the Delta writer with a Rust `unreachable!()`. It is the one cast this package makes, and it touches only columns the package itself generated.

    Args:
        schema: The schema that rejected the rows.
        failure: What `Schema.filter` reported.

    Returns:
        The invalid rows: the original columns in their own order, then a `String` rule column for every rule.
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

    **The rows are sorted, and they have to be.** `cooccurrence_counts()` builds its mapping out of a `group_by` with no `maintain_order`, so the order it hands over is arbitrary: the same frame twice already emits these rows differently, which makes two runs of the same data diff as though something changed. Biggest group first is also the reading order the table exists for, since the question it answers is which broken upstream field trips the most rows at once. Ties break on the names, which is the same sort this function already applies inside each set.

    Args:
        counts: How many rows broke each set of rules together. The key is a `frozenset` and therefore unordered, so it is sorted before rendering.

    Returns:
        One record per co-occurring set, most rows first, ready for the quarantine's materialization metadata.
    """
    # The count is negated so that a plain tuple sort puts the biggest group first and falls back to the names for a tie, in one pass and with no key function.
    ordered: list[tuple[int, str]] = sorted(
        (-n, ", ".join(sorted(check_name(rule) for rule in rules)))
        for rules, n in counts.items()
    )
    return dg.MetadataValue.table(
        [dg.TableRecord({"rules": rules, "count": -n}) for n, rules in ordered]
    )


def process(  # noqa: PLR0913 - hand-wiring needs everything the decorator decides to be passable by hand
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    context: dg.AssetExecutionContext,
    valid_out: str,
    quarantine_out: str | None = None,
    check_granularity: Granularity | None = None,
    multi_column_rules: MultiColumnRules | None = None,
    max_failure_samples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
    temp_dir: str | None = None,
) -> AssetYield:
    """Validates a transform's output and reports it to Dagster.

    Three phases and five exits. The shape check runs first, so a wrong-shaped frame never pays to be staged or filtered. A lazy frame is then staged to a local parquet and read back whole, which is what keeps the peak at the frame's size rather than the plan's; an eager one skips that phase, having nothing left to stream. Finally `Schema.filter` splits the rows, with `cast=False`: it is the only validation call, because `validate()` carries per-rule detail as a string and this package needs structured counts.

    Which of the five a run reaches is decided by the asset's shape, never by an argument's value. `quarantine_out` is the whole policy: with it, invalid rows are written next door and the run stays green; without it, the same rows fail the run. The one case it does not rescue is nothing surviving, where the valid out is skipped rather than materialized empty.

    Args:
        schema: The schema the frame must satisfy.
        frame: Whatever the transform returned.
        context: The executing asset's context, for resolving each out to its key.
        valid_out: The output name the validated frame materializes under.
        quarantine_out: The output name the invalid rows materialize under, or `None` when the asset declares no quarantine.
        check_granularity: How far the rules collapse. Pass the same value the check specs were derived with: the decorator resolves it once at definition time and hands the resolved value to both, so a run cannot report against a check list it did not declare.
        multi_column_rules: Where the rules no single column owns land at `column` granularity, on the same terms.
        max_failure_samples: How many of the rows a rule rejected reach that rule's check metadata. Unset resolves through the settings chain, which ships five.
        statistics: Whether each materialization carries statistics for what it wrote. Unset resolves through the settings chain, which ships it on.
        row_sample: How many of the valid output's rows reach its materialization metadata. Unset resolves through the settings chain, which ships five.
        temp_dir: Where a lazy frame is staged. Unset resolves through the settings chain, which ships the system temp directory. Read only on the lazy path, so an eager frame is unaffected by whatever it holds.

    Yields:
        A `MaterializeResult` per out that survived its outcome, and every check result: bundled onto the valid out's materialization where there is one, standalone where the valid out is skipped.

    Raises:
        InvalidSettingError: A setting resolved to a value outside its vocabulary.
        DagsterInvariantViolationError: The transform returned something that is not a polars frame.
        FileNotFoundError: `temp_dir` names a directory that does not exist, on a run that had a plan to stage.
        SchemaShapeError: The frame's columns or dtypes do not match the schema.
        ValidationAbortError: Rows were rejected and no quarantine is declared.
        NothingSurvivedError: Rows were rejected and none survived.
    """
    _require_frame(frame, valid_out)
    valid_key = context.asset_key_for_output(valid_out)
    # Resolved before the shape check so a mistyped environment variable fails the same way at every exit, rather than only on the runs that reach the one reading it.
    emit_statistics: bool = STATISTICS.resolve(statistics)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)
    staging_dir: str | None = TEMP_DIR.resolve(temp_dir)

    # --- Phase 1: the shape check ---
    problems: list[dict[str, str]] = shape_problems(schema, frame)
    if problems:
        # Exit: pipeline defect. Nothing is filtered and neither out is written, so a wrong-shaped frame cannot corrupt either table.
        yield _shape_failure(problems, asset_key=valid_key)
        raise SchemaShapeError(schema.__name__, problems)

    # --- Phase 2: staging ---
    # A plan streams to a local parquet and comes back as the frame it produced. An eager frame passes straight through, because there is nothing left to stream.
    materialized: pl.DataFrame = (
        _staged_frame(frame, temp_dir=staging_dir)
        if isinstance(frame, pl.LazyFrame)
        else frame
    )

    # --- Phase 3: the row filter ---
    # Eager either way by now, which is what `filter` would have done anyway: it collects internally, and `row_count` needs the length.
    result, failure = schema.filter(materialized, cast=False)
    # Annotated because `filter` returns dataframely's phantom `dy.DataFrame[Schema]`, and the out is declared as a plain polars frame.
    valid: pl.DataFrame = result
    rejected: int = len(failure)
    # A quarantine is consent to partial data, not to no data, so nothing surviving aborts even with one declared.
    aborting = bool(rejected) and (quarantine_out is None or not len(valid))
    checks = _check_results(
        schema,
        failure,
        asset_key=valid_key,
        aborting=aborting,
        check_granularity=check_granularity,
        multi_column_rules=multi_column_rules,
        max_failure_samples=failure_samples,
    )

    def valid_result() -> dg.MaterializeResult[pl.DataFrame]:
        """Builds the valid out's materialization, where it is yielded rather than ahead of every exit.

        Two of the five discard it, and since the statistics are a pass over the whole frame, building it early would charge an aborting run for a table nobody will see.
        """
        return dg.MaterializeResult(
            asset_key=valid_key,
            value=valid,
            metadata={
                "dagster/row_count": len(valid),
                **statistics_metadata(valid, enabled=emit_statistics),
                **sample_metadata(SAMPLE_KEY, sample_rows(valid, sampled_rows)),
            },
            check_results=checks,
        )

    if not rejected:
        # Exit: everything survived. The quarantine out is skipped rather than written empty, so an empty quarantine partition means something.
        yield valid_result()
        return

    if quarantine_out is None:
        # Exit: data defect with nowhere to route it, so consent to partial data was never given.
        # Both halves are discarded and the last-known-good table survives, but every rule still reports, so the failed run says what failed and by how much.
        yield from checks
        raise ValidationAbortError(schema.__name__, rejected, failure.counts())

    quarantine_key = context.asset_key_for_output(quarantine_out)
    # Bound once: the frame is written and summarized, and `quarantine_frame` rebuilds it out of `details()` on every call.
    invalid: pl.DataFrame = quarantine_frame(schema, failure)
    quarantine_result = dg.MaterializeResult(
        asset_key=quarantine_key,
        value=invalid,
        metadata={
            "dagster/row_count": rejected,
            "cooccurrence": _cooccurrence(failure.cooccurrence_counts()),
            # Summarized like any other table, because it is one somebody reads. What the rejected values look like in aggregate is the question the checks and `cooccurrence` do not answer: those say which rules failed and how often, never what the rows that failed them hold.
            **statistics_metadata(invalid, enabled=emit_statistics),
            # No row sample, deliberately. These rows already reach the event log once, through the check that rejected each of them and with the rule attached. A second copy here would carry less and cost the same.
        },
    )

    if not len(valid):
        # Exit: nothing survived. The rows are all inspectable next door, but the valid out is skipped so an empty table cannot replace a last-known-good snapshot.
        # The checks are yielded standalone: there is no valid materialization to bundle them onto.
        yield quarantine_result
        yield from checks
        raise NothingSurvivedError(
            schema.__name__, rejected, failure.counts(), quarantine_key.to_user_string()
        )

    # Exit: the middle case. The survivors are written, the rest are inspectable next door, and downstream proceeds on the data that is fine.
    yield valid_result()
    yield quarantine_result
