"""What a schema-backed asset runs after its decorated function: check the shape, stage, filter, then one of five exits.

The asset's declared shape is the failure policy. There is no lenient/strict flag anywhere, so the failure behaviour is visible in the definition rather than in an argument's value, and it cannot disagree with what the asset actually declares. Declaring a quarantine out is what splits three exits into five: it is the consent to partial data, and its absence is the refusal.

The middle phase is the only one a decorated function can skip, and its return type is what skips it: see `_staged_frame` for what a plan buys by staging. Validation itself is eager and stays that way, because this package does not promise to write a file, it promises to write a file and report on it: `dy.FailureInfo` is eager by construction, the statistics pass runs two global aggregates, and no exit can be chosen without counting both halves of the split. `docs/research/lazyframe-end-to-end.md` has the measurements.
"""

from collections.abc import Iterator, Mapping

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._checks import rule_results
from dagster_dataframely._frames import shape_problems, staging
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
from dagster_dataframely.errors import (
    NothingSurvivedError,
    SchemaShapeError,
    ValidationAbortError,
)

AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]


def _require_frame(frame: object, asset: str) -> None:
    """Rejects a return value the shape check cannot read.

    The parameter's annotation is a promise Dagster cannot enforce, because it calls the decorated function dynamically. Left alone, a forgotten return annotation surfaces two frames down as `'NoneType' object has no attribute 'collect_schema'`.

    Dagster's own error rather than the package's: this is a wiring mistake, not a data one, which is the line `_ParquetIOManager` already draws.

    A `dg.MaterializeResult` reaching here is hand-wiring, and the message says which decorator unwraps one. `dataframely_asset` takes the frame off it before `process` sees anything (#77), so on that path this guard sees only what the result carried.

    The message names three routes because two of them are new. Sending the reader to a plain `@dg.asset` was the whole of the old advice, which made it wrong for anyone who wanted metadata on a validated table; it is right for the one case it now closes, an asset that writes its own storage and never holds a frame at all. That reader gets told what they keep, since `schema_metadata` fills a plain asset's Columns tab and nothing about it needs the decorator.
    """
    if isinstance(frame, (pl.DataFrame, pl.LazyFrame)):
        return
    wrong_type: str = f"'{asset}' returned a {type(frame).__name__}. A schema-backed asset must return a Polars DataFrame or LazyFrame, because the shape check reads its columns and dtypes before anything is written. `dataframely_asset` also accepts a `dg.MaterializeResult` carrying one, which is how metadata, tags and a data version reach the materialization. An asset that writes its own storage has no frame for this package to validate, so write it as a plain `@dg.asset`, where `dagster_dataframely.wiring.schema_metadata` still fills its Columns tab."
    raise dg.DagsterInvariantViolationError(wrong_type)


def _staged_frame(frame: pl.LazyFrame, *, temp_dir: str | None) -> pl.DataFrame:
    """Streams a plan to a local parquet, reads it back whole, and removes the file.

    What this buys is the peak. The plan's high-water mark becomes the size of the frame it produced, which is the saving for a plan with a large intermediate: a join that fans out before filtering back down otherwise pays for the fan-out in memory. What it costs is one local write and one local read of that frame, which is why an eager return never comes here. A frame the user already materialized has nothing left to stream, so staging it would be pure cost.

    The file is gone before this returns, so no exit can leave one behind, including the two whose whole purpose is that nothing is written.

    Promoting it to the destination on a clean run, rather than letting the IO manager write the frame again, was considered and declined: the read back above is structural, so it saves one write and costs a boundary. `docs/research/lazyframe-end-to-end.md` §11 has the measurement.

    Args:
        frame: The plan to stage.
        temp_dir: Where the staging file goes, or `None` for wherever `tempfile` puts things. That absence is why the package default is not `tempfile.gettempdir()`: the decorator resolves every setting where the asset is *declared*, so an unset setting has to mean the temp directory of whichever process stages the frame.

    Returns:
        The frame the plan produced, read back whole.

    Raises:
        FileNotFoundError: `temp_dir` names a directory that does not exist.
    """
    with staging(temp_dir) as directory:
        path = directory / "staged.parquet"
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
        *rule_results(
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

    Two changes to what Dataframely hands over. The rule columns are renamed into the reserved namespace, so a column of this table and the asset check for the same rule are the same string. And they are cast from `Enum` to `String`, which is mandatory rather than defensive: a raw `Enum` panics the Delta writer with a Rust `unreachable!()`. It is the one cast this package makes, and it touches only columns the package itself generated.

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

    Rules are named as their asset checks, not as Dataframely names them. Both places this table sends a reader spell them that way: the check list, and the quarantine's own columns. The original name lives on `dy_rule` in each check's metadata.

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
    valid_key: dg.AssetKey,
    quarantine_key: dg.AssetKey | None = None,
    check_granularity: Granularity | None = None,
    multi_column_rules: MultiColumnRules | None = None,
    max_failure_samples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
    temp_dir: str | None = None,
) -> AssetYield:
    """Validates a decorated function's output and reports it to Dagster.

    Three phases and five exits. The shape check runs first, so a wrong-shaped frame never pays to be staged or filtered. A lazy frame is then staged to a local parquet and read back whole, which is what keeps the peak at the frame's size rather than the plan's; an eager one skips that phase, having nothing left to stream. Finally `Schema.filter` splits the rows, with `cast=False`: it is the only validation call, because `validate()` carries per-rule detail as a string and this package needs structured counts.

    Which of the five a run reaches is decided by the asset's shape, never by an argument's value. `quarantine_key` is the whole policy: with it, invalid rows are written next door and the run stays green; without it, the same rows fail the run. The one case it does not rescue is nothing surviving, where the valid out is skipped rather than materialized empty.

    Args:
        schema: The schema the frame must satisfy.
        frame: Whatever the decorated function returned.
        valid_key: The asset key the validated frame materializes under. Read it off the `AssetsDefinition` or resolve it with `context.asset_key_for_output(...)` rather than building it by hand: an out that declares `key_prefix` has a key its output name does not spell, and a key no out owns fails the step on the first yield with `Asset key ... not found in AssetsDefinition`. The decorator takes the first route, which is what leaves it callable outside a run (ADR-0002); the second needs one.
        quarantine_key: The asset key the invalid rows materialize under, or `None` when the asset declares no quarantine.
        check_granularity: How far the rules collapse. Pass the same value the check specs were derived with: the decorator resolves it once at definition time and hands the resolved value to both, so a run cannot report against a check list it did not declare.
        multi_column_rules: Where the rules no single column owns land at `column` granularity, on the same terms.
        max_failure_samples: How many of the rows a rule rejected reach that rule's check metadata. Unset resolves through the settings chain, which ships five.
        statistics: Whether each materialization carries statistics for what it wrote. Unset resolves through the settings chain, which ships it on.
        row_sample: How many of the valid output's rows reach its materialization metadata. Unset resolves through the settings chain, which ships five.
        temp_dir: Where a lazy frame is staged. Unset resolves through the settings chain, which ships the system temp directory. Read only on the lazy path, so an eager frame is unaffected by whatever it holds.

    Yields:
        A `MaterializeResult` per out that survived its outcome, then every check result standalone. Nothing is bundled onto a materialization, deliberately: direct invocation satisfies a check output only from a standalone `AssetCheckResult`, which is what makes an asset built on this callable in a unit test (ADR-0002). Every result carries its own `asset_key`, so a standalone yield is fully addressed.

    Raises:
        InvalidSettingError: A setting resolved to a value outside its vocabulary.
        DagsterInvariantViolationError: The decorated function returned something that is not a Polars frame.
        FileNotFoundError: `temp_dir` names a directory that does not exist, on a run that had a plan to stage.
        SchemaShapeError: The frame's columns or dtypes do not match the schema.
        ValidationAbortError: Rows were rejected and no quarantine is declared.
        NothingSurvivedError: Rows were rejected and none survived.
    """
    _require_frame(frame, valid_key.to_user_string())
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
    # Annotated because `filter` returns Dataframely's phantom `dy.DataFrame[Schema]`, and the out is declared as a plain Polars frame.
    valid: pl.DataFrame = result
    rejected: int = len(failure)
    # A quarantine is consent to partial data, not to no data, so nothing surviving aborts even with one declared.
    aborting = bool(rejected) and (quarantine_key is None or not len(valid))
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
        )

    if not rejected:
        # Exit: everything survived. The quarantine out is skipped rather than written empty, so an empty quarantine partition means something.
        yield valid_result()
        yield from checks
        return

    if quarantine_key is None:
        # Exit: data defect with nowhere to route it, so consent to partial data was never given.
        # Both halves are discarded and the last-known-good table survives, but every rule still reports, so the failed run says what failed and by how much.
        yield from checks
        raise ValidationAbortError(schema.__name__, rejected, failure.counts())

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
        yield quarantine_result
        yield from checks
        raise NothingSurvivedError(
            schema.__name__, rejected, failure.counts(), quarantine_key.to_user_string()
        )

    # Exit: the middle case. The survivors are written, the rest are inspectable next door, and downstream proceeds on the data that is fine.
    yield valid_result()
    yield quarantine_result
    yield from checks
