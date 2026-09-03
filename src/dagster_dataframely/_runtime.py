"""What a schema-backed asset runs after its decorated function: check the column schema, split the rows, then one of six exits.

The asset's declaration is the failure policy. There is no lenient/strict flag anywhere, so the failure behaviour is visible in the definition rather than in an argument's value, and it cannot disagree with what the asset actually declares. Declaring a quarantine splits four exits into six. It is the consent to partial data, and its absence is the refusal.

The sixth exit is the skip, and it is the one the asset's declaration does not decide. A decorated function that returns `None` says this partition has no source data and never will, which is neither a failure nor an empty table. Nothing is validated and nothing materializes, so the partition stays unmaterialized rather than going green with zero rows (#95).

A decorated function's return type decides nothing past the column-schema check. `Schema.filter` takes a plan, so one call splits both return types, on the streaming engine. A `DataFrame` costs a free `.lazy()`. A `LazyFrame` executes once, at the split, with its intermediates streamed and only what it produced held. The peak is that frame plus one boolean column per rule, in the engine's cache, and the two halves it is split into.

Validation materializes both halves, and stays that way. This package does not promise to write a file. It promises to write a file and report on it. Every exit past the split counts, samples, writes or profiles the halves, and none can be chosen without counting both. `docs/research/lazyframe-end-to-end.md` has the measurements, and its §12 has why the split runs in the engine rather than through a file on disk.
"""

from collections.abc import Iterator, Mapping

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._checks import rule_results
from dagster_dataframely._frames import column_schema_problems
from dagster_dataframely._naming import (
    COLUMN_SCHEMA_CHECK,
    check_name,
    validation_rules,
)
from dagster_dataframely._quarantine import QuarantineWriter
from dagster_dataframely._samples import VALID_SAMPLE_KEY, sample_metadata, sample_rows
from dagster_dataframely._settings import (
    MAX_FAILURE_SAMPLES,
    ROW_SAMPLE,
    STATISTICS,
    Granularity,
    MultiColumnRules,
)
from dagster_dataframely._statistics import statistics_metadata
from dagster_dataframely.errors import (
    ColumnSchemaError,
    NothingSurvivedError,
    ValidationAbortError,
)

AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]

#: Where the invalid rows went, as the writer rendered it. `address` rather than `path`, because the answer can be a database table. Dagster's own word, from `TableMetadataSet.extract_storage_address`.
_ADDRESS_KEY = "dataframely/quarantine_address"

#: How many rows were held back.
_INVALID_COUNT_KEY = "dataframely/invalid_count"

#: How many rows failed each set of rules together.
_INVALID_BY_RULES_KEY = "dataframely/invalid_by_rules"

#: A bounded sample of the rows that were held back, rule columns included.
_INVALID_SAMPLE_KEY = "dataframely/invalid_sample"

#: What the four invalid-row keys hold, spelled out because Dagster's own metadata union is wider than anything built here.
type InvalidMetadata = Mapping[str, str | int | dg.TableMetadataValue]


def _require_frame(frame: object, asset: str) -> None:
    """Refuse a return value neither the column-schema check nor the skip can read.

    The parameter's annotation is a promise Dagster cannot enforce, because it calls the decorated function dynamically. Left alone, a forgotten return annotation used to surface two frames down as `'NoneType' object has no attribute 'collect_schema'`.

    `None` is no longer that mistake. It is the skip, so the forgotten return annotation and the deliberate skip are now the same object and this guard cannot tell them apart. That trade is taken knowingly: the skip has to be spelled as a value for a decorated function to reach it at all, and `None` is the only value every early return already produces. What a forgotten `return` costs is a run that quietly materializes nothing, which the missing partition makes visible.

    Dagster's own error rather than the package's. This is a wiring mistake, not a data one.

    A `dg.MaterializeResult` reaching here is hand-wiring, and the message says which decorator unwraps one. `dy_asset` takes the frame off it before `process` sees anything (#77), so on that path this guard sees only what the result carried.

    The message names four routes. Sending the reader to a plain `@dg.asset` was the whole of the old advice, which made it wrong for anyone who wanted metadata on a validated table. It is right for the one case it still closes: an asset that writes its own storage and never holds a frame at all. That reader gets told what they keep, since `schema_metadata` fills a plain asset's Columns tab and nothing about it needs the decorator.
    """
    if frame is None or isinstance(frame, (pl.DataFrame, pl.LazyFrame)):
        return
    wrong_type: str = f"'{asset}' returned a {type(frame).__name__}. A schema-backed asset must return a Polars DataFrame or LazyFrame, because the column-schema check reads its columns and dtypes before anything is written. `dy_asset` also accepts a `dg.MaterializeResult` carrying one, which is how metadata, tags and a data version reach the materialization, and `None` to skip the asset where a partition has no source data. An asset that writes its own storage has no frame for this package to validate, so write it as a plain `@dg.asset`, where `dagster_dataframely.wiring.schema_metadata` still fills its Columns tab."
    raise dg.DagsterInvariantViolationError(wrong_type)


def _shape_failure(
    problems: list[dict[str, str]], *, asset_key: dg.AssetKey
) -> dg.AssetCheckResult:
    """Build the failing column-schema check, tabulating every offending column."""
    return dg.AssetCheckResult(
        check_name=COLUMN_SCHEMA_CHECK,
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
    """Build every check result for a run that made it past the column-schema check.

    Severity is derived here, once, from whether the valid table was written. That makes it a property of the run's outcome rather than of any one rule, so no code path can hand two sibling checks different severities. An invalid row with a quarantine to go to is a warning. The same row with nowhere to go, or with nothing left beside it, is an error.

    The column-schema check is not a rule, so it reports on its own at every granularity and never joins a rule set.
    """
    severity = dg.AssetCheckSeverity.ERROR if aborting else dg.AssetCheckSeverity.WARN
    return [
        dg.AssetCheckResult(
            check_name=COLUMN_SCHEMA_CHECK, asset_key=asset_key, passed=True
        ),
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
    """Build the frame the writer is handed.

    `FailureInfo.details()` rather than `invalid()`: the invalid rows plus a rule column for every rule, reading `valid` / `invalid` / `unknown`. Attribution has to be here because check-metadata samples are bounded. Without it the per-row detail exists nowhere at volume.

    Two changes to what Dataframely hands over. The rule columns are renamed into the reserved namespace, so a column of this table and the asset check for the same rule are the same string. And they are cast from `Enum` to `String`, which is mandatory rather than defensive: a raw `Enum` panics the Delta writer with a Rust `unreachable!()`. It is the one cast this package makes, and it touches only columns the package itself generated.

    Parameters
    ----------
    schema
        The schema the rows failed.
    failure
        What `Schema.filter` reported.

    Returns
    -------
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


def _addressed(
    checks: list[dg.AssetCheckResult], address: str
) -> list[dg.AssetCheckResult]:
    """Copy the quarantine's address onto every check result.

    For the exits that raise. There is no materialization to carry the address on those, and a reader looking at a failed run still has to be told where its evidence went. A run that materializes carries the address once, on the table, rather than once per check.

    Rebuilt rather than mutated, because `dg.AssetCheckResult` is a tuple. Every field is named, so a seventh added upstream would silently drop; `tests/test_upstream_characterization.py` pins the six and fails there instead.
    """
    return [
        dg.AssetCheckResult(
            passed=check.passed,
            asset_key=check.asset_key,
            check_name=check.check_name,
            metadata={**(check.metadata or {}), _ADDRESS_KEY: address},
            severity=check.severity,
            description=check.description,
        )
        for check in checks
    ]


def _cooccurrence(counts: Mapping[frozenset[str], int]) -> dg.TableMetadataValue:
    """Tabulate which rules a row broke together.

    One broken upstream field tripping three rules at once then reads as one row rather than as three unrelated counts.

    Rules are named as their asset checks, not as Dataframely names them. Both places this table sends a reader spell them that way: the check list, and the quarantine's own columns. The original name lives on `dy_rule` in each check's metadata.

    **The rows are sorted, and they have to be.** `cooccurrence_counts()` builds its mapping out of a `group_by` with no `maintain_order`, so the order it hands over is arbitrary. The same frame twice already emits these rows differently, which makes two runs of the same data diff as though something changed. Biggest group first is also the reading order the table exists for, since the question it answers is which broken upstream field trips the most rows at once. Ties break on the names, the same sort this function already applies inside each set.

    Parameters
    ----------
    counts
        How many rows broke each set of rules together. The key is a `frozenset` and therefore unordered, so it is sorted before rendering.

    Returns
    -------
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
    frame: pl.DataFrame | pl.LazyFrame | None,
    *,
    valid_key: dg.AssetKey,
    quarantine_writer: QuarantineWriter | None = None,
    check_granularity: Granularity | None = None,
    multi_column_rules: MultiColumnRules | None = None,
    max_failure_samples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
) -> AssetYield:
    """Validate a decorated function's output and report it to Dagster.

    Two phases and six exits. The skip takes the first exit and runs no phase at all. Otherwise the column-schema check runs first, off `collect_schema()`, so a frame whose columns do not match never executes. Then `Schema.filter` splits the rows, with `cast=False`, in one `collect_all` on the streaming engine. A `DataFrame` return takes the same call after a free `.lazy()`, so there is one split path and nothing downstream of it can tell the two returns apart. It is the only validation call, because `validate()` carries per-rule detail as a string and this package needs structured counts.

    Which of the other five a run reaches is decided by the asset's declaration, never by an argument's value. `quarantine_writer` is the whole policy. With it, invalid rows are written to the quarantine and the run stays green. Without it, the same rows fail the run. The one case it does not rescue is nothing surviving, where the table is skipped rather than materialized empty.

    **This writes the quarantine and never learns where it went.** The writer is called with the invalid rows and hands back an address: no directory, no file path, no IO manager, no context (ADR-0001). The decorator builds the writer, which is the one place the execution context is read; a hand-wirer builds their own, or reaches for `delegating_writer` and `file_writer`.

    **The skip still reports every check, and it has to.** A check spec is a non-optional op output whatever `output_required` the asset carries, so a step that returns without answering one dies on `did not return an output for non-optional output`. The rules therefore run over `Schema.create_empty()` and report what that says, which is a pass for every one of them. That is computed rather than asserted: each rule was evaluated, over zero rows, and none was violated. Dagster records the evaluations with no `target_materialization_data`, so a passing check on a skipped partition does not attach itself to some earlier run's materialization (#95).

    Parameters
    ----------
    schema
        The schema the frame must satisfy.
    frame
        Whatever the decorated function returned, or `None` to skip.
    valid_key
        The asset key the validated frame materializes under. `context.asset_key` is the whole of it on a single-asset step, which is what `dg.asset` builds. A key the asset does not own fails the step on the first yield with `Asset key ... not found in AssetsDefinition`, so build it from the definition rather than by hand. The decorator resolves it where the asset is declared, which is what leaves it callable outside a run (ADR-0002).
    quarantine_writer
        What puts the invalid rows somewhere a reader can open them, or `None` when the asset declares no quarantine. It is called once, with the quarantine frame, on each of the two exits a declared quarantine can reach with rows to hold: the partial one and the one where nothing survived. Its return value is reported as the quarantine's address. Passing `None` is the third of those exits, where there is nowhere to route the rows and the run aborts instead.
    check_granularity
        How far the rules collapse. Pass the same value the check specs were derived with. The decorator resolves it once at definition time and hands the resolved value to both, so a run cannot report against a check list it did not declare.
    multi_column_rules
        Where the rules no single column owns land at `column` granularity, on the same terms.
    max_failure_samples
        How many of the rows that failed a rule reach that rule's check metadata. Unset resolves through the settings chain, which ships five.
    statistics
        Whether each materialization carries statistics for what it wrote. Unset resolves through the settings chain, which ships it on.
    row_sample
        How many rows reach the materialization metadata, of what was written and of what was held back. Unset resolves through the settings chain, which ships five.

    Yields
    ------
    The valid table's `MaterializeResult` where the run wrote one, then every check result standalone. Nothing is bundled onto a materialization, deliberately. Direct invocation satisfies a check output only from a standalone `AssetCheckResult`, which is what makes an asset built on this callable in a unit test (ADR-0002). Every result carries its own `asset_key`, so a standalone yield is fully addressed. The quarantine is written rather than yielded, so it never appears here: it is evidence of a run, not an out.

    Raises
    ------
    InvalidSettingError
        A setting resolved to a value outside its vocabulary.
    DagsterInvariantViolationError
        The decorated function returned something that is neither a Polars frame nor `None`.
    ColumnSchemaError
        The frame's columns or dtypes do not match the schema.
    ValidationAbortError
        Rows failed validation and no quarantine is declared.
    NothingSurvivedError
        Rows failed validation and none survived.
    """
    _require_frame(frame, valid_key.to_user_string())
    # Resolved before the column-schema check so a mistyped environment variable fails the same way at every exit, rather than only on the runs that reach the one reading it.
    emit_statistics: bool = STATISTICS.resolve(statistics)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)

    if frame is None:
        # Exit: no source data. Neither output is yielded, so the partition stays unmaterialized rather than going green with zero rows, and the run stays green rather than reporting a defect that is not one.
        # The rules still report, over an empty frame, because a check spec is a non-optional op output and a step that answers none of them fails.
        _, nothing = schema.filter(schema.create_empty(), cast=False)
        yield from _check_results(
            schema,
            nothing,
            asset_key=valid_key,
            # Nothing failed and nothing was written, so the severity the other exits derive from the outcome has no failure to grade.
            aborting=False,
            check_granularity=check_granularity,
            multi_column_rules=multi_column_rules,
            max_failure_samples=failure_samples,
        )
        return

    # --- Phase 1: the column-schema check ---
    problems: list[dict[str, str]] = column_schema_problems(schema, frame)
    if problems:
        # Exit: pipeline defect. Nothing is filtered and neither output is written, so a frame whose columns do not match cannot corrupt either table.
        yield _shape_failure(problems, asset_key=valid_key)
        raise ColumnSchemaError(schema.__name__, problems)

    # --- Phase 2: the split ---
    # A plan executes here, once: `collect_all` runs both halves off one cached evaluation, so the source is not read twice.
    # The engine is named rather than left to `auto`. Polars falls back to the in-memory engine for anything streaming cannot run, so naming it never fails a plan. An `auto` that chose to collect would keep the plan's own peak.
    result, failure = schema.filter(frame.lazy(), cast=False).collect_all(
        engine="streaming"
    )
    # Annotated because `collect_all` returns Dataframely's phantom `dy.DataFrame[Schema]`, and the asset is declared as a plain Polars frame.
    valid: pl.DataFrame = result
    invalid_count: int = len(failure)
    # A quarantine is consent to partial data, not to no data, so nothing surviving aborts even with one declared.
    aborting = bool(invalid_count) and (quarantine_writer is None or not len(valid))
    checks = _check_results(
        schema,
        failure,
        asset_key=valid_key,
        aborting=aborting,
        check_granularity=check_granularity,
        multi_column_rules=multi_column_rules,
        max_failure_samples=failure_samples,
    )

    def valid_result(
        invalid_metadata: InvalidMetadata | None = None,
    ) -> dg.MaterializeResult[pl.DataFrame]:
        """Build the materialization, where it is yielded rather than ahead of every exit.

        Two of the five exits discard it, and the statistics are a pass over the whole frame, so building it early would charge an aborting run for a table nobody will see.

        Parameters
        ----------
        invalid_metadata
            What the run held back, on the one exit that both wrote a table and had rows fail. Absent on a clean run, where there is nothing to say.
        """
        return dg.MaterializeResult(
            asset_key=valid_key,
            value=valid,
            metadata={
                "dagster/row_count": len(valid),
                **statistics_metadata(valid, enabled=emit_statistics),
                **sample_metadata(VALID_SAMPLE_KEY, sample_rows(valid, sampled_rows)),
                **(invalid_metadata or {}),
            },
        )

    if not invalid_count:
        # Exit: everything survived. The writer is never called, so a clean run leaves no empty quarantine and an empty one means something.
        yield valid_result()
        yield from checks
        return

    if quarantine_writer is None:
        # Exit: data defect with nowhere to route it, so consent to partial data was never given.
        # Both halves are discarded and the last-known-good table survives, but every rule still reports, so the failed run says what failed and by how much.
        yield from checks
        raise ValidationAbortError(schema.__name__, invalid_count, failure.counts())

    # Bound once: the frame is written and sampled, and `quarantine_frame` rebuilds it out of `details()` on every call.
    invalid: pl.DataFrame = quarantine_frame(schema, failure)
    # Written before anything below can raise, so the rows that failed a run are readable whichever way it ends.
    address: str = quarantine_writer(invalid)
    invalid_metadata: InvalidMetadata = {
        _ADDRESS_KEY: address,
        _INVALID_COUNT_KEY: invalid_count,
        _INVALID_BY_RULES_KEY: _cooccurrence(failure.cooccurrence_counts()),
        # Sized by the same setting as the valid rows' sample, because one number governs how many real rows a run puts in the event log. Statistics are deliberately not computed: what the invalid values look like in aggregate is a question about a table nobody is going to consume.
        **sample_metadata(_INVALID_SAMPLE_KEY, sample_rows(invalid, sampled_rows)),
    }

    if not len(valid):
        # Exit: nothing survived. The rows are all written, but the table is skipped so an empty one cannot replace a last-known-good snapshot.
        yield from _addressed(checks, address)
        raise NothingSurvivedError(
            schema.__name__, invalid_count, failure.counts(), address
        )

    # Exit: the middle case. The survivors are written, the rest are readable at the address the materialization names, and downstream proceeds on the data that is fine.
    yield valid_result(invalid_metadata)
    yield from checks
