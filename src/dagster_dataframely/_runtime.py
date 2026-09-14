"""What a schema-backed asset runs after its decorated function: the column-schema check, `Schema.filter`, then one of six outcomes.

`USER_GUIDE.md` has the six and what each one writes.

The return type decides nothing past the column-schema check. `Schema.filter` takes a plan, so one call serves both return types on the streaming engine. A `DataFrame` costs a free `.lazy()`. A `LazyFrame` executes once, in that call, with its intermediates streamed and only its output held.

Validation materializes the valid rows and the invalid rows. This package promises to write a table and report on it, so every outcome past `Schema.filter` counts, samples, writes or profiles them, and none can be chosen without counting both. `docs/research/lazyframe-end-to-end.md` has the measurements; its §12 has why `Schema.filter` runs in the engine rather than through a file on disk.
"""

from collections.abc import Iterator, Mapping

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._checks import (
    column_schema_result,
    filtered,
    rule_columns,
    rule_results,
)
from dagster_dataframely._naming import check_name
from dagster_dataframely._quarantine import QuarantineWriter
from dagster_dataframely._rules import validate_namespace
from dagster_dataframely._samples import VALID_SAMPLE_KEY, sample_metadata, sample_rows
from dagster_dataframely._settings import (
    MAX_FAILURE_SAMPLES,
    ROW_SAMPLE,
    STATISTICS,
    Granularity,
    SchemaRules,
)
from dagster_dataframely._statistics import statistics_metadata
from dagster_dataframely.errors import NothingSurvivedError, ValidationAbortError

AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]
"""What a schema-backed asset yields: the valid table's materialization, then one standalone result per check. The annotation a hand-wired asset's compute function carries."""

_ADDRESS_KEY = "dataframely/quarantine_address"
"""Where the invalid rows went, as the writer rendered it. `address` rather than `path` because the answer can be a database table; the word is Dagster's, from `TableMetadataSet.extract_storage_address`."""

_INVALID_COUNT_KEY = "dataframely/invalid_count"
"""How many rows were held back."""

_INVALID_BY_RULES_KEY = "dataframely/invalid_by_rules"
"""How many rows failed each set of rules together."""

_INVALID_SAMPLE_KEY = "dataframely/invalid_sample"
"""A bounded sample of the rows that were held back, rule columns included."""

type InvalidMetadata = Mapping[str, str | int | dg.TableMetadataValue]
"""What the four invalid-row keys hold. Spelled out because Dagster's metadata union is wider than anything built here."""


def _require_frame(frame: object, asset: str) -> None:
    """Refuse a return value neither the column-schema check nor the skip can read.

    Dagster calls the decorated function dynamically, so it cannot enforce the return annotation. Before this guard, a forgotten return surfaced two frames down as `'NoneType' object has no attribute 'collect_schema'`.

    `check_results` has no counterpart and is not missing one. Nothing dynamic reaches it; `docs/out-of-scope/wiring-argument-type-guards.md` has that decision (#124).

    `None` is now the skip, so a forgotten return and a deliberate skip are the same object and this guard cannot tell them apart. The trade is taken knowingly: the skip has to be a value for a decorated function to reach it, and `None` is the only value every early return already produces.

    Dagster's own error, not the package's: this is a wiring mistake, not a data one.
    """
    if frame is None or isinstance(frame, (pl.DataFrame, pl.LazyFrame)):
        return
    wrong_type: str = f"'{asset}' returned a {type(frame).__name__}. A schema-backed asset must return a Polars DataFrame or LazyFrame, because the column-schema check reads its columns and dtypes before anything is written. `dd.asset` also accepts a `dg.MaterializeResult` carrying one, which is how metadata, tags and a data version reach the materialization, and `None` to skip the asset where a partition has no source data. An asset that writes its own storage has no frame for this package to validate, so write it as a plain `@dg.asset`, where `dagster_dataframely.wiring.schema_metadata` still fills its Columns tab."
    raise dg.DagsterInvariantViolationError(wrong_type)


def quarantine_frame(schema: type[dy.Schema], failure: dy.FailureInfo) -> pl.DataFrame:
    """Build the frame the writer is handed.

    `FailureInfo.details()` rather than `invalid()`: the invalid rows plus a rule column for every rule, reading `valid` / `invalid` / `unknown`. Check-metadata samples are bounded, so without these columns the per-row attribution exists nowhere at volume.

    Two changes to what Dataframely hands over. The rule columns are renamed into the reserved namespace, so a column of this table and the asset check for the same rule share one string. They are also cast from `Enum` to `String`, because a raw `Enum` panics the Delta writer with a Rust `unreachable!()`. It is the one cast this package makes, and it touches only columns the package generated.

    Returns
    -------
    The invalid rows: the original columns in their own order, then a `String` rule column for every rule.

    Raises
    ------
    ReservedColumnError
        A user column sits inside the reserved namespace. Without the guard the rename collides with it, and Polars reports a duplicate column that names nothing about this package.
    UnnameableColumnError
        A user column is spelled in characters Dagster refuses in a name.
    CheckNameCollisionError
        Two rules rewrite to the same check name, which the rename would also collapse.
    """
    validate_namespace(schema)
    # Bound once: `details()` rebuilds the frame on every call.
    details: pl.DataFrame = failure.details()
    renames: dict[str, str] = {
        rule: check_name(rule) for rule in rule_columns(schema, details)
    }
    return details.rename(renames).with_columns(
        pl.col(name).cast(pl.String) for name in renames.values()
    )


def _addressed(
    checks: list[dg.AssetCheckResult], address: str
) -> list[dg.AssetCheckResult]:
    """Copy the quarantine's address onto every check result.

    For the outcomes that raise. Those have no materialization to carry the address, and a reader of a failed run still needs to know where the evidence went. A run that materializes carries the address once, on the table.

    `_replace`, because `dg.AssetCheckResult` is a `NamedTuple`. It skips the normalization the constructor applies to metadata, so the address is wrapped as the `TextMetadataValue` it would have become.
    """
    return [
        check._replace(
            metadata={
                **(check.metadata or {}),
                _ADDRESS_KEY: dg.MetadataValue.text(address),
            }
        )
        for check in checks
    ]


def _cooccurrence(counts: Mapping[frozenset[str], int]) -> dg.TableMetadataValue:
    """Tabulate which rules a row broke together.

    One broken upstream field tripping three rules then reads as one row, not three unrelated counts.

    Rules carry their asset-check names, not Dataframely's. Both places this table sends a reader spell them that way: the check list and the quarantine's columns. The original name lives on `dy_rule` in each check's metadata.

    Rows sort biggest group first, ties by name. `cooccurrence_counts()` groups without `maintain_order`, so its order is arbitrary and two runs of the same data would diff. Biggest first is also the reading order: the table answers which upstream field trips the most rows at once.

    Parameters
    ----------
    counts
        How many rows broke each set of rules together. The key is a `frozenset`, so it is sorted before rendering.

    Returns
    -------
    One record per co-occurring set, most rows first, for the quarantine's materialization metadata.
    """
    # The count is negated so a plain tuple sort puts the biggest group first and breaks ties on the names, with no key function.
    ordered: list[tuple[int, str]] = sorted(
        (-n, ", ".join(sorted(check_name(rule) for rule in rules)))
        for rules, n in counts.items()
    )
    return dg.MetadataValue.table(
        [dg.TableRecord({"rules": rules, "count": -n}) for n, rules in ordered]
    )


def validation_results(  # noqa: PLR0913 - hand-wiring needs everything the decorator decides to be passable by hand
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame | None,
    *,
    valid_key: dg.AssetKey,
    quarantine_writer: QuarantineWriter | None = None,
    check_granularity: Granularity | None = None,
    schema_rules: SchemaRules | None = None,
    max_failure_samples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
) -> AssetYield:
    """Validate a decorated function's output and report it to Dagster.

    Two calls and six outcomes. The skip makes neither. Otherwise the column-schema check runs first, off `collect_schema()`, then `Schema.filter` separates the valid rows from the invalid rows, with `cast=False`, in one `collect_all` on the streaming engine. It is the only validation call: `validate()` carries per-rule detail as a string, and this package needs structured counts.

    The asset's declaration decides which of the other five a run reaches, never an argument's value. `quarantine_writer` is the whole policy. `USER_GUIDE.md` has the six as a table and as a flowchart.

    **This writes the quarantine and never learns where it went.** The writer takes the invalid rows and hands back an address: no directory, no file path, no IO manager, no context (ADR-0001).

    **The skip still reports every check.** A check spec is a non-optional op output whatever `output_required` the asset carries, so a step that answers none of them fails with `did not return an output for non-optional output`. The rules therefore run over `Schema.create_empty()`, and Dagster records the evaluations with no `target_materialization_data` (#95).

    Parameters
    ----------
    frame
        Whatever the decorated function returned, or `None` to skip.
    valid_key
        The asset key the validated frame materializes under. A key the asset does not own fails the step on the first yield, so build it from the definition rather than by hand. The decorator resolves it where the asset is declared, which leaves it callable outside a run (ADR-0002).
    quarantine_writer
        What puts the invalid rows somewhere a reader can open them, or `None` when the asset declares no quarantine. Called once, with the quarantine frame, on each of the two outcomes that have rows to hold. `None` makes the abort the third of those outcomes, because there is nowhere to route the rows.
    check_granularity
        How far the rules collapse. Pass the value the check specs were derived with. The decorator resolves it once at definition time and hands it to both, so a run cannot report against a check list it did not declare.
    schema_rules
        Where the schema-level rules land at `column` granularity, on the same terms.
    max_failure_samples
        How many rows that failed a rule reach that rule's check metadata. Unset resolves through the setting's sources.
    statistics
        Whether each materialization carries statistics for what it wrote. Unset resolves through the setting's sources.
    row_sample
        How many rows reach the materialization metadata. Unset resolves through the setting's sources.

    Yields
    ------
    The valid table's `MaterializeResult` where the run wrote one, then every check result standalone. Nothing is bundled onto a materialization: direct invocation satisfies a check output only from a standalone `AssetCheckResult`, and that keeps an asset built on this callable in a unit test (ADR-0002). The quarantine is written, not yielded, so it never appears here.

    Raises
    ------
    ReservedColumnError
        A user column sits inside the reserved namespace.
    UnnameableColumnError
        A user column is spelled in characters Dagster refuses in a name.
    CheckNameCollisionError
        Two rules rewrite to the same check name.
    InvalidSettingError
        A setting resolved to a value outside its allowed values.
    DagsterInvariantViolationError
        The decorated function returned something that is neither a Polars frame nor `None`.
    ColumnSchemaError
        The frame's columns or dtypes do not match the schema.
    ValidationAbortError
        Rows failed validation and no quarantine is declared.
    NothingSurvivedError
        Rows failed validation and none survived.
    """
    # Before the settings and before the frame is looked at. A schema this package cannot name is broken whatever was returned (ADR-0008).
    validate_namespace(schema)
    _require_frame(frame, valid_key.to_user_string())
    # Resolved before the column-schema check, so a mistyped environment variable fails the same way on every outcome and not only on the runs that reach the one reading it.
    emit_statistics: bool = STATISTICS.resolve(statistics)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)

    def checks_for(
        failure: dy.FailureInfo, *, aborting: bool
    ) -> list[dg.AssetCheckResult]:
        """Build every check result for a frame that passed the column-schema check.

        Severity derives here, once, from whether the valid table was written. It is a property of the run's outcome, not of any one rule, so no code path can hand two sibling checks different severities. An invalid row with a quarantine to go to is a warning. The same row with nowhere to go, or with nothing left beside it, is an error.

        A closure rather than a function of its own. Everything but the failure is settled before any outcome can be reached, so a function would take five arguments that are identical at both call sites, which is what the lint suppression this replaced was there to excuse.

        The column-schema check is not a rule. It reports on its own at every granularity and never joins a rule set.
        """
        return [
            column_schema_result(asset_key=valid_key),
            *rule_results(
                schema,
                failure,
                asset_key=valid_key,
                severity=dg.AssetCheckSeverity.ERROR
                if aborting
                else dg.AssetCheckSeverity.WARN,
                check_granularity=check_granularity,
                schema_rules=schema_rules,
                max_failure_samples=failure_samples,
            ),
        ]

    if frame is None:
        # The decorated function returned `None`. Neither output is yielded, so the partition stays unmaterialized and the run succeeds.
        # The rules still report, over an empty frame, because a check spec is a non-optional op output.
        _, nothing = schema.filter(schema.create_empty(), cast=False)
        # Nothing failed and nothing was written, so there is no failure to grade.
        yield from checks_for(nothing, aborting=False)
        return

    # The column-schema check, then `Schema.filter`. A mismatch reports through the check and raises out of here, so neither output is written and a mismatched frame cannot corrupt either table.
    valid, failure = yield from filtered(schema, frame, asset_key=valid_key)
    invalid_count: int = len(failure)
    # A quarantine is consent to partial data, not to no data, so nothing surviving aborts even with one declared.
    aborting = bool(invalid_count) and (quarantine_writer is None or not len(valid))
    checks = checks_for(failure, aborting=aborting)

    def valid_result(
        invalid_metadata: InvalidMetadata | None = None,
    ) -> dg.MaterializeResult[pl.DataFrame]:
        """Build the materialization where it is yielded, not ahead of every outcome.

        Two of the five outcomes discard it, and the statistics pass over the whole frame, so building it early would charge an aborting run for a table nobody sees.

        Parameters
        ----------
        invalid_metadata
            What the run held back, on the one outcome that both wrote a table and had rows fail. Absent on a clean run.
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
        # No rows failed. The writer is never called, so a clean run leaves no empty quarantine and an empty one means something.
        yield valid_result()
        yield from checks
        return

    if quarantine_writer is None:
        # Rows failed and no quarantine is declared, so there is nowhere to route them. Both the valid rows and the invalid rows are discarded and the last-known-good table survives. Every rule still reports, so the failed run says what failed and by how much.
        yield from checks
        raise ValidationAbortError(schema.__name__, invalid_count, failure.counts())

    # Bound once: the frame is written and sampled, and `quarantine_frame` rebuilds it out of `details()` on every call.
    invalid: pl.DataFrame = quarantine_frame(schema, failure)
    # Written before anything below can raise, so the rows that failed are readable whichever way the run ends.
    address: str = quarantine_writer(invalid)
    invalid_metadata: InvalidMetadata = {
        _ADDRESS_KEY: address,
        _INVALID_COUNT_KEY: invalid_count,
        _INVALID_BY_RULES_KEY: _cooccurrence(failure.cooccurrence_counts()),
        # Sized by the same setting as the valid rows' sample, because one number governs how many real rows a run puts in the event log. No statistics: nobody consumes the quarantine in aggregate.
        **sample_metadata(_INVALID_SAMPLE_KEY, sample_rows(invalid, sampled_rows)),
    }

    if not len(valid):
        # No rows survived. The rows are all written, but the table is skipped so an empty one cannot replace a last-known-good snapshot.
        yield from _addressed(checks, address)
        raise NothingSurvivedError(
            schema.__name__, invalid_count, failure.counts(), address
        )

    # Rows failed and the quarantine took them. The survivors are written, the rest are readable at the address the materialization names, and downstream proceeds on the data that is fine.
    yield valid_result(invalid_metadata)
    yield from checks
