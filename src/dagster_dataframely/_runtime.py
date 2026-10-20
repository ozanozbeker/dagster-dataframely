"""What an asset built with `dd.asset` runs after its decorated function.

Choosing an outcome requires collecting the valid and the invalid rows (`docs/pre-1.0.md`).
`Schema.filter` removes every invalid row, so the rows that failed only `"warn"` rules are read back from `FailureInfo` and appended to the valid rows, not to the error rows (`docs/out-of-scope/dbt-test-configs.md#store_failures-on-a-test-with-severity-warn`).
"""

from collections.abc import Iterator, Mapping
from collections.abc import Set as AbstractSet

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._checks import (
    column_schema_result,
    filtered,
    rule_check_results,
    rule_columns,
)
from dagster_dataframely._error_rows import ErrorRowsWriter
from dagster_dataframely._naming import asset_check_name
from dagster_dataframely._rules import described_rules, validate_namespace
from dagster_dataframely._samples import TABLE_SAMPLE_KEY, sample_metadata, sample_rows
from dagster_dataframely._settings import (
    MAX_FAILURE_SAMPLES,
    ROW_SAMPLE,
    STATISTICS,
    Granularity,
)
from dagster_dataframely._statistics import statistics_metadata
from dagster_dataframely.errors import NoValidRowsError, ValidationAbortError

AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]
"""What `validation_results` yields: the asset's materialization, if the run writes its table, then one result per check. Use it as the return annotation of a hand-wired asset's function."""

_ADDRESS_KEY = "dataframely/error_rows_address"
_ERROR_ROWS_COUNT_KEY = "dataframely/error_rows_count"
_ERROR_ROWS_BY_RULES_KEY = "dataframely/error_rows_by_rules"
_ERROR_ROWS_SAMPLE_KEY = "dataframely/error_rows_sample"

_VALID = "valid"

type ErrorRowsMetadata = Mapping[str, str | int | dg.TableMetadataValue]


def _require_frame(frame: object, asset: str) -> None:
    """Raise unless `frame` is a Polars frame or `None`.

    The error is Dagster's, because a wrong type is a wiring mistake, not bad data.
    """
    if frame is None or isinstance(frame, (pl.DataFrame, pl.LazyFrame)):
        return
    wrong_type: str = f"'{asset}' returned a {type(frame).__name__}. An asset built with `dd.asset` must return a Polars DataFrame or LazyFrame, because the column-schema check reads its columns and dtypes before anything is written. It can also return a `dg.MaterializeResult` whose `value` is the frame, to add metadata, tags or a data version, or `None` to skip the asset when a partition has no source data. If the asset writes its own storage, there is no frame to validate, so write it as a plain `@dg.asset` and call `dagster_dataframely.wiring.schema_metadata` to fill its Columns tab."
    raise dg.DagsterInvariantViolationError(wrong_type)


def _with_rule_columns(schema: type[dy.Schema], details: pl.DataFrame) -> pl.DataFrame:
    """Rename the rule columns to their check names, as `String`."""
    renames: dict[str, str] = {
        rule: asset_check_name(rule) for rule in rule_columns(schema, details)
    }
    return details.rename(renames).with_columns(
        pl.col(name).cast(pl.String) for name in renames.values()
    )


def _warned_and_error_rows(
    schema: type[dy.Schema], failure: dy.FailureInfo, errors: AbstractSet[str]
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Return the invalid rows that failed only `"warn"` rules, with the schema's columns, and the error rows as a writer receives them.

    `unknown` counts as failed; Dataframely reports it only with `cast=True` or for a `Collection`.
    """
    # Bound once: `details()` rebuilds the frame on every call.
    details: pl.DataFrame = failure.details()
    present: pl.Schema = details.collect_schema()
    failed: pl.Expr = pl.any_horizontal(
        pl.lit(value=False),
        *(pl.col(rule) != _VALID for rule in errors if rule in present),
    )
    return (
        details.filter(~failed).select(schema.column_names()),
        _with_rule_columns(schema, details.filter(failed)),
    )


def _error_rules(schema: type[dy.Schema]) -> set[str]:
    """Return the names of the schema's `"error"` rules."""
    return {
        name
        for name, rule in described_rules(schema).items()
        if rule.severity == "error"
    }


def error_rows_frame(schema: type[dy.Schema], failure: dy.FailureInfo) -> pl.DataFrame:
    """Return the rows that failed an `"error"` rule, as a writer receives them, with one rule column per rule.

    Returns
    -------
    The original columns in their own order, then a `String` rule column for every rule, named by `asset_check_name`. A rule column's value is `valid`, `invalid` or `unknown`.

    Raises
    ------
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
    InvalidSeverityError
        The schema declares a severity this package cannot read.
    """
    validate_namespace(schema)
    _, error_rows = _warned_and_error_rows(schema, failure, _error_rules(schema))
    return error_rows


def _addressed(
    checks: list[dg.AssetCheckResult], address: str
) -> list[dg.AssetCheckResult]:
    """Copy the error-rows address onto every check result.

    `_replace` skips the constructor's normalization, so `_addressed` wraps the address itself.
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
    """Tabulate how many rows failed each set of rules together.

    It sorts the sets because `cooccurrence_counts()` returns them in arbitrary order.
    """
    ordered: list[tuple[int, str]] = sorted(
        (-n, ", ".join(sorted(asset_check_name(rule) for rule in rules)))
        for rules, n in counts.items()
    )
    return dg.MetadataValue.table([
        dg.TableRecord({"rules": rules, "count": -n}) for n, rules in ordered
    ])


def validation_results(  # noqa: PLR0913 - hand-wiring passes all of these
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame | None,
    *,
    valid_key: dg.AssetKey,
    error_rows_writer: ErrorRowsWriter | None = None,
    check_granularity: Granularity | None = None,
    max_failure_samples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
    partitioned: bool = False,
) -> AssetYield:
    """Validate a frame and yield the asset's materialization and check results.

    Parameters
    ----------
    frame
        The frame to validate, or `None` to skip.
    valid_key
        The key the run materializes the table under: the asset's own key, such as `context.asset_key`. A key the asset does not have fails the step with `DagsterInvariantViolationError` on the first yield, and the asset writes nothing.
    error_rows_writer
        The writer for the rows that failed an `"error"` rule, called at most once, with the frame `error_rows_frame` returns. It receives every such row, because the error rows are the only copy of the rows that a run with `filter=True` leaves out of the table (`docs/out-of-scope/dbt-test-configs.md#limit`). When no such row exists, it receives zero rows. Passing one means `filter=True`, so pass `filter=True` to `asset_check_specs` too. With `None`, a row that fails an `"error"` rule fails the run with `ValidationAbortError`, and the asset writes nothing.
    check_granularity
        Pass the value `asset_check_specs` received, or the results name checks the asset does not declare. `None` uses `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY` if set, else `rule`.
    max_failure_samples
        A rule's check metadata has at most this many rows that failed it. `None` uses `DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES` if set, else `5`.
    statistics
        Whether the materialization metadata has statistics of the written rows. `None` uses `DAGSTER_DATAFRAMELY_STATISTICS` if set, else `True`.
    row_sample
        The materialization metadata has at most this many rows of the table, and this many error rows. `None` uses `DAGSTER_DATAFRAMELY_ROW_SAMPLE` if set, else `5`.
    partitioned
        Whether the asset has a `partitions_def`. With `True`, the row count is under `dagster/partition_row_count`, because Dagster defines `dagster/row_count` as the whole table's count.

    Yields
    ------
    The asset's `dg.MaterializeResult`, if the run writes its table, then one `dg.AssetCheckResult` per check. On a skip, it yields only the check results, and every check passes. It writes the error rows instead of yielding them.

    Raises
    ------
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
    InvalidSeverityError
        The schema declares a severity this package cannot read.
    InvalidSettingError
        A setting's argument or environment variable has a value the setting does not allow.
    DagsterInvariantViolationError
        `frame` is neither a Polars frame nor `None`.
    ColumnSchemaError
        The frame's columns or dtypes do not match the schema. It yields the column-schema check's failing result first, and writes nothing.
    ValidationAbortError
        A row failed an `"error"` rule and `error_rows_writer` is `None`, so the package writes nothing.
    NoValidRowsError
        Every row failed an `"error"` rule. It writes the error rows, and not the asset's table.
    """
    validate_namespace(schema)
    _require_frame(frame, valid_key.to_user_string())
    # Resolved before any outcome, so a mistyped environment variable fails every run.
    emit_statistics: bool = STATISTICS.resolve(statistics)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)

    def checks_for(failure: dy.FailureInfo) -> list[dg.AssetCheckResult]:
        """Return a passing column-schema result and every rule result."""
        return [
            column_schema_result(asset_key=valid_key),
            *rule_check_results(
                schema,
                failure,
                asset_key=valid_key,
                check_granularity=check_granularity,
                max_failure_samples=failure_samples,
            ),
        ]

    if frame is None:
        # A check spec is a non-optional op output, so the rules report over an empty frame.
        _, nothing = schema.filter(schema.create_empty(), cast=False)
        yield from checks_for(nothing)
        return

    valid, failure = yield from filtered(schema, frame, asset_key=valid_key)
    checks: list[dg.AssetCheckResult] = checks_for(failure)
    errors: set[str] = _error_rules(schema)
    warned, error_rows = _warned_and_error_rows(schema, failure, errors)
    table: pl.DataFrame = pl.concat([valid, warned])
    count_key: str = (
        "dagster/partition_row_count" if partitioned else "dagster/row_count"
    )

    def table_result(
        error_rows_metadata: ErrorRowsMetadata | None = None,
    ) -> dg.MaterializeResult[pl.DataFrame]:
        """Return the asset's materialization."""
        return dg.MaterializeResult(
            asset_key=valid_key,
            value=table,
            metadata={
                count_key: len(table),
                **statistics_metadata(table, enabled=emit_statistics),
                **sample_metadata(TABLE_SAMPLE_KEY, sample_rows(table, sampled_rows)),
                **(error_rows_metadata or {}),
            },
        )

    if error_rows.is_empty():
        if error_rows_writer is not None:
            # An IO manager has no delete, so only an empty write replaces the last run's rows.
            error_rows_writer(error_rows)
        yield table_result()
        yield from checks
        return

    error_counts: dict[str, int] = {
        rule: n for rule, n in failure.counts().items() if rule in errors
    }
    if error_rows_writer is None:
        yield from checks
        raise ValidationAbortError(schema.__name__, len(error_rows), error_counts)

    # The writer runs before anything below can raise, so the error rows are written however the run ends.
    address: str = error_rows_writer(error_rows)
    error_rows_metadata: ErrorRowsMetadata = {
        _ADDRESS_KEY: address,
        _ERROR_ROWS_COUNT_KEY: len(error_rows),
        _ERROR_ROWS_BY_RULES_KEY: _cooccurrence({
            together: n
            for together, n in failure.cooccurrence_counts().items()
            if together & errors
        }),
        **sample_metadata(
            _ERROR_ROWS_SAMPLE_KEY, sample_rows(error_rows, sampled_rows)
        ),
    }

    if table.is_empty():
        yield from _addressed(checks, address)
        raise NoValidRowsError(schema.__name__, len(error_rows), error_counts, address)

    yield table_result(error_rows_metadata)
    yield from checks
