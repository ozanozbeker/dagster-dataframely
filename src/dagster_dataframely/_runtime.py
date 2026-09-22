"""What an asset built with `dd.asset` runs after its decorated function.

Choosing an outcome requires collecting the valid and the invalid rows (`docs/pre-1.0.md`).
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
from dagster_dataframely.errors import NoValidRowsError, ValidationAbortError

AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]
"""What `validation_results` yields: the asset's materialization, if the run writes its table, then one result per check. Use it as the return annotation of a hand-wired asset's function."""

_ADDRESS_KEY = "dataframely/quarantine_address"
_INVALID_COUNT_KEY = "dataframely/invalid_count"
_INVALID_BY_RULES_KEY = "dataframely/invalid_by_rules"
_INVALID_SAMPLE_KEY = "dataframely/invalid_sample"

type InvalidMetadata = Mapping[str, str | int | dg.TableMetadataValue]


def _require_frame(frame: object, asset: str) -> None:
    """Raise unless `frame` is a Polars frame or `None`.

    The error is Dagster's, because a wrong type is a wiring mistake, not bad data.
    """
    if frame is None or isinstance(frame, (pl.DataFrame, pl.LazyFrame)):
        return
    wrong_type: str = f"'{asset}' returned a {type(frame).__name__}. An asset built with `dd.asset` must return a Polars DataFrame or LazyFrame, because the column-schema check reads its columns and dtypes before anything is written. It can also return a `dg.MaterializeResult` whose `value` is the frame, to add metadata, tags or a data version, or `None` to skip the asset when a partition has no source data. If the asset writes its own storage, there is no frame to validate, so write it as a plain `@dg.asset` and call `dagster_dataframely.wiring.schema_metadata` to fill its Columns tab."
    raise dg.DagsterInvariantViolationError(wrong_type)


def quarantine_frame(schema: type[dy.Schema], failure: dy.FailureInfo) -> pl.DataFrame:
    """Return the invalid rows as a writer receives them, with one rule column per rule.

    Returns
    -------
    The original columns in their own order, then a `String` rule column for every rule, named by `check_name`. A rule column's value is `valid`, `invalid` or `unknown`.

    Raises
    ------
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
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
    """Copy the quarantine address onto every check result.

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
        (-n, ", ".join(sorted(check_name(rule) for rule in rules)))
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
    quarantine_writer: QuarantineWriter | None = None,
    check_granularity: Granularity | None = None,
    schema_rules: SchemaRules | None = None,
    max_failure_samples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
) -> AssetYield:
    """Validate a frame and yield the asset's materialization and check results.

    Parameters
    ----------
    frame
        The frame to validate, or `None` to skip.
    valid_key
        The key the run materializes the valid rows under: the asset's own key, such as `context.asset_key`. A key the asset does not have fails the step with `DagsterInvariantViolationError` on the first yield, and the asset writes nothing.
    quarantine_writer
        The writer for the invalid rows, called at most once, with the frame `quarantine_frame` returns. With `None`, any invalid row fails the run with `ValidationAbortError`, and the asset writes nothing.
    check_granularity
        Pass the value `check_specs` received, or the results name checks the asset does not declare. `None` uses `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY` if set, else `rule`.
    schema_rules
        Pass the value `check_specs` received, as for `check_granularity`. `None` uses `DAGSTER_DATAFRAMELY_SCHEMA_RULES` if set, else `collapsed`.
    max_failure_samples
        A rule's check metadata has at most this many rows that failed it. `None` uses `DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES` if set, else `5`.
    statistics
        Whether the materialization metadata has statistics of the written rows. `None` uses `DAGSTER_DATAFRAMELY_STATISTICS` if set, else `True`.
    row_sample
        The materialization metadata has at most this many valid rows, and this many invalid rows. `None` uses `DAGSTER_DATAFRAMELY_ROW_SAMPLE` if set, else `5`.

    Yields
    ------
    The asset's `dg.MaterializeResult`, if the run writes its table, then one `dg.AssetCheckResult` per check. On a skip, it yields only the check results, and every check passes. It writes the quarantine instead of yielding it.

    Raises
    ------
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
    InvalidSettingError
        A setting's argument or environment variable has a value the setting does not allow.
    DagsterInvariantViolationError
        `frame` is neither a Polars frame nor `None`.
    ColumnSchemaError
        The frame's columns or dtypes do not match the schema. It yields the column-schema check's failing result first, and writes nothing.
    ValidationAbortError
        Rows failed validation and `quarantine_writer` is `None`, so the package writes nothing.
    NoValidRowsError
        Every row failed validation. It writes the quarantine, and not the asset's table.
    """
    validate_namespace(schema)
    _require_frame(frame, valid_key.to_user_string())
    # Resolved before any outcome, so a mistyped environment variable fails every run.
    emit_statistics: bool = STATISTICS.resolve(statistics)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)

    def checks_for(
        failure: dy.FailureInfo, *, aborting: bool
    ) -> list[dg.AssetCheckResult]:
        """Return a passing column-schema result and every rule result."""
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
        # A check spec is a non-optional op output, so the rules report over an empty frame.
        _, nothing = schema.filter(schema.create_empty(), cast=False)
        yield from checks_for(nothing, aborting=False)
        return

    valid, failure = yield from filtered(schema, frame, asset_key=valid_key)
    invalid_count: int = len(failure)
    aborting = bool(invalid_count) and (quarantine_writer is None or not len(valid))
    checks = checks_for(failure, aborting=aborting)

    def valid_result(
        invalid_metadata: InvalidMetadata | None = None,
    ) -> dg.MaterializeResult[pl.DataFrame]:
        """Return the asset's materialization."""
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
        yield valid_result()
        yield from checks
        return

    if quarantine_writer is None:
        yield from checks
        raise ValidationAbortError(schema.__name__, invalid_count, failure.counts())

    invalid: pl.DataFrame = quarantine_frame(schema, failure)
    # The writer runs before anything below can raise, so the quarantine has the rows however the run ends.
    address: str = quarantine_writer(invalid)
    invalid_metadata: InvalidMetadata = {
        _ADDRESS_KEY: address,
        _INVALID_COUNT_KEY: invalid_count,
        _INVALID_BY_RULES_KEY: _cooccurrence(failure.cooccurrence_counts()),
        **sample_metadata(_INVALID_SAMPLE_KEY, sample_rows(invalid, sampled_rows)),
    }

    if not len(valid):
        yield from _addressed(checks, address)
        raise NoValidRowsError(
            schema.__name__, invalid_count, failure.counts(), address
        )

    yield valid_result(invalid_metadata)
    yield from checks
