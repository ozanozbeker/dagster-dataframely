"""An asset built with `dd.asset` passes what its decorated function returns to `stream_results`.

Whether the run writes the table depends on both the valid and the invalid rows, so `check_and_filter` collects both (`docs/pre-1.0.md#validation-collects-both-sets-of-rows-so-nothing-streams-to-the-destination`).
`Schema.filter` removes every invalid row, so `stream_results` reads the rows that failed only `"warn"` rules back from `FailureInfo` and appends them to the valid rows, not to the error rows (`docs/out-of-scope/dbt-test-configs.md#store_failures-on-a-test-with-severity-warn`).
"""

from collections.abc import Iterator, Mapping

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._checks import (
    build_column_schema_result,
    check_and_filter,
    evaluate_rule_checks,
)
from dagster_dataframely._error_rows import (
    ErrorRowsWriter,
    separate_warned_and_error_rows,
)
from dagster_dataframely._naming import get_check_name_for_rule
from dagster_dataframely._rules import get_error_rule_names, validate_namespace
from dagster_dataframely._samples import (
    TABLE_SAMPLE_KEY,
    build_sample_metadata,
    sample_rows,
)
from dagster_dataframely._settings import (
    MAX_FAILURE_EXAMPLES,
    ROW_SAMPLE,
    STATISTICS,
    Granularity,
)
from dagster_dataframely._statistics import build_statistics_metadata
from dagster_dataframely.errors import (
    MaterializeResultFieldError,
    MaterializeResultValueError,
    NoValidRowsError,
    ValidationAbortError,
)

_ADDRESS_KEY = "dataframely/error_rows_address"
_ERROR_ROWS_COUNT_KEY = "dataframely/error_rows_count"
_ERROR_ROWS_BY_RULES_KEY = "dataframely/error_rows_by_rules"
_ERROR_ROWS_SAMPLE_KEY = "dataframely/error_rows_sample"

type ErrorRowsMetadata = Mapping[str, str | int | dg.TableMetadataValue]

ReturnedResult = dg.MaterializeResult[pl.DataFrame] | dg.MaterializeResult[pl.LazyFrame]
"""This union names both frame types, because `dg.MaterializeResult` is invariant in its value type."""

DecoratedReturn = pl.DataFrame | pl.LazyFrame | ReturnedResult | None


def _require_frame(frame: object, asset: str) -> None:
    """Raise unless `frame` is a Polars frame or `None`.

    The error is Dagster's, because a wrong type is a bug in the code, not bad data.
    """
    if frame is None or isinstance(frame, (pl.DataFrame, pl.LazyFrame)):
        return
    wrong_type: str = f"'{asset}' returned a {type(frame).__name__}. The decorated function of an asset built with `dd.asset` returns a Polars DataFrame or LazyFrame, a `dg.MaterializeResult` whose `value` is the frame, or `None` to skip. The column-schema check reads the frame's columns and dtypes before the asset writes anything. An asset that writes its own storage has no frame to validate, so write it as a plain `@dg.asset` and call `dd.wiring.get_column_schema_metadata` to fill its Columns tab."
    raise dg.DagsterInvariantViolationError(wrong_type)


def _unpack_return(
    returned: DecoratedReturn, asset: str
) -> tuple[pl.DataFrame | pl.LazyFrame | None, ReturnedResult | None]:
    """Separate the frame to validate from a returned result.

    `dg.MaterializeResult(value=None)` raises instead of skipping, because a skip has no materialization to copy the result's fields onto.
    """
    if not isinstance(returned, dg.MaterializeResult):
        _require_frame(returned, asset)
        return returned, None
    if returned.asset_key is not None:
        raise MaterializeResultFieldError(asset, "asset_key")
    if returned.check_results:
        raise MaterializeResultFieldError(asset, "check_results")
    # The default `value` is a sentinel, not `None`; `tests/test_upstream_characterization.py` pins it.
    if not isinstance(returned.value, (pl.DataFrame, pl.LazyFrame)):
        raise MaterializeResultValueError(asset)
    return returned.value, returned


def _add_address(
    checks: list[dg.AssetCheckResult], address: str
) -> list[dg.AssetCheckResult]:
    """Copy the error-rows address onto every check result.

    `_replace` skips the constructor's normalization, so `_add_address` wraps the address itself.
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


def _tabulate_cooccurrence(
    counts: Mapping[frozenset[str], int],
) -> dg.TableMetadataValue:
    """Tabulate how many rows failed each set of rules together.

    It sorts the sets because `cooccurrence_counts()` returns them in arbitrary order.
    """
    ordered: list[tuple[int, str]] = sorted(
        (-n, ", ".join(sorted(get_check_name_for_rule(rule) for rule in rules)))
        for rules, n in counts.items()
    )
    return dg.MetadataValue.table([
        dg.TableRecord({"rules": rules, "count": -n}) for n, rules in ordered
    ])


def stream_results(  # noqa: PLR0913 - hand-wiring passes all of these
    schema: type[dy.Schema],
    returned: DecoratedReturn,
    *,
    asset_key: dg.AssetKey,
    error_rows_writer: ErrorRowsWriter | None = None,
    check_granularity: Granularity | None = None,
    max_failure_examples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
    partitioned: bool = False,
) -> Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]:
    """Validate a frame and yield the asset's materialization and check results.

    Parameters
    ----------
    schema
        The schema to validate against.
    returned
        The frame to validate, a `dg.MaterializeResult` whose `value` is the frame, or `None` to skip. The materialization has a returned result's `metadata`, `data_version` and `tags`, and the package's own metadata keys take precedence. `dg.MaterializeResult(value=None)` raises instead of skipping.
    asset_key
        The key the run materializes the table under: the asset's own key, such as `context.asset_key`. A key the asset does not have fails the step with `DagsterInvariantViolationError` on the first yield, and the asset writes nothing.
    error_rows_writer
        The writer for the rows that failed an `"error"` rule, called at most once, with the frame `get_error_rows` returns. It receives every such row, because the error rows are the only copy of the rows that a run with `filter=True` leaves out of the table (`docs/out-of-scope/dbt-test-configs.md#limit`). When no such row exists, it receives zero rows. Passing one means `filter=True`, so pass `filter=True` to `build_asset_check_specs` too. With `None`, a row that fails an `"error"` rule fails the run with `ValidationAbortError`, and the asset writes nothing.
    check_granularity
        Pass the value `build_asset_check_specs` received, or the results name checks the asset does not declare. `None` uses `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY` if set, else `rule`.
    max_failure_examples
        A rule's check metadata has at most this many rows that failed it. `None` uses `DAGSTER_DATAFRAMELY_MAX_FAILURE_EXAMPLES` if set, else `5`.
    statistics
        Whether the materialization metadata has statistics of the written rows. `None` uses `DAGSTER_DATAFRAMELY_STATISTICS` if set, else `True`.
    row_sample
        The materialization metadata has at most this many rows of the table, and this many error rows. `None` uses `DAGSTER_DATAFRAMELY_ROW_SAMPLE` if set, else `5`.
    partitioned
        Whether the asset has a `partitions_def`. With `True`, the row count is under `dagster/partition_row_count`, because Dagster defines `dagster/row_count` as the whole table's count.

    Yields
    ------
    :
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
        `returned` is not a Polars frame, a `dg.MaterializeResult` or `None`.
    MaterializeResultFieldError
        `returned` is a `dg.MaterializeResult` that sets `asset_key` or `check_results`.
    MaterializeResultValueError
        `returned` is a `dg.MaterializeResult` whose `value` is not a Polars frame.
    ColumnSchemaError
        The frame's columns or dtypes do not match the schema. It yields the column-schema check's failing result first, and writes nothing.
    ValidationAbortError
        A row failed an `"error"` rule and `error_rows_writer` is `None`, so the package writes nothing.
    NoValidRowsError
        Every row failed an `"error"` rule. It writes the error rows, and not the asset's table.

    Examples
    --------
    A hand-wired asset declares the specs and yields from `stream_results`:

    ```python
    from collections.abc import Iterator

    import dagster as dg
    import polars as pl

    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema, storefront_orders


    @dg.asset(
        metadata=dd.wiring.get_column_schema_metadata(OrdersSchema),
        check_specs=dd.wiring.build_asset_check_specs(OrdersSchema, asset="orders"),
    )
    def orders(
        context: dg.AssetExecutionContext,
    ) -> Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]:
        yield from dd.wiring.stream_results(
            OrdersSchema, storefront_orders, asset_key=context.asset_key
        )


    dg.materialize([orders]).success
    ```
    """
    validate_namespace(schema)
    frame, returned_result = _unpack_return(returned, asset_key.to_user_string())
    # Resolved before anything is written, so a malformed environment variable fails every run.
    emit_statistics: bool = STATISTICS.resolve(statistics)
    failure_examples: int = MAX_FAILURE_EXAMPLES.resolve(max_failure_examples)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)

    def evaluate_checks_for(failure: dy.FailureInfo) -> list[dg.AssetCheckResult]:
        """Return a passing column-schema result and every rule result."""
        return [
            build_column_schema_result(asset_key=asset_key),
            *evaluate_rule_checks(
                schema,
                failure,
                asset_key=asset_key,
                check_granularity=check_granularity,
                max_failure_examples=failure_examples,
            ),
        ]

    if frame is None:
        # A check spec is a non-optional op output, so this evaluates every check on an empty frame.
        _, nothing = schema.filter(schema.create_empty(), cast=False)
        yield from evaluate_checks_for(nothing)
        return

    valid, failure = yield from check_and_filter(schema, frame, asset_key=asset_key)
    checks: list[dg.AssetCheckResult] = evaluate_checks_for(failure)
    errors: set[str] = get_error_rule_names(schema)
    warned, error_rows = separate_warned_and_error_rows(schema, failure, errors)
    table: pl.DataFrame = pl.concat([valid, warned])
    count_key: str = (
        "dagster/partition_row_count" if partitioned else "dagster/row_count"
    )

    def build_table_result(
        build_error_rows_metadata: ErrorRowsMetadata | None = None,
    ) -> dg.MaterializeResult[pl.DataFrame]:
        """Return the asset's materialization, with the returned result's fields."""
        return dg.MaterializeResult(
            asset_key=asset_key,
            value=table,
            metadata={
                # First, so the package's own keys take precedence.
                **(
                    (returned_result.metadata or {})
                    if returned_result is not None
                    else {}
                ),
                count_key: len(table),
                **build_statistics_metadata(table, enabled=emit_statistics),
                **build_sample_metadata(
                    TABLE_SAMPLE_KEY, sample_rows(table, sampled_rows)
                ),
                **(build_error_rows_metadata or {}),
            },
            data_version=None
            if returned_result is None
            else returned_result.data_version,
            tags=None if returned_result is None else returned_result.tags,
        )

    if error_rows.is_empty():
        if error_rows_writer is not None:
            # An IO manager has no delete, so only an empty write replaces the last run's rows.
            error_rows_writer(error_rows)
        yield build_table_result()
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
    build_error_rows_metadata: ErrorRowsMetadata = {
        _ADDRESS_KEY: address,
        _ERROR_ROWS_COUNT_KEY: len(error_rows),
        _ERROR_ROWS_BY_RULES_KEY: _tabulate_cooccurrence({
            together: n
            for together, n in failure.cooccurrence_counts().items()
            if together & errors
        }),
        **build_sample_metadata(
            _ERROR_ROWS_SAMPLE_KEY, sample_rows(error_rows, sampled_rows)
        ),
    }

    if table.is_empty():
        yield from _add_address(checks, address)
        raise NoValidRowsError(schema.__name__, len(error_rows), error_counts, address)

    yield build_table_result(build_error_rows_metadata)
    yield from checks
