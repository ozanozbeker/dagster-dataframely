"""`check_results`, the evaluating counterpart to `check_specs` (#80).

Most of these call the function, with no run, no IO manager and no `tmp_path`. That is the property ADR-0001 and ADR-0002 exist for, and it is what the arrangement this function serves is made of: an asset that reports checks and writes nothing.

The load-bearing assertion is that the names it yields equal the names `check_specs` declares. A checks-only step answers its outputs by name, so the two agreeing is the whole feature, and the two disagreeing is the bug this was filed for.

One test runs. It builds the arrangement a user writes and asserts the step completes, because nothing short of a run proves the outputs were answered.
"""

from collections.abc import Callable, Iterator
from pathlib import Path

import dagster as dg
import dataframely as dy
import polars as pl
import pytest

from dagster_dataframely._settings import Granularity
from dagster_dataframely.errors import ColumnSchemaError
from dagster_dataframely.wiring import check_results, check_specs, schema_metadata
from tests.scenario import (
    Orders,
    clean_orders,
    hopeless_orders,
    mixed_orders,
    storage,
    wrong_dtype_orders,
)

KEY = dg.AssetKey(["orders"])
WARN = dg.AssetCheckSeverity.WARN
ERROR = dg.AssetCheckSeverity.ERROR

COLUMN_SCHEMA = "dy_schema__columns"
"""Spelled out rather than imported, so a rename has to be made here deliberately."""


def _results(
    frame: pl.DataFrame | pl.LazyFrame,
    severity: dg.AssetCheckSeverity = WARN,
    **settings: object,
) -> list[dg.AssetCheckResult]:
    return list(
        check_results(
            Orders,
            frame,
            asset_key=KEY,
            severity=severity,
            **settings,  # pyrefly: ignore[bad-argument-type]
        )
    )


def _rules(results: list[dg.AssetCheckResult]) -> list[dg.AssetCheckResult]:
    return [result for result in results if result.check_name != COLUMN_SCHEMA]


def _before_the_raise(frame: pl.DataFrame) -> list[dg.AssetCheckResult]:
    """Collect what a column-schema mismatch reported before it raised.

    A `list()` call would lose them along with the exception, and what reaches Dagster before the raise is exactly what these tests are about. `extend` keeps what it has already appended when the iterator it is draining raises, which is the same order Dagster receives them in.
    """
    results: list[dg.AssetCheckResult] = []
    with pytest.raises(ColumnSchemaError):
        results.extend(check_results(Orders, frame, asset_key=KEY, severity=WARN))
    return results


# --- the names agree with the specs ---
@pytest.mark.parametrize("granularity", ["rule", "column", "schema"])
def test_the_results_answer_exactly_the_specs_at_every_granularity(
    granularity: Granularity,
):
    """The issue's own subject, as an assertion.

    Order too, not just membership: a reader comparing a check list against a run should find them in one order.
    """
    specs = check_specs(Orders, asset=KEY, check_granularity=granularity)
    results = _results(mixed_orders(), check_granularity=granularity)

    assert [result.check_name for result in results] == [spec.name for spec in specs]


def test_multi_column_rules_reaches_the_results_too():
    """The second grouping setting. Only `column` granularity reads it, so it needs its own case."""
    settings = {"check_granularity": "column", "multi_column_rules": "per_rule"}
    specs = check_specs(Orders, asset=KEY, **settings)  # pyrefly: ignore[bad-argument-type]
    results = _results(mixed_orders(), **settings)

    assert [result.check_name for result in results] == [spec.name for spec in specs]


def test_a_schema_with_no_rules_still_answers_the_column_schema_check():
    """`check_specs` declares one check for such a schema, so one result answers it."""

    class Blob(dy.Schema):
        payload = dy.String(nullable=True)

    results = list(
        check_results(Blob, Blob.create_empty(), asset_key=KEY, severity=WARN)
    )

    assert [result.check_name for result in results] == [COLUMN_SCHEMA]
    assert results[0].passed


# --- what a clean frame reports ---
def test_a_clean_frame_passes_every_check():
    results = _results(clean_orders())

    assert all(result.passed for result in results)


def test_every_result_carries_the_asset_key():
    """A standalone result has no materialization to infer it from."""
    assert all(result.asset_key == KEY for result in _results(clean_orders()))


# --- severity is the caller's ---
@pytest.mark.parametrize("severity", [WARN, ERROR])
def test_the_rule_checks_carry_the_severity_the_caller_passed(
    severity: dg.AssetCheckSeverity,
):
    """`WARN` is what `process` cannot reach here: with no quarantine writer every failing run is aborting, so it grades everything `ERROR`."""
    assert all(
        result.severity == severity
        for result in _rules(_results(mixed_orders(), severity))
    )


def test_the_rules_that_failed_are_the_rules_that_failed():
    failed = {
        result.check_name for result in _results(mixed_orders()) if not result.passed
    }

    assert failed == {
        "dy_rule__amount__min",
        "dy_rule__email__check__lowercase",
        "dy_rule__paid_orders_have_amount",
    }


def test_a_failing_check_carries_its_count_and_a_sample():
    metadata = dict(
        next(
            result
            for result in _results(mixed_orders())
            if result.check_name == "dy_rule__amount__min"
        ).metadata
        or {}
    )

    assert metadata["dy_failed_count"].value == 1
    assert "dy_failed_sample" in metadata


def test_max_failure_samples_reaches_the_results():
    metadata = dict(
        next(
            result
            for result in _results(mixed_orders(), max_failure_samples=0)
            if result.check_name == "dy_rule__amount__min"
        ).metadata
        or {}
    )

    assert metadata["dy_failed_count"].value == 1
    assert "dy_failed_sample" not in metadata


# --- the column schema ---
def test_a_column_schema_mismatch_reports_that_check_and_raises():
    """Nothing evaluated the rules, so nothing reports for them. Dagster fails the step on the raise, before it looks for the outputs they would have answered."""
    results = _before_the_raise(wrong_dtype_orders())

    assert [result.check_name for result in results] == [COLUMN_SCHEMA]
    assert not results[0].passed


def test_the_column_schema_failure_is_an_error_whatever_the_caller_asked_for():
    """A mismatch is a pipeline defect, not a data one, and its check is the blocking one. Grading it `WARN` because a caller asked for `WARN` would leave a drifted table feeding downstream."""
    assert _before_the_raise(wrong_dtype_orders())[0].severity == ERROR


def test_the_column_schema_failure_tabulates_every_offending_column():
    """The metadata reaches Dagster despite the raise that follows it, which is the point of yielding before raising rather than after."""
    errors = dict(_before_the_raise(wrong_dtype_orders())[0].metadata or {})[
        "dy_schema__errors"
    ]

    assert isinstance(errors, dg.TableMetadataValue)
    assert [dict(record.data) for record in errors.records] == [
        {"column": "quantity", "expected": "Int32", "actual": "Int64"}
    ]


# --- the failure policy stays with the decorator ---
@pytest.mark.parametrize("frame", [mixed_orders, hopeless_orders])
def test_no_data_reaches_the_two_errors_that_carry_the_failure_policy(
    frame: Callable[[], pl.DataFrame],
):
    """`process` raises `ValidationAbortError` on the first of these frames and `NothingSurvivedError` on the second. Neither question belongs to a caller that writes nothing."""
    results = _results(frame())

    assert len(results) == len(check_specs(Orders, asset=KEY))


def test_nothing_surviving_is_not_a_special_case():
    results = _results(hopeless_orders())

    assert not next(r for r in results if r.check_name == "dy_rule__amount__min").passed
    assert next(r for r in results if r.check_name == COLUMN_SCHEMA).passed


# --- eager and lazy ---
def test_a_lazy_frame_reports_exactly_what_an_eager_one_does():
    assert _results(mixed_orders().lazy()) == _results(mixed_orders())


# --- the arrangement a user writes ---
@dg.asset(name="orders", metadata=schema_metadata(Orders))
def _orders() -> pl.DataFrame:
    return mixed_orders()


@dg.multi_asset_check(specs=check_specs(Orders, asset=KEY))
def _orders_checks(orders: pl.DataFrame) -> Iterator[dg.AssetCheckResult]:
    yield from check_results(Orders, orders, asset_key=KEY, severity=WARN)


def test_a_checks_only_asset_answers_every_output_it_declared(tmp_path: Path):
    """The whole feature, in the shape the README teaches.

    The step completing is the assertion. Before this function existed the same arrangement died on `did not return an output for non-optional output`.
    """
    result = dg.materialize(
        [_orders, _orders_checks], resources=storage(tmp_path), raise_on_error=False
    )
    evaluations = {e.check_name: e for e in result.get_asset_check_evaluations()}

    assert result.success
    assert set(evaluations) == {spec.name for spec in check_specs(Orders, asset=KEY)}
    assert not evaluations["dy_rule__amount__min"].passed
    assert evaluations["dy_rule__amount__min"].severity == WARN


def test_the_table_is_written_whatever_the_checks_say(tmp_path: Path):
    """The trade this arrangement makes: the IO manager wrote before anything was validated, so every row is on disk including the three that failed."""
    dg.materialize(
        [_orders, _orders_checks], resources=storage(tmp_path), raise_on_error=False
    )

    assert len(pl.read_parquet(tmp_path / "orders.parquet")) == len(mixed_orders())
