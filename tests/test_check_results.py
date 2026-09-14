"""`check_results`, the evaluating counterpart to `check_specs` (#80).

Most of these call the function, with no run, no IO manager and no `tmp_path`. That is the property ADR-0001 and ADR-0002 exist for, and it is what the arrangement this function serves is made of: an asset that reports checks and writes nothing.

The load-bearing assertion is that the names it yields equal the names `check_specs` declares. A checks-only step answers its outputs by name, so the two agreeing is the whole feature, and the two disagreeing is the bug this was filed for.

Only what this entry point decides. `check_results` and `validation_results` build their results from the same `column_schema_result` and `rule_results` pair, so the counts, the samples and the error table are asserted once, through a run, in `test_asset_runtime.py`. What is left here is what the two callers do differently: the names and their order, the stated severity, and the asset key a standalone result has to carry itself.

One test runs. It builds the arrangement a user writes and asserts the step completes, because nothing short of a run proves the outputs were answered.
"""

from collections.abc import Callable, Iterator
from pathlib import Path

import dagster as dg
import dataframely as dy
import polars as pl
import pytest

from dagster_dataframely._rules import described_rules
from dagster_dataframely._settings import Granularity
from dagster_dataframely.errors import ColumnSchemaError, InvalidSettingError
from dagster_dataframely.wiring import check_results, check_specs, schema_metadata
from tests.scenario import (
    Orders,
    clean_orders,
    mixed_orders,
    no_valid_orders,
    storage,
    wrong_dtype_orders,
)

KEY = dg.AssetKey(["orders"])
WARN = dg.AssetCheckSeverity.WARN
ERROR = dg.AssetCheckSeverity.ERROR

COLUMN_SCHEMA = "dy_schema__columns"
"""Spelled out rather than imported, so a rename has to be made here deliberately."""

_FAILURE_SAMPLES_ENV = "DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES"

_RULE_ORDER = list(described_rules(Orders).values())
"""`Orders`'s rules in the schema's own order, which is the order every granularity's checks are measured against.

Read through `described_rules`, so the expectation below shares nothing with the `_rule_sets` call that builds both the specs and the results. `test_rules.py` pins `described_rules` against Dataframely's own dict.
"""

_RULE_BEARING_COLUMNS = list(
    dict.fromkeys(rule.column for rule in _RULE_ORDER if rule.column is not None)
)
"""Every column that owns a rule, in the order its first rule appears."""

_CHECKS: dict[Granularity, list[str]] = {
    "rule": [rule.check_name for rule in _RULE_ORDER],
    "column": [f"dy_col__{column}" for column in _RULE_BEARING_COLUMNS]
    + ["dy_schema__rules"],
    "schema": ["dy_schema__rules"],
}
"""The rule checks each granularity declares, in order, after the column-schema check."""


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

    Order too, not just membership: a reader comparing a check list against a run should find them in one order, and that order is the schema's own.

    Both sides are held against `_CHECKS` rather than against each other. The specs and the results read one `_rule_sets` call, so comparing the two agrees on whatever order that call produced, including one no schema wrote.
    """
    specs = check_specs(Orders, asset=KEY, check_granularity=granularity)
    results = _results(mixed_orders(), check_granularity=granularity)
    expected = [COLUMN_SCHEMA, *_CHECKS[granularity]]

    assert [spec.name for spec in specs] == expected
    assert [result.check_name for result in results] == expected


def test_schema_rules_reach_the_results_too():
    """The second grouping setting. Only `column` granularity reads it, so it needs its own case."""
    settings = {"check_granularity": "column", "schema_rules": "per_rule"}
    specs = check_specs(Orders, asset=KEY, **settings)  # pyrefly: ignore[bad-argument-type]
    results = _results(mixed_orders(), **settings)

    assert [result.check_name for result in results] == [spec.name for spec in specs]


def test_a_malformed_house_setting_refuses_a_hand_wired_call(
    monkeypatch: pytest.MonkeyPatch,
):
    """The `Raises` block promises `InvalidSettingError`, and only the decorator's definition-time refusal was ever driven.

    A hand-wirer has no definition time to fail at, so the typo has to reach them where they drain the generator instead.
    """
    monkeypatch.setenv(_FAILURE_SAMPLES_ENV, "five")

    with pytest.raises(InvalidSettingError) as raised:
        _results(mixed_orders())

    assert f"got 'five' from the environment variable {_FAILURE_SAMPLES_ENV}" in str(
        raised.value
    )


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
def test_every_result_carries_the_asset_key():
    """A standalone result has no materialization to infer it from."""
    assert all(result.asset_key == KEY for result in _results(clean_orders()))


# --- severity is the caller's ---
@pytest.mark.parametrize("severity", [WARN, ERROR])
def test_the_rule_checks_carry_the_severity_the_caller_passed(
    severity: dg.AssetCheckSeverity,
):
    """`WARN` is what `validation_results` cannot reach here: with no quarantine writer every failing run is aborting, so it grades everything `ERROR`."""
    assert all(
        result.severity == severity
        for result in _rules(_results(mixed_orders(), severity))
    )


# --- the column schema ---
def test_the_column_schema_failure_is_an_error_whatever_the_caller_asked_for():
    """A mismatch is a pipeline defect, not a data one, and its check is the blocking one. Grading it `WARN` because a caller asked for `WARN` would leave a drifted table feeding downstream.

    What that failing result holds is `column_schema_result`'s, which a run pins in `test_asset_runtime.py`. Only the severity is this caller's.
    """
    assert _before_the_raise(wrong_dtype_orders())[0].severity == ERROR


# --- the failure policy stays with the decorator ---
@pytest.mark.parametrize("frame", [mixed_orders, no_valid_orders])
def test_no_data_reaches_the_two_errors_that_carry_the_failure_policy(
    frame: Callable[[], pl.DataFrame],
):
    """`validation_results` raises `ValidationAbortError` on the first of these frames and `NothingSurvivedError` on the second. Neither question belongs to a caller that writes nothing."""
    results = _results(frame())

    assert len(results) == len(check_specs(Orders, asset=KEY))


def test_nothing_surviving_is_not_a_special_case():
    results = _results(no_valid_orders())

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
