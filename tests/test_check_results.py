"""Tests for `check_results`, mostly called directly, with no run.

`test_asset_runtime.py` asserts the result contents, which come from the same functions `validation_results` uses. This file asserts the check names, the severity and the asset key.
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
"""A literal, not an import, so renaming the check fails these tests."""

_FAILURE_SAMPLES_ENV = "DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES"

_RULE_ORDER = list(described_rules(Orders).values())
"""Read with `described_rules`, not `_rule_sets`, which builds both the specs and the results."""

_COLUMNS_WITH_RULES = list(
    dict.fromkeys(rule.column for rule in _RULE_ORDER if rule.column is not None)
)

_CHECKS: dict[Granularity, list[str]] = {
    "rule": [rule.check_name for rule in _RULE_ORDER],
    "column": [f"dy_col__{column}" for column in _COLUMNS_WITH_RULES]
    + ["dy_schema__rules"],
    "schema": ["dy_schema__rules"],
}


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
    """Return the results yielded before `ColumnSchemaError`, which `list()` would discard."""
    results: list[dg.AssetCheckResult] = []
    with pytest.raises(ColumnSchemaError):
        results.extend(check_results(Orders, frame, asset_key=KEY, severity=WARN))
    return results


@pytest.mark.parametrize("granularity", ["rule", "column", "schema"])
def test_the_results_answer_exactly_the_specs_at_every_granularity(
    granularity: Granularity,
):
    """The specs and the results name the same checks, in the schema's rule order."""
    specs = check_specs(Orders, asset=KEY, check_granularity=granularity)
    results = _results(mixed_orders(), check_granularity=granularity)
    expected = [COLUMN_SCHEMA, *_CHECKS[granularity]]

    assert [spec.name for spec in specs] == expected
    assert [result.check_name for result in results] == expected


def test_schema_rules_reach_the_results_too():
    """The results match the specs with `schema_rules` set, which only `column` granularity reads."""
    settings = {"check_granularity": "column", "schema_rules": "per_rule"}
    specs = check_specs(Orders, asset=KEY, **settings)  # pyrefly: ignore[bad-argument-type, open-unpacking]
    results = _results(mixed_orders(), **settings)

    assert [result.check_name for result in results] == [spec.name for spec in specs]


def test_an_invalid_environment_setting_raises_in_a_hand_wired_call(
    monkeypatch: pytest.MonkeyPatch,
):
    """A hand-wired call has no definition time, so `check_results` raises on iteration."""
    monkeypatch.setenv(_FAILURE_SAMPLES_ENV, "five")

    with pytest.raises(InvalidSettingError) as raised:
        _results(mixed_orders())

    assert f"got 'five' from the environment variable {_FAILURE_SAMPLES_ENV}" in str(
        raised.value
    )


def test_a_schema_with_no_rules_yields_only_the_column_schema_result():
    class Blob(dy.Schema):
        payload = dy.String(nullable=True)

    results = list(
        check_results(Blob, Blob.create_empty(), asset_key=KEY, severity=WARN)
    )

    assert [result.check_name for result in results] == [COLUMN_SCHEMA]
    assert results[0].passed


def test_every_result_has_the_asset_key():
    """A standalone result has no materialization to infer it from."""
    assert all(result.asset_key == KEY for result in _results(clean_orders()))


@pytest.mark.parametrize("severity", [WARN, ERROR])
def test_every_rule_result_has_the_severity_the_caller_passed(
    severity: dg.AssetCheckSeverity,
):
    assert all(
        result.severity == severity
        for result in _rules(_results(mixed_orders(), severity))
    )


def test_the_column_schema_failure_is_an_error_whatever_the_caller_passed():
    """A blocking check stops downstream assets only at `ERROR`."""
    assert _before_the_raise(wrong_dtype_orders())[0].severity == ERROR


@pytest.mark.parametrize("frame", [mixed_orders, no_valid_orders])
def test_failing_rows_raise_no_error(
    frame: Callable[[], pl.DataFrame],
):
    """`check_results` writes no rows, so it raises neither `ValidationAbortError` nor `NoValidRowsError`."""
    results = _results(frame())

    assert len(results) == len(check_specs(Orders, asset=KEY))


def test_no_valid_rows_is_not_a_special_case():
    results = _results(no_valid_orders())

    assert not next(r for r in results if r.check_name == "dy_rule__amount__min").passed
    assert next(r for r in results if r.check_name == COLUMN_SCHEMA).passed


def test_a_lazy_frame_reports_exactly_what_an_eager_one_does():
    assert _results(mixed_orders().lazy()) == _results(mixed_orders())


# --- the hand-wiring guide's example ---
@dg.asset(name="orders", metadata=schema_metadata(Orders))
def _orders() -> pl.DataFrame:
    return mixed_orders()


@dg.multi_asset_check(specs=check_specs(Orders, asset=KEY))
def _orders_checks(orders: pl.DataFrame) -> Iterator[dg.AssetCheckResult]:
    yield from check_results(Orders, orders, asset_key=KEY, severity=WARN)


def test_a_multi_asset_check_yields_a_result_for_every_spec(tmp_path: Path):
    """A missing result fails the step, so the run succeeding is the main assertion."""
    result = dg.materialize(
        [_orders, _orders_checks], resources=storage(tmp_path), raise_on_error=False
    )
    evaluations = {e.check_name: e for e in result.get_asset_check_evaluations()}

    assert result.success
    assert set(evaluations) == {spec.name for spec in check_specs(Orders, asset=KEY)}
    assert not evaluations["dy_rule__amount__min"].passed
    assert evaluations["dy_rule__amount__min"].severity == WARN


def test_the_table_is_written_whatever_the_checks_report(tmp_path: Path):
    """The IO manager writes the table before the checks run."""
    dg.materialize(
        [_orders, _orders_checks], resources=storage(tmp_path), raise_on_error=False
    )

    assert len(pl.read_parquet(tmp_path / "orders.parquet")) == len(mixed_orders())
