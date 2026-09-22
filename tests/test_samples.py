"""The row samples, asserted in the metadata a run records.

The materialization has samples of the valid and invalid rows, and a failing check has a sample of the rows that failed it.
"""

import datetime as dt
from decimal import Decimal
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest

import dagster_dataframely as dd
from dagster_dataframely.errors import InvalidSettingError
from tests.scenario import (
    Orders,
    check_evaluations,
    clean_orders,
    materializations,
    materialize,
    mixed_orders,
    records,
)

_FAILURE_SAMPLES_ENV = "DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES"
_ROW_SAMPLE_ENV = "DAGSTER_DATAFRAMELY_ROW_SAMPLE"

_GOOD_KEY = dg.AssetKey(["orders"])

_DEFAULT = 5
"""Both sample settings default to this."""


def _many_orders(rows: int) -> pl.DataFrame:
    """Return one order with `rows` lines, which keeps every row valid."""
    base = clean_orders().head(1)
    return pl.concat(
        base.with_columns(
            line_no=pl.lit(line, pl.Int32), tracking_id=pl.lit(f"TRK-{line}")
        )
        for line in range(1, rows + 1)
    )


def _invalid_orders(invalid: int) -> pl.DataFrame:
    """Return one valid row and `invalid` rows that fail only `amount|min`."""
    amounts = [Decimal("10.00")] + [Decimal("-1.00")] * invalid
    return _many_orders(invalid + 1).with_columns(
        amount=pl.Series(amounts, dtype=pl.Decimal(10, 2))
    )


# --- the materialization's samples ---
@dd.asset(Orders, name="orders")
def _clean() -> pl.DataFrame:
    return clean_orders()


def test_a_materialization_has_a_sample_of_the_rows_it_wrote(tmp_path: Path):
    metadata = materializations(materialize(tmp_path, _clean))[_GOOD_KEY].metadata
    sampled = records(metadata["dataframely/valid_sample"])

    assert [row["order_id"] for row in sampled] == ["ORD-1", "ORD-2", "ORD-3"]
    assert [row["email"] for row in sampled] == clean_orders()["email"].to_list()


def test_the_row_sample_is_bounded(tmp_path: Path):
    @dd.asset(Orders, name="orders")
    def wide() -> pl.DataFrame:
        return _many_orders(_DEFAULT * 2)

    metadata = materializations(materialize(tmp_path, wide))[_GOOD_KEY].metadata

    assert len(records(metadata["dataframely/valid_sample"])) == _DEFAULT


def test_the_row_sample_is_the_head_rather_than_any_other_draw(tmp_path: Path):
    """The head is the only sample a re-read of the same data reproduces."""

    @dd.asset(Orders, name="orders")
    def wide() -> pl.DataFrame:
        return _many_orders(_DEFAULT * 2)

    metadata = materializations(materialize(tmp_path, wide))[_GOOD_KEY].metadata
    sampled = records(metadata["dataframely/valid_sample"])

    assert [row["line_no"] for row in sampled] == list(range(1, _DEFAULT + 1))


def test_the_row_sample_shows_every_column_the_row_holds(tmp_path: Path):
    metadata = materializations(materialize(tmp_path, _clean))[_GOOD_KEY].metadata

    assert [
        list(record) for record in records(metadata["dataframely/valid_sample"])
    ] == [list(Orders.columns())] * 3


def test_a_row_sample_of_zero_leaves_the_key_absent_rather_than_empty(tmp_path: Path):
    """An empty table would look like a run that wrote no rows."""

    @dd.asset(Orders, name="orders", row_sample=0)
    def unsampled() -> pl.DataFrame:
        return clean_orders()

    metadata = materializations(materialize(tmp_path, unsampled))[_GOOD_KEY].metadata

    assert "dataframely/valid_sample" not in metadata


def test_a_frame_with_no_rows_materializes_without_a_sample(tmp_path: Path):
    """Dagster raises on a table value with no records and no schema, so an empty sample would fail the run."""

    @dd.asset(Orders, name="orders")
    def nothing_today() -> pl.DataFrame:
        return clean_orders().head(0)

    result = materialize(tmp_path, nothing_today)
    metadata = materializations(result)[_GOOD_KEY].metadata

    assert result.success
    assert "dataframely/valid_sample" not in metadata


def test_the_environment_variable_sets_the_row_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "1")

    @dd.asset(Orders, name="orders")
    def orders() -> pl.DataFrame:
        return clean_orders()

    metadata = materializations(materialize(tmp_path, orders))[_GOOD_KEY].metadata

    assert len(records(metadata["dataframely/valid_sample"])) == 1


def test_a_cell_no_table_record_can_hold_is_rendered_as_the_value_it_is(tmp_path: Path):
    """A `Decimal` becomes its string, not a float, so `10.00` stays `10.00`."""
    metadata = materializations(materialize(tmp_path, _clean))[_GOOD_KEY].metadata
    first = records(metadata["dataframely/valid_sample"])[0]

    assert first["amount"] == "10.00"
    assert first["ordered_at"] == str(dt.datetime(2026, 8, 1, 12, 0))  # noqa: DTZ001 - the schema declares no time zone
    assert first["fulfilled_in"] == str(dt.timedelta(hours=26))
    assert first["payload"] == str(b"\x00\x01")
    assert first["tags"] == str(["priority"])
    assert first["note"] is None


def test_both_samples_appear_in_the_one_materialization_under_their_own_keys(
    tmp_path: Path,
):
    @dd.asset(Orders, name="orders", quarantine=True)
    def with_quarantine() -> pl.DataFrame:
        return mixed_orders()

    metadata = materializations(materialize(tmp_path, with_quarantine))[
        _GOOD_KEY
    ].metadata

    assert len(records(metadata["dataframely/valid_sample"])) == 3
    assert len(records(metadata["dataframely/invalid_sample"])) == 3


def test_the_invalid_sample_is_bounded_by_the_same_setting(tmp_path: Path):
    @dd.asset(Orders, name="orders", quarantine=True, row_sample=1)
    def bounded() -> pl.DataFrame:
        return mixed_orders()

    metadata = materializations(materialize(tmp_path, bounded))[_GOOD_KEY].metadata

    assert len(records(metadata["dataframely/valid_sample"])) == 1
    assert len(records(metadata["dataframely/invalid_sample"])) == 1


def test_an_invalid_sample_has_the_rule_columns(tmp_path: Path):
    @dd.asset(Orders, name="orders", quarantine=True)
    def with_quarantine() -> pl.DataFrame:
        return mixed_orders()

    metadata = materializations(materialize(tmp_path, with_quarantine))[
        _GOOD_KEY
    ].metadata
    (first, *_) = records(metadata["dataframely/invalid_sample"])

    assert list(first)[: len(Orders.columns())] == list(Orders.columns())
    assert any(name.startswith("dy_rule__") for name in first)


# --- the failing rows in a check's metadata ---
@dd.asset(Orders, name="orders", quarantine=True)
def _with_quarantine() -> pl.DataFrame:
    return mixed_orders()


def test_a_failing_check_has_the_rows_that_failed_it(tmp_path: Path):
    result = materialize(tmp_path, _with_quarantine)
    metadata = check_evaluations(result)["dy_rule__amount__min"].metadata
    sampled = records(metadata["dy_failed_sample"])

    assert metadata["dy_failed_count"].value == 1
    assert [row["order_id"] for row in sampled] == ["ORD-4"]
    assert sampled[0]["amount"] == "-4.00"


def test_a_sampled_row_holds_the_columns_of_the_data_and_no_rule_columns(
    tmp_path: Path,
):
    result = materialize(tmp_path, _with_quarantine)
    sampled = records(
        check_evaluations(result)["dy_rule__amount__min"].metadata["dy_failed_sample"]
    )

    assert list(sampled[0]) == list(Orders.columns())


def test_a_passing_check_has_no_sample(tmp_path: Path):
    result = materialize(tmp_path, _with_quarantine)
    metadata = check_evaluations(result)["dy_rule__quantity__min"].metadata

    assert "dy_failed_sample" not in metadata
    assert {"dy_rule", "dy_rule__expr"} <= set(metadata)


def test_the_failure_sample_is_bounded(tmp_path: Path):
    @dd.asset(Orders, name="orders", quarantine=True)
    def many_invalid() -> pl.DataFrame:
        return _invalid_orders(_DEFAULT * 2)

    result = materialize(tmp_path, many_invalid)
    metadata = check_evaluations(result)["dy_rule__amount__min"].metadata

    assert metadata["dy_failed_count"].value == _DEFAULT * 2
    assert len(records(metadata["dy_failed_sample"])) == _DEFAULT


def test_a_failure_sample_of_zero_leaves_the_key_absent_rather_than_empty(
    tmp_path: Path,
):
    @dd.asset(
        Orders,
        name="orders",
        quarantine=True,
        max_failure_samples=0,
    )
    def unsampled() -> pl.DataFrame:
        return mixed_orders()

    result = materialize(tmp_path, unsampled)
    metadata = check_evaluations(result)["dy_rule__amount__min"].metadata

    assert "dy_failed_sample" not in metadata
    assert metadata["dy_failed_count"].value == 1


def test_the_environment_variable_sets_the_failure_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv(_FAILURE_SAMPLES_ENV, "0")

    @dd.asset(Orders, name="orders", quarantine=True)
    def orders() -> pl.DataFrame:
        return mixed_orders()

    result = materialize(tmp_path, orders)

    assert (
        "dy_failed_sample"
        not in check_evaluations(result)["dy_rule__amount__min"].metadata
    )


def test_an_aborting_run_still_samples_what_failed(tmp_path: Path):
    """The run writes nothing, so the check's sample is the only record of the invalid rows."""

    @dd.asset(Orders, name="orders")
    def aborting() -> pl.DataFrame:
        return mixed_orders()

    result = materialize(tmp_path, aborting, raise_on_error=False)
    sampled = records(
        check_evaluations(result)["dy_rule__amount__min"].metadata["dy_failed_sample"]
    )

    assert not result.success
    assert [row["order_id"] for row in sampled] == ["ORD-4"]


# --- collapsed checks ---
@dd.asset(Orders, name="orders", quarantine=True, check_granularity="column")
def _by_column() -> pl.DataFrame:
    return mixed_orders()


def test_a_collapsed_check_shows_which_rule_each_sampled_row_failed(tmp_path: Path):
    """The `dy_` namespace keeps the `dy_rule` column apart from the schema's columns."""
    result = materialize(tmp_path, _by_column)
    sampled = records(
        check_evaluations(result)["dy_col__amount"].metadata["dy_failed_sample"]
    )

    assert [row["dy_rule"] for row in sampled] == ["amount|min"]
    assert list(sampled[0]) == ["dy_rule", *Orders.columns()]


def test_a_collapsed_check_samples_every_rule_something_failed(tmp_path: Path):
    """The bound is per rule, so a rule one row failed still appears beside a rule many rows failed."""
    result = materialize(tmp_path, _by_column)
    sampled = records(
        check_evaluations(result)["dy_schema__rules"].metadata["dy_failed_sample"]
    )

    assert [row["dy_rule"] for row in sampled] == ["paid_orders_have_amount"]
    assert [row["order_id"] for row in sampled] == ["ORD-6"]


# --- three separate settings ---
def test_turning_the_statistics_off_leaves_both_samples_on(tmp_path: Path):
    @dd.asset(Orders, name="orders", quarantine=True, statistics=False)
    def without_statistics() -> pl.DataFrame:
        return mixed_orders()

    result = materialize(tmp_path, without_statistics)
    metadata = materializations(result)[_GOOD_KEY].metadata

    assert not [
        key for key in metadata if key.startswith("dataframely/valid_statistics/")
    ]
    assert "dataframely/valid_sample" in metadata
    assert (
        "dy_failed_sample" in check_evaluations(result)["dy_rule__amount__min"].metadata
    )


def test_turning_both_samples_off_leaves_the_statistics_on(tmp_path: Path):
    @dd.asset(
        Orders,
        name="orders",
        quarantine=True,
        row_sample=0,
        max_failure_samples=0,
    )
    def unsampled() -> pl.DataFrame:
        return mixed_orders()

    result = materialize(tmp_path, unsampled)
    metadata = materializations(result)[_GOOD_KEY].metadata

    assert "dataframely/valid_statistics/numeric" in metadata
    assert "dataframely/valid_sample" not in metadata
    assert (
        "dy_failed_sample"
        not in check_evaluations(result)["dy_rule__amount__min"].metadata
    )


def test_a_sample_outside_the_allowed_values_raises_at_definition_time():
    wrong: Any = -1

    with pytest.raises(InvalidSettingError) as raised:

        @dd.asset(Orders, name="misconfigured", row_sample=wrong)
        def _misconfigured() -> pl.DataFrame:
            return pl.DataFrame()

    assert "row_sample" in str(raised.value)
