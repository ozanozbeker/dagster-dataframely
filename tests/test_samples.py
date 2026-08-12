"""The two row samples, asserted through the metadata a run emits.

Both write real data into the event log, so both are asserted where that lands rather than at the function that renders them: the check's metadata for the rows a rule rejected, and the valid out's materialization's for the rows that survived.

Both are opt-out, so almost every asset in this file declares nothing about them. What each test that does declare something covers is the off switch, which is the half of an opt-out setting that has to work.
"""

import datetime as dt
from collections.abc import Mapping
from decimal import Decimal
from pathlib import Path
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl
import pytest

from dagster_dataframely import (
    DataframelyParquetIOManager,
    InvalidSettingError,
    dataframely_asset,
)
from tests.scenario import Orders, clean_orders, mixed_orders

_FAILURE_SAMPLES_ENV = "DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES"
_ROW_SAMPLE_ENV = "DAGSTER_DATAFRAMELY_ROW_SAMPLE"

_GOOD_KEY = dg.AssetKey(["orders"])
_QUARANTINE_KEY = dg.AssetKey(["orders_quarantine"])

#: The package ships this many of each. Spelled once so a changed default fails in one place.
_DEFAULT = 5


def _many_orders(rows: int) -> pl.DataFrame:
    """One order with a dense line sequence, which is the only shape that grows without breaking the primary key, `tracking_id`'s uniqueness or `line_numbers_are_dense`."""
    base = clean_orders().head(1)
    return pl.concat(
        base.with_columns(
            line_no=pl.lit(line, pl.Int32), tracking_id=pl.lit(f"TRK-{line}")
        )
        for line in range(1, rows + 1)
    )


def _rejected_orders(rejects: int) -> pl.DataFrame:
    """One surviving row and `rejects` rows that `amount|min` alone rejects."""
    amounts = [Decimal("10.00")] + [Decimal("-1.00")] * rejects
    return _many_orders(rejects + 1).with_columns(
        amount=pl.Series(amounts, dtype=pl.Decimal(10, 2))
    )


def _materialize(
    tmp_path: Path, *assets: dg.AssetsDefinition, raise_on_error: bool = True
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        list(assets),
        resources={"io_manager": DataframelyParquetIOManager(base_dir=str(tmp_path))},
        raise_on_error=raise_on_error,
    )


def _materialized(
    result: dg.ExecuteInProcessResult, key: dg.AssetKey
) -> Mapping[str, dg.MetadataValue[Any]]:
    return next(
        event.step_materialization_data.materialization.metadata
        for event in result.get_asset_materialization_events()
        if event.asset_key == key
    )


def _records(value: dg.MetadataValue[Any]) -> list[dict[str, Any]]:
    assert isinstance(value, dg.TableMetadataValue)
    return [dict(record.data) for record in value.records]


def _check_metadata(
    result: dg.ExecuteInProcessResult, check: str
) -> Mapping[str, dg.MetadataValue[Any]]:
    return next(
        e.metadata
        for e in result.get_asset_check_evaluations()
        if e.check_name == check
    )


# --- the valid output's row sample ---
@dataframely_asset(schema=Orders, name="orders")
def _clean() -> pl.DataFrame:
    return clean_orders()


def test_a_materialization_carries_a_sample_of_the_rows_it_wrote(tmp_path: Path):
    """The display key is short and unprefixed, like `stats/*` and unlike the machine carrier: the difference in length is the signal."""
    metadata = _materialized(_materialize(tmp_path, _clean), _GOOD_KEY)
    sampled = _records(metadata["sample"])

    assert [row["order_id"] for row in sampled] == ["ORD-1", "ORD-2", "ORD-3"]
    assert [row["email"] for row in sampled] == clean_orders()["email"].to_list()


def test_the_row_sample_is_bounded(tmp_path: Path):
    """The bound is the whole point: an unbounded sample is the asset's data in the event log."""

    @dataframely_asset(schema=Orders, name="orders")
    def wide() -> pl.DataFrame:
        return _many_orders(_DEFAULT * 2)

    metadata = _materialized(_materialize(tmp_path, wide), _GOOD_KEY)

    assert len(_records(metadata["sample"])) == _DEFAULT


def test_the_row_sample_shows_every_column_the_row_holds(tmp_path: Path):
    metadata = _materialized(_materialize(tmp_path, _clean), _GOOD_KEY)

    assert [list(record) for record in _records(metadata["sample"])] == [
        list(Orders.columns())
    ] * 3


def test_a_row_sample_of_zero_leaves_the_key_absent_rather_than_empty(tmp_path: Path):
    """Absent, not empty: an empty table in the UI reads as a run that wrote no rows, which is a different thing from a setting somebody turned off."""

    @dataframely_asset(schema=Orders, name="orders", row_sample=0)
    def unsampled() -> pl.DataFrame:
        return clean_orders()

    metadata = _materialized(_materialize(tmp_path, unsampled), _GOOD_KEY)

    assert "sample" not in metadata
    assert "dagster/row_count" in metadata


def test_a_frame_with_no_rows_materializes_without_a_sample(tmp_path: Path):
    """A valid frame with nothing in it is an ordinary outcome: a partition nothing was written to this time.

    There is no row to show, so the key is absent for the same reason a passing check's is. Dagster enforces the same judgement from the other side, refusing a table value with no records and no schema, so an empty one here would take the run down.
    """

    @dataframely_asset(schema=Orders, name="orders")
    def nothing_today() -> pl.DataFrame:
        return clean_orders().head(0)

    result = _materialize(tmp_path, nothing_today)
    metadata = _materialized(result, _GOOD_KEY)

    assert result.success
    assert metadata["dagster/row_count"].value == 0
    assert "sample" not in metadata


def test_the_environment_tier_sets_the_house_row_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """A platform engineer turns both samples off for a whole code location without touching an asset."""
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "1")

    @dataframely_asset(schema=Orders, name="orders")
    def housed() -> pl.DataFrame:
        return clean_orders()

    metadata = _materialized(_materialize(tmp_path, housed), _GOOD_KEY)

    assert len(_records(metadata["sample"])) == 1


def test_a_cell_no_table_record_can_hold_is_rendered_as_the_value_it_is(tmp_path: Path):
    """`TableRecord` takes strings, numbers, bools and nulls, and this frame holds a `Decimal`, a `Datetime`, a `Duration`, a `Binary` and a `List`.

    A `Decimal` becomes a string rather than the float the statistics tables use. These are rows somebody stored, so `10.00` has to stay `10.00`.
    """
    metadata = _materialized(_materialize(tmp_path, _clean), _GOOD_KEY)
    first = _records(metadata["sample"])[0]

    assert first["amount"] == "10.00"
    assert first["ordered_at"] == str(dt.datetime(2026, 8, 1, 12, 0))  # noqa: DTZ001 - the schema declares no time zone
    assert first["fulfilled_in"] == str(dt.timedelta(hours=26))
    assert first["payload"] == str(b"\x00\x01")
    assert first["tags"] == str(["priority"])
    assert first["note"] is None


def test_the_quarantine_carries_no_row_sample(tmp_path: Path):
    """The invalid rows reach the event log once, through the checks that rejected them and with the rule attached. A second unattributed copy here would say less and cost the same."""

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def quarantined() -> pl.DataFrame:
        return mixed_orders()

    result = _materialize(tmp_path, quarantined)

    assert "sample" in _materialized(result, _GOOD_KEY)
    assert "sample" not in _materialized(result, _QUARANTINE_KEY)


# --- the failing rows in a check's metadata ---
@dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
def _quarantined() -> pl.DataFrame:
    return mixed_orders()


def test_a_failing_check_carries_the_rows_it_rejected(tmp_path: Path):
    """What a red check raises and the counts cannot answer: not that `amount|min` failed once, but which row did it."""
    result = _materialize(tmp_path, _quarantined)
    metadata = _check_metadata(result, "dy_rule__amount__min")
    sampled = _records(metadata["dy_failed_sample"])

    assert metadata["dy_failed_count"].value == 1
    assert [row["order_id"] for row in sampled] == ["ORD-4"]
    assert sampled[0]["amount"] == "-4.00"


def test_a_sampled_row_holds_the_columns_of_the_data_and_no_rule_columns(
    tmp_path: Path,
):
    """The rule columns are the quarantine's shape, not a sample's: the check already says which rule this is."""
    result = _materialize(tmp_path, _quarantined)
    sampled = _records(
        _check_metadata(result, "dy_rule__amount__min")["dy_failed_sample"]
    )

    assert list(sampled[0]) == list(Orders.columns())


def test_a_passing_check_carries_no_sample(tmp_path: Path):
    """There is nothing to show, and an empty table would suggest there was."""
    result = _materialize(tmp_path, _quarantined)
    metadata = _check_metadata(result, "dy_rule__quantity__min")

    assert "dy_failed_sample" not in metadata
    assert {"dy_rule", "dy_rule__expr"} <= set(metadata)


def test_the_failure_sample_is_bounded(tmp_path: Path):
    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def many_rejects() -> pl.DataFrame:
        return _rejected_orders(_DEFAULT * 2)

    result = _materialize(tmp_path, many_rejects)
    metadata = _check_metadata(result, "dy_rule__amount__min")

    assert metadata["dy_failed_count"].value == _DEFAULT * 2
    assert len(_records(metadata["dy_failed_sample"])) == _DEFAULT


def test_the_bound_is_the_packages_own_and_not_dataframelys(tmp_path: Path):
    """`dy.Config.set_max_failure_examples` governs the string `validate` builds for its error. It does not touch the `filter` path this package uses, so a project that tightened it still gets the sample it asked this package for."""

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def many_rejects() -> pl.DataFrame:
        return _rejected_orders(_DEFAULT * 2)

    with dy.Config(max_failure_examples=1):
        result = _materialize(tmp_path, many_rejects)

    metadata = _check_metadata(result, "dy_rule__amount__min")

    assert len(_records(metadata["dy_failed_sample"])) == _DEFAULT


def test_a_failure_sample_of_zero_leaves_the_key_absent_rather_than_empty(
    tmp_path: Path,
):
    @dataframely_asset(
        schema=Orders,
        name="orders",
        quarantine=dg.AssetOut(),
        max_failure_samples=0,
    )
    def unsampled() -> pl.DataFrame:
        return mixed_orders()

    result = _materialize(tmp_path, unsampled)
    metadata = _check_metadata(result, "dy_rule__amount__min")

    assert "dy_failed_sample" not in metadata
    assert metadata["dy_failed_count"].value == 1


def test_the_environment_tier_sets_the_house_failure_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv(_FAILURE_SAMPLES_ENV, "0")

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def housed() -> pl.DataFrame:
        return mixed_orders()

    result = _materialize(tmp_path, housed)

    assert "dy_failed_sample" not in _check_metadata(result, "dy_rule__amount__min")


def test_an_aborting_run_still_samples_what_it_rejected(tmp_path: Path):
    """The run writes nothing, so the checks are the only place the invalid rows exist. That is the run where a sample is worth most."""

    @dataframely_asset(schema=Orders, name="orders")
    def aborting() -> pl.DataFrame:
        return mixed_orders()

    result = _materialize(tmp_path, aborting, raise_on_error=False)
    sampled = _records(
        _check_metadata(result, "dy_rule__amount__min")["dy_failed_sample"]
    )

    assert not result.success
    assert [row["order_id"] for row in sampled] == ["ORD-4"]


# --- collapsed checks ---
@dataframely_asset(
    schema=Orders, name="orders", quarantine=dg.AssetOut(), check_granularity="column"
)
def _by_column() -> pl.DataFrame:
    return mixed_orders()


def test_a_collapsed_check_says_which_rule_rejected_each_sampled_row(tmp_path: Path):
    """A rule set stands for several rules, so a row in its sample has to name the one that put it there. `dy_rule` is the same key a rule check carries it under, and the reserved namespace is what makes it a column name no schema can collide with."""
    result = _materialize(tmp_path, _by_column)
    sampled = _records(_check_metadata(result, "dy_col__amount")["dy_failed_sample"])

    assert [row["dy_rule"] for row in sampled] == ["amount|min"]
    assert list(sampled[0]) == ["dy_rule", *Orders.columns()]


def test_a_collapsed_check_samples_every_rule_that_rejected_something(tmp_path: Path):
    """Bounded per rule rather than per check, which is what keeps the rule that rejected one row visible beside the rule that rejected a thousand."""
    result = _materialize(tmp_path, _by_column)
    sampled = _records(_check_metadata(result, "dy_schema__rules")["dy_failed_sample"])

    assert [row["dy_rule"] for row in sampled] == ["paid_orders_have_amount"]
    assert [row["order_id"] for row in sampled] == ["ORD-6"]


# --- the settings are three, not one ---
def test_turning_the_statistics_off_leaves_both_samples_on(tmp_path: Path):
    """Consenting to summary statistics is not consenting to raw values, and the converse holds too: they are separate settings because they are separate consents."""

    @dataframely_asset(
        schema=Orders, name="orders", quarantine=dg.AssetOut(), statistics=False
    )
    def unprofiled() -> pl.DataFrame:
        return mixed_orders()

    result = _materialize(tmp_path, unprofiled)
    metadata = _materialized(result, _GOOD_KEY)

    assert not [key for key in metadata if key.startswith("stats/")]
    assert "sample" in metadata
    assert "dy_failed_sample" in _check_metadata(result, "dy_rule__amount__min")


def test_turning_both_samples_off_leaves_the_statistics_on(tmp_path: Path):
    @dataframely_asset(
        schema=Orders,
        name="orders",
        quarantine=dg.AssetOut(),
        row_sample=0,
        max_failure_samples=0,
    )
    def unsampled() -> pl.DataFrame:
        return mixed_orders()

    result = _materialize(tmp_path, unsampled)
    metadata = _materialized(result, _GOOD_KEY)

    assert "stats/numeric" in metadata
    assert "sample" not in metadata
    assert "dy_failed_sample" not in _check_metadata(result, "dy_rule__amount__min")


def test_a_sample_outside_the_vocabulary_raises_at_the_door():
    """Definition time, like every other setting, so a misconfiguration never reaches a run."""
    wrong: Any = -1

    with pytest.raises(InvalidSettingError) as raised:

        @dataframely_asset(schema=Orders, name="misconfigured", row_sample=wrong)
        def _misconfigured() -> pl.DataFrame:
            return pl.DataFrame()

    assert "row_sample" in str(raised.value)
