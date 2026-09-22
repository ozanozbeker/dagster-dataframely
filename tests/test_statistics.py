"""Tests for the statistics tables in a materialization's metadata.

The tests run an asset instead of calling `_statistics`, because a user reads the tables only on a materialization.
"""

import datetime as dt
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl
import pytest

import dagster_dataframely as dd
from tests.scenario import (
    Orders,
    clean_orders,
    materializations,
    materialize,
    mixed_orders,
    records,
)

_STATISTICS_ENV = "DAGSTER_DATAFRAMELY_STATISTICS"


class Shipment(dy.Schema):
    """The dtypes `Orders` lacks, so the two schemas together have every dtype a statistics table covers, plus both nested dtypes.

    Every column is nullable, so a row or a whole column can be null.
    """

    weight = dy.Float64(nullable=True)
    ordered_on = dy.Date(nullable=True)
    opens_at = dy.Time(nullable=True)
    fulfilled_in = dy.Duration(nullable=True)
    region = dy.Categorical(nullable=True)
    is_gift = dy.Bool(nullable=True)
    address = dy.Struct({"city": dy.String()}, nullable=True)


_SHIPMENT_SCHEMA: dict[str, pl.DataType] = {
    "weight": pl.Float64(),
    "ordered_on": pl.Date(),
    "opens_at": pl.Time(),
    "fulfilled_in": pl.Duration("us"),
    "region": pl.Categorical(),
    "is_gift": pl.Boolean(),
    "address": pl.Struct({"city": pl.String()}),
}

_DURATIONS: list[dt.timedelta | None] = [
    dt.timedelta(days=8),
    dt.timedelta(minutes=1, seconds=30),
    dt.timedelta(hours=2, minutes=5),
    None,
]


def _shipment_frame(durations: list[dt.timedelta | None]) -> pl.DataFrame:
    """Return three rows with values and one all-null row."""
    return pl.DataFrame(
        {
            "weight": [1.23456789, 2.0, 4.0, None],
            "ordered_on": [
                dt.date(2024, 1, 1),
                dt.date(2024, 1, 9),
                dt.date(2024, 1, 5),
                None,
            ],
            "opens_at": [dt.time(1, 0), dt.time(3, 5), dt.time(2, 0), None],
            "fulfilled_in": durations,
            # The empty string makes `n_empty` non-zero.
            "region": ["US", "CANADA", "", None],
            "is_gift": [True, False, False, None],
            "address": [{"city": "NY"}, {"city": "LA"}, {"city": "SF"}, None],
        },
        schema=_SHIPMENT_SCHEMA,
    )


@dd.asset(Orders, name="orders")
def _orders() -> pl.DataFrame:
    return clean_orders()


@dd.asset(Shipment, name="shipment")
def _shipment() -> pl.DataFrame:
    return _shipment_frame(_DURATIONS)


def _metadata(
    tmp_path: Path, asset: dg.AssetsDefinition, key: str = "orders"
) -> Mapping[str, dg.MetadataValue[Any]]:
    """Return the metadata of `key`'s materialization from one run."""
    return materializations(materialize(tmp_path, asset))[dg.AssetKey([key])].metadata


def _table(
    metadata: Mapping[str, dg.MetadataValue[Any]], group: str
) -> dict[str, dict[str, Any]]:
    """Return one group's table as rows keyed by column name."""
    value = metadata[f"dataframely/valid_statistics/{group}"]
    return {str(row["column"]): row for row in records(value)}


def _groups(metadata: Mapping[str, dg.MetadataValue[Any]]) -> set[str]:
    return {key for key in metadata if key.startswith("dataframely/valid_statistics/")}


def _statistics_columns(metadata: Mapping[str, dg.MetadataValue[Any]]) -> set[str]:
    """Return every column in any statistics table."""
    return {
        column
        for group in _groups(metadata)
        for column in _table(
            metadata, group.removeprefix("dataframely/valid_statistics/")
        )
    }


def test_a_materialization_carries_one_table_per_group_present(tmp_path: Path):
    metadata = _metadata(tmp_path, _shipment, key="shipment")

    assert _groups(metadata) == {
        "dataframely/valid_statistics/numeric",
        "dataframely/valid_statistics/temporal",
        "dataframely/valid_statistics/string",
        "dataframely/valid_statistics/boolean",
    }


def test_a_group_the_frame_has_no_column_of_is_not_emitted(tmp_path: Path):
    class Weights(dy.Schema):
        weight = dy.Float64(nullable=False)

    @dd.asset(Weights, name="weights")
    def weights() -> pl.DataFrame:
        return pl.DataFrame({"weight": [1.0, 2.0]})

    metadata = _metadata(tmp_path, weights, key="weights")

    assert _groups(metadata) == {"dataframely/valid_statistics/numeric"}


def test_a_nested_column_reaches_no_table_and_gets_none_of_its_own(tmp_path: Path):
    shipped = _metadata(tmp_path, _shipment, key="shipment")
    ordered = _metadata(tmp_path, _orders)

    assert "address" not in _statistics_columns(shipped)
    assert "tags" not in _statistics_columns(ordered)
    # No boolean table, because `Orders` has no `Bool` column.
    assert _groups(ordered) == {
        "dataframely/valid_statistics/numeric",
        "dataframely/valid_statistics/temporal",
        "dataframely/valid_statistics/string",
    }


def test_the_groups_are_emitted_as_tables_rather_than_markdown(tmp_path: Path):
    metadata = _metadata(tmp_path, _shipment, key="shipment")

    assert all(
        isinstance(metadata[group], dg.TableMetadataValue)
        for group in _groups(metadata)
    )


def test_a_group_table_runs_one_row_per_column_in_the_frames_own_order(tmp_path: Path):
    string = _table(_metadata(tmp_path, _orders), "string")

    assert list(string) == [
        "order_id",
        "email",
        "tracking_id",
        "status",
        "payload",
        "note",
    ]


def test_the_numeric_group_reports_seven_statistics_per_column(tmp_path: Path):
    """Derived statistics round to four places, and `min` and `max` keep the stored value."""
    metadata = _metadata(tmp_path, _shipment, key="shipment")

    assert _table(metadata, "numeric")["weight"] == {
        "column": "weight",
        "count": 4,
        "null_count": 1,
        "mean": 2.4115,
        "std": 1.4279,
        "min": 1.23456789,
        "p50": 2.0,
        "max": 4.0,
    }


def test_a_decimal_column_emits_without_crashing(tmp_path: Path):
    """A `Decimal` column's statistics are floats, because `TableRecord` rejects a `Decimal`."""
    amount = _table(_metadata(tmp_path, _orders), "numeric")["amount"]

    assert amount["min"] == 10.0
    assert amount["max"] == 99.0
    assert amount["mean"] == 44.8333
    assert all(
        isinstance(cell, (int, float))
        for key, cell in amount.items()
        if key != "column"
    )


def test_a_date_column_renders_as_a_date_rather_than_a_datetime(tmp_path: Path):
    ordered_on = _table(_metadata(tmp_path, _shipment, key="shipment"), "temporal")[
        "ordered_on"
    ]

    assert ordered_on["min"] == "2024-01-01"
    assert ordered_on["max"] == "2024-01-09"


def test_a_temporal_column_reports_the_span_between_its_bounds(tmp_path: Path):
    temporal = _table(_metadata(tmp_path, _shipment, key="shipment"), "temporal")

    assert temporal["ordered_on"]["span"] == "8d"
    assert temporal["opens_at"] == {
        "column": "opens_at",
        "count": 4,
        "null_count": 1,
        "min": "01:00:00",
        "max": "03:05:00",
        "span": "2h 5m",
    }


def test_a_duration_renders_in_polars_own_friendly_style(tmp_path: Path):
    fulfilled_in = _table(_metadata(tmp_path, _shipment, key="shipment"), "temporal")[
        "fulfilled_in"
    ]

    assert fulfilled_in["min"] == "1m 30s"
    assert fulfilled_in["max"] == "8d"
    assert fulfilled_in["span"] == "7d 23h 58m 30s"


def test_an_all_null_duration_column_states_nothing_rather_than_zero(tmp_path: Path):
    @dd.asset(Shipment, name="shipment")
    def unfulfilled() -> pl.DataFrame:
        return _shipment_frame([None, None, None, None])

    fulfilled_in = _table(_metadata(tmp_path, unfulfilled, key="shipment"), "temporal")[
        "fulfilled_in"
    ]

    assert fulfilled_in == {
        "column": "fulfilled_in",
        "count": 4,
        "null_count": 4,
        "min": None,
        "max": None,
        "span": None,
    }


def test_the_string_group_shows_no_values_from_the_data(tmp_path: Path):
    """The string table has only counts and lengths, because a `min` would show a real address in the event log."""
    string = _table(_metadata(tmp_path, _orders), "string")

    assert string["email"] == {
        "column": "email",
        "count": 3,
        "null_count": 0,
        "n_unique": 3,
        "min_len": 13,
        "max_len": 13,
        "n_empty": 0,
    }
    assert "@" not in str(string)


def test_the_string_group_covers_every_dtype_it_claims(tmp_path: Path):
    """`String`, `Categorical`, `Enum` and `Binary` columns share the string table."""
    ordered = _table(_metadata(tmp_path, _orders), "string")
    shipped = _table(_metadata(tmp_path, _shipment, key="shipment"), "string")

    # `status` is an `Enum` and `payload` is `Binary`.
    assert {"order_id", "email", "tracking_id", "status", "payload", "note"} <= set(
        ordered
    )
    assert ordered["payload"]["min_len"] == 2
    assert shipped["region"] == {
        "column": "region",
        "count": 4,
        "null_count": 1,
        "n_unique": 3,
        "min_len": 0,
        "max_len": 6,
        "n_empty": 1,
    }


def test_the_boolean_group_reports_both_counts_and_the_rate(tmp_path: Path):
    is_gift = _table(_metadata(tmp_path, _shipment, key="shipment"), "boolean")[
        "is_gift"
    ]

    assert is_gift == {
        "column": "is_gift",
        "count": 4,
        "null_count": 1,
        "n_true": 1,
        "n_false": 2,
        "true_rate": 0.3333,
    }


def test_a_materialization_carries_statistics_unless_someone_says_otherwise(
    tmp_path: Path,
):
    assert _groups(_metadata(tmp_path, _orders))


def test_the_setting_off_at_the_asset_suppresses_the_pass(tmp_path: Path):
    """With `statistics=False`, the package still writes `dagster/row_count`, because it is not a statistic."""

    @dd.asset(Orders, name="orders", quarantine=True, statistics=False)
    def without_statistics() -> pl.DataFrame:
        return mixed_orders()

    materialized = materializations(materialize(tmp_path, without_statistics))
    (orders,) = materialized.values()

    assert set(materialized) == {dg.AssetKey(["orders"])}
    assert not _groups(orders.metadata)
    assert orders.metadata["dagster/row_count"].value == 3


def test_the_setting_off_in_the_environment_suppresses_the_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    monkeypatch.setenv(_STATISTICS_ENV, "false")

    @dd.asset(Orders, name="orders")
    def statistics_off_by_environment() -> pl.DataFrame:
        return clean_orders()

    assert not _groups(_metadata(tmp_path, statistics_off_by_environment))


def test_the_quarantine_carries_no_statistics(tmp_path: Path):
    """Statistics cover only the valid rows, because nothing downstream reads the quarantine (ADR-0004)."""

    @dd.asset(Orders, name="orders", quarantine=True)
    def quarantined() -> pl.DataFrame:
        return mixed_orders()

    metadata = _metadata(tmp_path, quarantined)

    assert _groups(metadata)
    assert "dataframely/invalid_count" in metadata
    # Rule columns are strings, so statistics over the invalid rows would add them to the string table.
    assert not [
        column for column in _statistics_columns(metadata) if column.startswith("dy_")
    ]
