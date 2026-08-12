"""The statistics pass, asserted through the metadata a materialization carries.

The families and the duration rendering are never called directly. A data consumer meets them only as the tables on a materialization, so that is the one place they are asserted, and a pass that computed the same numbers somewhere unreachable would fail every test here.

Statistics are opt-out, so almost every asset in this file declares nothing about them: the tables are what a materialization carries until someone turns them off.
"""

import datetime as dt
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl
import pytest

import dagster_dataframely
from dagster_dataframely import DataframelyParquetIOManager, dataframely_asset
from tests.scenario import Orders, clean_orders, mixed_orders

_STATISTICS_ENV = "DAGSTER_DATAFRAMELY_STATISTICS"


class Profile(dy.Schema):
    """The dtypes `Orders` does not carry, one per gap it leaves.

    `Orders` already exercises `Int32`, `Decimal`, `String`, `Enum`, `Binary`, `Datetime`, `Duration` and a `List`. What it has no column of is `Float64`, `Date`, `Time`, `Categorical`, `Bool` and `Struct`, which is exactly this schema. Between the two, every dtype the four tables claim reaches a table, and both shapes that claim none reach nothing.

    Every column is nullable so one row can be entirely null: a null is what separates `count` from `null_count`, and an all-null column is the case where a `min` does not exist.
    """

    weight = dy.Float64(nullable=True)
    ordered_on = dy.Date(nullable=True)
    opens_at = dy.Time(nullable=True)
    fulfilled_in = dy.Duration(nullable=True)
    region = dy.Categorical(nullable=True)
    is_gift = dy.Bool(nullable=True)
    address = dy.Struct({"city": dy.String()}, nullable=True)


_PROFILE_SCHEMA: dict[str, pl.DataType] = {
    "weight": pl.Float64(),
    "ordered_on": pl.Date(),
    "opens_at": pl.Time(),
    "fulfilled_in": pl.Duration("us"),
    "region": pl.Categorical(),
    "is_gift": pl.Boolean(),
    "address": pl.Struct({"city": pl.String()}),
}

#: The three durations the ticket names, `8d`, `1m 30s` and `2h 5m`, plus the null every column here carries.
_DURATIONS: list[dt.timedelta | None] = [
    dt.timedelta(days=8),
    dt.timedelta(minutes=1, seconds=30),
    dt.timedelta(hours=2, minutes=5),
    None,
]


def profile_frame(durations: list[dt.timedelta | None]) -> pl.DataFrame:
    """Four rows: three carrying values and one entirely null.

    `weight` is deliberately awkward: its mean and standard deviation both run past four decimal places while its minimum is a value someone stored, so one frame shows both halves of the rounding rule.
    """
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
            # An empty string beside two of different lengths, so `n_empty` and the two length bounds each report something of their own.
            "region": ["US", "CANADA", "", None],
            "is_gift": [True, False, False, None],
            "address": [{"city": "NY"}, {"city": "LA"}, {"city": "SF"}, None],
        },
        schema=_PROFILE_SCHEMA,
    )


@dataframely_asset(schema=Orders, name="orders")
def _orders() -> pl.DataFrame:
    return clean_orders()


@dataframely_asset(schema=Profile, name="profile")
def _profile() -> pl.DataFrame:
    return profile_frame(_DURATIONS)


def _materialized(
    tmp_path: Path, asset: dg.AssetsDefinition
) -> dict[str, Mapping[str, dg.MetadataValue[Any]]]:
    """Materializes one asset and returns every materialization it emitted, keyed by asset name."""
    result = dg.materialize(
        [asset],
        resources={"io_manager": DataframelyParquetIOManager(base_dir=str(tmp_path))},
    )
    return {
        event.asset_key.to_user_string(): (
            event.step_materialization_data.materialization.metadata
        )
        for event in result.get_asset_materialization_events()
        if event.asset_key is not None
    }


def _metadata(
    tmp_path: Path, asset: dg.AssetsDefinition, key: str = "orders"
) -> Mapping[str, dg.MetadataValue[Any]]:
    """The metadata on one of them."""
    return _materialized(tmp_path, asset)[key]


def _table(
    metadata: Mapping[str, dg.MetadataValue[Any]], family: str
) -> dict[str, dict[str, Any]]:
    """Reads one family's table back as a row per column, keyed by column name."""
    value = metadata[f"stats/{family}"]
    assert isinstance(value, dg.TableMetadataValue)
    rows = [dict(record.data) for record in value.records]
    return {str(row["column"]): row for row in rows}


def _families(metadata: Mapping[str, dg.MetadataValue[Any]]) -> set[str]:
    return {key for key in metadata if key.startswith("stats/")}


def _profiled_columns(metadata: Mapping[str, dg.MetadataValue[Any]]) -> set[str]:
    """Every column that reached a table, whichever family it landed in."""
    return {
        column
        for family in _families(metadata)
        for column in _table(metadata, family.removeprefix("stats/"))
    }


# --- which families are emitted ---
def test_a_materialization_carries_one_table_per_family_present(tmp_path: Path):
    """A `skimr`-style profile: four tables grouped by dtype family, so a distribution read needs no separate tool."""
    metadata = _metadata(tmp_path, _profile, key="profile")

    assert _families(metadata) == {
        "stats/numeric",
        "stats/temporal",
        "stats/string",
        "stats/boolean",
    }


def test_a_family_the_frame_has_no_column_of_is_not_emitted(tmp_path: Path):
    """An empty table would be a row of nothing in the UI, permanently."""

    class Weights(dy.Schema):
        weight = dy.Float64(nullable=False)

    @dataframely_asset(schema=Weights, name="weights")
    def weights() -> pl.DataFrame:
        return pl.DataFrame({"weight": [1.0, 2.0]})

    metadata = _metadata(tmp_path, weights, key="weights")

    assert _families(metadata) == {"stats/numeric"}


def test_a_nested_column_reaches_no_table_and_gets_none_of_its_own(tmp_path: Path):
    """`List` and `Struct` have nothing to say beyond their counts, and a fifth table holding two columns is not worth the row it takes."""
    profiled = _metadata(tmp_path, _profile, key="profile")
    ordered = _metadata(tmp_path, _orders)

    assert "address" not in _profiled_columns(profiled)
    assert "tags" not in _profiled_columns(ordered)
    # `Orders` carries no `Bool`, so the fourth table is absent for a second reason.
    assert _families(ordered) == {"stats/numeric", "stats/temporal", "stats/string"}


def test_the_families_are_emitted_as_tables_rather_than_markdown(tmp_path: Path):
    """A table value renders as a full-featured HTML table in the UI; markdown renders as printed text. Judged in the running UI."""
    metadata = _metadata(tmp_path, _profile, key="profile")

    assert all(
        isinstance(metadata[family], dg.TableMetadataValue)
        for family in _families(metadata)
    )


# --- the numeric family ---
def test_the_numeric_family_reports_seven_statistics_per_column(tmp_path: Path):
    metadata = _metadata(tmp_path, _profile, key="profile")

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


def test_a_computed_cell_is_rounded_and_an_observed_one_is_exact(tmp_path: Path):
    """`min` and `max` are values that exist in the data, so the UI never shows a number nobody stored. `mean`, `std` and `p50` are derived, so four places is enough of them."""
    weight = _table(_metadata(tmp_path, _profile, key="profile"), "numeric")["weight"]

    assert weight["min"] == 1.23456789
    assert weight["mean"] == round(2.41152263, 4)


def test_a_decimal_column_emits_without_crashing(tmp_path: Path):
    """The numeric family picks up `Decimal`, and the aggregate returns a Python `Decimal` that the table record type rejects outright, so a money column would otherwise take the whole materialization down."""
    amount = _table(_metadata(tmp_path, _orders), "numeric")["amount"]

    assert amount["min"] == 10.0
    assert amount["max"] == 99.0
    assert amount["mean"] == 44.8333
    assert all(
        isinstance(cell, (int, float))
        for key, cell in amount.items()
        if key != "column"
    )


# --- the temporal family ---
def test_a_date_column_renders_as_a_date_rather_than_a_datetime(tmp_path: Path):
    """What routing through `describe()` costs: it stringifies with per-source-dtype formatting, and a `Date` comes back as a datetime nobody stored."""
    ordered_on = _table(_metadata(tmp_path, _profile, key="profile"), "temporal")[
        "ordered_on"
    ]

    assert ordered_on["min"] == "2024-01-01"
    assert ordered_on["max"] == "2024-01-09"


def test_a_temporal_column_reports_the_span_between_its_bounds(tmp_path: Path):
    temporal = _table(_metadata(tmp_path, _profile, key="profile"), "temporal")

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
    """`8d`, `1m 30s`, `2h 5m`. ISO-8601 is what a reader gets from the obvious call, and a span is the one cell nobody can read that way."""
    fulfilled_in = _table(_metadata(tmp_path, _profile, key="profile"), "temporal")[
        "fulfilled_in"
    ]

    assert fulfilled_in["min"] == "1m 30s"
    assert fulfilled_in["max"] == "8d"
    assert fulfilled_in["span"] == "7d 23h 58m 30s"


def test_a_negative_duration_keeps_its_sign(tmp_path: Path):
    """A duration is a difference, so it is signed. A profile that dropped the sign would read as its own opposite."""

    @dataframely_asset(schema=Profile, name="profile")
    def refunds() -> pl.DataFrame:
        return profile_frame(
            [
                dt.timedelta(seconds=-90),
                dt.timedelta(seconds=-30),
                dt.timedelta(0),
                None,
            ]
        )

    fulfilled_in = _table(_metadata(tmp_path, refunds, key="profile"), "temporal")[
        "fulfilled_in"
    ]

    assert fulfilled_in["min"] == "-1m -30s"
    assert fulfilled_in["max"] == "0µs"


def test_an_all_null_duration_column_states_nothing_rather_than_zero(tmp_path: Path):
    """A column nobody filled in has no bounds and no span. Zero would claim it did."""

    @dataframely_asset(schema=Profile, name="profile")
    def unfulfilled() -> pl.DataFrame:
        return profile_frame([None, None, None, None])

    fulfilled_in = _table(_metadata(tmp_path, unfulfilled, key="profile"), "temporal")[
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


# --- the string family ---
def test_the_string_family_carries_no_value_bearing_statistic(tmp_path: Path):
    """The one rule the setting does not cover, so the whole row is asserted rather than the absence of one cell: a `min` here would print a real address permanently into a shared, exported event log."""
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


def test_the_string_family_covers_every_dtype_it_claims(tmp_path: Path):
    """`String`, `Categorical`, `Enum` and `Binary`: four dtypes whose useful statistics are all lengths and counts, so they read as one table rather than four."""
    ordered = _table(_metadata(tmp_path, _orders), "string")
    profiled = _table(_metadata(tmp_path, _profile, key="profile"), "string")

    # `status` is an `Enum` and `payload` is `Binary`.
    assert {"order_id", "email", "tracking_id", "status", "payload", "note"} <= set(
        ordered
    )
    assert ordered["payload"]["min_len"] == 2
    assert profiled["region"] == {
        "column": "region",
        "count": 4,
        "null_count": 1,
        "n_unique": 3,
        "min_len": 0,
        "max_len": 6,
        "n_empty": 1,
    }


# --- the boolean family ---
def test_the_boolean_family_reports_both_counts_and_the_rate(tmp_path: Path):
    """The rate is the computed one of the three, so it rounds where the counts do not."""
    is_gift = _table(_metadata(tmp_path, _profile, key="profile"), "boolean")["is_gift"]

    assert is_gift == {
        "column": "is_gift",
        "count": 4,
        "null_count": 1,
        "n_true": 1,
        "n_false": 2,
        "true_rate": 0.3333,
    }


# --- the setting ---
def test_a_materialization_is_profiled_unless_someone_says_otherwise(tmp_path: Path):
    """Opt-out, on by default: the asset declares nothing and the tables are there."""
    assert _families(_metadata(tmp_path, _orders))


def test_the_setting_off_at_the_asset_suppresses_the_pass_on_both_outs(tmp_path: Path):
    """Off is off for the whole run, not for the table the reader happened to be thinking of."""

    @dataframely_asset(
        schema=Orders, name="orders", quarantine=dg.AssetOut(), statistics=False
    )
    def unprofiled() -> pl.DataFrame:
        return mixed_orders()

    materialized = _materialized(tmp_path, unprofiled)

    assert set(materialized) == {"orders", "orders_quarantine"}
    assert not [family for m in materialized.values() for family in _families(m)]
    assert materialized["orders"]["dagster/row_count"].value == 3


def test_the_setting_off_in_the_environment_suppresses_the_pass(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The house-style tier: a platform engineer turns the pass off for a whole code location without touching an asset."""
    monkeypatch.setenv(_STATISTICS_ENV, "false")

    @dataframely_asset(schema=Orders, name="orders")
    def house_style() -> pl.DataFrame:
        return clean_orders()

    assert not _families(_metadata(tmp_path, house_style))


def test_the_quarantine_is_profiled_too(tmp_path: Path):
    """The rejected rows are a table a consumer reads, and what the values in them look like is the question the checks do not answer."""

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def quarantined() -> pl.DataFrame:
        return mixed_orders()

    metadata = _metadata(tmp_path, quarantined, key="orders_quarantine")

    assert _families(metadata)
    assert _table(metadata, "numeric")["amount"]["min"] == -4.0


# --- how the numbers are computed ---
def test_no_module_routes_through_describe():
    """`describe()` stringifies with per-source-dtype formatting and cannot be cast back: a `Date` mean renders as a datetime, a `Duration` mean as a clock time, and `min` mixes numbers, bare strings and dates in a single column.

    Asserted against the source because the alternative is asserting every wrong rendering it would produce, one at a time, forever.
    """
    sources = list(Path(next(iter(dagster_dataframely.__path__))).glob("*.py"))
    calls = [
        f"{source.name}:{number}"
        for source in sources
        for number, line in enumerate(source.read_text().splitlines(), start=1)
        # `_csv_codecs.describe` is the package's own, naming encoded columns in a log line. The call barred here is the frame method of the same name.
        if ".describe(" in line.replace("_csv_codecs.describe(", "")
    ]

    assert sources
    assert calls == []
