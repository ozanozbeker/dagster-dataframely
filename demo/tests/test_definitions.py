"""The code location loads, and each asset still does what the README and screenshots show.

A demo fails silently: the library moves, the autoloader picks up a module that no longer defines what it used to, and nobody notices until the webserver is open in front of an audience. These turn that into a red run.

Everything reads the resolved asset graph rather than `Definitions.assets`. The unresolved list holds each `quarantine_spec` twice, because the autoloader reaches a module-level spec by two routes and Dagster dedupes on resolve.
"""

import collections
import datetime as dt
import inspect
from collections.abc import Iterable
from pathlib import Path

import dagster as dg
import dagster_dataframely_demo
import polars as pl
import pytest

# Private in Dagster, and the type `resolve_asset_graph()` returns: nothing public names it.
from dagster._core.definitions.assets.graph.asset_graph import AssetGraph
from dagster_dataframely_demo import _data
from dagster_dataframely_demo.defs.gold.rollups import orders_snapshot
from dagster_dataframely_demo.defs.silver.customers import Customers
from dagster_dataframely_demo.defs.silver.legacy import legacy_orders
from dagster_dataframely_demo.defs.silver.marketplace import (
    finance_orders,
    marketing_orders,
)
from dagster_dataframely_demo.defs.silver.partitions import regional_orders
from dagster_dataframely_demo.defs.silver.partner import partner_orders
from dagster_dataframely_demo.schema import Orders

import dagster_dataframely as dd

EXPECTED_GROUPS = {
    "bronze": {
        "raw_orders",
        "raw_customers",
        "raw_marketplace_orders",
        "raw_partner_orders",
        "raw_legacy_orders",
    },
    "silver": {
        "orders",
        "customers",
        "marketing_orders",
        "marketing_orders_quarantine",
        "finance_orders",
        "partner_orders",
        "legacy_orders",
        "legacy_orders_quarantine",
        "daily_orders",
        "regional_orders",
        "regional_orders_quarantine",
        "warehouse_orders",
    },
    "gold": {"weekly_orders", "high_value_orders", "orders_snapshot"},
}

# What each granularity collapses the schema's rules to, counting the blocking
# `dy_schema__columns` check. The README and the screenshots quote these.
EXPECTED_CHECKS = {"orders": 24, "customers": 8, "daily_orders": 14, "legacy_orders": 2}

EXPECTED_QUARANTINES = {
    "marketing_orders_quarantine": "marketing_orders",
    "legacy_orders_quarantine": "legacy_orders",
    "regional_orders_quarantine": "regional_orders",
}

# Every rule the marketplace feed breaks. `ORD-0015` breaks three at once, and both of
# `ORD-0016`'s lines fail the density rule together.
MARKETPLACE_BROKEN_RULES = {
    "amount|min",
    "email|check__lowercase",
    "line_numbers_are_dense",
    "order_id|regex",
    "paid_orders_have_amount",
    "priority|is_in",
    "quantity|max",
}


@pytest.fixture(scope="module")
def defs() -> dg.Definitions:
    """Load the code location exactly as `dg dev` does."""
    return dg.load_from_defs_folder(
        path_within_project=Path(dagster_dataframely_demo.__file__).parent
    )


@pytest.fixture(scope="module")
def graph(defs: dg.Definitions) -> AssetGraph:
    return defs.resolve_asset_graph()


Yielded = list[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]


def _names(keys: Iterable[dg.AssetKey]) -> set[str]:
    return {key.to_user_string() for key in keys}


def _events(asset: dg.AssetsDefinition, *args: object) -> Yielded:
    """Call the asset and drain it; `__call__` is typed `-> object` upstream, as in `tests/scenario.py`."""
    return list(asset(*args))  # pyrefly: ignore[bad-argument-type]


def test_every_group_holds_the_assets_the_readme_lists(graph: AssetGraph):
    groups: dict[str, set[str]] = collections.defaultdict(set)
    for key in graph.get_all_asset_keys():
        groups[graph.get(key).group_name].add(key.to_user_string())
    assert dict(groups) == EXPECTED_GROUPS


def test_one_io_manager_is_bound(defs: dg.Definitions):
    assert sorted(defs.resources or {}) == ["io_manager"]


def test_check_granularity_collapses_the_rules_as_documented(graph: AssetGraph):
    counts = collections.Counter(
        check.asset_key.to_user_string() for check in graph.asset_check_keys
    )
    assert {key: counts[key] for key in EXPECTED_CHECKS} == EXPECTED_CHECKS


def test_bronze_holds_every_root(graph: AssetGraph):
    roots = {
        key.to_user_string()
        for key in graph.get_all_asset_keys()
        if not graph.get(key).parent_keys
    }
    assert roots == EXPECTED_GROUPS["bronze"]


def test_nothing_has_more_than_one_parent(graph: AssetGraph):
    fanned_in = {
        key.to_user_string()
        for key in graph.get_all_asset_keys()
        if len(graph.get(key).parent_keys) > 1
    }
    assert fanned_in == set()


def test_every_quarantine_hangs_off_its_own_asset(graph: AssetGraph):
    parents = {
        key.to_user_string(): _names(graph.get(key).parent_keys)
        for key in graph.get_all_asset_keys()
        if key.to_user_string() in EXPECTED_QUARANTINES
    }
    assert parents == {key: {valid} for key, valid in EXPECTED_QUARANTINES.items()}


def test_the_grid_quarantine_is_partitioned_like_its_asset(graph: AssetGraph):
    partitioned = {
        key.to_user_string()
        for key in graph.get_all_asset_keys()
        if key.to_user_string() in EXPECTED_QUARANTINES
        and graph.get(key).partitions_def is not None
    }
    assert partitioned == {"regional_orders_quarantine"}


def test_the_marketplace_feed_breaks_the_rules_it_is_known_for():
    valid, failure = Orders.filter(_data.marketplace_orders(), cast=False)
    assert valid.height == 12
    assert len(failure) == 8
    assert {
        rule for rule, count in failure.counts().items() if count
    } == MARKETPLACE_BROKEN_RULES


def test_the_customer_export_is_clean():
    """The README opens on this table, so it has to validate whole."""
    valid, failure = Customers.filter(_data.storefront_customers(), cast=False)
    assert valid.height == 5
    assert len(failure) == 0


def test_every_legacy_line_fails():
    valid, failure = Orders.filter(_data.legacy_orders(), cast=False)
    assert valid.height == 0
    assert len(failure) == 3


def test_the_partner_feed_only_disagrees_on_quantity():
    drifted = {
        name
        for name, dtype in _data.partner_orders().schema.items()
        if dtype != _data.storefront_orders().schema[name]
    }
    assert drifted == {"quantity"}


def test_the_daily_partitions_hold_disjoint_orders():
    days = [dt.date(2026, 8, day) for day in range(1, 6)]
    combined = pl.concat(
        _data.orders_on(_data.storefront_orders(), day) for day in days
    )
    assert combined.height == _data.storefront_orders().height
    _, failure = Orders.filter(combined, cast=False)
    assert len(failure) == 0


def test_every_order_lands_in_exactly_one_region_cell():
    days = [dt.date(2026, 8, day) for day in range(1, 6)]
    cells = [
        _data.orders_in(_data.storefront_orders(), day, region)
        for day in days
        for region in ("apac", "eu", "us")
        if not (region == "apac" and day < _data.APAC_LAUNCH)
    ]
    assert pl.concat(cells).height == _data.storefront_orders().height


@pytest.fixture
def quarantine_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_QUARANTINE_DIR", str(tmp_path))
    return tmp_path


def test_apac_skips_the_days_before_it_opened():
    key = dg.MultiPartitionKey({"day": "2026-08-02", "region": "apac"})
    context = dg.build_asset_context(partition_key=key)
    events = _events(regional_orders, context, _data.storefront_orders())
    assert not any(isinstance(event, dg.MaterializeResult) for event in events)


def test_finance_stops_the_load_on_one_bad_line():
    with pytest.raises(dd.errors.ValidationAbortError):
        _events(finance_orders, _data.marketplace_orders())


def test_the_partner_feed_stops_at_the_column_schema_check():
    with pytest.raises(dd.errors.ColumnSchemaError, match="quantity"):
        _events(partner_orders, _data.partner_orders())


def test_nothing_survives_the_legacy_migration(quarantine_dir: Path):
    with pytest.raises(dd.errors.NothingSurvivedError):
        _events(legacy_orders, dg.build_asset_context(), _data.legacy_orders())
    assert (
        pl.read_parquet(quarantine_dir / "legacy_orders_quarantine.parquet").height == 3
    )


def test_marketing_keeps_the_valid_lines_and_quarantines_the_rest(quarantine_dir: Path):
    events = _events(
        marketing_orders, dg.build_asset_context(), _data.marketplace_orders()
    )
    materialization = next(e for e in events if isinstance(e, dg.MaterializeResult))
    assert materialization.value.height == 12
    assert (
        pl.read_parquet(quarantine_dir / "marketing_orders_quarantine.parquet").height
        == 8
    )


def test_a_returned_result_is_inspectable_by_calling_the_asset():
    events = _events(orders_snapshot, _data.storefront_orders())
    materialization = next(e for e in events if isinstance(e, dg.MaterializeResult))
    metadata = materialization.metadata or {}

    assert materialization.value.height == 12
    # The package counts the valid rows itself and applies that key last, so the 999 loses.
    assert metadata["dagster/row_count"] == 12
    assert metadata["source"] == "storefront"
    assert materialization.data_version == dg.DataVersion("2026-08-05")


def test_an_asset_without_a_description_shows_the_schemas(graph: AssetGraph):
    """`cleandoc` because the decorator applies it; a raw docstring would render as a code block."""
    description = graph.get(dg.AssetKey(["orders"])).description
    assert description == inspect.cleandoc(Orders.__doc__ or "")
