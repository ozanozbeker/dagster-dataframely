"""Smoke tests: the code location loads and still holds what the README claims.

A demo's failure mode is silent rot. The library moves, `dg` autoloading picks up a module that no longer defines what it used to, and nobody notices until the webserver is open in front of an audience. Asserting the counts here is what turns that into a red run instead.

Everything reads the resolved asset graph rather than `Definitions.assets`. The unresolved list holds each `quarantine_spec` twice, because the autoloader reaches a module-level spec by two routes and Dagster dedupes on resolve; counting the list would pin that duplication as if it were a fact about the demo.
"""

import collections
import datetime as dt
import inspect
from pathlib import Path

import dagster as dg
import dagster_dataframely_demo
import polars as pl
import pytest

# Private in Dagster, and the type `resolve_asset_graph()` returns: nothing public names it.
# Imported for the annotation alone, the way `tests/` in the library above reaches for a
# `dagster._core` name it needs.
from dagster._core.definitions.assets.graph.asset_graph import AssetGraph
from dagster_dataframely_demo import _data
from dagster_dataframely_demo.defs.metadata import annotated_orders
from dagster_dataframely_demo.schema import Orders

EXPECTED_GROUPS = {
    "base": 4,
    "catalog": 2,
    "failure/column_schema": 1,
    "failure/no_quarantine": 1,
    "failure/nothing_survives": 2,
    "failure/quarantine": 2,
    "failure/skip": 1,
    "granularity": 4,
    "lazy": 2,
    "metadata": 2,
    "partitions": 4,
    "wiring": 4,
}

# What each setting collapses the schema's rules down to, counting the blocking
# `dy_schema__columns` check. These are the numbers the README quotes.
EXPECTED_CHECKS = {
    "orders": 24,
    "orders_by_rule": 24,
    "orders_by_column": 14,
    "orders_by_column_per_rule": 16,
    "orders_by_schema": 2,
    "unfiltered_orders": 24,
}

# Every quarantine the demo gives a node, against the asset it must hang off. Spelled out
# rather than derived from the `_quarantine` suffix, so a quarantine that stops being
# declared fails here instead of quietly leaving one fewer key to check.
EXPECTED_QUARANTINES = {
    "doomed_orders_quarantine": "doomed_orders",
    "hand_wired_orders_quarantine": "hand_wired_orders",
    "quarantined_orders_quarantine": "quarantined_orders",
    "regional_orders_quarantine": "regional_orders",
}

# Every rule the defective frame is built to break, one per row bar the two that
# share `ORD-0015` and the two lines of `ORD-0016`.
EXPECTED_BROKEN_RULES = {
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
    """The same location resolved, which is where a `quarantine_spec` becomes one node rather than two."""
    return defs.resolve_asset_graph()


def _key(graph: AssetGraph, name: str) -> dg.AssetKey:
    return next(k for k in graph.get_all_asset_keys() if k.to_user_string() == name)


def test_every_group_holds_the_assets_the_readme_lists(graph: AssetGraph):
    counts = collections.Counter(
        graph.get(key).group_name for key in graph.get_all_asset_keys()
    )
    assert dict(counts) == EXPECTED_GROUPS


def test_one_io_manager_is_bound(defs: dg.Definitions):
    """The demo brings its own, because this package ships none."""
    assert sorted(defs.resources or {}) == ["io_manager"]


def test_check_granularity_collapses_the_rules_as_documented(graph: AssetGraph):
    counts = collections.Counter(
        check.asset_key.to_user_string() for check in graph.asset_check_keys
    )
    assert {key: counts[key] for key in EXPECTED_CHECKS} == EXPECTED_CHECKS


def test_base_is_the_only_group_with_roots_in_it(graph: AssetGraph):
    """The graph is only readable while every group is a chain hanging off `base`.

    A new asset that builds its own frame instead of taking one is the exact regression this catches, because it costs nothing to write and adds a root nobody notices until the lineage view is on a projector.
    """
    roots = {
        key.to_user_string()
        for key in graph.get_all_asset_keys()
        if not graph.get(key).parent_keys
    }

    assert roots == {
        "defective_raw_orders",
        "hopeless_raw_orders",
        "mistyped_raw_orders",
        "raw_orders",
    }


def test_nothing_has_more_than_one_parent(graph: AssetGraph):
    """One edge in means the eye can follow a group without tracing which of three upstreams fed which asset. Nothing here needs a second input, so a second one is a mistake rather than a design."""
    fanned_in = {
        key.to_user_string(): sorted(
            p.to_user_string() for p in graph.get(key).parent_keys
        )
        for key in graph.get_all_asset_keys()
        if len(graph.get(key).parent_keys) > 1
    }

    assert fanned_in == {}


def test_every_quarantine_hangs_off_its_own_valid_asset(graph: AssetGraph):
    """ADR-0003, and the lineage screenshot this project exists to supply.

    `wiring` is the reason this reads the whole location rather than one asset: there the spec is built beside an asset the decorator did not declare, so it can drift from what the decorator does without anything else noticing.
    """
    parents = {
        key: sorted(p.to_user_string() for p in graph.get(_key(graph, key)).parent_keys)
        for key in EXPECTED_QUARANTINES
    }

    assert parents == {key: [valid] for key, valid in EXPECTED_QUARANTINES.items()}


def test_every_quarantine_is_partitioned_like_its_asset(graph: AssetGraph):
    """A spec built from the definition carries its partitions; one built from a key does not.

    `regional_orders` is the only partitioned one, so this is the assertion that the demo passed the definition rather than the key.
    """
    partitioned = {
        key
        for key in EXPECTED_QUARANTINES
        if graph.get(_key(graph, key)).partitions_def is not None
    }

    assert partitioned == {"regional_orders_quarantine"}


def test_the_defective_frame_breaks_the_rules_the_demo_advertises():
    """`failure/quarantine` and `failure/no_quarantine` are only worth looking at if the data still fails."""
    valid, failure = Orders.filter(_data.defective_orders(), cast=False)
    assert valid.height == 12
    assert len(failure) == 8
    assert {
        rule for rule, count in failure.counts().items() if count
    } == EXPECTED_BROKEN_RULES


def test_the_partitions_hold_disjoint_orders():
    """The fan-in in `partitions` is only valid while no order is split across two days.

    Restamping every line onto every day would duplicate the primary key the moment `orders_rollup` concatenated two partitions, and splitting an order across days would take out `line_numbers_are_dense` for both halves.
    """
    days = [dt.date(2026, 8, day) for day in range(1, 6)]
    combined = pl.concat(_data.orders_on(_data.clean_orders(), day) for day in days)

    assert combined.height == _data.clean_orders().height
    _, failure = Orders.filter(combined, cast=False)
    assert len(failure) == 0


def test_the_described_assets_show_their_own_prose(graph: AssetGraph):
    """Every other asset passes `description=`, or the whole demo would read as one sentence.

    Naming the one asset that does not is also the assertion that the fallback still works. `cleandoc` because that is what the decorator applies: a raw docstring keeps its source indentation, which the catalog would render as a code block.
    """
    fallback = inspect.cleandoc(Orders.__doc__ or "")
    described = [
        key.to_user_string()
        for key in graph.get_all_asset_keys()
        if graph.get(key).description == fallback
    ]

    assert described == ["orders_undescribed"]


def test_a_returned_result_is_inspectable_by_calling_the_asset():
    """Direct invocation, which is what `metadata` claims and what a user's own tests would do."""
    events = list(annotated_orders(_data.clean_orders()))  # pyrefly: ignore[bad-argument-type]
    materialization = next(
        event for event in events if isinstance(event, dg.MaterializeResult)
    )
    metadata = materialization.metadata or {}

    assert materialization.value.height == 12
    # The package counts the valid rows itself and applies that key last, so the 999 loses.
    assert metadata["dagster/row_count"] == 12
    assert metadata["source"] == "stripe"
    assert materialization.data_version == dg.DataVersion("2026-08-05")
