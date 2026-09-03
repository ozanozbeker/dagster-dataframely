"""A decorated function that returns a `dg.MaterializeResult` instead of a bare frame.

`@dg.asset` accepts one, and Dagster's own docs teach it for attaching metadata, so refusing it cost parity with the decorator this one is modelled on (#77). The result's `value` is the frame to validate. Its metadata, data version and tags fold into the materialization the package yields for the valid out.

Every field is asserted twice where both routes can see it: once by calling, once through `dg.materialize`. Metadata survives either route. A data version and tags are event-level: a call hands back the `dg.MaterializeResult` carrying them, and a run turns them into event tags, so both routes need pinning.
"""

import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dagster_dataframely import dy_asset
from dagster_dataframely.errors import (
    MaterializeResultFieldError,
    MaterializeResultValueError,
    NothingSurvivedError,
    ValidationAbortError,
)
from tests.scenario import (
    Orders,
    clean_orders,
    cooccurring_orders,
    hopeless_orders,
    mixed_orders,
    storage,
)

_VALID = dg.AssetKey(["orders"])
_Yielded = list[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]


def _call(asset: dg.AssetsDefinition) -> _Yielded:
    """Call the asset and drain what comes back."""
    return list(asset())  # pyrefly: ignore[bad-argument-type]


def _results(events: _Yielded) -> dict[dg.AssetKey, dg.MaterializeResult[pl.DataFrame]]:
    """The materialization each output produced, keyed by asset."""
    return {
        event.asset_key: event
        for event in events
        if isinstance(event, dg.MaterializeResult) and event.asset_key is not None
    }


def _materialize(tmp_path: Path, asset: dg.AssetsDefinition):
    return dg.materialize(
        [asset],
        resources=storage(tmp_path),
    )


def _materializations(
    result: dg.ExecuteInProcessResult,
) -> dict[dg.AssetKey, dg.AssetMaterialization]:
    """Every materialization the run emitted, whole: this file asserts tags as well as metadata."""
    return {
        event.asset_key: event.step_materialization_data.materialization
        for event in result.get_asset_materialization_events()
        if event.asset_key is not None
    }


def _metadata(
    result: dg.ExecuteInProcessResult, key: dg.AssetKey
) -> Mapping[str, dg.MetadataValue[Any]]:
    return _materializations(result)[key].metadata


# --- what folds in ---
def test_a_returned_result_carries_its_metadata_onto_the_valid_out():
    """The gap #77 opened with: one line of metadata on a validated table, without giving up the schema."""

    @dy_asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=clean_orders(), metadata={"source": "stripe", "batch": 7}
        )

    metadata = _results(_call(orders))[_VALID].metadata or {}

    assert metadata["source"] == "stripe"
    assert metadata["batch"] == 7


def test_a_run_records_the_returned_metadata_beside_the_packages_own(tmp_path: Path):
    """Beside, not instead: the row count the package emits is still there."""

    @dy_asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(value=clean_orders(), metadata={"source": "stripe"})

    metadata = _metadata(_materialize(tmp_path, orders), _VALID)

    assert metadata["source"].value == "stripe"
    assert metadata["dagster/row_count"].value == 3


def test_the_packages_own_key_wins_a_collision():
    """The same precedence the decorator applies to definition metadata. `dagster/row_count` matters most: Dagster reads it, and a decorated function that overwrote it would put a count nothing counted in the catalog.

    Asserted on the call, not through a run. An IO manager that counts rows writes the same key last, so a run's materialization cannot tell the package's precedence from the manager's.
    """

    @dy_asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=clean_orders(), metadata={"dagster/row_count": 999}
        )

    called = _results(_call(orders))[_VALID].metadata or {}

    assert called["dagster/row_count"] == 3


def test_a_returned_data_version_and_tags_reach_the_valid_result():
    """Neither has another route. `context.set_data_version` carries no `@public`, and the context exposes nothing for a materialization's tags."""

    @dy_asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=clean_orders(),
            data_version=dg.DataVersion("v1"),
            tags={"run/flavour": "backfill"},
        )

    result = _results(_call(orders))[_VALID]

    assert result.data_version == dg.DataVersion("v1")
    assert result.tags == {"run/flavour": "backfill"}


def test_a_run_turns_the_returned_data_version_and_tags_into_event_tags(tmp_path: Path):
    """Dagster decides where they land: both become tags on the materialization event."""

    @dy_asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=clean_orders(),
            data_version=dg.DataVersion("v1"),
            tags={"run/flavour": "backfill"},
        )

    tags = _materializations(_materialize(tmp_path, orders))[_VALID].tags or {}

    assert tags["dagster/data_version"] == "v1"
    assert tags["run/flavour"] == "backfill"


def test_a_lazy_return_folds_the_same_way():
    """`fold` sits over what `process` yields. `Schema.filter` takes both frame kinds through the same call, so laziness changes nothing."""

    @dy_asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.LazyFrame]:
        return dg.MaterializeResult(value=clean_orders().lazy(), metadata={"m": 1})

    result = _results(_call(orders))[_VALID]

    assert (result.metadata or {})["m"] == 1
    assert_frame_equal(result.value, clean_orders())


# --- what a quarantine changes about `fold` ---
def test_a_quarantined_asset_folds_onto_the_one_materialization(tmp_path: Path):
    """The quarantine is written, not materialized, so the returned result has one place to land. The package's own keys still win a collision."""

    @dy_asset(Orders, name="orders", quarantine=True)
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=cooccurring_orders(),
            metadata={"source": "stripe", "dataframely/invalid_count": 99},
            data_version=dg.DataVersion("v1"),
            tags={"run/flavour": "backfill"},
        )

    materializations = _materializations(_materialize(tmp_path, orders))
    (event,) = materializations.values()
    tags = event.tags or {}

    assert set(materializations) == {_VALID}
    assert event.metadata["source"] == dg.MetadataValue.text("stripe")
    assert event.metadata["dataframely/invalid_count"] == dg.MetadataValue.int(1)
    assert tags["dagster/data_version"] == "v1"
    assert tags["run/flavour"] == "backfill"


# --- what stays refused ---
# Every refusal is asserted twice, once by calling and once through a run. `unwrap` runs inside
# the wrapper's generator, so nothing happens until something advances it. A refusal that only
# surfaced on a direct call would let a run write a table the package never validated.
# Declared as bare returns, not decorated assets: the two tests below share the same seven cases,
# and a decorated asset cannot be re-declared per test without a name collision.
_REFUSALS = [
    pytest.param(
        lambda: dg.MaterializeResult(metadata={"source": "stripe"}),
        MaterializeResultValueError,
        # The message names the alternative: whoever wrote this was trying to attach metadata.
        "context.add_asset_metadata",
        id="no value",
    ),
    pytest.param(
        lambda: dg.MaterializeResult(value=[1, 2, 3]),
        MaterializeResultValueError,
        "carries no frame",
        id="a value that is not a frame",
    ),
    pytest.param(
        # A bare `None` is the skip; this is not. A returned result puts something on a materialization, and a skipped run has none, so the rest of this object has nowhere to go (#95).
        lambda: dg.MaterializeResult(value=None, metadata={"delivered": False}),
        MaterializeResultValueError,
        "carries no frame",
        id="a value of None",
    ),
    pytest.param(
        lambda: dg.MaterializeResult(
            asset_key=dg.AssetKey(["elsewhere"]), value=clean_orders()
        ),
        MaterializeResultFieldError,
        "`asset_key`",
        id="asset_key",
    ),
    pytest.param(
        lambda: dg.MaterializeResult(
            value=clean_orders(),
            check_results=[dg.AssetCheckResult(check_name="mine", passed=True)],
        ),
        MaterializeResultFieldError,
        "`check_results`",
        id="check_results",
    ),
    pytest.param(
        # The legacy spelling, which Dagster's docs steer away from in new code. It reaches the frame guard like everything else unreadable.
        lambda: dg.Output(clean_orders(), metadata={"source": "stripe"}),
        dg.DagsterInvariantViolationError,
        "'orders' returned a Output",
        id="dg.Output",
    ),
    pytest.param(
        # Not `None`, which the guard lets through as the skip (#95). A string is the nearest thing that is still a mistake.
        lambda: "orders",
        dg.DagsterInvariantViolationError,
        "'orders' returned a str",
        id="something that is not a frame",
    ),
]


def _refusing(fn: Callable[[], object]) -> dg.AssetsDefinition:
    """Declare the asset under the one name every message above expects.

    The decorated functions carry no annotation: each returns what the decorator's own type says it cannot.
    """
    return dy_asset(Orders, name="orders")(fn)  # pyrefly: ignore[bad-argument-type]


@pytest.mark.parametrize(("fn", "error", "says"), _REFUSALS)
def test_a_refused_return_names_what_is_wrong(
    fn: Callable[[], object], error: type[Exception], says: str
):
    with pytest.raises(error, match=re.escape(says)):
        _call(_refusing(fn))


@pytest.mark.parametrize(("fn", "error", "says"), _REFUSALS)
def test_a_refused_return_fails_the_run_and_writes_nothing(
    tmp_path: Path,
    fn: Callable[[], object],
    error: type[Exception],
    says: str,
):
    with pytest.raises(error, match=re.escape(says)):
        _materialize(tmp_path, _refusing(fn))

    assert not list(tmp_path.rglob("*.parquet"))


# --- no valid materialization to fold onto ---
def test_a_run_that_writes_no_table_still_raises_its_own_error(tmp_path: Path):
    """Nothing survived, so `fold` has only checks to pass through and no materialization to land on. The error `process` raises must reach the caller unchanged; the stage wrapping it must not swallow it."""

    @dy_asset(Orders, name="orders", quarantine=True)
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=hopeless_orders(), metadata={"source": "stripe"}
        )

    with pytest.raises(NothingSurvivedError):
        _materialize(tmp_path, orders)


def test_an_abort_with_no_quarantine_still_raises_its_own_error():
    """Rows failed and no quarantine is declared, so the run yields only checks and no materialization. `fold` has nothing to land on and must not invent one."""

    @dy_asset(Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(value=mixed_orders(), metadata={"source": "stripe"})

    with pytest.raises(ValidationAbortError):
        _call(orders)


def test_the_frame_guard_names_every_route_out():
    """Giving up the schema used to be the only advice, which was wrong for anyone who wanted metadata on a validated table. It is now the last of four routes, aimed at the one reader it fits: an asset that writes its own storage and never holds a frame."""
    with pytest.raises(dg.DagsterInvariantViolationError) as raised:
        _call(_refusing(lambda: "orders"))
    message = str(raised.value)

    assert "Polars DataFrame or LazyFrame" in message
    assert "dg.MaterializeResult" in message
    assert "`None` to skip" in message
    assert "plain `@dg.asset`" in message
    assert "schema_metadata" in message


# --- what a bare frame still does ---
def test_a_bare_frame_carries_no_data_version_and_no_tags():
    """The guarantee `fold` rests on: an asset that returns a frame produces what it produced before #77."""

    @dy_asset(Orders, name="orders")
    def orders() -> pl.DataFrame:
        return clean_orders()

    result = _results(_call(orders))[_VALID]

    assert result.data_version is None
    assert result.tags is None


def test_a_bare_frame_and_a_returned_result_agree_on_everything_else(tmp_path: Path):
    """Same metadata keys, same rows on disk. Only what the result carried is different."""

    @dy_asset(Orders, name="orders")
    def bare() -> pl.DataFrame:
        return clean_orders()

    @dy_asset(Orders, name="orders")
    def returning_a_result() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(value=clean_orders())

    from_bare = _metadata(_materialize(tmp_path / "bare", bare), _VALID)
    from_result = _metadata(
        _materialize(tmp_path / "result", returning_a_result), _VALID
    )

    assert set(from_bare) == set(from_result)
    assert_frame_equal(
        pl.read_parquet(tmp_path / "bare" / "orders.parquet"),
        pl.read_parquet(tmp_path / "result" / "orders.parquet"),
    )
