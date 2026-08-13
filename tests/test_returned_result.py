"""A transform that returns a `dg.MaterializeResult` instead of a bare frame.

`@dg.asset` accepts one, and it is what Dagster's own docs teach for attaching metadata, so refusing it cost parity with the decorator this one is modelled on (#77). The result's `value` is the frame to validate; its metadata, data version and tags fold into the materialization the package yields for the valid out.

Every shape is asserted twice where both can see it, once by calling and once through `dg.materialize`. Metadata survives either route, but a data version and tags are event-level: a call hands back the `dg.MaterializeResult` carrying them and a run is where they become event tags, so both are worth pinning.
"""

import re
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dagster_dataframely import DataframelyParquetIOManager, dataframely_asset
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
)

_VALID = dg.AssetKey(["orders"])
_QUARANTINE = dg.AssetKey(["orders_quarantine"])
_Yielded = list[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]


def _call(asset: dg.AssetsDefinition) -> _Yielded:
    """Calls the asset and drains what comes back."""
    return list(asset())  # pyrefly: ignore[bad-argument-type]


def _results(events: _Yielded) -> dict[dg.AssetKey, dg.MaterializeResult[pl.DataFrame]]:
    """The materialization each out produced, keyed by asset."""
    return {
        event.asset_key: event
        for event in events
        if isinstance(event, dg.MaterializeResult) and event.asset_key is not None
    }


def _materialize(tmp_path: Path, asset: dg.AssetsDefinition):
    return dg.materialize(
        [asset],
        resources={"io_manager": DataframelyParquetIOManager(base_dir=str(tmp_path))},
    )


def _materializations(
    result: dg.ExecuteInProcessResult,
) -> dict[dg.AssetKey, dg.AssetMaterialization]:
    """Every materialization the run emitted, whole, because tags matter here as well as metadata."""
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

    @dataframely_asset(schema=Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=clean_orders(), metadata={"source": "stripe", "batch": 7}
        )

    metadata = _results(_call(orders))[_VALID].metadata or {}

    assert metadata["source"] == "stripe"
    assert metadata["batch"] == 7


def test_a_run_records_the_returned_metadata_beside_the_packages_own(tmp_path: Path):
    """Beside, not instead: the row count the package emits is still there."""

    @dataframely_asset(schema=Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(value=clean_orders(), metadata={"source": "stripe"})

    metadata = _metadata(_materialize(tmp_path, orders), _VALID)

    assert metadata["source"].value == "stripe"
    assert metadata["dagster/row_count"].value == 3


def test_the_packages_own_key_wins_a_collision(tmp_path: Path):
    """The precedence the decorator already uses for definition metadata, applied to the returned result. `dagster/row_count` specifically: Dagster reads it, and a transform that overwrote it would make the catalog state a count nothing counted."""

    @dataframely_asset(schema=Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=clean_orders(), metadata={"dagster/row_count": 999}
        )

    called = _results(_call(orders))[_VALID].metadata or {}
    ran = _metadata(_materialize(tmp_path, orders), _VALID)

    assert called["dagster/row_count"] == 3
    assert ran["dagster/row_count"].value == 3


def test_a_returned_data_version_and_tags_reach_the_valid_result():
    """Neither has any other route. `context.set_data_version` carries no `@public`, and the context exposes nothing at all for a materialization's tags."""

    @dataframely_asset(schema=Orders, name="orders")
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
    """Where they end up is Dagster's business, and both land as tags on the materialization event."""

    @dataframely_asset(schema=Orders, name="orders")
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
    """The fold sits over what `process` yields, so the staging phase in between changes nothing about it."""

    @dataframely_asset(schema=Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.LazyFrame]:
        return dg.MaterializeResult(value=clean_orders().lazy(), metadata={"m": 1})

    result = _results(_call(orders))[_VALID]

    assert (result.metadata or {})["m"] == 1
    assert_frame_equal(result.value, clean_orders())


# --- what the quarantine keeps ---
def test_the_quarantine_keeps_the_tags_dagster_gives_it():
    """The valid out only, on the reasoning `_returns.fold` carries."""

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=cooccurring_orders(),
            metadata={"source": "stripe"},
            data_version=dg.DataVersion("v1"),
            tags={"run/flavour": "backfill"},
        )

    quarantined = _results(_call(orders))[_QUARANTINE]

    assert "source" not in (quarantined.metadata or {})
    assert quarantined.data_version is None
    assert quarantined.tags is None


def test_a_run_leaves_the_quarantines_event_tags_alone(tmp_path: Path):
    """The one run that writes both, so the two events can be compared side by side.

    The quarantine still carries a `dagster/data_version`, because Dagster hashes one for every materialization it did not get one for. What it does not carry is the returned one, which is exactly what `dagster/data_version_is_user_provided` distinguishes.
    """

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=cooccurring_orders(),
            data_version=dg.DataVersion("v1"),
            tags={"run/flavour": "backfill"},
        )

    materializations = _materializations(_materialize(tmp_path, orders))
    valid = materializations[_VALID].tags or {}
    quarantined = materializations[_QUARANTINE].tags or {}

    assert valid["dagster/data_version"] == "v1"
    assert valid["dagster/data_version_is_user_provided"] == "true"
    assert valid["run/flavour"] == "backfill"
    assert quarantined["dagster/data_version"] != "v1"
    assert "dagster/data_version_is_user_provided" not in quarantined
    assert "run/flavour" not in quarantined


# --- what stays refused ---
# Every refusal is asserted twice, once by calling and once through a run. The unwrap runs inside
# the wrapper's generator, so nothing happens until something advances it: a refusal that only
# surfaced on a direct call would let a run write a table the package never validated.
# Declared as bare returns rather than as decorated assets, because the two tests below need the
# same six shapes and a decorated asset cannot be re-declared per test without a name collision.
_REFUSALS = [
    pytest.param(
        lambda: dg.MaterializeResult(metadata={"source": "stripe"}),
        MaterializeResultValueError,
        # The message has to name the alternative, because attaching metadata is exactly what somebody writing this was trying to do.
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
        # The legacy spelling, which Dagster's own docs steer away from in greenfield code. It reaches the frame guard, where everything unreadable ends up.
        lambda: dg.Output(clean_orders(), metadata={"source": "stripe"}),
        dg.DagsterInvariantViolationError,
        "'orders' returned a Output",
        id="dg.Output",
    ),
    pytest.param(
        lambda: None,
        dg.DagsterInvariantViolationError,
        "'orders' returned a NoneType",
        id="nothing at all",
    ),
]


def _refusing(transform: Callable[[], object]) -> dg.AssetsDefinition:
    """Declares the asset under the one name every message above expects.

    The transforms are annotated nowhere, because there is nothing to annotate them as: every one of them returns what the decorator's own type says it cannot.
    """
    return dataframely_asset(schema=Orders, name="orders")(transform)  # pyrefly: ignore[bad-argument-type]


@pytest.mark.parametrize(("transform", "error", "says"), _REFUSALS)
def test_a_refused_return_names_what_is_wrong(
    transform: Callable[[], object], error: type[Exception], says: str
):
    with pytest.raises(error, match=re.escape(says)):
        _call(_refusing(transform))


@pytest.mark.parametrize(("transform", "error", "says"), _REFUSALS)
def test_a_refused_return_fails_the_run_and_writes_nothing(
    tmp_path: Path,
    transform: Callable[[], object],
    error: type[Exception],
    says: str,
):
    with pytest.raises(error, match=re.escape(says)):
        _materialize(tmp_path, _refusing(transform))

    assert not list(tmp_path.rglob("*.parquet"))


# --- an exit with no valid materialization to fold onto ---
def test_an_exit_that_skips_the_valid_out_still_raises_its_own_error():
    """Nothing survived, so the fold has only the quarantine's materialization and the checks to pass through. The error `process` raises has to reach the caller unchanged rather than being swallowed by the stage wrapping it."""

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(
            value=hopeless_orders(), metadata={"source": "stripe"}
        )

    with pytest.raises(NothingSurvivedError):
        _call(orders)


def test_an_abort_with_no_quarantine_still_raises_its_own_error():
    """The exit that yields no materialization at all, only checks. The fold has nothing to fold onto and must not invent one."""

    @dataframely_asset(schema=Orders, name="orders")
    def orders() -> dg.MaterializeResult[pl.DataFrame]:
        return dg.MaterializeResult(value=mixed_orders(), metadata={"source": "stripe"})

    with pytest.raises(ValidationAbortError):
        _call(orders)


def test_the_frame_guard_names_every_route_out():
    """Giving up the schema used to be the whole of the advice, which is wrong for anyone who wanted metadata on a validated table. It is now the last of three, and right for the one reader it is left for: an asset that writes its own storage and never holds a frame."""
    with pytest.raises(dg.DagsterInvariantViolationError) as raised:
        _call(_refusing(lambda: None))
    message = str(raised.value)

    assert "polars DataFrame or LazyFrame" in message
    assert "dg.MaterializeResult" in message
    assert "plain `@dg.asset`" in message
    assert "schema_metadata" in message


# --- what a bare frame still does ---
def test_a_bare_frame_carries_no_data_version_and_no_tags():
    """The guarantee the fold rests on: an asset that returns a frame produces exactly what it produced before #77."""

    @dataframely_asset(schema=Orders, name="orders")
    def orders() -> pl.DataFrame:
        return clean_orders()

    result = _results(_call(orders))[_VALID]

    assert result.data_version is None
    assert result.tags is None


def test_a_bare_frame_and_a_returned_result_agree_on_everything_else(tmp_path: Path):
    """Same metadata keys, same rows on disk. Only what the result carried is different."""

    @dataframely_asset(schema=Orders, name="orders")
    def bare() -> pl.DataFrame:
        return clean_orders()

    @dataframely_asset(schema=Orders, name="orders")
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
