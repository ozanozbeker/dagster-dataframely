"""What a partitioned `@dataframely_asset` actually does, asserted rather than assumed.

Partitioning is forwarded, not designed around (#25), so the risk was never that the mechanics are wrong; it is that nobody looked. This file is the executable half of `docs/research/partitioned-assets.md`. Every claim that document makes is pinned here, including the two that are Dagster's behaviour rather than this package's, so a release that changes either one fails a test instead of leaving the document quietly wrong.

Static partitions throughout except where a test says otherwise: they name the frame each partition gets, which makes the fixture readable, and a date would only obscure it.
"""

import inspect
from pathlib import Path

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dagster_dataframely import DataframelyParquetIOManager, dataframely_asset
from tests.scenario import Orders, clean_orders, mixed_orders, wrong_dtype_orders

_PARTITIONS = dg.StaticPartitionsDefinition(["clean", "mixed", "wrong"])
_FRAMES = {"clean": clean_orders, "mixed": mixed_orders, "wrong": wrong_dtype_orders}

_GOOD_KEY = dg.AssetKey(["orders"])
_QUARANTINE_KEY = dg.AssetKey(["orders_quarantine"])
_AMOUNT_MIN = dg.AssetCheckKey(_GOOD_KEY, "dy_rule__amount__min")


@dataframely_asset(
    schema=Orders, name="orders", quarantine=dg.AssetOut(), partitions_def=_PARTITIONS
)
def _orders() -> pl.DataFrame:
    # The transform takes no `context` parameter, so a partitioned one reaches its key the way the wrapper reaches the context.
    return _FRAMES[dg.AssetExecutionContext.get().partition_key]()


def _materialize(
    tmp_path: Path,
    partition_key: str,
    *,
    instance: dg.DagsterInstance | None = None,
    raise_on_error: bool = True,
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [_orders],
        partition_key=partition_key,
        instance=instance,
        resources={"io_manager": DataframelyParquetIOManager(base_dir=str(tmp_path))},
        raise_on_error=raise_on_error,
    )


def _evaluations(
    result: dg.ExecuteInProcessResult,
) -> dict[str, dg.AssetCheckEvaluation]:
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


def _partitions(result: dg.ExecuteInProcessResult) -> dict[dg.AssetKey, str | None]:
    """The partition each materialization landed under, keyed by asset."""
    return {
        event.asset_key: event.step_materialization_data.materialization.partition
        for event in result.get_asset_materialization_events()
        if event.asset_key is not None
    }


def _row_counts(result: dg.ExecuteInProcessResult) -> dict[dg.AssetKey, object]:
    return {
        event.asset_key: dict(event.step_materialization_data.materialization.metadata)[
            "dagster/row_count"
        ].value
        for event in result.get_asset_materialization_events()
        if event.asset_key is not None
    }


# --- the quarantine cannot escape its asset's partitioning ---
def test_both_outs_carry_the_assets_partitioning():
    assert {spec.key: spec.partitions_def for spec in _orders.specs} == {
        _GOOD_KEY: _PARTITIONS,
        _QUARANTINE_KEY: _PARTITIONS,
    }


def test_an_out_cannot_declare_partitioning_of_its_own():
    """Structural rather than enforced: `partitions_def` lives on the `multi_asset`, and `dg.AssetOut` has no such parameter, so there is no spelling in which a quarantine escapes its asset's partitioning and becomes an unpartitioned pile of bad rows."""
    assert "partitions_def" not in inspect.signature(dg.AssetOut.__init__).parameters


# --- the state machine runs per partition, on that partition's frame ---
def test_each_partition_materializes_its_own_frame(tmp_path: Path):
    assert _materialize(tmp_path, "clean").success
    assert _materialize(tmp_path, "mixed").success

    assert_frame_equal(
        pl.read_parquet(tmp_path / "orders" / "clean.parquet"), clean_orders()
    )
    assert pl.read_parquet(tmp_path / "orders" / "mixed.parquet").height == 3


def test_the_quarantine_lands_under_the_partition_that_produced_it(tmp_path: Path):
    result = _materialize(tmp_path, "mixed")

    assert _partitions(result) == {_GOOD_KEY: "mixed", _QUARANTINE_KEY: "mixed"}
    assert pl.read_parquet(tmp_path / "orders_quarantine" / "mixed.parquet").height == 3


def test_a_clean_partition_skips_the_quarantine(tmp_path: Path):
    """Same rule as unpartitioned, and it is what makes an empty quarantine partition mean something."""
    result = _materialize(tmp_path, "clean")

    assert set(_partitions(result)) == {_GOOD_KEY}
    assert not (tmp_path / "orders_quarantine").exists()


def test_row_count_is_the_partitions_good_count(tmp_path: Path):
    """The partition's own count, not the asset's: `dg.build_metadata_bounds_checks` then trends a partition against itself."""
    _materialize(tmp_path, "clean")
    counts = _row_counts(_materialize(tmp_path, "mixed"))

    assert counts == {_GOOD_KEY: 3, _QUARANTINE_KEY: 3}


# --- a partition whose frame drifts ---
def test_a_drifting_partition_aborts_at_the_gate_before_any_row_check_reports(
    tmp_path: Path,
):
    result = _materialize(tmp_path, "wrong", raise_on_error=False)
    evaluations = _evaluations(result)

    assert not result.success
    assert set(evaluations) == {"dy_schema__dtypes"}
    assert not evaluations["dy_schema__dtypes"].passed
    assert not result.get_asset_materialization_events()


def test_a_drifting_partition_leaves_every_other_partition_alone(tmp_path: Path):
    """One partition is one run, so a pipeline defect in today's data cannot reach yesterday's table."""
    _materialize(tmp_path, "clean")
    _materialize(tmp_path, "wrong", raise_on_error=False)

    assert [path.name for path in sorted((tmp_path / "orders").iterdir())] == [
        "clean.parquet"
    ]
    assert_frame_equal(
        pl.read_parquet(tmp_path / "orders" / "clean.parquet"), clean_orders()
    )


# --- what Dagster does with per-partition check evaluations ---
@pytest.mark.parametrize(
    ("partition_key", "failing"),
    [
        ("clean", set()),
        (
            "mixed",
            {
                "dy_rule__amount__min",
                "dy_rule__email__check__lowercase",
                "dy_rule__paid_orders_have_amount",
            },
        ),
        ("wrong", {"dy_schema__dtypes"}),
    ],
)
def test_every_partition_reports_its_own_checks_and_names_no_partition(
    tmp_path: Path, partition_key: str, failing: set[str]
):
    """Each partition's checks are decided by its own frame, and none of them says which partition that was. That is the finding this ticket exists to record: Dagster stamps a partition onto an evaluation only when the check's own spec carries a `partitions_def`, which is a preview API this package does not use, so every rule reports against the asset instead."""
    result = _materialize(tmp_path, partition_key, raise_on_error=False)
    evaluations = _evaluations(result)

    assert {name for name, e in evaluations.items() if not e.passed} == failing
    assert all(e.partition is None for e in evaluations.values())


def test_check_history_across_a_backfill_is_one_timeline_per_check(tmp_path: Path):
    """A backfill is one run per partition, and every run appends to the same per-check timeline. The failing partition ran first here, so the catalog's latest word on `amount|min` is the clean partition's: a partition-scoped failure is legible only by walking the history."""
    with dg.DagsterInstance.ephemeral() as instance:
        _materialize(tmp_path, "mixed", instance=instance)
        _materialize(tmp_path, "clean", instance=instance)

        # Private upstream storage, and the only seam that answers "what would the catalog show" without a webserver. Newest first.
        history = instance.event_log_storage.get_asset_check_execution_history(
            check_key=_AMOUNT_MIN, limit=10
        )
        evaluations = [
            record.evaluation
            for record in history
            if isinstance(record.evaluation, dg.AssetCheckEvaluation)
        ]

        assert [e.passed for e in evaluations] == [True, False]
        assert all(e.partition is None for e in evaluations)


def test_a_history_row_is_traceable_to_its_partition_through_the_materialization(
    tmp_path: Path,
):
    """The one route back. Dagster scopes `target_materialization_data` to the step's partition even when the check itself is unpartitioned, so a history row can be attributed after the fact, by hand, off the storage id it points at."""
    with dg.DagsterInstance.ephemeral() as instance:
        _materialize(tmp_path, "mixed", instance=instance)
        _materialize(tmp_path, "clean", instance=instance)

        latest = instance.event_log_storage.get_asset_check_execution_history(
            check_key=_AMOUNT_MIN, limit=1
        )[0].evaluation
        assert isinstance(latest, dg.AssetCheckEvaluation)
        assert latest.target_materialization_data is not None

        materializations = instance.fetch_materializations(
            dg.AssetRecordsFilter(asset_key=_GOOD_KEY), limit=10
        ).records
        partition_by_storage_id = {
            record.storage_id: record.partition_key for record in materializations
        }

        assert (
            partition_by_storage_id[latest.target_materialization_data.storage_id]
            == "clean"
        )


def test_a_time_window_partition_orphans_the_planned_check_row(tmp_path: Path):
    """Dagster's behaviour, not this package's, and the reason the finding above is worse on dates than on the static partitions the rest of this file uses.

    A time-window run carries a partitions subset, so the check's planned row is stamped with the partition. The evaluation arrives with none, and the update that would close the row matches on partition, so it misses: the planned row is left behind for good and the result is inserted as a second, partition-less row. The catalog therefore holds one never-executed row per partition alongside a timeline that names none of them.
    """
    daily = dg.DailyPartitionsDefinition(start_date="2026-01-01")

    @dataframely_asset(schema=Orders, name="daily_orders", partitions_def=daily)
    def daily_orders() -> pl.DataFrame:
        return clean_orders()

    check_key = dg.AssetCheckKey(dg.AssetKey(["daily_orders"]), "dy_rule__amount__min")

    with dg.DagsterInstance.ephemeral() as instance:
        dg.materialize(
            [daily_orders],
            partition_key="2026-01-01",
            instance=instance,
            resources={
                "io_manager": DataframelyParquetIOManager(base_dir=str(tmp_path))
            },
        )
        history = instance.event_log_storage.get_asset_check_execution_history(
            check_key=check_key, limit=10
        )

    assert [record.partition for record in history] == [None, "2026-01-01"]
    assert isinstance(history[0].evaluation, dg.AssetCheckEvaluation)
    # The row carrying the partition is the planned one, which now never resolves.
    assert not isinstance(history[1].evaluation, dg.AssetCheckEvaluation)


# --- backfill policy ---
def test_a_single_run_backfill_is_refused_by_the_io_manager(tmp_path: Path):
    """`backfill_policy` forwards like every other `multi_asset` parameter, but `dg.BackfillPolicy.single_run()` cannot reach storage: `UPathIOManager` resolves one path per output and refuses a range. The refusal is upstream's, it names the fix, and it arrives on the first run rather than after a wrong write, so the door leaves it alone rather than rejecting the policy it cannot know the manager for."""

    @dataframely_asset(
        schema=Orders,
        name="ranged",
        partitions_def=_PARTITIONS,
        backfill_policy=dg.BackfillPolicy.single_run(),
    )
    def ranged() -> pl.DataFrame:
        return clean_orders()

    job = dg.Definitions(
        assets=[ranged],
        resources={"io_manager": DataframelyParquetIOManager(base_dir=str(tmp_path))},
    ).resolve_implicit_global_asset_job_def()

    result = job.execute_in_process(
        tags={
            "dagster/asset_partition_range_start": "clean",
            "dagster/asset_partition_range_end": "mixed",
        },
        raise_on_error=False,
    )
    failure = result.failure_data_for_node("ranged")

    assert not result.success
    assert failure is not None
    assert "multiple partitions" in str(failure.error)
    assert "multi-run backfill policy" in str(failure.error)
    assert not list(tmp_path.rglob("*.parquet"))
