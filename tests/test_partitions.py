"""What a partitioned `@dy_asset` does, asserted rather than assumed.

Partitioning is forwarded, not designed around (#25). The risk was not wrong mechanics but that nobody looked. This file is the executable version of `docs/research/partitioned-assets.md`. Every claim that document makes is covered here, including the two that are Dagster's behaviour rather than this package's, so a release that changes either one fails a test instead of leaving the document wrong.

Static partitions throughout, except where a test says otherwise. Their keys name the frame each partition gets, which keeps the fixture readable. A date would obscure it.
"""

from pathlib import Path

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dagster_dataframely import dy_asset
from tests.scenario import (
    Orders,
    clean_orders,
    mixed_orders,
    storage,
    wrong_dtype_orders,
)

_PARTITIONS = dg.StaticPartitionsDefinition(["clean", "mixed", "wrong"])
_FRAMES = {"clean": clean_orders, "mixed": mixed_orders, "wrong": wrong_dtype_orders}

_GOOD_KEY = dg.AssetKey(["orders"])
_QUARANTINE_KEY = dg.AssetKey(["orders_quarantine"])
_AMOUNT_MIN = dg.AssetCheckKey(_GOOD_KEY, "dy_rule__amount__min")


@dy_asset(Orders, name="orders", quarantine=True, partitions_def=_PARTITIONS)
def _orders() -> pl.DataFrame:
    # Reached through `.get()` rather than a `context` parameter, which this decorated function is free to declare (ADR-0002). Both work inside a run. Covering the accessor here keeps a Dagster release that broke it visible.
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
        resources=storage(tmp_path),
        raise_on_error=raise_on_error,
    )


def _evaluations(
    result: dg.ExecuteInProcessResult,
) -> dict[str, dg.AssetCheckEvaluation]:
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


def _partitions(result: dg.ExecuteInProcessResult) -> dict[dg.AssetKey, str | None]:
    """The partition each materialization was written under, keyed by asset."""
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
def test_the_asset_carries_its_partitioning():
    assert {spec.key: spec.partitions_def for spec in _orders.specs} == {
        _GOOD_KEY: _PARTITIONS
    }


# --- validation runs per partition, on that partition's frame ---
def test_each_partition_materializes_its_own_frame(tmp_path: Path):
    assert _materialize(tmp_path, "clean").success
    assert _materialize(tmp_path, "mixed").success

    assert_frame_equal(
        pl.read_parquet(tmp_path / "orders" / "clean.parquet"), clean_orders()
    )
    assert pl.read_parquet(tmp_path / "orders" / "mixed.parquet").height == 3


def test_the_quarantine_lands_under_the_partition_that_produced_it(tmp_path: Path):
    """The borrowed output context carries the step's own partition key, so a backfill of one partition rewrites one quarantine and leaves the others alone."""
    result = _materialize(tmp_path, "mixed")

    assert _partitions(result) == {_GOOD_KEY: "mixed"}
    assert pl.read_parquet(tmp_path / "orders_quarantine" / "mixed.parquet").height == 3


def test_a_clean_partition_skips_the_quarantine(tmp_path: Path):
    """Same rule as unpartitioned, so an empty quarantine partition means something."""
    result = _materialize(tmp_path, "clean")

    assert set(_partitions(result)) == {_GOOD_KEY}
    assert not (tmp_path / "orders_quarantine").exists()


def test_row_count_is_the_partitions_valid_count(tmp_path: Path):
    """The partition's own count, not the asset's, so `dg.build_metadata_bounds_checks` trends a partition against itself."""
    _materialize(tmp_path, "clean")
    counts = _row_counts(_materialize(tmp_path, "mixed"))

    assert counts == {_GOOD_KEY: 3}


# --- a partition whose frame drifts ---
def test_a_drifting_partition_aborts_at_the_column_schema_check_before_any_row_check_reports(
    tmp_path: Path,
):
    result = _materialize(tmp_path, "wrong", raise_on_error=False)
    evaluations = _evaluations(result)

    assert not result.success
    assert set(evaluations) == {"dy_schema__columns"}
    assert not evaluations["dy_schema__columns"].passed
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
        ("wrong", {"dy_schema__columns"}),
    ],
)
def test_every_partition_reports_its_own_checks_and_names_no_partition(
    tmp_path: Path, partition_key: str, failing: set[str]
):
    """Each partition's own frame decides its checks, and none of them says which partition that was. This is the finding the ticket records. Dagster stamps a partition onto an evaluation only when the check's own spec carries a `partitions_def`, a preview API this package does not use, so every rule reports against the asset."""
    result = _materialize(tmp_path, partition_key, raise_on_error=False)
    evaluations = _evaluations(result)

    assert {name for name, e in evaluations.items() if not e.passed} == failing
    assert all(e.partition is None for e in evaluations.values())


def test_a_backfill_appends_every_partition_to_one_check_history(tmp_path: Path):
    """A backfill is one run per partition, and every run appends to the same per-check history. The failing partition ran first here, so the catalog's latest word on `amount|min` is the clean partition's. A per-partition failure is legible only by walking the history."""
    with dg.DagsterInstance.ephemeral() as instance:
        _materialize(tmp_path, "mixed", instance=instance)
        _materialize(tmp_path, "clean", instance=instance)

        # Private upstream storage: the only route to what the catalog would show without a webserver. Newest first.
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
    """The one route back. Dagster scopes `target_materialization_data` to the step's partition even when the check is unpartitioned, so a history row can be attributed by hand, after the fact, through the storage id it points at."""
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
    """Dagster's behaviour, not this package's. It makes the finding above worse on dates than on the static partitions the rest of this file uses.

    A time-window run carries a partitions subset, so the check's planned row is stamped with the partition. The evaluation arrives with none. The update that would close the row matches on partition, so it misses. The planned row stays for good, and the result is inserted as a second, partition-less row. The catalog then holds one never-executed row per partition beside a history that names none of them.
    """
    daily = dg.DailyPartitionsDefinition(start_date="2026-01-01")

    @dy_asset(Orders, name="daily_orders", partitions_def=daily)
    def daily_orders() -> pl.DataFrame:
        return clean_orders()

    check_key = dg.AssetCheckKey(dg.AssetKey(["daily_orders"]), "dy_rule__amount__min")

    with dg.DagsterInstance.ephemeral() as instance:
        dg.materialize(
            [daily_orders],
            partition_key="2026-01-01",
            instance=instance,
            resources=storage(tmp_path),
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
    """`backfill_policy` forwards like every other `dg.asset` parameter, but `dg.BackfillPolicy.single_run()` cannot reach storage: `UPathIOManager` resolves one path per output and refuses a range. The refusal is upstream's, names the fix, and arrives on the first run rather than after a wrong write. So the decorator does not reject the policy; it cannot know which manager will run."""

    @dy_asset(
        Orders,
        name="ranged",
        partitions_def=_PARTITIONS,
        backfill_policy=dg.BackfillPolicy.single_run(),
    )
    def ranged() -> pl.DataFrame:
        return clean_orders()

    job = dg.Definitions(
        assets=[ranged],
        resources=storage(tmp_path),
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


# --- a partition with no source data ---
# The motivating case (#95): a monthly x distributor grid where one distributor left the
# marketplace. Its historical cells hold real data and must stay. Its recent cells have no file
# and never will, which is neither a failure nor an empty report.
_GRID = dg.MultiPartitionsDefinition(
    {
        "month": dg.StaticPartitionsDefinition(["2026-01", "2026-02"]),
        "distributor": dg.StaticPartitionsDefinition(["trading", "departed"]),
    }
)
_DEPARTED = dg.MultiPartitionKey({"month": "2026-02", "distributor": "departed"})
_REPORTS_KEY = dg.AssetKey(["reports"])


@dy_asset(Orders, name="reports", partitions_def=_GRID)
def _reports() -> pl.DataFrame | None:
    keys = dg.AssetExecutionContext.get().partition_key.keys_by_dimension
    if keys == {"month": "2026-02", "distributor": "departed"}:
        return None
    return clean_orders()


def _materialize_cell(
    tmp_path: Path,
    partition_key: dg.MultiPartitionKey,
    *,
    instance: dg.DagsterInstance | None = None,
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [_reports],
        partition_key=partition_key,
        instance=instance,
        resources=storage(tmp_path),
        raise_on_error=False,
    )


def test_a_partition_with_no_source_data_stays_unmaterialized(tmp_path: Path):
    """Writing zero rows would read as an empty report arriving, and failing the run would read as a broken pipeline. Neither happened, so the cell gets no materialization and the run still succeeds."""
    result = _materialize_cell(tmp_path, _DEPARTED)

    assert result.success
    assert _partitions(result) == {}
    assert not list(tmp_path.rglob("*.parquet"))


def test_a_skipped_partition_leaves_every_other_cell_alone(tmp_path: Path):
    """The reason the skip is per-partition. A departed distributor still owns its history, so skipping this month's cell cannot touch last month's file."""
    kept = dg.MultiPartitionKey({"month": "2026-01", "distributor": "departed"})

    assert _materialize_cell(tmp_path, kept).success
    assert _materialize_cell(tmp_path, _DEPARTED).success

    written = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*.parquet"))

    assert written == ["reports/departed/2026-01.parquet"]


def test_a_skipped_partition_still_reports_its_checks(tmp_path: Path):
    """A step that leaves a check spec unanswered fails outright, so the skip rests on this assertion."""
    result = _materialize_cell(tmp_path, _DEPARTED)
    evaluations = _evaluations(result)

    assert set(evaluations) == {spec.name for spec in _reports.check_specs}
    assert all(e.passed for e in evaluations.values())


def test_a_skip_does_not_stamp_a_later_check_onto_an_earlier_partitions_table(
    tmp_path: Path,
):
    """`test_a_history_row_is_traceable_to_its_partition_through_the_materialization` shows a check's only route back to a partition is `target_materialization_data`. A skipped run writes no materialization, so that route is empty; it must not point at the last cell that had data."""
    with dg.DagsterInstance.ephemeral() as instance:
        traded = dg.MultiPartitionKey({"month": "2026-01", "distributor": "trading"})
        _materialize_cell(tmp_path, traded, instance=instance)
        _materialize_cell(tmp_path, _DEPARTED, instance=instance)

        latest = instance.event_log_storage.get_asset_check_execution_history(
            check_key=dg.AssetCheckKey(_REPORTS_KEY, "dy_rule__amount__min"), limit=1
        )[0].evaluation

        assert isinstance(latest, dg.AssetCheckEvaluation)
        assert latest.passed
        assert latest.target_materialization_data is None
