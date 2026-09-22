"""Tests for partitioned assets, covering every claim in `docs/pre-1.0.md`.

Some tests pin Dagster's own behaviour, so a Dagster release that changes it fails a test instead of leaving the document wrong.
Most tests use static partitions, whose keys name the frame the asset returns for each partition.
"""

from pathlib import Path

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import dagster_dataframely as dd
from tests.scenario import (
    Orders,
    check_evaluations,
    clean_orders,
    materializations,
    materialize,
    mixed_orders,
    storage,
    wrong_dtype_orders,
)

_PARTITIONS = dg.StaticPartitionsDefinition(["clean", "mixed", "wrong"])
_FRAMES = {"clean": clean_orders, "mixed": mixed_orders, "wrong": wrong_dtype_orders}

_GOOD_KEY = dg.AssetKey(["orders"])
_QUARANTINE_KEY = dg.AssetKey(["orders_quarantine"])
_AMOUNT_MIN = dg.AssetCheckKey(_GOOD_KEY, "dy_rule__amount__min")


@dd.asset(Orders, name="orders", quarantine=True, partitions_def=_PARTITIONS)
def _orders() -> pl.DataFrame:
    # `.get()` instead of a `context` parameter, so the suite also covers the accessor.
    return _FRAMES[dg.AssetExecutionContext.get().partition_key]()


def _partitions(result: dg.ExecuteInProcessResult) -> dict[dg.AssetKey, str | None]:
    """Return the partition of each materialization, keyed by asset key."""
    return {key: m.partition for key, m in materializations(result).items()}


def test_the_asset_carries_its_partitioning():
    """The only spec is the asset's, so the quarantine has no partitioning of its own."""
    assert {spec.key: spec.partitions_def for spec in _orders.specs} == {
        _GOOD_KEY: _PARTITIONS
    }


def test_each_partition_materializes_its_own_frame(tmp_path: Path):
    assert materialize(tmp_path, _orders, partition_key="clean").success
    assert materialize(tmp_path, _orders, partition_key="mixed").success

    assert_frame_equal(
        pl.read_parquet(tmp_path / "orders" / "clean.parquet"), clean_orders()
    )
    assert pl.read_parquet(tmp_path / "orders" / "mixed.parquet").height == 3


def test_the_quarantine_is_written_under_the_partition_that_produced_it(tmp_path: Path):
    result = materialize(tmp_path, _orders, partition_key="mixed")

    assert _partitions(result) == {_GOOD_KEY: "mixed"}
    assert pl.read_parquet(tmp_path / "orders_quarantine" / "mixed.parquet").height == 3


def test_a_clean_partition_skips_the_quarantine(tmp_path: Path):
    result = materialize(tmp_path, _orders, partition_key="clean")

    assert set(_partitions(result)) == {_GOOD_KEY}
    assert not (tmp_path / "orders_quarantine").exists()


# No row-count test: `dagster-polars` overwrites `dagster/row_count`, so `test_asset_runtime.py` tests it.


def test_a_drifting_partition_aborts_at_the_column_schema_check_before_any_row_check_reports(
    tmp_path: Path,
):
    result = materialize(tmp_path, _orders, partition_key="wrong", raise_on_error=False)
    evaluations = check_evaluations(result)

    assert not result.success
    assert set(evaluations) == {"dy_schema__columns"}
    assert not evaluations["dy_schema__columns"].passed
    assert not result.get_asset_materialization_events()


def test_a_drifting_partition_leaves_every_other_partition_alone(tmp_path: Path):
    materialize(tmp_path, _orders, partition_key="clean")
    materialize(tmp_path, _orders, partition_key="wrong", raise_on_error=False)

    assert [path.name for path in sorted((tmp_path / "orders").iterdir())] == [
        "clean.parquet"
    ]
    assert_frame_equal(
        pl.read_parquet(tmp_path / "orders" / "clean.parquet"), clean_orders()
    )


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
    """Dagster records a partition on a check evaluation only when the check spec has a `partitions_def`, a preview API this package does not use."""
    result = materialize(
        tmp_path, _orders, partition_key=partition_key, raise_on_error=False
    )
    evaluations = check_evaluations(result)

    assert {name for name, e in evaluations.items() if not e.passed} == failing
    assert all(e.partition is None for e in evaluations.values())


def test_a_backfill_appends_every_partition_to_one_check_history(tmp_path: Path):
    with dg.DagsterInstance.ephemeral() as instance:
        materialize(tmp_path, _orders, partition_key="mixed", instance=instance)
        materialize(tmp_path, _orders, partition_key="clean", instance=instance)

        # Private Dagster API: the only way to read check history without a webserver, newest first.
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
    """Dagster sets `target_materialization_data` to the step's partitioned materialization, even for an unpartitioned check."""
    with dg.DagsterInstance.ephemeral() as instance:
        materialize(tmp_path, _orders, partition_key="mixed", instance=instance)
        materialize(tmp_path, _orders, partition_key="clean", instance=instance)

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


def test_a_time_window_partition_leaves_the_planned_check_row_unresolved(
    tmp_path: Path,
):
    """Dagster writes the result as a second row, because the planned row has the partition and the result has none."""
    daily = dg.DailyPartitionsDefinition(start_date="2026-01-01")

    @dd.asset(Orders, name="daily_orders", partitions_def=daily)
    def daily_orders() -> pl.DataFrame:
        return clean_orders()

    check_key = dg.AssetCheckKey(dg.AssetKey(["daily_orders"]), "dy_rule__amount__min")

    with dg.DagsterInstance.ephemeral() as instance:
        materialize(
            tmp_path, daily_orders, partition_key="2026-01-01", instance=instance
        )
        history = instance.event_log_storage.get_asset_check_execution_history(
            check_key=check_key, limit=10
        )

    assert [record.partition for record in history] == [None, "2026-01-01"]
    assert isinstance(history[0].evaluation, dg.AssetCheckEvaluation)
    assert not isinstance(history[1].evaluation, dg.AssetCheckEvaluation)


def test_a_single_run_backfill_is_rejected_by_the_io_manager(tmp_path: Path):
    """`UPathIOManager` raises on a partition range, before it writes anything."""

    @dd.asset(
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


# A distributor that left: `departed` has source data for 2026-01 and none for 2026-02.
_MULTI_PARTITIONS = dg.MultiPartitionsDefinition({
    "month": dg.StaticPartitionsDefinition(["2026-01", "2026-02"]),
    "distributor": dg.StaticPartitionsDefinition(["trading", "departed"]),
})
_DEPARTED = dg.MultiPartitionKey({"month": "2026-02", "distributor": "departed"})
_REPORTS_KEY = dg.AssetKey(["reports"])


@dd.asset(Orders, name="reports", partitions_def=_MULTI_PARTITIONS)
def _reports() -> pl.DataFrame | None:
    keys = dg.AssetExecutionContext.get().partition_key.keys_by_dimension
    if keys == {"month": "2026-02", "distributor": "departed"}:
        return None
    return clean_orders()


def test_a_partition_with_no_source_data_stays_unmaterialized(tmp_path: Path):
    result = materialize(
        tmp_path, _reports, partition_key=_DEPARTED, raise_on_error=False
    )

    assert result.success
    assert _partitions(result) == {}
    assert not list(tmp_path.rglob("*.parquet"))


def test_a_skipped_partition_leaves_every_other_partition_alone(tmp_path: Path):
    kept = dg.MultiPartitionKey({"month": "2026-01", "distributor": "departed"})

    assert materialize(
        tmp_path, _reports, partition_key=kept, raise_on_error=False
    ).success
    assert materialize(
        tmp_path, _reports, partition_key=_DEPARTED, raise_on_error=False
    ).success

    written = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*.parquet"))

    assert written == ["reports/departed/2026-01.parquet"]


def test_a_skipped_partition_still_reports_its_checks(tmp_path: Path):
    """Every check has a result after a skip, because Dagster fails a step that leaves a check spec without one."""
    result = materialize(
        tmp_path, _reports, partition_key=_DEPARTED, raise_on_error=False
    )
    evaluations = check_evaluations(result)

    assert set(evaluations) == {spec.name for spec in _reports.check_specs}
    assert all(e.passed for e in evaluations.values())


def test_a_skip_does_not_stamp_a_later_check_onto_an_earlier_partitions_table(
    tmp_path: Path,
):
    """After a skip, `target_materialization_data` is `None`, not the earlier partition's materialization."""
    with dg.DagsterInstance.ephemeral() as instance:
        traded = dg.MultiPartitionKey({"month": "2026-01", "distributor": "trading"})
        materialize(
            tmp_path,
            _reports,
            partition_key=traded,
            instance=instance,
            raise_on_error=False,
        )
        materialize(
            tmp_path,
            _reports,
            partition_key=_DEPARTED,
            instance=instance,
            raise_on_error=False,
        )

        latest = instance.event_log_storage.get_asset_check_execution_history(
            check_key=dg.AssetCheckKey(_REPORTS_KEY, "dy_rule__amount__min"), limit=1
        )[0].evaluation

        assert isinstance(latest, dg.AssetCheckEvaluation)
        assert latest.passed
        assert latest.target_materialization_data is None
