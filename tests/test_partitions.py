"""Tests for partitioned assets, covering every claim in `docs/pre-1.0.md`.

Some tests pin Dagster's own behaviour, so a Dagster release that changes it fails a test instead of leaving the document wrong.
Most tests use static partitions, whose keys name the frame the asset returns for each partition.
"""

from collections.abc import Callable
from pathlib import Path

import dagster as dg
import dataframely as dy
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import dagster_dataframely as dd
from dagster_dataframely.wiring import get_table_schema, stream_results
from tests.scenario import (
    Orders,
    check_evaluations,
    clean_orders,
    materializations,
    materialize,
    mixed_orders,
    storage,
    warehouse,
    wrong_dtype_orders,
)

_PARTITIONS = dg.StaticPartitionsDefinition(["clean", "mixed", "wrong"])
_FRAMES = {"clean": clean_orders, "mixed": mixed_orders, "wrong": wrong_dtype_orders}

_GOOD_KEY = dg.AssetKey(["orders"])
_ERROR_ROWS_KEY = dg.AssetKey(["orders__error_rows"])
_AMOUNT_MIN = dg.AssetCheckKey(_GOOD_KEY, "dy_rule__amount__min")


@dd.asset(Orders, name="orders", filter=True, partitions_def=_PARTITIONS)
def _orders() -> pl.DataFrame:
    # `.get()` instead of a `context` parameter, so the suite also covers the accessor.
    return _FRAMES[dg.AssetExecutionContext.get().partition_key]()


def _materialization(called: object) -> dg.MaterializeResult[pl.DataFrame]:
    """Return the materialization a call yielded."""
    materialization = dd.testing.DirectInvocationResult.from_invocation(
        called
    ).materialization
    assert materialization is not None
    return materialization


def _partitions(result: dg.ExecuteInProcessResult) -> dict[dg.AssetKey, str | None]:
    """Return the partition of each materialization, keyed by asset key."""
    return {key: m.partition for key, m in materializations(result).items()}


class _Days(dy.Schema):
    day = dy.String()
    amount = dy.Int64(min=0)


_DAILY_ERROR_ROWS_KEY = dg.AssetKey(["daily__error_rows"])


@dd.asset(
    _Days,
    name="daily",
    filter=True,
    partitions_def=dg.DailyPartitionsDefinition(start_date="2026-01-01"),
    backfill_policy=dg.BackfillPolicy.single_run(),
    metadata={"partition_expr": "day"},
)
def _daily(context: dg.AssetExecutionContext) -> pl.DataFrame:
    """Return one valid row and one row that fails `amount`'s minimum for each day in the run."""
    days = context.partition_keys
    return pl.DataFrame({"day": days * 2, "amount": [1] * len(days) + [-1] * len(days)})


class TestPartitionsDef:
    """The `partitions_def` on the asset spec and every check spec."""

    def test_the_asset_carries_its_partitioning(self):
        """The only spec is the asset's, so the error rows have no partitioning of their own."""
        assert {spec.key: spec.partitions_def for spec in _orders.specs} == {
            _GOOD_KEY: _PARTITIONS
        }

    def test_every_check_has_the_assets_partitioning(self):
        assert all(spec.partitions_def == _PARTITIONS for spec in _orders.check_specs)


class TestPartitionRuns:
    """Runs of a partitioned asset."""

    def test_each_partition_materializes_its_own_frame(self, tmp_path: Path):
        assert materialize(tmp_path, _orders, partition_key="clean").success
        assert materialize(tmp_path, _orders, partition_key="mixed").success

        assert_frame_equal(
            pl.read_parquet(tmp_path / "orders" / "clean.parquet"), clean_orders()
        )
        assert pl.read_parquet(tmp_path / "orders" / "mixed.parquet").height == 3

    def test_a_partitions_materialization_has_the_schemas_column_schema(
        self, tmp_path: Path
    ):
        result = materialize(tmp_path, _orders, partition_key="clean")
        metadata = materializations(result)[_GOOD_KEY].metadata

        assert metadata["dagster/column_schema"].value == get_table_schema(Orders)

    def test_the_error_rows_is_written_under_the_partition_that_produced_it(
        self, tmp_path: Path
    ):
        result = materialize(tmp_path, _orders, partition_key="mixed")

        assert _partitions(result) == {_GOOD_KEY: "mixed", _ERROR_ROWS_KEY: "mixed"}
        assert (
            pl.read_parquet(tmp_path / "orders__error_rows" / "mixed.parquet").height
            == 3
        )

    def test_a_clean_rerun_empties_only_its_own_partitions_error_rows(
        self, tmp_path: Path
    ):
        days = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])

        def orders(frame: Callable[[], pl.DataFrame]) -> dg.AssetsDefinition:
            @dd.asset(Orders, name="orders", filter=True, partitions_def=days)
            def orders() -> pl.DataFrame:
                return frame()

            return orders

        materialize(tmp_path, orders(mixed_orders), partition_key="2026-01-02")
        materialize(tmp_path, orders(mixed_orders), partition_key="2026-01-03")
        result = materialize(tmp_path, orders(clean_orders), partition_key="2026-01-03")
        filtering = tmp_path / "orders__error_rows"

        assert _partitions(result) == {
            _GOOD_KEY: "2026-01-03",
            _ERROR_ROWS_KEY: "2026-01-03",
        }
        assert pl.read_parquet(filtering / "2026-01-02.parquet").height == 3
        assert pl.read_parquet(filtering / "2026-01-03.parquet").height == 0

    def test_a_drifting_partition_aborts_at_the_column_schema_check_before_any_row_check_reports(
        self,
        tmp_path: Path,
    ):
        result = materialize(
            tmp_path, _orders, partition_key="wrong", raise_on_error=False
        )
        evaluations = check_evaluations(result)

        assert not result.success
        assert set(evaluations) == {"dy_schema__columns"}
        assert not evaluations["dy_schema__columns"].passed
        assert not result.get_asset_materialization_events()

    def test_a_drifting_partition_leaves_every_other_partition_alone(
        self, tmp_path: Path
    ):
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
    def test_every_partition_reports_its_own_checks_under_its_partition(
        self, tmp_path: Path, partition_key: str, failing: set[str]
    ):
        result = materialize(
            tmp_path, _orders, partition_key=partition_key, raise_on_error=False
        )
        evaluations = check_evaluations(result)

        assert {name for name, e in evaluations.items() if not e.passed} == failing
        assert all(e.partition == partition_key for e in evaluations.values())

    def test_a_clean_partition_run_last_keeps_the_dirty_partitions_failure(
        self,
        tmp_path: Path,
    ):
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

            assert [(e.partition, e.passed) for e in evaluations] == [
                ("clean", True),
                ("mixed", False),
            ]

    def test_a_time_window_partition_resolves_its_planned_check_row(
        self, tmp_path: Path
    ):
        """Dagster updates the planned row in place, because the planned row and the result have the same partition."""
        daily = dg.DailyPartitionsDefinition(start_date="2026-01-01")

        @dd.asset(Orders, name="daily_orders", partitions_def=daily)
        def daily_orders() -> pl.DataFrame:
            return clean_orders()

        check_key = dg.AssetCheckKey(
            dg.AssetKey(["daily_orders"]), "dy_rule__amount__min"
        )

        with dg.DagsterInstance.ephemeral() as instance:
            materialize(
                tmp_path, daily_orders, partition_key="2026-01-01", instance=instance
            )
            history = instance.event_log_storage.get_asset_check_execution_history(
                check_key=check_key, limit=10
            )

        assert [record.partition for record in history] == ["2026-01-01"]
        assert isinstance(history[0].evaluation, dg.AssetCheckEvaluation)

    def test_a_single_run_backfill_is_rejected_by_the_io_manager(self, tmp_path: Path):
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

    def test_a_single_run_backfill_materializes_the_error_rows_for_every_partition_in_the_range(
        self, tmp_path: Path
    ):
        """A run over a partition key range logs an error-rows materialization for each partition, with the range's count."""
        with dg.DagsterInstance.ephemeral() as instance:
            result = dg.materialize(
                [_daily],
                instance=instance,
                resources=warehouse(tmp_path),
                tags={
                    "dagster/asset_partition_range_start": "2026-01-01",
                    "dagster/asset_partition_range_end": "2026-01-02",
                },
            )
            partitions = instance.get_materialized_partitions(_DAILY_ERROR_ROWS_KEY)

        events = [
            event.step_materialization_data.materialization
            for event in result.get_asset_materialization_events()
            if event.asset_key == _DAILY_ERROR_ROWS_KEY
        ]

        assert partitions == {"2026-01-01", "2026-01-02"}
        assert [
            (event.partition, event.metadata["dagster/partition_row_count"].value)
            for event in events
        ] == [("2026-01-01", 2), ("2026-01-02", 2)]
        assert all("dagster/row_count" not in event.metadata for event in events)


@dd.asset(Orders, name="orders", partitions_def=_PARTITIONS)
def _counted() -> pl.DataFrame:
    return clean_orders()


# Each test calls the asset instead of running it, because in a run `dagster-polars` writes the same key.
class TestRowCount:
    """The key the materialization files the row count under."""

    def test_a_partitioned_asset_counts_the_partitions_rows(self):
        metadata = _materialization(_counted()).metadata or {}

        assert metadata["dagster/partition_row_count"] == 3
        assert "dagster/row_count" not in metadata

    def test_the_package_replaces_only_a_returned_partition_row_count(self):
        @dd.asset(Orders, name="orders", partitions_def=_PARTITIONS)
        def orders() -> dg.MaterializeResult[pl.DataFrame]:
            return dg.MaterializeResult(
                value=clean_orders(),
                metadata={
                    "dagster/partition_row_count": 999,
                    "dagster/row_count": 12,
                },
            )

        metadata = _materialization(orders()).metadata or {}

        assert metadata["dagster/partition_row_count"] == 3
        assert metadata["dagster/row_count"] == 12

    @pytest.mark.parametrize(
        ("partitioned", "key", "absent"),
        [
            (False, "dagster/row_count", "dagster/partition_row_count"),
            (True, "dagster/partition_row_count", "dagster/row_count"),
        ],
    )
    def test_a_hand_wired_asset_counts_under_the_key_partitioned_selects(
        self, partitioned: bool, key: str, absent: str
    ):
        (materialization,) = [
            r
            for r in stream_results(
                Orders, clean_orders(), asset_key=_GOOD_KEY, partitioned=partitioned
            )
            if isinstance(r, dg.MaterializeResult)
        ]
        metadata = materialization.metadata or {}

        assert metadata[key] == 3
        assert absent not in metadata


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


class TestSkippedPartition:
    """A run of one partition whose decorated function returns `None`."""

    def test_a_partition_with_no_source_data_stays_unmaterialized(self, tmp_path: Path):
        result = materialize(
            tmp_path, _reports, partition_key=_DEPARTED, raise_on_error=False
        )

        assert result.success
        assert _partitions(result) == {}
        assert not list(tmp_path.rglob("*.parquet"))

    def test_a_skipped_partition_leaves_every_other_partition_alone(
        self, tmp_path: Path
    ):
        kept = dg.MultiPartitionKey({"month": "2026-01", "distributor": "departed"})

        assert materialize(
            tmp_path, _reports, partition_key=kept, raise_on_error=False
        ).success
        assert materialize(
            tmp_path, _reports, partition_key=_DEPARTED, raise_on_error=False
        ).success

        written = sorted(
            str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*.parquet")
        )

        assert written == ["reports/departed/2026-01.parquet"]

    def test_a_skipped_partition_still_reports_its_checks(self, tmp_path: Path):
        """Every check has a result after a skip, because Dagster fails a step that leaves a check spec without one."""
        result = materialize(
            tmp_path, _reports, partition_key=_DEPARTED, raise_on_error=False
        )
        evaluations = check_evaluations(result)

        assert set(evaluations) == {spec.name for spec in _reports.check_specs}
        assert all(e.passed for e in evaluations.values())

    def test_a_skip_does_not_stamp_a_later_check_onto_an_earlier_partitions_table(
        self,
        tmp_path: Path,
    ):
        """After a skip, `target_materialization_data` is `None`, not the earlier partition's materialization."""
        with dg.DagsterInstance.ephemeral() as instance:
            traded = dg.MultiPartitionKey({
                "month": "2026-01",
                "distributor": "trading",
            })
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
                check_key=dg.AssetCheckKey(_REPORTS_KEY, "dy_rule__amount__min"),
                limit=1,
            )[0].evaluation

            assert isinstance(latest, dg.AssetCheckEvaluation)
            assert latest.passed
            assert latest.target_materialization_data is None
