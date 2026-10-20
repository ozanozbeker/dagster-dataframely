"""The asset checks `dd.build_rule_checks` returns, and what a run of them reports for a table something else wrote."""

from collections.abc import Mapping
from pathlib import Path

import dagster as dg
import dataframely as dy
import duckdb
import polars as pl
import pytest
from dagster_polars import PolarsParquetIOManager

import dagster_dataframely as dd
from dagster_dataframely.errors import ColumnSchemaError
from dagster_dataframely.wiring import build_asset_check_specs
from tests.scenario import WAREHOUSE_SCHEMA, check_evaluations, warehouse

_WARN = {"dagster_dataframely": {"severity": {"max": "warn"}}}


class EventsSchema(dy.Schema):
    event_id = dy.Int64(primary_key=True)
    amount = dy.Int64(nullable=False, min=0)
    score = dy.Int64(nullable=True, max=10, metadata=_WARN)


KEY = dg.AssetKey(["events"])
ERROR = dg.AssetCheckSeverity.ERROR
WARN = dg.AssetCheckSeverity.WARN

CLEAN = pl.DataFrame({"event_id": [1, 2], "amount": [5, 6], "score": [1, 2]})
FAILING_ERROR = pl.DataFrame({"event_id": [1, 2], "amount": [5, -1], "score": [1, 2]})
FAILING_WARN = pl.DataFrame({"event_id": [1, 2], "amount": [5, 6], "score": [1, 20]})

SETTINGS: list[dict[str, str]] = [
    {"check_granularity": "rule"},
    {"check_granularity": "column"},
]

LAKE = dg.AssetSpec(KEY).with_io_manager_key("lake")
"""A table written outside Dagster, which the `lake` IO manager reads."""


def lake(tmp_path: Path, frame: pl.DataFrame) -> dict[str, PolarsParquetIOManager]:
    """Write `frame` where `PolarsParquetIOManager` reads `events`, and return the resources that read it."""
    frame.write_parquet(tmp_path / "events.parquet")
    return {"lake": PolarsParquetIOManager(base_dir=str(tmp_path))}


def stored(tmp_path: Path, amount_type: str) -> Mapping[str, object]:
    """Create the `events` table with SQL, as dbt does, and return the resources that read it."""
    resources = warehouse(tmp_path)
    with duckdb.connect(str(tmp_path / "warehouse.duckdb")) as connection:
        connection.sql(
            f"create table {WAREHOUSE_SCHEMA}.events "
            f"(event_id BIGINT, amount {amount_type}, score BIGINT)"
        )
        connection.sql(
            f"insert into {WAREHOUSE_SCHEMA}.events values (1, 5, 1), (2, -1, 2)"  # noqa: S608
        )
    return resources


def run(
    checks: dg.AssetChecksDefinition,
    resources: Mapping[str, object],
    *,
    spec: dg.AssetSpec = LAKE,
    selection: dg.AssetSelection | None = None,
    raise_on_error: bool = True,
) -> dg.ExecuteInProcessResult:
    """Run only the checks, as the UI's "Execute" button does for an external asset."""
    return dg.materialize(
        [spec, checks],
        resources=resources,
        selection=selection or dg.AssetSelection.checks(checks),
        raise_on_error=raise_on_error,
    )


class TestDefinition:
    """The definition `dd.build_rule_checks` returns."""

    @pytest.mark.parametrize("settings", SETTINGS, ids=str)
    def test_the_checks_are_the_specs_asset_check_specs_returns(
        self, settings: dict[str, str]
    ):
        checks = dd.build_rule_checks(EventsSchema, asset="events", **settings)  # pyrefly: ignore[bad-argument-type]
        specs = build_asset_check_specs(EventsSchema, asset="events", **settings)  # pyrefly: ignore[bad-argument-type]

        assert [spec.key for spec in checks.check_specs] == [spec.key for spec in specs]

    def test_the_input_is_a_data_frame(self):
        checks = dd.build_rule_checks(EventsSchema, asset=KEY)

        assert {
            input_def.name: input_def.dagster_type.typing_type
            for input_def in checks.node_def.input_defs
        } == {"table": pl.DataFrame}

    @pytest.mark.parametrize(
        ("asset", "name"),
        [
            ("events", "events__checks"),
            (dg.AssetKey(["sales", "events"]), "sales__events__checks"),
        ],
    )
    def test_the_default_name_is_the_key_with_a_checks_suffix(
        self, asset: str | dg.AssetKey, name: str
    ):
        assert dd.build_rule_checks(EventsSchema, asset=asset).node_def.name == name

    def test_a_name_replaces_the_default(self):
        checks = dd.build_rule_checks(EventsSchema, asset=KEY, name="audit")

        assert checks.node_def.name == "audit"

    def test_the_default_name_loads_beside_the_asset_op(self):
        """A step named like the asset's op would fail to load, as the snake-cased schema name `events` does."""

        @dg.asset(key=KEY)
        def events() -> None: ...

        dg.Definitions.validate_loadable(
            dg.Definitions(
                assets=[events],
                asset_checks=[dd.build_rule_checks(EventsSchema, asset=KEY)],
            )
        )

    def test_only_an_error_rule_check_is_blocking(self):
        checks = dd.build_rule_checks(EventsSchema, asset=KEY)

        assert {spec.name: spec.blocking for spec in checks.check_specs} == {
            "dy_schema__columns": True,
            "dy_rule__event_id__nullability": True,
            "dy_rule__amount__nullability": True,
            "dy_rule__amount__min": True,
            "dy_rule__score__max": False,
            "dy_rule__primary_key": True,
        }

    def test_every_spec_has_the_partitions(self):
        daily = dg.DailyPartitionsDefinition("2026-01-01")

        checks = dd.build_rule_checks(EventsSchema, asset=KEY, partitions_def=daily)

        assert {spec.partitions_def for spec in checks.check_specs} == {daily}


class TestRun:
    """A run of the checks over a table that Dagster did not write."""

    @pytest.mark.parametrize("settings", SETTINGS, ids=str)
    def test_the_results_answer_exactly_the_specs(
        self, tmp_path: Path, settings: dict[str, str]
    ):
        checks = dd.build_rule_checks(EventsSchema, asset=KEY, **settings)  # pyrefly: ignore[bad-argument-type]

        result = run(checks, lake(tmp_path, CLEAN))

        assert list(check_evaluations(result)) == [
            spec.name for spec in checks.check_specs
        ]

    def test_the_settings_resolve_once_where_the_checks_are_declared(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """A run in an environment with another granularity still answers the declared specs."""
        monkeypatch.setenv("DAGSTER_DATAFRAMELY_CHECK_GRANULARITY", "column")
        checks = dd.build_rule_checks(EventsSchema, asset=KEY)
        monkeypatch.setenv("DAGSTER_DATAFRAMELY_CHECK_GRANULARITY", "rule")

        result = run(checks, lake(tmp_path, CLEAN))

        assert list(check_evaluations(result)) == [
            "dy_schema__columns",
            "dy_col__event_id",
            "dy_col__amount",
            "dy_col__score",
            "dy_rule__primary_key",
        ]

    def test_a_failing_error_rule_fails_at_error_and_fails_the_step(
        self, tmp_path: Path
    ):
        checks = dd.build_rule_checks(EventsSchema, asset=KEY)

        result = run(checks, lake(tmp_path, FAILING_ERROR), raise_on_error=False)

        evaluation = check_evaluations(result)["dy_rule__amount__min"]
        assert not result.success
        assert not evaluation.passed
        assert evaluation.severity == ERROR
        assert evaluation.metadata["dataframely/failed_row_count"].value == 1

    def test_a_failing_warn_rule_only_warns(self, tmp_path: Path):
        checks = dd.build_rule_checks(EventsSchema, asset=KEY)

        result = run(checks, lake(tmp_path, FAILING_WARN))

        evaluation = check_evaluations(result)["dy_rule__score__max"]
        assert result.success
        assert not evaluation.passed
        assert evaluation.severity == WARN

    def test_a_run_that_selects_one_check_evaluates_only_that_check(
        self, tmp_path: Path
    ):
        checks = dd.build_rule_checks(EventsSchema, asset=KEY)
        selected = dg.AssetCheckKey(KEY, "dy_rule__amount__min")

        result = run(
            checks,
            lake(tmp_path, CLEAN),
            selection=dg.AssetSelection.checks(selected),
        )

        assert list(check_evaluations(result)) == ["dy_rule__amount__min"]


class TestWarehouse:
    """A run over a DuckDB table that SQL wrote."""

    def test_an_io_manager_that_loads_only_data_frames_reads_the_table(
        self, tmp_path: Path
    ):
        checks = dd.build_rule_checks(EventsSchema, asset=KEY)

        result = run(
            checks,
            stored(tmp_path, "BIGINT"),
            spec=dg.AssetSpec(KEY),
            raise_on_error=False,
        )

        evaluations = check_evaluations(result)
        assert evaluations["dy_schema__columns"].passed
        assert not evaluations["dy_rule__amount__min"].passed

    def test_a_stored_dtype_that_differs_raises_with_a_fix_for_the_table(
        self, tmp_path: Path
    ):
        """SQL `INTEGER` reads back as `Int32`, and the schema declares `Int64`."""
        checks = dd.build_rule_checks(EventsSchema, asset=KEY)

        with pytest.raises(ColumnSchemaError) as raised:
            run(checks, stored(tmp_path, "INTEGER"), spec=dg.AssetSpec(KEY))

        assert str(raised.value) == (
            "Column 'amount' (expected Int64, got Int32) does not match EventsSchema. "
            "Declare the dtype the table stores in EventsSchema, or change what writes the table. "
            "This package never casts for you."
        )


class TestPartitions:
    """A checks-only run of one partition of a partitioned asset."""

    def test_a_run_reads_only_its_partition(self, tmp_path: Path):
        parts = dg.StaticPartitionsDefinition(["clean", "failing"])

        @dg.asset(key=KEY, partitions_def=parts)
        def events(context: dg.AssetExecutionContext) -> pl.DataFrame:
            return CLEAN if context.partition_key == "clean" else FAILING_ERROR

        checks = dd.build_rule_checks(EventsSchema, asset=KEY, partitions_def=parts)
        definitions = dg.Definitions(
            assets=[events],
            asset_checks=[checks],
            resources={"io_manager": PolarsParquetIOManager(base_dir=str(tmp_path))},
        )
        job = definitions.resolve_implicit_global_asset_job_def()
        for partition in ["clean", "failing"]:
            job.get_subset(
                asset_selection={KEY}, asset_check_selection=set()
            ).execute_in_process(partition_key=partition)

        result = job.get_subset(
            asset_selection=set(), asset_check_selection=set(checks.check_keys)
        ).execute_in_process(partition_key="clean")

        evaluations = check_evaluations(result)
        assert all(evaluation.passed for evaluation in evaluations.values())
        assert {evaluation.partition for evaluation in evaluations.values()} == {
            "clean"
        }
