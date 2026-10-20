"""A rule's severity sets what a run writes, the severity of its check result, and whether its check spec is blocking."""

from collections.abc import Sequence
from pathlib import Path
from typing import ClassVar

import dagster as dg
import dataframely as dy
import polars as pl
import pytest

import dagster_dataframely as dd
from dagster_dataframely.errors import (
    DagsterDataframelyError,
    NoValidRowsError,
    ValidationAbortError,
)
from dagster_dataframely.wiring import (
    build_asset_check_specs,
    evaluate_checks,
    stream_results,
)
from tests.scenario import Yielded, check_evaluations, materialize

_WARN = {"dagster_dataframely": {"severity": {"max_length": "warn"}}}


class Shipments(dy.Schema):
    shipment_id = dy.String(primary_key=True)
    weight = dy.Int64(nullable=False, min=1)
    email = dy.String(nullable=False, max_length=10, metadata=_WARN)
    note = dy.String(nullable=True, max_length=3, metadata=_WARN)


_KEY = dg.AssetKey(["shipments"])
_ADDRESS = "shipments__error_rows"


def _frame(*rows: tuple[str, int, str]) -> pl.DataFrame:
    return pl.DataFrame(
        [{"shipment_id": s, "weight": w, "email": e, "note": None} for s, w, e in rows],
        schema={
            "shipment_id": pl.String,
            "weight": pl.Int64,
            "email": pl.String,
            "note": pl.String,
        },
    )


def _clean() -> pl.DataFrame:
    return _frame(("A", 1, "a@x.io"), ("B", 2, "b@x.io"))


def _warn_only() -> pl.DataFrame:
    """`B` fails only `email|max_length`, a `"warn"` rule."""
    return _frame(("A", 1, "a@x.io"), ("B", 2, "b@long-domain.io"))


def _mixed() -> pl.DataFrame:
    """`A` is valid, `B` fails a `"warn"` rule, `C` an `"error"` rule, and `D` both."""
    return _frame(
        ("A", 1, "a@x.io"),
        ("B", 2, "b@long-domain.io"),
        ("C", 0, "c@x.io"),
        ("D", 0, "d@long-domain.io"),
    )


def _all_error() -> pl.DataFrame:
    return _frame(("A", 0, "a@x.io"), ("B", 0, "b@long-domain.io"))


def _drained(
    frame: pl.DataFrame, *, filtering: bool
) -> tuple[Yielded, list[pl.DataFrame], DagsterDataframelyError | None]:
    """Return what `stream_results` yielded and wrote, and the error it raised, if any."""
    written: list[pl.DataFrame] = []

    def writer(rows: pl.DataFrame) -> str:
        written.append(rows)
        return _ADDRESS

    yielded: Yielded = []
    try:
        yielded.extend(
            stream_results(
                Shipments,
                frame,
                asset_key=_KEY,
                error_rows_writer=writer if filtering else None,
            )
        )
    except DagsterDataframelyError as error:
        return yielded, written, error
    return yielded, written, None


def _table(yielded: Yielded) -> pl.DataFrame | None:
    tables = [r.value for r in yielded if isinstance(r, dg.MaterializeResult)]
    return tables[0] if tables else None


def _failed(
    yielded: Sequence[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult],
) -> dict[str, dg.AssetCheckSeverity]:
    return {
        str(r.check_name): r.severity
        for r in yielded
        if isinstance(r, dg.AssetCheckResult) and not r.passed
    }


def _ids(frame: pl.DataFrame | None) -> set[str]:
    assert frame is not None
    return set(frame["shipment_id"])


class TestWithoutFilter:
    def test_a_warn_failure_writes_every_row_and_warns(self):
        yielded, _, error = _drained(_warn_only(), filtering=False)

        assert error is None
        assert _ids(_table(yielded)) == {"A", "B"}
        assert _failed(yielded) == {
            "dy_rule__email__max_length": dg.AssetCheckSeverity.WARN
        }

    def test_the_rows_that_failed_only_warn_rules_follow_the_valid_rows(self):
        """`Schema.filter` removes them, so the package appends them after the valid rows."""
        frame = _frame(("A", 1, "a@long-domain.io"), ("B", 2, "b@x.io"))
        yielded, _, _ = _drained(frame, filtering=False)
        table = _table(yielded)

        assert table is not None
        assert table["shipment_id"].to_list() == ["B", "A"]

    def test_an_error_failure_writes_nothing_and_each_check_has_its_rules_severity(
        self,
    ):
        yielded, _, error = _drained(_mixed(), filtering=False)

        assert isinstance(error, ValidationAbortError)
        assert _table(yielded) is None
        assert _failed(yielded) == {
            "dy_rule__weight__min": dg.AssetCheckSeverity.ERROR,
            "dy_rule__email__max_length": dg.AssetCheckSeverity.WARN,
        }

    def test_the_abort_message_counts_only_the_error_rules(self):
        _, _, error = _drained(_mixed(), filtering=False)

        message = str(error)
        assert "2 rows failed an 'error' rule of Shipments" in message
        assert "'weight|min'" in message
        assert "email|max_length" not in message
        assert "`filter=True`" in message


class TestWithFilter:
    def test_the_error_rows_hold_exactly_the_rows_that_failed_an_error_rule(self):
        yielded, (error_rows,), error = _drained(_mixed(), filtering=True)

        assert error is None
        assert _ids(error_rows) == {"C", "D"}
        assert _ids(_table(yielded)) == {"A", "B"}

    def test_the_two_tables_add_up_to_the_frame(self):
        yielded, (error_rows,), _ = _drained(_mixed(), filtering=True)
        table = _table(yielded)

        assert table is not None
        assert len(table) + len(error_rows) == len(_mixed())
        assert _ids(table).isdisjoint(_ids(error_rows))

    def test_the_table_keeps_the_schemas_columns_in_order(self):
        yielded, _, _ = _drained(_mixed(), filtering=True)
        table = _table(yielded)

        assert table is not None
        assert table.columns == list(Shipments.columns())

    def test_the_error_checks_fail_at_error_and_the_run_succeeds(self):
        yielded, _, error = _drained(_mixed(), filtering=True)

        assert error is None
        assert _failed(yielded) == {
            "dy_rule__weight__min": dg.AssetCheckSeverity.ERROR,
            "dy_rule__email__max_length": dg.AssetCheckSeverity.WARN,
        }

    def test_only_warn_failures_write_every_row_and_empty_error_rows(self):
        yielded, (error_rows,), error = _drained(_warn_only(), filtering=True)

        assert error is None
        assert error_rows.is_empty()
        assert _ids(_table(yielded)) == {"A", "B"}

    def test_every_row_failing_an_error_rule_raises_and_writes_only_the_error_rows(
        self,
    ):
        yielded, (error_rows,), error = _drained(_all_error(), filtering=True)

        assert isinstance(error, NoValidRowsError)
        assert _table(yielded) is None
        assert len(error_rows) == 2
        checks = [r for r in yielded if isinstance(r, dg.AssetCheckResult)]
        assert all(
            (r.metadata or {})["dataframely/error_rows_address"].value == _ADDRESS
            for r in checks
        )

    def test_the_materialization_counts_the_error_rows(self):
        yielded, _, _ = _drained(_mixed(), filtering=True)
        (materialization,) = [r for r in yielded if isinstance(r, dg.MaterializeResult)]
        metadata = materialization.metadata or {}

        assert metadata["dagster/row_count"] == 2
        assert metadata["dataframely/error_rows_count"] == 2
        assert metadata["dataframely/error_rows_address"] == _ADDRESS
        assert {"dataframely/table_sample", "dataframely/error_rows_sample"} <= set(
            metadata
        )


def _blocking(specs: list[dg.AssetCheckSpec]) -> set[str]:
    return {spec.name for spec in specs if spec.blocking}


class TestBlockingSpecs:
    @pytest.mark.parametrize(
        ("granularity", "blocking"),
        [
            pytest.param(
                "rule",
                {
                    "dy_schema__columns",
                    "dy_rule__primary_key",
                    "dy_rule__shipment_id__nullability",
                    "dy_rule__weight__nullability",
                    "dy_rule__weight__min",
                    "dy_rule__email__nullability",
                },
                id="rule",
            ),
            pytest.param(
                "column",
                {
                    "dy_schema__columns",
                    "dy_rule__primary_key",
                    "dy_col__shipment_id",
                    "dy_col__weight",
                    "dy_col__email",
                },
                id="column",
            ),
        ],
    )
    def test_without_filter_a_check_is_blocking_when_it_has_an_error_rule(
        self, granularity: dd.Granularity, blocking: set[str]
    ):
        specs = build_asset_check_specs(
            Shipments, asset=_KEY, check_granularity=granularity
        )

        assert _blocking(specs) == blocking

    @pytest.mark.parametrize("granularity", ["rule", "column"])
    def test_with_filter_only_the_column_schema_check_is_blocking(
        self, granularity: dd.Granularity
    ):
        specs = build_asset_check_specs(
            Shipments, asset=_KEY, check_granularity=granularity, filter=True
        )

        assert _blocking(specs) == {"dy_schema__columns"}

    @pytest.mark.parametrize("filtering", [False, True])
    def test_the_decorator_passes_filter_to_the_specs(self, filtering: bool):
        @dd.asset(Shipments, filter=filtering)
        def shipments() -> pl.DataFrame:
            return _clean()

        expected = build_asset_check_specs(Shipments, asset=_KEY, filter=filtering)
        assert _blocking(list(shipments.check_specs)) == _blocking(expected)


class TestCollapsedCheck:
    def test_a_collapsed_check_warns_when_only_its_warn_rule_failed(
        self, tmp_path: Path
    ):
        @dd.asset(Shipments, check_granularity="column")
        def shipments() -> pl.DataFrame:
            return _warn_only()

        result = materialize(tmp_path, shipments)
        email = check_evaluations(result)["dy_col__email"]

        assert result.success
        assert not email.passed
        assert email.severity == dg.AssetCheckSeverity.WARN

    def test_a_collapsed_check_is_an_error_when_its_error_rule_failed(self):
        yielded = list(
            evaluate_checks(
                Shipments, _mixed(), asset_key=_KEY, check_granularity="column"
            )
        )

        assert _failed(yielded)["dy_col__weight"] == dg.AssetCheckSeverity.ERROR
        assert _failed(yielded)["dy_col__email"] == dg.AssetCheckSeverity.WARN


class TestCheckResults:
    def test_each_result_has_its_rules_severity(self):
        yielded = list(evaluate_checks(Shipments, _mixed(), asset_key=_KEY))

        assert _failed(yielded) == {
            "dy_rule__weight__min": dg.AssetCheckSeverity.ERROR,
            "dy_rule__email__max_length": dg.AssetCheckSeverity.WARN,
        }


class WarnOnTheClass(dy.Schema):
    shipment_id = dy.String(primary_key=True)

    __dagster_dataframely__: ClassVar = {"severity": {"primary_key": "warn"}}


def test_a_duplicate_key_declared_warn_is_written():
    frame = pl.DataFrame({"shipment_id": ["A", "A"]})
    yielded = list(stream_results(WarnOnTheClass, frame, asset_key=_KEY))

    assert _ids(_table(yielded)) == {"A"}
    assert len(_table(yielded)) == 2  # pyrefly: ignore[bad-argument-type]
    assert _failed(yielded) == {"dy_rule__primary_key": dg.AssetCheckSeverity.WARN}
