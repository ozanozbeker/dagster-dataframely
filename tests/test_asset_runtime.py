"""Runtime behaviour of `@dataframely_asset`, asserted through `dg.materialize`.

The seam is what Dagster ends up holding: the materialization events, the check evaluations, the metadata on both, and the bytes on disk. Three of the five state-machine outcomes are reachable without a quarantine, and those are the three here; the other two arrive with #19.
"""

from pathlib import Path
from typing import override

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dagster_dataframely import (
    DataframelyParquetIOManager,
    SchemaGateError,
    ValidationAbortError,
    dataframely_asset,
)
from tests.scenario import (
    Orders,
    clean_orders,
    hopeless_orders,
    mixed_orders,
    wrong_dtype_orders,
)


@dg.asset(name="raw_orders")
def _raw_orders() -> pl.DataFrame:
    return clean_orders()


@dataframely_asset(schema=Orders)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return raw_orders


def _materialize(
    tmp_path: Path, *assets: dg.AssetsDefinition, raise_on_error: bool = True
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        list(assets),
        resources={"io_manager": DataframelyParquetIOManager(base_dir=str(tmp_path))},
        raise_on_error=raise_on_error,
    )


def _evaluations(
    result: dg.ExecuteInProcessResult,
) -> dict[str, dg.AssetCheckEvaluation]:
    return {e.check_name: e for e in result.get_asset_check_evaluations()}


# --- a clean frame ---
def test_a_clean_frame_materializes_the_transforms_output(tmp_path: Path):
    result = _materialize(tmp_path, _raw_orders, orders)

    assert result.success
    assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())


def test_a_clean_run_emits_row_count(tmp_path: Path):
    """The good count specifically, so `dg.build_metadata_bounds_checks` needs no knob from this package."""
    result = _materialize(tmp_path, _raw_orders, orders)
    event = next(
        e
        for e in result.get_asset_materialization_events()
        if e.asset_key == dg.AssetKey(["orders"])
    )
    metadata = dict(event.step_materialization_data.materialization.metadata)

    assert metadata["dagster/row_count"].value == 3


def test_a_clean_run_reports_every_rule_as_passing_at_warn(tmp_path: Path):
    """Specs derive from the schema, so absence never breaks a rule's history."""
    evaluations = _evaluations(_materialize(tmp_path, _raw_orders, orders))
    rules = {name: e for name, e in evaluations.items() if name.startswith("dy_rule__")}

    assert len(evaluations) == len(list(orders.check_specs))
    assert len(rules) == len(evaluations) - 1  # every check but the gate
    assert all(e.passed for e in evaluations.values())
    assert all(e.severity == dg.AssetCheckSeverity.WARN for e in rules.values())


def test_a_check_carries_its_rule_and_the_live_expression(tmp_path: Path):
    """A tightened bound is then visible in the check's own timeline rather than orphaning its history."""
    evaluation = _evaluations(_materialize(tmp_path, _raw_orders, orders))[
        "dy_rule__amount__min"
    ]
    metadata = dict(evaluation.metadata)

    assert metadata["dy_rule"].value == "amount|min"
    assert 'col("amount")' in str(metadata["dy_rule__expr"].value)


def test_a_lazy_transform_is_collected_before_the_write(tmp_path: Path):
    """`filter` collects internally and `dagster/row_count` needs the length regardless, so the good half reaches the IO manager eager."""

    @dataframely_asset(schema=Orders, name="orders_lazy")
    def lazy_orders() -> pl.LazyFrame:
        return clean_orders().lazy()

    assert _materialize(tmp_path, lazy_orders).success
    assert_frame_equal(
        pl.read_parquet(tmp_path / "orders_lazy.parquet"), clean_orders()
    )


def test_the_schema_carrier_reaches_the_io_manager_live_on_both_paths():
    """Parquet is self-describing and needs nothing from the definition, but CSV cannot be read back without the schema (#22). This asserts the channel works before anything depends on it, in-process, where the class is the same object rather than a name."""
    seen: dict[str, object] = {}

    class Spy(dg.IOManager):
        @override
        def handle_output(self, context: dg.OutputContext, obj: object) -> None:
            seen["write"] = dict(context.definition_metadata or {}).get(
                "dagster_dataframely/schema"
            )

        @override
        def load_input(self, context: dg.InputContext) -> pl.DataFrame:
            upstream = context.upstream_output
            assert upstream is not None
            seen["read"] = dict(upstream.definition_metadata or {}).get(
                "dagster_dataframely/schema"
            )
            return clean_orders()

    @dataframely_asset(schema=Orders, name="orders")
    def carrier_source() -> pl.DataFrame:
        return clean_orders()

    @dg.asset(name="reader")
    def reader(orders: pl.DataFrame) -> None:
        pass

    result = dg.materialize([carrier_source, reader], resources={"io_manager": Spy()})

    assert result.success
    assert set(seen) == {"write", "read"}
    assert all(
        getattr(carrier, "instance", None) is Orders for carrier in seen.values()
    )


def test_a_transform_that_returns_no_frame_says_so(tmp_path: Path):
    """The gate reads columns and dtypes off the return value, so a forgotten annotation would otherwise surface as an `AttributeError` two frames inside the package. Dagster's own error, not the package's: this is a wiring mistake, not a data one, which is the same line `_ParquetIOManager` draws."""

    # pyrefly rejects this call outright, which is the point: the runtime guard is for everyone who does not run a type checker, exactly like the Collection guard.
    @dataframely_asset(schema=Orders, name="orders")  # pyrefly: ignore[bad-argument-type]
    def forgot_the_frame():
        return None

    with pytest.raises(
        dg.DagsterInvariantViolationError, match="polars DataFrame or LazyFrame"
    ) as raised:
        _materialize(tmp_path, forgot_the_frame)

    assert "'orders' returned a NoneType" in str(raised.value)
    assert not list(tmp_path.rglob("*.parquet"))


# --- a wrong-shaped frame ---
@dataframely_asset(schema=Orders, name="orders")
def _wrong_dtype() -> pl.DataFrame:
    return wrong_dtype_orders()


def test_a_wrong_dtype_aborts_and_names_the_column(tmp_path: Path):
    """A pipeline defect, not a data defect: the message has to be readable without opening a traceback."""
    with pytest.raises(SchemaGateError) as raised:
        _materialize(tmp_path, _wrong_dtype)

    message = str(raised.value)

    assert "Column 'quantity' (expected Int32, got Int64) does not match Orders" in (
        message
    )


def test_a_missing_column_aborts_too(tmp_path: Path):
    @dataframely_asset(schema=Orders, name="orders")
    def missing_column() -> pl.DataFrame:
        return clean_orders().drop("quantity")

    with pytest.raises(SchemaGateError) as raised:
        _materialize(tmp_path, missing_column)

    assert "'quantity' (expected Int32, got <missing>)" in str(raised.value)


def test_the_gate_error_reads_as_plural_for_several_columns(tmp_path: Path):
    """The message is the only thing a user sees of this error, so it agrees in number."""

    @dataframely_asset(schema=Orders, name="orders")
    def several() -> pl.DataFrame:
        return (
            clean_orders().drop("email").with_columns(pl.col("quantity").cast(pl.Int64))
        )

    with pytest.raises(SchemaGateError) as raised:
        _materialize(tmp_path, several)

    assert "Columns 'email' (expected String, got <missing>), 'quantity' " in str(
        raised.value
    )
    assert "do not match Orders" in str(raised.value)


def test_the_gate_writes_nothing(tmp_path: Path):
    _materialize(tmp_path, _wrong_dtype, raise_on_error=False)

    assert not list(tmp_path.rglob("*.parquet"))


def test_the_gate_runs_before_row_filtering(tmp_path: Path):
    """Only the gate reports. No rule check evaluates, because no row was ever filtered."""
    result = _materialize(tmp_path, _wrong_dtype, raise_on_error=False)
    evaluations = _evaluations(result)
    gate = evaluations["dy_schema__dtypes"]

    assert not result.success
    assert set(evaluations) == {"dy_schema__dtypes"}
    assert not gate.passed
    assert gate.severity == dg.AssetCheckSeverity.ERROR


def test_the_gate_check_tabulates_every_offending_column(tmp_path: Path):
    result = _materialize(tmp_path, _wrong_dtype, raise_on_error=False)
    metadata = dict(_evaluations(result)["dy_schema__dtypes"].metadata)
    errors = metadata["dy_schema__errors"]

    assert isinstance(errors, dg.TableMetadataValue)
    assert [dict(record.data) for record in errors.records] == [
        {"column": "quantity", "expected": "Int32", "actual": "Int64"}
    ]


# --- rejected rows with no quarantine ---
@dataframely_asset(schema=Orders, name="orders")
def _mixed() -> pl.DataFrame:
    return mixed_orders()


def test_rejected_rows_with_no_quarantine_fail_the_run(tmp_path: Path):
    """A partial table must never silently replace a good one."""
    with pytest.raises(ValidationAbortError) as raised:
        _materialize(tmp_path, _mixed)

    assert "Orders rejected 3 rows" in str(raised.value)
    assert "1 by 'amount|min'" in str(raised.value)
    assert "never discards rows on your behalf" in str(raised.value)


def test_the_abort_writes_nothing(tmp_path: Path):
    result = _materialize(tmp_path, _mixed, raise_on_error=False)

    assert not result.success
    assert not result.get_asset_materialization_events()
    assert not list(tmp_path.rglob("*.parquet"))


def test_the_abort_still_reports_every_rule(tmp_path: Path):
    """A failed run still says what failed and by how much."""
    evaluations = _evaluations(_materialize(tmp_path, _mixed, raise_on_error=False))
    failed = {name for name, e in evaluations.items() if not e.passed}

    assert evaluations["dy_schema__dtypes"].passed
    assert failed == {
        "dy_rule__amount__min",
        "dy_rule__email__check__lowercase",
        "dy_rule__paid_orders_have_amount",
    }
    assert (
        dict(evaluations["dy_rule__amount__min"].metadata)["dy_failed_count"].value == 1
    )


def test_a_frame_where_nothing_survives_aborts_the_same_way(tmp_path: Path):
    """Indistinguishable from the mixed frame today, because with no quarantine both discard everything. #19 is what makes them differ, so this pins `hopeless_orders` as still hopeless until then."""

    @dataframely_asset(schema=Orders, name="orders")
    def hopeless() -> pl.DataFrame:
        return hopeless_orders()

    with pytest.raises(ValidationAbortError) as raised:
        _materialize(tmp_path, hopeless)

    assert "Orders rejected 2 rows, 2 by 'amount|min'" in str(raised.value)
    assert not list(tmp_path.rglob("*.parquet"))


def test_the_abort_raises_every_rule_check_to_error(tmp_path: Path):
    """Severity is the run's outcome, not the rule's: nothing landed, so nothing is a warning."""
    evaluations = _evaluations(_materialize(tmp_path, _mixed, raise_on_error=False))
    rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

    assert rules
    assert all(e.severity == dg.AssetCheckSeverity.ERROR for e in rules)
