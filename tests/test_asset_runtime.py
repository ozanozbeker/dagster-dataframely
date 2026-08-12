"""Runtime behaviour of `@dataframely_asset`, asserted through `dg.materialize`.

The seam is what Dagster ends up holding: the materialization events, the check evaluations, the metadata on both, and the bytes on disk. All five state-machine rule_columns are here; which two of them a frame reaches is decided by whether the asset declares a quarantine.
"""

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, override

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dagster_dataframely import (
    DataframelyParquetIOManager,
    NothingSurvivedError,
    SchemaShapeError,
    ValidationAbortError,
    dataframely_asset,
)
from tests.scenario import (
    Orders,
    clean_orders,
    cooccurring_orders,
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


def _materialized(
    result: dg.ExecuteInProcessResult,
) -> dict[dg.AssetKey, Mapping[str, dg.MetadataValue[Any]]]:
    """Every materialization the run emitted, keyed by asset, with its metadata."""
    return {
        event.asset_key: event.step_materialization_data.materialization.metadata
        for event in result.get_asset_materialization_events()
        if event.asset_key is not None
    }


# --- a clean frame ---
def test_a_clean_frame_materializes_the_transforms_output(tmp_path: Path):
    result = _materialize(tmp_path, _raw_orders, orders)

    assert result.success
    assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())


def test_a_clean_run_emits_row_count(tmp_path: Path):
    """The valid count specifically, so `dg.build_metadata_bounds_checks` needs no setting from this package."""
    result = _materialize(tmp_path, _raw_orders, orders)
    metadata = _materialized(result)[dg.AssetKey(["orders"])]

    assert metadata["dagster/row_count"].value == 3


def test_a_clean_run_reports_every_rule_as_passing_at_warn(tmp_path: Path):
    """Specs derive from the schema, so absence never breaks a rule's history."""
    evaluations = _evaluations(_materialize(tmp_path, _raw_orders, orders))
    rules = {name: e for name, e in evaluations.items() if name.startswith("dy_rule__")}

    assert len(evaluations) == len(list(orders.check_specs))
    assert len(rules) == len(evaluations) - 1  # every check but the shape check
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


def test_a_lazy_transform_lands_and_is_read_back_whole(tmp_path: Path):
    """The round trip through the staged parquet has to be lossless, and this schema is where that is worth asserting: `Decimal`, `Duration`, `Enum`, `Binary` and `List` are the dtypes a round trip could quietly change, and the shape check has already run by the time the staging happens, so a changed dtype would surface as a filter failure rather than as a shape one."""

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
    """The shape check reads columns and dtypes off the return value, so a forgotten annotation would otherwise surface as an `AttributeError` two frames inside the package. Dagster's own error, not the package's: this is a wiring mistake, not a data one, which is the same line `_ParquetIOManager` draws."""

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
    with pytest.raises(SchemaShapeError) as raised:
        _materialize(tmp_path, _wrong_dtype)

    message = str(raised.value)

    assert "Column 'quantity' (expected Int32, got Int64) does not match Orders" in (
        message
    )


def test_a_missing_column_aborts_too(tmp_path: Path):
    @dataframely_asset(schema=Orders, name="orders")
    def missing_column() -> pl.DataFrame:
        return clean_orders().drop("quantity")

    with pytest.raises(SchemaShapeError) as raised:
        _materialize(tmp_path, missing_column)

    assert "'quantity' (expected Int32, got <missing>)" in str(raised.value)


def test_the_shape_error_reads_as_plural_for_several_columns(tmp_path: Path):
    """The message is the only thing a user sees of this error, so it agrees in number."""

    @dataframely_asset(schema=Orders, name="orders")
    def several() -> pl.DataFrame:
        return (
            clean_orders().drop("email").with_columns(pl.col("quantity").cast(pl.Int64))
        )

    with pytest.raises(SchemaShapeError) as raised:
        _materialize(tmp_path, several)

    assert "Columns 'email' (expected String, got <missing>), 'quantity' " in str(
        raised.value
    )
    assert "do not match Orders" in str(raised.value)


def test_the_shape_check_writes_nothing(tmp_path: Path):
    _materialize(tmp_path, _wrong_dtype, raise_on_error=False)

    assert not list(tmp_path.rglob("*.parquet"))


def test_the_shape_check_runs_before_row_filtering(tmp_path: Path):
    """Only the shape check reports. No rule check evaluates, because no row was ever filtered."""
    result = _materialize(tmp_path, _wrong_dtype, raise_on_error=False)
    evaluations = _evaluations(result)
    shape = evaluations["dy_schema__dtypes"]

    assert not result.success
    assert set(evaluations) == {"dy_schema__dtypes"}
    assert not shape.passed
    assert shape.severity == dg.AssetCheckSeverity.ERROR


def test_the_shape_check_tabulates_every_offending_column(tmp_path: Path):
    result = _materialize(tmp_path, _wrong_dtype, raise_on_error=False)
    metadata = dict(_evaluations(result)["dy_schema__dtypes"].metadata)
    errors = metadata["dy_schema__errors"]

    assert isinstance(errors, dg.TableMetadataValue)
    assert [dict(record.data) for record in errors.records] == [
        {"column": "quantity", "expected": "Int32", "actual": "Int64"}
    ]


# --- invalid rows with no quarantine ---
@dataframely_asset(schema=Orders, name="orders")
def _mixed() -> pl.DataFrame:
    return mixed_orders()


def test_rejected_rows_with_no_quarantine_fail_the_run(tmp_path: Path):
    """A partial table must never silently replace a complete one."""
    with pytest.raises(ValidationAbortError) as raised:
        _materialize(tmp_path, _mixed)

    assert "Orders rejected 3 rows" in str(raised.value)
    assert "1 by 'amount|min'" in str(raised.value)
    assert "never discards rows on your behalf" in str(raised.value)


def test_the_abort_names_the_quarantine_as_the_fix(tmp_path: Path):
    """This error is the one place a user who has not read the README learns the option exists, so it names the keyword rather than gesturing at it."""
    with pytest.raises(ValidationAbortError) as raised:
        _materialize(tmp_path, _mixed)

    assert "quarantine=dg.AssetOut()" in str(raised.value)


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
    """With no quarantine the two frames are indistinguishable, because both discard everything. Declaring one is what makes them differ."""

    @dataframely_asset(schema=Orders, name="orders")
    def hopeless() -> pl.DataFrame:
        return hopeless_orders()

    with pytest.raises(ValidationAbortError) as raised:
        _materialize(tmp_path, hopeless)

    assert "Orders rejected 2 rows, 2 by 'amount|min'" in str(raised.value)
    assert not list(tmp_path.rglob("*.parquet"))


def test_the_abort_raises_every_rule_check_to_error(tmp_path: Path):
    """Severity is the run's outcome, not the rule's: nothing was written, so nothing is a warning."""
    evaluations = _evaluations(_materialize(tmp_path, _mixed, raise_on_error=False))
    rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

    assert rules
    assert all(e.severity == dg.AssetCheckSeverity.ERROR for e in rules)


# --- invalid rows with a quarantine, some surviving ---
_GOOD_KEY = dg.AssetKey(["orders"])
_QUARANTINE_KEY = dg.AssetKey(["orders_quarantine"])


@dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
def _quarantined() -> pl.DataFrame:
    return mixed_orders()


def test_the_survivors_land_and_the_rest_go_next_door(tmp_path: Path):
    """The middle case: 3 valid rows are written, 3 invalid ones are inspectable, and downstream proceeds."""
    result = _materialize(tmp_path, _quarantined)

    assert result.success
    assert set(_materialized(result)) == {_GOOD_KEY, _QUARANTINE_KEY}
    assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())
    assert pl.read_parquet(tmp_path / "orders_quarantine.parquet").height == 3


def test_a_quarantined_run_stays_green_with_every_check_at_warn(tmp_path: Path):
    """Consent to partial data was given by declaring the out, so a invalid row is a warning rather than a failure."""
    evaluations = _evaluations(_materialize(tmp_path, _quarantined))
    failed = {name for name, e in evaluations.items() if not e.passed}
    rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

    assert failed == {
        "dy_rule__amount__min",
        "dy_rule__email__check__lowercase",
        "dy_rule__paid_orders_have_amount",
    }
    assert all(e.severity == dg.AssetCheckSeverity.WARN for e in rules)


def test_every_rule_check_carries_its_rule_and_expression_whichever_way_it_went(
    tmp_path: Path,
):
    """The two keys are unconditional. A check whose metadata appeared only on the runs it failed would have a timeline with holes in it, and the expression is what makes a tightened bound visible in that timeline rather than orphaning its history."""
    evaluations = _evaluations(_materialize(tmp_path, _quarantined))
    rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

    assert any(e.passed for e in rules)
    assert any(not e.passed for e in rules)
    assert all({"dy_rule", "dy_rule__expr"} <= set(e.metadata) for e in rules)


def test_downstream_proceeds_on_the_data_that_is_fine(tmp_path: Path):
    """Landing the survivors is only worth anything if the run does not stop there. The rule checks are non-blocking, so a `WARN` never holds a consumer back."""
    seen: dict[str, int] = {}

    @dg.asset(name="reconciled")
    def reconciled(orders: pl.DataFrame) -> None:
        seen["rows"] = len(orders)

    result = _materialize(tmp_path, _quarantined, reconciled)

    assert result.success
    assert seen == {"rows": 3}


def test_the_quarantine_holds_the_original_columns_plus_a_rule_column_each(
    tmp_path: Path,
):
    """`invalid()` was rejected as the content: check-metadata samples are bounded, so without per-row attribution here it exists nowhere at volume."""
    _materialize(tmp_path, _quarantined)
    quarantine = pl.read_parquet(tmp_path / "orders_quarantine.parquet")
    rule_columns = [name for name in quarantine.columns if name.startswith("dy_rule__")]
    checks = {spec.name for spec in _quarantined.check_specs} - {"dy_schema__dtypes"}

    assert quarantine.columns[: len(Orders.columns())] == list(Orders.columns())
    assert set(quarantine.columns) == set(Orders.columns()) | set(rule_columns)
    # Byte-identical to the check names, asserted against the written frame rather than
    # only the declaration, so the two surfaces cannot drift apart.
    assert set(rule_columns) == checks


def test_the_rule_columns_are_cast_from_enum_to_string(tmp_path: Path):
    """Mandatory, not defensive: a raw `Enum` panics the Delta writer with a Rust `unreachable!()`."""
    _materialize(tmp_path, _quarantined)
    quarantine = pl.read_parquet(tmp_path / "orders_quarantine.parquet")
    rule_columns = [name for name in quarantine.columns if name.startswith("dy_rule__")]

    assert all(quarantine.schema[name] == pl.String for name in rule_columns)


def test_a_rule_column_says_which_rule_rejected_which_row(tmp_path: Path):
    _materialize(tmp_path, _quarantined)
    quarantine = pl.read_parquet(tmp_path / "orders_quarantine.parquet").sort(
        "order_id"
    )
    rejected_by_min = quarantine.filter(pl.col("dy_rule__amount__min") == "invalid")

    assert quarantine["dy_rule__amount__min"].to_list() == [
        "invalid",
        "valid",
        "valid",
    ]
    assert rejected_by_min["order_id"].to_list() == ["ORD-4"]


def test_the_quarantine_emits_its_own_row_count(tmp_path: Path):
    metadata = _materialized(_materialize(tmp_path, _quarantined))

    assert metadata[_GOOD_KEY]["dagster/row_count"].value == 3
    assert metadata[_QUARANTINE_KEY]["dagster/row_count"].value == 3


def test_the_quarantine_emits_rule_cooccurrence_counts(tmp_path: Path):
    """One broken upstream field tripping three rules at once is one row here, not three unrelated counts."""

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def cooccurring() -> pl.DataFrame:
        return cooccurring_orders()

    metadata = _materialized(_materialize(tmp_path, cooccurring))
    cooccurrence = metadata[_QUARANTINE_KEY]["cooccurrence"]

    assert isinstance(cooccurrence, dg.TableMetadataValue)
    assert [dict(record.data) for record in cooccurrence.records] == [
        {
            "rules": (
                "dy_rule__amount__min, dy_rule__email__check__lowercase, "
                "dy_rule__paid_orders_have_amount"
            ),
            "count": 1,
        }
    ]


def test_the_cooccurrence_table_leads_with_the_set_that_broke_the_most_rows(
    tmp_path: Path,
):
    """`cooccurrence_counts()` groups without `maintain_order`, so what it hands over is in no order at all: the same frame twice emits these rows differently, and two runs of the same data then diff as though something changed.

    Sorted here rather than upstream, on the two keys the table is read by: how many rows a set broke, then the names, which is the sort already applied inside each set.
    """
    duplicate_break = (
        mixed_orders()
        .filter(pl.col("order_id") == "ORD-4")  # amount|min alone
        .with_columns(
            pl.lit("ORD-7").alias("order_id"),
            pl.lit("TRK-ORD-7-1").alias("tracking_id"),
        )
    )

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def repeated() -> pl.DataFrame:
        return pl.concat([mixed_orders(), duplicate_break])

    metadata = _materialized(_materialize(tmp_path, repeated))
    cooccurrence = metadata[_QUARANTINE_KEY]["cooccurrence"]

    assert isinstance(cooccurrence, dg.TableMetadataValue)
    assert [dict(record.data) for record in cooccurrence.records] == [
        {"rules": "dy_rule__amount__min", "count": 2},
        {"rules": "dy_rule__email__check__lowercase", "count": 1},
        {"rules": "dy_rule__paid_orders_have_amount", "count": 1},
    ]


def test_a_clean_run_skips_the_quarantine_entirely(tmp_path: Path):
    """An empty quarantine partition then means something."""

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def spotless() -> pl.DataFrame:
        return clean_orders()

    result = _materialize(tmp_path, spotless)

    assert result.success
    assert set(_materialized(result)) == {_GOOD_KEY}
    assert not (tmp_path / "orders_quarantine.parquet").exists()


# --- invalid rows with a quarantine, none surviving ---
@dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
def _nothing_survived() -> pl.DataFrame:
    return hopeless_orders()


def test_nothing_surviving_materializes_the_quarantine_and_skips_the_valid_out(
    tmp_path: Path,
):
    """The load-bearing case: an empty table must never silently replace a last-known-good snapshot."""
    result = _materialize(tmp_path, _nothing_survived, raise_on_error=False)

    assert not result.success
    assert set(_materialized(result)) == {_QUARANTINE_KEY}
    assert pl.read_parquet(tmp_path / "orders_quarantine.parquet").height == 2
    assert not (tmp_path / "orders.parquet").exists()


def test_nothing_surviving_leaves_the_last_known_good_table_intact(tmp_path: Path):
    """The same key, written clean and then run again on a frame nothing survives."""

    @dataframely_asset(schema=Orders, name="orders", quarantine=dg.AssetOut())
    def spotless() -> pl.DataFrame:
        return clean_orders()

    _materialize(tmp_path, spotless)
    _materialize(tmp_path, _nothing_survived, raise_on_error=False)

    assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())


def test_nothing_surviving_fails_the_run_and_names_the_damage(tmp_path: Path):
    with pytest.raises(NothingSurvivedError) as raised:
        _materialize(tmp_path, _nothing_survived)

    assert "Orders rejected all 2 rows" in str(raised.value)
    assert "2 by 'amount|min'" in str(raised.value)
    assert "orders_quarantine" in str(raised.value)


def test_nothing_surviving_raises_every_rule_check_to_error(tmp_path: Path):
    """Severity is the run's outcome, not the rule's: nothing was written to the valid table, so nothing is a warning."""
    result = _materialize(tmp_path, _nothing_survived, raise_on_error=False)
    evaluations = _evaluations(result)
    rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

    assert evaluations["dy_schema__dtypes"].passed
    assert rules
    assert all(e.severity == dg.AssetCheckSeverity.ERROR for e in rules)
    assert (
        dict(evaluations["dy_rule__amount__min"].metadata)["dy_failed_count"].value == 2
    )


# --- the temp file ---
# One entry per exit of the state machine, each as the frame that reaches it and the quarantine that decides it.
_EXITS = [
    pytest.param(clean_orders, None, id="everything survived"),
    pytest.param(mixed_orders, None, id="no quarantine"),
    pytest.param(mixed_orders, dg.AssetOut(), id="some survived"),
    pytest.param(hopeless_orders, dg.AssetOut(), id="nothing survived"),
    pytest.param(wrong_dtype_orders, None, id="shape"),
]


def _both_ways(
    frame: Callable[[], pl.DataFrame],
    quarantine: dg.AssetOut | None,
    *,
    temp_dir: str | None = None,
) -> tuple[dg.AssetsDefinition, dg.AssetsDefinition]:
    """The same transform declared twice, handing back the same frame eagerly and lazily.

    Same name, same schema, same rows: the two runs differ in the return type and in nothing else, which is what makes their events comparable.
    """

    @dataframely_asset(
        schema=Orders, name="orders", quarantine=quarantine, temp_dir=temp_dir
    )
    def eager() -> pl.DataFrame:
        return frame()

    @dataframely_asset(
        schema=Orders, name="orders", quarantine=quarantine, temp_dir=temp_dir
    )
    def lazy() -> pl.LazyFrame:
        return frame().lazy()

    return eager, lazy


def _reported(result: dg.ExecuteInProcessResult) -> object:
    """Everything the package itself told Dagster, in one comparable value.

    `path` is dropped because it names the directory a run wrote to, and each of the two runs gets its own. Nothing else is normalised, `cooccurrence` included: every table this package emits is ordered by construction, so two runs of the same rows are equal value for value.
    """
    return (
        {
            key.to_user_string(): {
                name: value for name, value in metadata.items() if name != "path"
            }
            for key, metadata in _materialized(result).items()
        },
        {
            name: (evaluation.passed, evaluation.severity, dict(evaluation.metadata))
            for name, evaluation in _evaluations(result).items()
        },
    )


def _written(root: Path) -> dict[str, pl.DataFrame]:
    """Every table a run left on disk, keyed by file name."""
    return {path.name: pl.read_parquet(path) for path in root.rglob("*.parquet")}


@pytest.mark.parametrize(("frame", "quarantine"), _EXITS)
def test_a_lazy_return_reports_exactly_what_an_eager_one_does(
    tmp_path: Path, frame: Callable[[], pl.DataFrame], quarantine: dg.AssetOut | None
):
    """The staging file moves where the data is materialized, not what happens to it afterwards.

    Asserted at every exit and over everything the package emits: the outs that materialized, the row counts, the statistics, the samples, the co-occurrence table, every check with its severity and metadata, and the bytes on disk.
    """
    eager, lazy = _both_ways(frame, quarantine)

    from_eager = _materialize(tmp_path / "eager", eager, raise_on_error=False)
    from_lazy = _materialize(tmp_path / "lazy", lazy, raise_on_error=False)

    assert from_lazy.success == from_eager.success
    assert _reported(from_lazy) == _reported(from_eager)
    assert set(_written(tmp_path / "lazy")) == set(_written(tmp_path / "eager"))
    for name, table in _written(tmp_path / "lazy").items():
        assert_frame_equal(table, _written(tmp_path / "eager")[name])


@pytest.mark.parametrize(("frame", "quarantine"), _EXITS)
def test_the_landing_is_removed_whichever_exit_the_run_takes(
    tmp_path: Path, frame: Callable[[], pl.DataFrame], quarantine: dg.AssetOut | None
):
    """Including the two exits whose whole purpose is that nothing is written, which are the ones a `finally` would be needed for if the file outlived the read-back."""
    staging = tmp_path / "staging"
    staging.mkdir()
    _, lazy = _both_ways(frame, quarantine, temp_dir=str(staging))

    _materialize(tmp_path / "store", lazy, raise_on_error=False)

    assert list(staging.iterdir()) == []


def test_the_landing_sinks_with_the_streaming_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The one clause of this design with no observable consequence, and the one the whole argument rests on.

    A staging step that collected the plan and wrote the frame would produce byte-identical results, pass every other assertion here, and keep exactly the peak the staging exists to remove. So the call is pinned rather than the output: `sink_parquet`, with the engine named.

    Counted as a set rather than a list, because polars reaches its own `sink_parquet` again on the way through and the number of times it does is its business, not this package's.
    """
    engines: list[object] = []
    sink = pl.LazyFrame.sink_parquet

    def spy(frame: pl.LazyFrame, path: Path, **kwargs: Any) -> Any:
        engines.append(kwargs.get("engine"))
        return sink(frame, path, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", spy)
    _, lazy = _both_ways(clean_orders, None)

    assert _materialize(tmp_path, lazy).success
    assert set(engines) == {"streaming"}


def test_an_eager_return_never_lands(tmp_path: Path):
    """A frame the user already materialized has nothing left to stream, so staging it would be pure cost.

    Asserted by pointing the staging file at a directory that does not exist: a run that would stage there cannot succeed, and this one does.
    """
    eager, _ = _both_ways(clean_orders, None, temp_dir=str(tmp_path / "absent"))

    assert _materialize(tmp_path / "store", eager).success


def test_a_lazy_return_lands_where_temp_dir_says(tmp_path: Path):
    """The other half of the same assertion, and the reason the setting exists: the default is the container's ephemeral disk, so a deployment has to be able to move it.

    A missing directory raises rather than being created, deliberately. The setting is set to move the staging file off that disk, so a mistyped path quietly created there is the failure somebody set it to avoid.
    """
    absent = tmp_path / "absent"
    _, lazy = _both_ways(clean_orders, None, temp_dir=str(absent))

    with pytest.raises(FileNotFoundError) as raised:
        _materialize(tmp_path / "store", lazy)

    assert str(absent) in str(raised.value)


def test_the_temp_dir_environment_variable_reaches_the_landing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    """The house-style tier, asserted through a materialization rather than through `resolve`: the decorator reads the variable where the asset is declared, so the value it resolved has to survive the trip to the executing step and reach the staging file."""
    absent = tmp_path / "absent"
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_TEMP_DIR", str(absent))
    _, lazy = _both_ways(clean_orders, None)

    with pytest.raises(FileNotFoundError) as raised:
        _materialize(tmp_path / "store", lazy)

    assert str(absent) in str(raised.value)


def test_the_shape_check_runs_before_the_landing(tmp_path: Path):
    """Resolving a plan's columns and dtypes costs nothing, so a wrong-shaped frame is refused before a single row is streamed.

    Asserted through a staging file that cannot work: the shape error is what arrives, so nothing ever tried to write there.
    """
    _, lazy = _both_ways(wrong_dtype_orders, None, temp_dir=str(tmp_path / "absent"))

    with pytest.raises(SchemaShapeError):
        _materialize(tmp_path / "store", lazy)


# --- collapsed checks ---
@dataframely_asset(
    schema=Orders, name="orders", quarantine=dg.AssetOut(), check_granularity="column"
)
def _by_column() -> pl.DataFrame:
    return mixed_orders()


@dataframely_asset(
    schema=Orders, name="orders", quarantine=dg.AssetOut(), check_granularity="schema"
)
def _by_schema() -> pl.DataFrame:
    return mixed_orders()


def _members(evaluation: dg.AssetCheckEvaluation) -> dict[object, object]:
    """The failure count each member rule of a collapsed check reported."""
    table = dict(evaluation.metadata)["dy_rules"]
    assert isinstance(table, dg.TableMetadataValue)
    return {record.data["rule"]: record.data["failed"] for record in table.records}


def test_a_collapsed_check_fails_when_any_rule_it_reports_for_failed(tmp_path: Path):
    """Collapsing changes how many checks there are, not what a run found."""
    evaluations = _evaluations(_materialize(tmp_path, _by_column))
    failed = {name for name, e in evaluations.items() if not e.passed}

    assert failed == {"dy_col__amount", "dy_col__email", "dy_schema__rules"}
    assert all(
        e.severity == dg.AssetCheckSeverity.WARN
        for name, e in evaluations.items()
        if name != "dy_schema__dtypes"
    )


def test_a_collapsed_check_reports_the_failure_count_of_every_member_rule(
    tmp_path: Path,
):
    """The count is what collapsing would otherwise cost, so a rule set carries one per member rather than a single total: rules are not comparable by row, because one row can break several."""
    evaluations = _evaluations(_materialize(tmp_path, _by_column))

    assert _members(evaluations["dy_col__email"]) == {
        "email|nullability": 0,
        "email|check__lowercase": 1,
        "email|max_length": 0,
    }
    assert _members(evaluations["dy_col__amount"]) == {
        "amount|nullability": 0,
        "amount|min": 1,
    }


def test_a_collapsed_check_carries_the_live_expression_of_every_member(tmp_path: Path):
    """The same property a rule check has, kept per member: a tightened bound shows up in the timeline rather than orphaning it."""
    evaluation = _evaluations(_materialize(tmp_path, _by_column))["dy_col__amount"]
    table = dict(evaluation.metadata)["dy_rules"]

    assert isinstance(table, dg.TableMetadataValue)
    assert all(record.data["expr"] for record in table.records)
    assert any('col("amount")' in str(record.data["expr"]) for record in table.records)


def test_schema_granularity_reports_every_rule_through_one_check(tmp_path: Path):
    evaluations = _evaluations(_materialize(tmp_path, _by_schema))
    members = _members(evaluations["dy_schema__rules"])

    assert set(evaluations) == {"dy_schema__dtypes", "dy_schema__rules"}
    assert len(members) == len(Orders._validation_rules(with_cast=False))
    assert {rule: count for rule, count in members.items() if count} == {
        "amount|min": 1,
        "email|check__lowercase": 1,
        "paid_orders_have_amount": 1,
    }


def test_a_clean_run_passes_every_collapsed_check(tmp_path: Path):
    """A rule set reports 0 per member rather than going quiet, so a clean run is a row in its history."""

    @dataframely_asset(schema=Orders, name="orders", check_granularity="column")
    def spotless() -> pl.DataFrame:
        return clean_orders()

    evaluations = _evaluations(_materialize(tmp_path, spotless))

    assert all(e.passed for e in evaluations.values())
    assert set(_members(evaluations["dy_col__email"]).values()) == {0}


def test_collapsing_the_checks_does_not_collapse_the_quarantine(tmp_path: Path):
    """Per-row attribution stays per rule at every granularity: the check list is a display decision, and the quarantine is the data."""
    _materialize(tmp_path, _by_column)
    quarantine = pl.read_parquet(tmp_path / "orders_quarantine.parquet")

    assert "dy_rule__amount__min" in quarantine.columns
    assert not [name for name in quarantine.columns if name.startswith("dy_col__")]
