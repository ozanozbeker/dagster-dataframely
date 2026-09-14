"""Runtime behaviour of `@dd.asset`, asserted through `dg.materialize`.

Every test asserts what Dagster ends up holding: the materialization events, the check evaluations, the metadata on both, and the bytes on disk. All six outcomes are covered. The frame and the quarantine declaration decide which of the five validating outcomes a frame reaches. The sixth is the skip, which has no frame to decide anything (#95).
"""

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

import dagster_dataframely as dd
from dagster_dataframely._settings import Granularity
from dagster_dataframely.errors import (
    ColumnSchemaError,
    DagsterDataframelyError,
    NothingSurvivedError,
    ValidationAbortError,
)
from dagster_dataframely.wiring import (
    AssetYield,
    check_specs,
    schema_metadata,
    validation_results,
)
from tests.scenario import (
    Orders,
    Yielded,
    check_evaluations,
    clean_orders,
    cooccurring_orders,
    materializations,
    materialize,
    mixed_orders,
    no_valid_orders,
    results,
    wrong_dtype_orders,
)


@dg.asset(name="raw_orders")
def _raw_orders() -> pl.DataFrame:
    return clean_orders()


@dd.asset(Orders)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return raw_orders


# --- a clean frame ---
def test_a_clean_frame_materializes_what_was_returned(tmp_path: Path):
    result = materialize(tmp_path, _raw_orders, orders)

    assert result.success
    assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())


def test_a_clean_run_emits_row_count():
    """The count is the valid rows, so `dg.build_metadata_bounds_checks` needs no setting from this package.

    Asserted on what the step yields, not on what the run recorded. `dagster-polars` counts the same rows itself, so a run's materialization cannot say which of the two put the key there.
    """
    yielded = results(orders(clean_orders()))  # pyrefly: ignore[bad-argument-type]
    metadata = yielded[dg.AssetKey(["orders"])].metadata or {}

    assert metadata["dagster/row_count"] == 3


def test_a_clean_run_reports_every_rule_as_passing_at_warn(tmp_path: Path):
    """Specs derive from the schema, so absence never breaks a rule's history."""
    evaluations = check_evaluations(materialize(tmp_path, _raw_orders, orders))
    rules = {name: e for name, e in evaluations.items() if name.startswith("dy_rule__")}

    assert len(evaluations) == len(list(orders.check_specs))
    assert len(rules) == len(evaluations) - 1  # every check but the column-schema check
    assert all(e.passed for e in evaluations.values())
    assert all(e.severity == dg.AssetCheckSeverity.WARN for e in rules.values())


def test_a_check_carries_its_rule_and_the_live_expression(tmp_path: Path):
    """A tightened bound then shows in the check's own history instead of orphaning it."""
    evaluation = check_evaluations(materialize(tmp_path, _raw_orders, orders))[
        "dy_rule__amount__min"
    ]
    metadata = dict(evaluation.metadata)

    assert metadata["dy_rule"].value == "amount|min"
    assert 'col("amount")' in str(metadata["dy_rule__expr"].value)


# The frame guard is `test_returned_result.py`'s: `_REFUSALS` drives every refused return
# through both a call and a run, and `test_the_frame_guard_names_every_route_out` pins the
# message. Nothing about it is specific to this module's arrangement.


# --- the object returned decides, never the annotation ---
# `@dg.asset` infers the output's `dagster_type` from the return annotation and fails the run
# on a mismatch. This decorator cannot. The annotation describes what the decorated function
# returned, `dagster_type` describes what the output stores, and filtering materializes the valid
# rows and the invalid rows, so the output holds a `DataFrame` whatever the function returned.
# Both mismatches below succeed. `tests/test_upstream_characterization.py` pins the plain-asset
# side of the contrast.
@pytest.mark.parametrize(
    ("lazy", "annotation"),
    [
        pytest.param(True, pl.DataFrame, id="lazy under an eager annotation"),
        pytest.param(False, pl.LazyFrame, id="eager under a lazy annotation"),
    ],
)
def test_an_annotation_that_disagrees_with_the_return_changes_nothing(
    tmp_path: Path, lazy: bool, annotation: type
):
    """Both write the same table. Only a type checker holds an annotation here, and it does: pyrefly reports `bad-return` on either pairing spelled out literally, so neither can be written as an ordinary decorated function."""

    def fn():
        return clean_orders().lazy() if lazy else clean_orders()

    # Set by hand so one body can carry an annotation that contradicts it. The function is declared here, because setting it on an imported one would leave the annotation on a module-level object every other test shares.
    fn.__annotations__ = {"return": annotation}
    asset = dd.asset(Orders, name="orders")(fn)

    assert materialize(tmp_path, asset).success
    assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())


# --- a decorated function that takes the context ---
# Nothing in the package enables this: `functools.wraps` puts the decorated function's signature in
# front of Dagster, so the parameter binds as it does on a bare `@dg.asset`.
# It is the supported way a partitioned asset reaches its own key, so a wrapper change that
# shadowed the signature has to fail a test here.
_DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])


def test_a_decorated_function_can_take_a_bare_context(tmp_path: Path):
    """The parameter is unannotated: that is the one spelling that survives a user-side `from __future__ import annotations`."""
    seen: dict[str, str] = {}

    @dd.asset(Orders, name="orders", partitions_def=_DAYS)
    # Unannotated: that is the spelling under test.
    def orders(context) -> pl.DataFrame:  # pyrefly: ignore[implicit-any-parameter]
        seen["partition"] = context.partition_key
        return clean_orders()

    result = materialize(tmp_path, orders, partition_key="2026-01-02")

    assert result.success
    assert seen == {"partition": "2026-01-02"}


def test_a_decorated_function_can_take_the_context_alongside_an_upstream_frame(
    tmp_path: Path,
):
    """The context binds first and the frames follow, so taking one does not cost the ordinary parameter binding."""
    seen: dict[str, object] = {}

    @dd.asset(Orders, name="orders")
    def orders(
        context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
    ) -> pl.DataFrame:
        seen["asset"] = context.asset_key.to_user_string()
        seen["rows"] = len(raw_orders)
        return raw_orders

    result = materialize(tmp_path, _raw_orders, orders)

    assert result.success
    assert seen == {"asset": "orders", "rows": 3}


def test_a_quarantined_function_keeps_the_context_it_declared(tmp_path: Path):
    """A quarantine prepends the context only to a function that declared none, so the one that did has to receive the step's own context rather than a second one.

    Partitioned, because reading its own partition key is why a quarantined asset declares the parameter at all, and a body handed the wrong context would read the wrong key.
    """
    seen: dict[str, str] = {}

    @dd.asset(Orders, name="orders", quarantine=True, partitions_def=_DAYS)
    def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
        seen["partition"] = context.partition_key
        return mixed_orders()

    result = materialize(tmp_path, orders, partition_key="2026-01-02")
    quarantine = pl.read_parquet(tmp_path / "orders_quarantine" / "2026-01-02.parquet")

    assert result.success
    assert seen == {"partition": "2026-01-02"}
    assert len(quarantine) == 3


# --- two assets sharing a name ---
def test_two_assets_sharing_a_name_under_different_prefixes_both_write(tmp_path: Path):
    """The op name is the step key, so the two steps must differ for the run to execute at all (#70). Both tables land under the prefixes their keys spell."""

    def shipments(prefix: str) -> dg.AssetsDefinition:
        @dd.asset(Orders, key_prefix=prefix, name="shipments")
        def _shipments() -> pl.DataFrame:
            return clean_orders()

        return _shipments

    result = materialize(tmp_path, shipments("alpha"), shipments("beta"))

    assert result.success
    assert_frame_equal(
        pl.read_parquet(tmp_path / "alpha" / "shipments.parquet"), clean_orders()
    )
    assert_frame_equal(
        pl.read_parquet(tmp_path / "beta" / "shipments.parquet"), clean_orders()
    )


# --- a frame whose columns do not match ---
@dd.asset(Orders, name="orders")
def _wrong_dtype() -> pl.DataFrame:
    return wrong_dtype_orders()


def test_a_wrong_dtype_aborts_and_names_the_column(tmp_path: Path):
    """A pipeline defect, not a data defect: the message has to be readable without opening a traceback."""
    with pytest.raises(ColumnSchemaError) as raised:
        materialize(tmp_path, _wrong_dtype)

    message = str(raised.value)

    assert "Column 'quantity' (expected Int32, got Int64) does not match Orders" in (
        message
    )


def test_a_missing_column_aborts_too(tmp_path: Path):
    @dd.asset(Orders, name="orders")
    def missing_column() -> pl.DataFrame:
        return clean_orders().drop("quantity")

    with pytest.raises(ColumnSchemaError) as raised:
        materialize(tmp_path, missing_column)

    assert "'quantity' (expected Int32, got <missing>)" in str(raised.value)


def test_the_column_schema_error_reads_as_plural_for_several_columns(tmp_path: Path):
    """The message is the only thing a user sees of this error, so it agrees in number."""

    @dd.asset(Orders, name="orders")
    def several() -> pl.DataFrame:
        return (
            clean_orders().drop("email").with_columns(pl.col("quantity").cast(pl.Int64))
        )

    with pytest.raises(ColumnSchemaError) as raised:
        materialize(tmp_path, several)

    assert "Columns 'email' (expected String, got <missing>), 'quantity' " in str(
        raised.value
    )
    assert "do not match Orders" in str(raised.value)


def test_the_column_schema_check_runs_before_row_filtering(tmp_path: Path):
    """Only the column-schema check reports, and nothing is written. No rule check evaluates, because no row was ever filtered."""
    result = materialize(tmp_path, _wrong_dtype, raise_on_error=False)
    evaluations = check_evaluations(result)
    column_schema = evaluations["dy_schema__columns"]

    assert not result.success
    assert set(evaluations) == {"dy_schema__columns"}
    assert not column_schema.passed
    assert column_schema.severity == dg.AssetCheckSeverity.ERROR
    assert not list(tmp_path.rglob("*.parquet"))


def test_the_column_schema_check_tabulates_every_offending_column(tmp_path: Path):
    result = materialize(tmp_path, _wrong_dtype, raise_on_error=False)
    metadata = dict(check_evaluations(result)["dy_schema__columns"].metadata)
    errors = metadata["dy_schema__errors"]

    assert isinstance(errors, dg.TableMetadataValue)
    assert [dict(record.data) for record in errors.records] == [
        {"column": "quantity", "expected": "Int32", "actual": "Int64"}
    ]


# --- invalid rows with no quarantine ---
@dd.asset(Orders, name="orders")
def _mixed() -> pl.DataFrame:
    return mixed_orders()


def test_invalid_rows_with_no_quarantine_fail_the_run(tmp_path: Path):
    """A partial table must never silently replace a complete one.

    The message names `quarantine=True` because this error is the one place a user who has not read the README learns the keyword exists.
    """
    with pytest.raises(ValidationAbortError) as raised:
        materialize(tmp_path, _mixed)

    assert "3 rows failed Orders validation" in str(raised.value)
    assert "1 by 'amount|min'" in str(raised.value)
    assert "never discards rows on your behalf" in str(raised.value)
    assert "quarantine=True" in str(raised.value)


def test_the_abort_writes_nothing(tmp_path: Path):
    result = materialize(tmp_path, _mixed, raise_on_error=False)

    assert not result.success
    assert not result.get_asset_materialization_events()
    assert not list(tmp_path.rglob("*.parquet"))


def test_the_abort_still_reports_every_rule(tmp_path: Path):
    """A failed run still says what failed and by how much."""
    evaluations = check_evaluations(materialize(tmp_path, _mixed, raise_on_error=False))
    failed = {name for name, e in evaluations.items() if not e.passed}

    assert evaluations["dy_schema__columns"].passed
    assert failed == {
        "dy_rule__amount__min",
        "dy_rule__email__check__lowercase",
        "dy_rule__paid_orders_have_amount",
    }
    assert (
        dict(evaluations["dy_rule__amount__min"].metadata)["dy_failed_count"].value == 1
    )


def test_a_frame_where_nothing_survives_aborts_the_same_way(tmp_path: Path):
    """With no quarantine the two frames are indistinguishable, because both discard everything. Only a declared quarantine separates them."""

    @dd.asset(Orders, name="orders")
    def nothing_survives() -> pl.DataFrame:
        return no_valid_orders()

    with pytest.raises(ValidationAbortError) as raised:
        materialize(tmp_path, nothing_survives)

    assert "2 rows failed Orders validation, 2 by 'amount|min'" in str(raised.value)
    assert not list(tmp_path.rglob("*.parquet"))


def test_the_abort_raises_every_rule_check_to_error(tmp_path: Path):
    """Severity is the run's outcome, not the rule's: nothing was written, so nothing is a warning."""
    evaluations = check_evaluations(materialize(tmp_path, _mixed, raise_on_error=False))
    rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

    assert rules
    assert all(e.severity == dg.AssetCheckSeverity.ERROR for e in rules)


# --- invalid rows with a quarantine, some surviving ---
_GOOD_KEY = dg.AssetKey(["orders"])


@dd.asset(Orders, name="orders", quarantine=True)
def _quarantined() -> pl.DataFrame:
    return mixed_orders()


def test_the_valid_rows_are_written_and_the_rest_are_quarantined(tmp_path: Path):
    """The middle case: 3 valid rows are written, 3 invalid ones are readable beside them, and downstream proceeds.

    One materialization, because the quarantine is evidence of a run, not a `dg.AssetOut` (ADR-0004). The file still lands beside the table, because the asset's own manager placed it (ADR-0006).
    """
    result = materialize(tmp_path, _quarantined)

    assert result.success
    assert set(materializations(result)) == {_GOOD_KEY}
    assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())
    assert pl.read_parquet(tmp_path / "orders_quarantine.parquet").height == 3


def test_the_materialization_says_where_the_rest_went(tmp_path: Path):
    """The address is computed, not read back off the manager. The borrowed context is not a real output, so whatever `path` or `Query` the manager emitted went nowhere (ADR-0006)."""
    metadata = materializations(materialize(tmp_path, _quarantined))[_GOOD_KEY].metadata

    assert metadata["dataframely/quarantine_address"] == dg.MetadataValue.text(
        "orders_quarantine"
    )
    assert metadata["dataframely/invalid_count"] == dg.MetadataValue.int(3)
    assert isinstance(metadata["dataframely/invalid_sample"], dg.TableMetadataValue)


def test_an_upstream_and_a_quarantine_leave_the_graph_alone(tmp_path: Path):
    """A quarantine adds no node, so an asset that declares one has the lineage it would have had without one."""

    @dd.asset(Orders, name="orders", quarantine=True)
    def downstream(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return mixed_orders()

    result = materialize(tmp_path, _raw_orders, downstream)

    assert result.success
    assert set(materializations(result)) == {dg.AssetKey(["raw_orders"]), _GOOD_KEY}
    assert len(result.get_step_success_events()) == 2


def test_a_quarantined_run_succeeds_with_every_check_at_warn(tmp_path: Path):
    """Declaring the quarantine consents to partial data, so an invalid row is a warning, not a failure."""
    evaluations = check_evaluations(materialize(tmp_path, _quarantined))
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
    """The two keys are unconditional. A check whose metadata appeared only on failed runs would have holes in its history."""
    evaluations = check_evaluations(materialize(tmp_path, _quarantined))
    rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

    assert any(e.passed for e in rules)
    assert any(not e.passed for e in rules)
    assert all({"dy_rule", "dy_rule__expr"} <= set(e.metadata) for e in rules)


def test_downstream_proceeds_on_the_data_that_is_fine(tmp_path: Path):
    """Writing the survivors is only worth anything if the run does not stop there. The rule checks are non-blocking, so a `WARN` never holds a consumer back."""
    seen: dict[str, int] = {}

    @dg.asset(name="reconciled")
    def reconciled(orders: pl.DataFrame) -> None:
        seen["rows"] = len(orders)

    result = materialize(tmp_path, _quarantined, reconciled)

    assert result.success
    assert seen == {"rows": 3}


def test_the_quarantine_holds_the_original_columns_plus_a_rule_column_each(
    tmp_path: Path,
):
    """`invalid()` was rejected as the content. Check-metadata samples are bounded, so without per-row attribution here the detail exists nowhere at volume."""
    materialize(tmp_path, _quarantined)
    quarantine = pl.read_parquet(tmp_path / "orders_quarantine.parquet")
    rule_columns = [name for name in quarantine.columns if name.startswith("dy_rule__")]
    checks = {spec.name for spec in _quarantined.check_specs} - {"dy_schema__columns"}

    assert quarantine.columns[: len(Orders.columns())] == list(Orders.columns())
    assert set(quarantine.columns) == set(Orders.columns()) | set(rule_columns)
    # Asserted against the written frame, not only the declaration, so the rule columns and
    # the check names cannot drift apart.
    assert set(rule_columns) == checks


def test_the_rule_columns_are_cast_from_enum_to_string(tmp_path: Path):
    """Mandatory, not defensive: a raw `Enum` panics the Delta writer with a Rust `unreachable!()`."""
    materialize(tmp_path, _quarantined)
    quarantine = pl.read_parquet(tmp_path / "orders_quarantine.parquet")
    rule_columns = [name for name in quarantine.columns if name.startswith("dy_rule__")]

    assert all(quarantine.schema[name] == pl.String for name in rule_columns)


def test_a_rule_column_says_which_rule_each_row_failed(tmp_path: Path):
    materialize(tmp_path, _quarantined)
    quarantine = pl.read_parquet(tmp_path / "orders_quarantine.parquet").sort(
        "order_id"
    )
    invalid_by_min = quarantine.filter(pl.col("dy_rule__amount__min") == "invalid")

    assert quarantine["dy_rule__amount__min"].to_list() == [
        "invalid",
        "valid",
        "valid",
    ]
    assert invalid_by_min["order_id"].to_list() == ["ORD-4"]


def test_the_materialization_tabulates_the_rules_that_failed_together(tmp_path: Path):
    """One broken upstream field tripping three rules at once is one row here, not three unrelated counts."""

    @dd.asset(Orders, name="orders", quarantine=True)
    def cooccurring() -> pl.DataFrame:
        return cooccurring_orders()

    metadata = materializations(materialize(tmp_path, cooccurring))[_GOOD_KEY].metadata
    cooccurrence = metadata["dataframely/invalid_by_rules"]

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


def test_the_invalid_by_rules_table_leads_with_the_set_that_broke_the_most_rows(
    tmp_path: Path,
):
    """Rows sort biggest group first, ties by name. `cooccurrence_counts()` groups without `maintain_order`, so its order is arbitrary and two runs of the same data would diff.

    The sort happens here, not upstream, on the two keys the table is read by: how many rows a set broke, then the names, the sort already applied inside each set.
    """
    duplicate_break = (
        mixed_orders()
        .filter(pl.col("order_id") == "ORD-4")  # amount|min alone
        .with_columns(
            pl.lit("ORD-7").alias("order_id"),
            pl.lit("TRK-ORD-7-1").alias("tracking_id"),
        )
    )

    @dd.asset(Orders, name="orders", quarantine=True)
    def repeated() -> pl.DataFrame:
        return pl.concat([mixed_orders(), duplicate_break])

    metadata = materializations(materialize(tmp_path, repeated))[_GOOD_KEY].metadata
    cooccurrence = metadata["dataframely/invalid_by_rules"]

    assert isinstance(cooccurrence, dg.TableMetadataValue)
    assert [dict(record.data) for record in cooccurrence.records] == [
        {"rules": "dy_rule__amount__min", "count": 2},
        {"rules": "dy_rule__email__check__lowercase", "count": 1},
        {"rules": "dy_rule__paid_orders_have_amount", "count": 1},
    ]


# --- invalid rows with a quarantine, none surviving ---
@dd.asset(Orders, name="orders", quarantine=True)
def _nothing_survived() -> pl.DataFrame:
    return no_valid_orders()


def test_nothing_surviving_writes_the_quarantine_and_materializes_nothing(
    tmp_path: Path,
):
    """An empty table must never silently replace a last-known-good snapshot.

    The rows still land. The run is over and nothing materializes, so the file is the only account of what happened.
    """
    result = materialize(tmp_path, _nothing_survived, raise_on_error=False)

    assert not result.success
    assert materializations(result) == {}
    assert pl.read_parquet(tmp_path / "orders_quarantine.parquet").height == 2
    assert not (tmp_path / "orders.parquet").exists()


def test_nothing_surviving_leaves_the_last_known_good_table_intact(tmp_path: Path):
    """The same key, written clean and then run again on a frame nothing survives."""

    @dd.asset(Orders, name="orders", quarantine=True)
    def spotless() -> pl.DataFrame:
        return clean_orders()

    materialize(tmp_path, spotless)
    materialize(tmp_path, _nothing_survived, raise_on_error=False)

    assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), clean_orders())


def test_nothing_surviving_fails_the_run_and_names_the_damage(tmp_path: Path):
    with pytest.raises(NothingSurvivedError) as raised:
        materialize(tmp_path, _nothing_survived)

    assert "All 2 rows failed Orders validation" in str(raised.value)
    assert "2 by 'amount|min'" in str(raised.value)
    assert "orders_quarantine" in str(raised.value)


def test_nothing_surviving_raises_every_rule_check_to_error(tmp_path: Path):
    """Same rule as the abort: nothing was written to the valid table, so nothing is a warning."""
    result = materialize(tmp_path, _nothing_survived, raise_on_error=False)
    evaluations = check_evaluations(result)
    rules = [e for name, e in evaluations.items() if name.startswith("dy_rule__")]

    assert evaluations["dy_schema__columns"].passed
    assert rules
    assert all(e.severity == dg.AssetCheckSeverity.ERROR for e in rules)
    assert (
        dict(evaluations["dy_rule__amount__min"].metadata)["dy_failed_count"].value == 2
    )


# --- no source data ---
# A partition that has no file and never will is neither a failure nor an empty table (#95).
# The skip is asserted here against what Dagster ends up holding, and in `TestOutcomeSelection`
# against what leaves the generator.
@dd.asset(Orders, name="orders")
def _skipping() -> pl.DataFrame | None:
    return None


@dd.asset(Orders, name="orders", quarantine=True)
def _skipping_with_quarantine() -> pl.DataFrame | None:
    return None


@pytest.mark.parametrize(
    "asset", [_skipping, _skipping_with_quarantine], ids=["bare", "quarantined"]
)
def test_a_skipped_run_succeeds_and_materializes_nothing(
    tmp_path: Path, asset: dg.AssetsDefinition
):
    """Failing the run would say the pipeline is broken, and writing zero rows would say an empty report arrived. Neither is true, so the partition stays unmaterialized and the step still succeeds."""
    result = materialize(tmp_path, asset)

    assert result.success
    assert materializations(result) == {}
    assert not list(tmp_path.rglob("*.parquet"))


def test_a_skipped_runs_checks_claim_no_materialization(tmp_path: Path):
    """The skip was designed against one worry: a passing check on a skipped partition claiming the last run that did have data. Dagster leaves the target empty because this run has no materialization to point at, so the check reports on nothing and says so."""
    evaluations = check_evaluations(materialize(tmp_path, _skipping))

    assert all(e.target_materialization_data is None for e in evaluations.values())


@pytest.mark.parametrize("granularity", ["rule", "column", "schema"])
def test_a_skip_answers_whatever_check_list_the_asset_declared(
    tmp_path: Path, granularity: Granularity
):
    """Collapsing the rules into checks changes how many checks there are, so the skip must answer the list the asset declared, not a list of its own."""

    @dd.asset(Orders, name="orders", check_granularity=granularity)
    def skipping() -> pl.DataFrame | None:
        return None

    evaluations = check_evaluations(materialize(tmp_path, skipping))

    assert set(evaluations) == {spec.name for spec in skipping.check_specs}
    assert all(e.passed for e in evaluations.values())


def test_a_hand_wired_plain_asset_reaches_the_skip_through_output_required(
    tmp_path: Path,
):
    """The README tells a hand-wirer that `output_required=False` buys the skip on a single-out asset, so this pins the claim. `validation_results` is the same function either way (ADR-0001), so the two agree by construction."""

    @dg.asset(
        name="orders",
        output_required=False,
        metadata=schema_metadata(Orders),
        check_specs=check_specs(Orders, asset="orders"),
    )
    def orders_by_hand(context: dg.AssetExecutionContext) -> AssetYield:
        yield from validation_results(Orders, None, valid_key=context.asset_key)

    result = materialize(tmp_path, orders_by_hand)

    assert result.success
    assert materializations(result) == {}
    assert len(check_evaluations(result)) == len(list(orders_by_hand.check_specs))


# --- the lazy return ---
# One entry per outcome, each as the frame that reaches it and the quarantine that decides it.
_OUTCOMES = [
    pytest.param(clean_orders, False, id="everything survived"),
    pytest.param(mixed_orders, False, id="no quarantine"),
    pytest.param(mixed_orders, True, id="some survived"),
    pytest.param(no_valid_orders, True, id="nothing survived"),
    pytest.param(wrong_dtype_orders, False, id="column_schema"),
]


def _both_ways(
    frame: Callable[[], pl.DataFrame], quarantine: bool
) -> tuple[dg.AssetsDefinition, dg.AssetsDefinition]:
    """Declare the same decorated function twice, returning the same frame eagerly and lazily.

    Same name, same schema, same rows: the two runs differ in the return type and nothing else, so their events are comparable.
    """

    @dd.asset(Orders, name="orders", quarantine=quarantine)
    def eager() -> pl.DataFrame:
        return frame()

    @dd.asset(Orders, name="orders", quarantine=quarantine)
    def lazy() -> pl.LazyFrame:
        return frame().lazy()

    return eager, lazy


# Nothing compares the two returns outcome by outcome. `validation_results` calls
# `frame.lazy()` unconditionally and no code past it branches on the return type, so the
# equality has one code path to hold it up rather than five. `check_results` states it
# directly in two lines (`test_a_lazy_frame_reports_exactly_what_an_eager_one_does`), and
# `test_a_lazy_return_folds_the_same_way` covers the decorator's own stage.


@pytest.mark.parametrize("lazily", [False, True], ids=["eager", "lazy"])
def test_the_filter_runs_on_the_streaming_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lazily: bool
):
    """The engine choice has no observable output, so this covers the call rather than the result.

    Filtering on the in-memory engine would produce byte-identical results and pass every other assertion here. It would also keep the plan's own peak, which the streaming engine exists to remove. Both return types run, because there is one path through `Schema.filter` and a `DataFrame` return takes it too.
    """
    engines: list[object] = []
    collect_all = pl.collect_all

    def spy(frames: Any, **kwargs: Any) -> Any:
        engines.append(kwargs.get("engine"))
        return collect_all(frames, **kwargs)

    monkeypatch.setattr(pl, "collect_all", spy)
    eager, lazy = _both_ways(clean_orders, quarantine=False)

    assert materialize(tmp_path, lazy if lazily else eager).success
    assert engines == ["streaming"]


def _counted(frame: pl.DataFrame) -> tuple[pl.LazyFrame, list[int]]:
    """Build a plan over `frame` that records the height of every batch the engine pulls through it.

    Rows seen divided by rows held gives the execution count: the streaming engine may batch a frame, but every execution pulls every row through once.
    """
    seen: list[int] = []

    def count(batch: pl.DataFrame) -> pl.DataFrame:
        seen.append(batch.height)
        return batch

    return frame.lazy().map_batches(count, streamable=True), seen


def test_a_lazy_return_executes_its_plan_once(tmp_path: Path):
    """`collect_all` runs the valid rows and the invalid rows off one cached evaluation, so the source is pulled through once.

    Collecting each of them on its own would pass every other assertion here and read the source twice. For a plan with an expensive upstream, that read is the whole cost of the run.
    """
    plan, seen = _counted(clean_orders())

    @dd.asset(Orders, name="orders")
    def lazy() -> pl.LazyFrame:
        return plan

    assert materialize(tmp_path, lazy).success
    assert sum(seen) == clean_orders().height


def test_a_plan_that_fails_the_column_schema_check_never_executes(tmp_path: Path):
    """Resolving a plan's columns and dtypes costs nothing, so a frame whose columns do not match is refused before a single row is pulled through it."""
    plan, seen = _counted(wrong_dtype_orders())

    @dd.asset(Orders, name="orders")
    def lazy() -> pl.LazyFrame:
        return plan

    with pytest.raises(ColumnSchemaError):
        materialize(tmp_path, lazy)
    assert seen == []


# --- collapsed checks ---
@dd.asset(Orders, name="orders", quarantine=True, check_granularity="column")
def _by_column() -> pl.DataFrame:
    return mixed_orders()


@dd.asset(Orders, name="orders", quarantine=True, check_granularity="schema")
def _by_schema() -> pl.DataFrame:
    return mixed_orders()


def _members(evaluation: dg.AssetCheckEvaluation) -> dict[object, object]:
    """Read the failure count each member rule of a collapsed check reported."""
    table = dict(evaluation.metadata)["dy_rules"]
    assert isinstance(table, dg.TableMetadataValue)
    return {record.data["rule"]: record.data["failed"] for record in table.records}


def test_a_collapsed_check_fails_when_any_rule_it_reports_for_failed(tmp_path: Path):
    """Collapsing the rules into checks changes how many checks there are, not what a run found."""
    evaluations = check_evaluations(materialize(tmp_path, _by_column))
    failed = {name for name, e in evaluations.items() if not e.passed}

    assert failed == {"dy_col__amount", "dy_col__email", "dy_schema__rules"}
    assert all(
        e.severity == dg.AssetCheckSeverity.WARN
        for name, e in evaluations.items()
        if name != "dy_schema__columns"
    )


def test_a_collapsed_check_reports_the_failure_count_of_every_member_rule(
    tmp_path: Path,
):
    """Collapsing the rules would otherwise lose the per-rule count, so a rule set carries one per member, not a single total. A total would not add up by row, because one row can break several rules."""
    evaluations = check_evaluations(materialize(tmp_path, _by_column))

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
    """The same property a rule check has, kept per member: a tightened bound shows in the collapsed check's history instead of orphaning it."""
    evaluation = check_evaluations(materialize(tmp_path, _by_column))["dy_col__amount"]
    table = dict(evaluation.metadata)["dy_rules"]

    assert isinstance(table, dg.TableMetadataValue)
    assert all(record.data["expr"] for record in table.records)
    assert any('col("amount")' in str(record.data["expr"]) for record in table.records)


def test_schema_granularity_reports_every_rule_through_one_check(tmp_path: Path):
    evaluations = check_evaluations(materialize(tmp_path, _by_schema))
    members = _members(evaluations["dy_schema__rules"])

    assert set(evaluations) == {"dy_schema__columns", "dy_schema__rules"}
    assert len(members) == len(Orders._validation_rules(with_cast=False))
    assert {rule: count for rule, count in members.items() if count} == {
        "amount|min": 1,
        "email|check__lowercase": 1,
        "paid_orders_have_amount": 1,
    }


def test_a_clean_run_passes_every_collapsed_check(tmp_path: Path):
    """A rule set reports 0 per member rather than going quiet, so a clean run is a row in its history."""

    @dd.asset(Orders, name="orders", check_granularity="column")
    def spotless() -> pl.DataFrame:
        return clean_orders()

    evaluations = check_evaluations(materialize(tmp_path, spotless))

    assert all(e.passed for e in evaluations.values())
    assert set(_members(evaluations["dy_col__email"]).values()) == {0}


def test_collapsing_the_checks_does_not_collapse_the_quarantine(tmp_path: Path):
    """Per-row attribution stays per rule at every granularity: the check list is a display decision, and the quarantine is the data."""
    materialize(tmp_path, _by_column)
    quarantine = pl.read_parquet(tmp_path / "orders_quarantine.parquet")

    assert "dy_rule__amount__min" in quarantine.columns
    assert not [name for name in quarantine.columns if name.startswith("dy_col__")]


# --- the six outcomes, reached by calling `validation_results` directly ---
class TestOutcomeSelection:
    """Which of the six outcomes a frame reaches, asserted by calling `validation_results` directly.

    The tests above assert what Dagster ends up holding, which takes a run. These assert which objects leave the generator, which writer calls it made and which error ended it. None of that needs a run, an IO manager or a `tmp_path`.

    The writer appends to a list and returns a fixed address. That is all `validation_results` knows about a writer. `tests/test_quarantine.py` asserts a real one against a real manager.

    The helpers live on this class because they read a drained generator, where `tests/scenario.py`'s read a finished run. `results` is the exception and is imported: which keys got a materialization is the same question either way.

    The key is built by hand because no asset declares it. That is safe only because nothing consumes it.
    """

    VALID = dg.AssetKey(["orders"])
    ADDRESS = "orders_quarantine"

    @classmethod
    def _drained(
        cls, frame: pl.DataFrame | None, *, quarantine: bool
    ) -> tuple[Yielded, list[pl.DataFrame], DagsterDataframelyError | None]:
        """Run `validation_results` to exhaustion, keeping what it yielded, what it wrote, and whatever ended it.

        Three of the six outcomes raise after yielding, so draining with `list()` alone would discard the results that say what happened.
        """
        written: list[pl.DataFrame] = []

        def writer(invalid: pl.DataFrame) -> str:
            written.append(invalid)
            return cls.ADDRESS

        yielded: Yielded = []
        results = validation_results(
            Orders,
            frame,
            valid_key=cls.VALID,
            quarantine_writer=writer if quarantine else None,
        )
        try:
            for result in results:
                yielded.append(result)
        except DagsterDataframelyError as error:
            return yielded, written, error
        return yielded, written, None

    @staticmethod
    def _metadata(yielded: Yielded) -> Mapping[str, Any]:
        """Read the one materialization's metadata, or nothing when the outcome yielded none."""
        materializations = [r for r in yielded if isinstance(r, dg.MaterializeResult)]
        if not materializations:
            return {}
        (materialization,) = materializations
        return materialization.metadata or {}

    @staticmethod
    def _checks(yielded: Yielded) -> list[dg.AssetCheckResult]:
        """List every check result. All are standalone, at every outcome (ADR-0002)."""
        return [result for result in yielded if isinstance(result, dg.AssetCheckResult)]

    @classmethod
    def _rules(cls, yielded: Yielded) -> list[dg.AssetCheckResult]:
        """List the rule checks alone.

        The column-schema check is not a rule: it is built without a severity, so it keeps Dagster's default instead of the one the run's outcome derives.
        """
        return [c for c in cls._checks(yielded) if c.check_name != "dy_schema__columns"]

    def test_a_clean_frame_never_calls_a_declared_writer(self):
        """An empty quarantine is not written, so an empty quarantine means something.

        Declared, because `written == []` states nothing on an asset with no writer to call.
        """
        yielded, written, error = self._drained(clean_orders(), quarantine=True)

        assert error is None
        assert list(results(yielded)) == [self.VALID]
        assert written == []

    def test_a_clean_frame_says_nothing_about_invalid_rows(self):
        """The four keys are absent, not zero. A run that held nothing back has nothing to report, and `dataframely/invalid_count: 0` would read as a claim about a quarantine that does not exist.

        Read as the difference between the two runs rather than as a prefix. The valid rows' own sample is under `dataframely/` too, so a prefix would catch a key a clean run is meant to carry.
        """
        clean, _, _ = self._drained(clean_orders(), quarantine=True)
        partial, _, _ = self._drained(mixed_orders(), quarantine=True)

        assert set(self._metadata(partial)) - set(self._metadata(clean)) == {
            "dataframely/quarantine_address",
            "dataframely/invalid_count",
            "dataframely/invalid_by_rules",
            "dataframely/invalid_sample",
        }

    def test_a_wrong_column_schema_reports_the_check_and_writes_nothing(self):
        yielded, written, error = self._drained(wrong_dtype_orders(), quarantine=True)

        assert isinstance(error, ColumnSchemaError)
        assert list(results(yielded)) == []
        assert written == []
        assert [c.check_name for c in self._checks(yielded)] == ["dy_schema__columns"]

    def test_invalid_rows_with_no_writer_write_nothing_but_still_report(self):
        yielded, written, error = self._drained(mixed_orders(), quarantine=False)

        assert isinstance(error, ValidationAbortError)
        assert list(results(yielded)) == []
        assert written == []
        assert any(not c.passed for c in self._checks(yielded))

    def test_invalid_rows_with_a_writer_write_the_table_and_the_quarantine(self):
        yielded, written, error = self._drained(mixed_orders(), quarantine=True)
        (invalid,) = written

        assert error is None
        assert list(results(yielded)) == [self.VALID]
        assert len(invalid) == 3
        assert "dy_rule__amount__min" in invalid.columns

    def test_a_partial_run_reports_the_invalid_rows_on_the_materialization(self):
        """The materialization is where a reader already looks. Only the writer knows where the rows went, so the address is all the run can say about the place."""
        yielded, _, _ = self._drained(mixed_orders(), quarantine=True)
        metadata = self._metadata(yielded)

        assert metadata["dataframely/quarantine_address"] == self.ADDRESS
        assert metadata["dataframely/invalid_count"] == 3
        assert isinstance(
            metadata["dataframely/invalid_by_rules"], dg.TableMetadataValue
        )
        assert isinstance(metadata["dataframely/invalid_sample"], dg.TableMetadataValue)

    def test_the_invalid_sample_is_bounded_by_the_row_sample_setting(self):
        """One number governs both samples, so consenting to real rows in the event log is one decision."""
        yielded, _, _ = self._drained(mixed_orders(), quarantine=True)
        sample = self._metadata(yielded)["dataframely/invalid_sample"]
        assert isinstance(sample, dg.TableMetadataValue)

        bounded = validation_results(
            Orders,
            mixed_orders(),
            valid_key=self.VALID,
            quarantine_writer=lambda _: self.ADDRESS,
            row_sample=1,
        )
        (materialization,) = [r for r in bounded if isinstance(r, dg.MaterializeResult)]
        narrowed = (materialization.metadata or {})["dataframely/invalid_sample"]
        assert isinstance(narrowed, dg.TableMetadataValue)

        assert len(sample.records) == 3
        assert len(narrowed.records) == 1

    def test_nothing_surviving_writes_the_rows_and_yields_no_table(self):
        """Every row is written to the quarantine. The valid table is skipped, not materialized empty, so a last-known-good table survives."""
        yielded, written, error = self._drained(no_valid_orders(), quarantine=True)
        (invalid,) = written

        assert isinstance(error, NothingSurvivedError)
        assert list(results(yielded)) == []
        assert len(invalid) == 2

    def test_an_abort_puts_the_address_on_every_check_instead(self):
        """No materialization can carry it, and a failed run still has to say where its evidence went.

        Compared as a `TextMetadataValue`, which is what `_addressed` puts there: `_replace` skips the normalization the constructor does, so the address is wrapped by hand.
        """
        yielded, _, _ = self._drained(no_valid_orders(), quarantine=True)
        checks = self._checks(yielded)

        assert checks
        assert all(
            (c.metadata or {})["dataframely/quarantine_address"]
            == dg.MetadataValue.text(self.ADDRESS)
            for c in checks
        )

    def test_the_abort_names_the_address_the_writer_returned(self):
        _, _, error = self._drained(no_valid_orders(), quarantine=True)

        assert self.ADDRESS in str(error)

    def test_nothing_surviving_without_a_writer_aborts_instead(self):
        """A quarantine turns three outcomes into five; without one this is the same abort as any other failing row."""
        yielded, written, error = self._drained(no_valid_orders(), quarantine=False)

        assert isinstance(error, ValidationAbortError)
        assert list(results(yielded)) == []
        assert written == []

    def test_no_check_ever_rides_a_materialization(self):
        """Checks are yielded standalone, including on the outcome that writes a table to bundle them onto.

        One outcome, because `checks_for` builds every result the same way whatever the outcome, so five would exercise one code path five times. Standalone checks keep an asset built on `validation_results` callable in a unit test: direct invocation satisfies a check output only from a standalone result (ADR-0002). No test above states this, because a run flattens the two forms into one event stream.
        """
        yielded, _, _ = self._drained(mixed_orders(), quarantine=True)

        assert not [
            result
            for result in yielded
            if isinstance(result, dg.MaterializeResult) and result.check_results
        ]
        assert self._checks(yielded)

    def test_severity_follows_the_runs_outcome_rather_than_the_rule(self):
        """The same invalid rows warn when they have somewhere to go and error when they do not."""
        warned, _, _ = self._drained(mixed_orders(), quarantine=True)
        errored, _, _ = self._drained(mixed_orders(), quarantine=False)

        assert {c.severity for c in self._rules(warned)} == {dg.AssetCheckSeverity.WARN}
        assert {c.severity for c in self._rules(errored)} == {
            dg.AssetCheckSeverity.ERROR
        }

    # --- the sixth outcome: the decorated function returned `None` ---
    @pytest.mark.parametrize("quarantine", [False, True], ids=["bare", "quarantined"])
    def test_a_skip_writes_no_table_and_raises_nothing(self, quarantine: bool):
        """The one outcome the asset's declaration does not decide. A quarantine changes what an invalid row costs, and a skip has no rows."""
        yielded, written, error = self._drained(None, quarantine=quarantine)

        assert error is None
        assert list(results(yielded)) == []
        assert written == []

    def test_a_skip_still_answers_every_check(self):
        """A check spec is a non-optional op output, so a step that answers none of them fails with `did not return an output for non-optional output`. `test_a_skipped_run_succeeds_and_materializes_nothing` asserts the same against a real run."""
        skipped, _, _ = self._drained(None, quarantine=False)
        clean, _, _ = self._drained(clean_orders(), quarantine=False)

        assert {c.check_name for c in self._checks(skipped)} == {
            c.check_name for c in self._checks(clean)
        }

    def test_a_skips_checks_pass_because_the_rules_ran_over_an_empty_frame(self):
        """The pass is computed, not fabricated. Every rule evaluates over zero rows and none fails, so the result reads the same as a clean run."""
        yielded, _, _ = self._drained(None, quarantine=False)

        assert all(c.passed for c in self._checks(yielded))
        assert {c.severity for c in self._rules(yielded)} == {
            dg.AssetCheckSeverity.WARN
        }
