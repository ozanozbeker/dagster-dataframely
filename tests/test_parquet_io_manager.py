"""Tests for `DataframelyParquetIOManager`.

The seam is `dg.materialize` in-process against `tmp_path`. What Dagster ends up holding is the whole external behaviour of an IO manager: the materialization metadata, and the bytes on disk.
The manager is schema-agnostic, so every asset here is a plain `@dg.asset` returning a polars frame. That is the difference `test_csv_io_manager.py` exists to cover: everything both managers share is exercised here, on the format that needs no schema.
"""

import datetime as dt
import logging
from decimal import Decimal
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from dagster_dataframely import (
    DataframelyParquetIOManager,
    DataFramePartitions,
    LazyFramePartitions,
    UnwritableDtypeError,
)

# The dtypes a round trip could plausibly get wrong: Decimal, Duration, Binary and a nested List. Nulls ride along in every column that admits them.
_ORDERS = pl.DataFrame(
    {
        "order_id": ["a", "b"],
        "amount": pl.Series(
            [Decimal("1.50"), Decimal("2.25")], dtype=pl.Decimal(10, 2)
        ),
        "placed_at": pl.Series(
            [dt.datetime(2026, 1, 1, 12, tzinfo=dt.UTC), None],
            dtype=pl.Datetime("us", "UTC"),
        ),
        "took": pl.Series([dt.timedelta(hours=2), None], dtype=pl.Duration("us")),
        "payload": pl.Series([b"\x00\x01", None], dtype=pl.Binary),
        "tags": pl.Series([["x", "y"], []], dtype=pl.List(pl.String)),
    }
)


@dg.asset(name="orders")
def _orders() -> pl.DataFrame:
    return _ORDERS


_DAYS = dg.StaticPartitionsDefinition(["2026-01-01", "2026-01-02"])


def _stamped(day: str) -> pl.DataFrame:
    """`_ORDERS` carrying the partition key it belongs to, so a file holding the wrong partition's rows is visible."""
    return _ORDERS.with_columns(pl.lit(day).alias("day"))


@dg.asset(name="orders_by_day", partitions_def=_DAYS)
def _orders_by_day() -> pl.DataFrame:
    return _stamped(dg.AssetExecutionContext.get().partition_key)


def _materialize(
    tmp_path: Path,
    *assets: dg.AssetsDefinition,
    instance: dg.DagsterInstance | None = None,
    partition_key: str | None = None,
    selection: str | None = None,
    raise_on_error: bool = True,
) -> dg.ExecuteInProcessResult:
    """Runs `assets` with the manager rooted at `tmp_path`."""
    return dg.materialize(
        list(assets),
        resources={"io_manager": DataframelyParquetIOManager(base_dir=str(tmp_path))},
        instance=instance,
        partition_key=partition_key,
        selection=selection,
        raise_on_error=raise_on_error,
    )


@pytest.fixture
def orders_metadata(tmp_path: Path) -> dict[str, dg.MetadataValue]:
    """Materializes `orders` once and returns the metadata on its materialization."""
    result = _materialize(tmp_path, _orders)
    (event,) = result.get_asset_materialization_events()
    return dict(event.step_materialization_data.materialization.metadata)


def test_a_written_frame_reads_back_equal(tmp_path: Path) -> None:
    """The round trip is the manager's whole job; every dtype in `_ORDERS` survives it."""
    read_back: list[pl.DataFrame] = []

    @dg.asset(name="orders_copy")
    def orders_copy(orders: pl.DataFrame) -> None:
        read_back.append(orders)

    assert _materialize(tmp_path, _orders, orders_copy).success
    assert_frame_equal(read_back[0], _ORDERS)


def test_a_lazy_annotation_reads_back_an_unexecuted_scan(tmp_path: Path) -> None:
    """A read has no object to dispatch on, so the input annotation is the only signal for what to build. `pl.LazyFrame` gets a scan; the frame it collects to is the one a `pl.DataFrame` annotation would have handed over whole (#52)."""
    read_back: list[object] = []

    @dg.asset(name="orders_copy")
    def orders_copy(orders: pl.LazyFrame) -> None:
        read_back.append(orders)

    assert _materialize(tmp_path, _orders, orders_copy).success
    (scan,) = read_back
    assert isinstance(scan, pl.LazyFrame)
    assert_frame_equal(scan.collect(), _ORDERS)


def test_a_downstream_query_pushes_down_into_the_scan(tmp_path: Path) -> None:
    """What the annotation is for, and the assertion that separates a scan from a frame someone called `.lazy()` on: the projection and the predicate land on the scan node itself, so the columns and rows the query drops are never decoded."""
    plans: list[str] = []

    @dg.asset(name="orders_copy")
    def orders_copy(orders: pl.LazyFrame) -> None:
        plans.append(
            orders.select("order_id").filter(pl.col("order_id") == "a").explain()
        )

    assert _materialize(tmp_path, _orders, orders_copy).success
    (plan,) = plans
    assert "Parquet SCAN" in plan
    assert f"PROJECT 1/{_ORDERS.width} COLUMNS" in plan
    assert "SELECTION" in plan


@dg.asset(name="orders")
def _lazy_orders() -> pl.LazyFrame:
    return _ORDERS.lazy()


@dg.asset(name="orders")
def _failing_orders() -> pl.LazyFrame:
    """A plan that resolves and then fails.

    The cast is legal to the schema resolver, so nothing refuses it before the rows move: `order_id` resolves to `Int64` and the conversion fails on the first row the engine reads. That is the only kind of failure this design has to survive, because a plan that cannot resolve never reaches the write at all.
    """
    return _ORDERS.lazy().with_columns(pl.col("order_id").cast(pl.Int64))


def test_a_lazy_output_streams_to_storage_and_reports_what_landed(
    tmp_path: Path,
) -> None:
    """The one path in this package where laziness runs end to end, because it is the only one with nothing to report on: no schema means no validation, no per-rule checks and no statistics pass, so nothing forces the plan into memory (#54).

    The absence of a warning is half of what this asserts. The line that used to stand here said a lazy sink was ruled out, and it is what shipping this removed.
    """
    # `caplog` never sees this. `context.log` writes to the event log, not the stdlib
    # logger tree, and the event log is where a user reads it anyway.
    with dg.DagsterInstance.ephemeral() as instance:
        result = _materialize(tmp_path, _lazy_orders, instance=instance)
        warnings = [
            entry.user_message
            for entry in instance.all_logs(result.run_id)
            if entry.level == logging.WARNING
        ]
    (event,) = result.get_asset_materialization_events()
    metadata = dict(event.step_materialization_data.materialization.metadata)
    written = tmp_path / "orders.parquet"

    assert result.success
    assert_frame_equal(pl.read_parquet(written), _ORDERS)
    assert metadata["bytes_written"].value == written.stat().st_size
    assert metadata["dagster/storage_kind"].value == "parquet"
    assert not warnings


def test_the_plan_streams_to_a_file_that_is_not_the_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Both halves of the write, pinned as calls because neither has an observable result.

    A manager that collected the plan and wrote the frame would leave the same bytes on disk and keep exactly the peak the sink exists to remove. And a manager that sank at the destination would leave the same bytes again, having truncated whatever was there before it knew the plan worked.

    Counted as a set rather than a list, because polars reaches its own `sink_parquet` again on the way through and the number of times it does is its business, not this package's.
    """
    sunk: list[tuple[str, object]] = []
    sink = pl.LazyFrame.sink_parquet

    def spy(frame: pl.LazyFrame, path: Path, **kwargs: Any) -> Any:
        sunk.append((str(path), kwargs.get("engine")))
        return sink(frame, path, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "sink_parquet", spy)

    assert _materialize(tmp_path, _lazy_orders).success
    assert {engine for _, engine in sunk} == {"streaming"}
    assert all(path != str(tmp_path / "orders.parquet") for path, _ in sunk)


def test_a_local_promote_renames_rather_than_rewriting_in_place(tmp_path: Path) -> None:
    """A rename is one metadata operation instead of a second copy of the whole file, and it is atomic, so a local destination is never an open empty file waiting to be filled.

    Asserted on the inode, which is what separates the two: a rewrite through the destination's own handle keeps the file that was already there and fills it, while a rename replaces it.
    """
    assert _materialize(tmp_path, _orders).success
    written = tmp_path / "orders.parquet"
    before = written.stat().st_ino

    assert _materialize(tmp_path, _lazy_orders).success
    assert written.stat().st_ino != before
    assert_frame_equal(pl.read_parquet(written), _ORDERS)


def test_a_failing_plan_writes_nothing_to_the_destination(tmp_path: Path) -> None:
    """Sinking at the destination would leave a zero-byte file where the plan died, or a non-empty partial one where it died late. Promoting on success is what keeps the failure invisible from storage."""
    result = _materialize(tmp_path, _failing_orders, raise_on_error=False)

    assert not result.success
    assert not list(tmp_path.rglob("*.parquet"))


def test_a_failing_plan_leaves_the_file_already_there_untouched(tmp_path: Path) -> None:
    """The same invariant `NothingSurvivedError` protects on the validation side, arriving from the storage side: a failed run does not replace a last-known-good file with a broken one."""
    assert _materialize(tmp_path, _orders).success
    written = tmp_path / "orders.parquet"
    before = written.read_bytes()

    assert not _materialize(tmp_path, _failing_orders, raise_on_error=False).success
    assert written.read_bytes() == before


def test_the_sink_lands_where_the_temp_dir_variable_says(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same knob `dataframely_asset` lands its own transform through, read here from the environment because a manager has no asset argument to take it from.

    A missing directory raises rather than being created, deliberately. The knob is set to move the landing off a container's ephemeral disk, so a mistyped path quietly created there is the failure somebody set it to avoid.
    """
    absent = tmp_path / "absent"
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_TEMP_DIR", str(absent))

    with pytest.raises(FileNotFoundError) as raised:
        _materialize(tmp_path, _lazy_orders)

    assert str(absent) in str(raised.value)
    assert not list(tmp_path.rglob("*.parquet"))


def test_an_eager_output_never_lands(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A frame the caller already materialized has nothing left to stream, so it is written where it always was.

    Asserted by pointing the landing at a directory that does not exist: a run that would land there cannot succeed, and this one does.
    """
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_TEMP_DIR", str(tmp_path / "absent"))

    assert _materialize(tmp_path, _orders).success
    assert_frame_equal(pl.read_parquet(tmp_path / "orders.parquet"), _ORDERS)


def test_each_partition_round_trips_under_its_own_key(tmp_path: Path) -> None:
    """`UPathIOManager.handle_output` resolves the partition path before it calls `dump_to_path`, so this manager's hooks are partition-blind by design and the layout falls out of the base class. Nothing here is the manager's own code, which is exactly why it is pinned: it works by inheritance and would otherwise break silently (#25)."""
    read_back: dict[str, pl.DataFrame] = {}

    @dg.asset(name="orders_copy", partitions_def=_DAYS)
    def orders_copy(orders_by_day: pl.DataFrame) -> None:
        read_back[dg.AssetExecutionContext.get().partition_key] = orders_by_day

    for day in _DAYS.get_partition_keys():
        assert _materialize(
            tmp_path, _orders_by_day, orders_copy, partition_key=day
        ).success

    assert sorted(path.name for path in (tmp_path / "orders_by_day").iterdir()) == [
        "2026-01-01.parquet",
        "2026-01-02.parquet",
    ]
    assert read_back["2026-01-01"].equals(_stamped("2026-01-01"))
    assert read_back["2026-01-02"].equals(_stamped("2026-01-02"))


def test_a_fan_in_arrives_as_a_dict_keyed_by_partition(tmp_path: Path) -> None:
    """An unpartitioned asset depending on every partition of a partitioned one gets a frame per partition, because the base manager calls `load_from_path` once per key and assembles the results. `load_from_path` is typed for a single file, which hides that `load_input` can return a dict, so the shape a user has to annotate is pinned here rather than left to be discovered at runtime.

    Annotated with the exported alias rather than the literal shape, because the alias is what the README tells a user to write. That makes this the alias's behavioural pin as well: Dagster type-checks the input against it, as the test below shows, so an alias that drifted from what the manager assembles fails here rather than in someone's project (#35).
    """
    read_back: dict[str, object] = {}

    @dg.asset(name="rollup")
    def rollup(orders_by_day: DataFramePartitions) -> None:
        read_back.update(orders_by_day)

    for day in _DAYS.get_partition_keys():
        _materialize(tmp_path, _orders_by_day, partition_key=day)

    assert _materialize(tmp_path, _orders_by_day, rollup, selection="rollup").success
    assert set(read_back) == set(_DAYS.get_partition_keys())
    assert all(isinstance(frame, pl.DataFrame) for frame in read_back.values())


def test_a_lazy_fan_in_arrives_as_a_dict_of_scans(tmp_path: Path) -> None:
    """The fan-in shape has to be unwrapped to find the element type, because `dict[str, pl.LazyFrame]` is what a user annotates and `pl.LazyFrame` is what decides the read. `LazyFramePartitions` is the exported spelling, so this is its behavioural pin as well (#52)."""
    read_back: dict[str, object] = {}

    @dg.asset(name="rollup")
    def rollup(orders_by_day: LazyFramePartitions) -> None:
        read_back.update(orders_by_day)

    for day in _DAYS.get_partition_keys():
        _materialize(tmp_path, _orders_by_day, partition_key=day)

    assert _materialize(tmp_path, _orders_by_day, rollup, selection="rollup").success
    assert set(read_back) == set(_DAYS.get_partition_keys())
    assert all(isinstance(scan, pl.LazyFrame) for scan in read_back.values())


def test_a_missing_partition_is_still_skipped_on_the_lazy_path(tmp_path: Path) -> None:
    """`allow_missing_partitions` is implemented by catching `FileNotFoundError` out of `load_from_path`, and a scan raises none: it returns a plan, and the miss surfaces at the caller's `collect()` long after the manager could have handled it.

    What keeps the miss inside the manager is that the scan is built on the handle the manager opens, so the open is what raises, on the lazy path and the eager one alike (#52).
    """
    read_back: dict[str, object] = {}

    @dg.asset(
        name="rollup",
        ins={"orders_by_day": dg.AssetIn(metadata={"allow_missing_partitions": True})},
    )
    def rollup(orders_by_day: LazyFramePartitions) -> None:
        read_back.update(orders_by_day)

    _materialize(tmp_path, _orders_by_day, partition_key="2026-01-01")

    assert _materialize(tmp_path, _orders_by_day, rollup, selection="rollup").success
    assert set(read_back) == {"2026-01-01"}


def test_the_natural_fan_in_annotation_is_rejected(tmp_path: Path) -> None:
    """The trap that makes the annotation above worth pinning: `pl.DataFrame` is the obvious spelling and it fails the Dagster type check, after the frames have already been read."""

    @dg.asset(name="rollup")
    def rollup(orders_by_day: pl.DataFrame) -> None:
        pass

    for day in _DAYS.get_partition_keys():
        _materialize(tmp_path, _orders_by_day, partition_key=day)

    with pytest.raises(dg.DagsterTypeCheckDidNotPass, match="DataFrame"):
        _materialize(tmp_path, _orders_by_day, rollup, selection="rollup")


def test_the_manager_emits_path_bytes_written_and_storage_kind(
    tmp_path: Path, orders_metadata: dict[str, dg.MetadataValue]
) -> None:
    """These three are what varied this run. `bytes_written` comes off the disk, so it reports the compression actually achieved rather than an in-memory estimate."""
    written = tmp_path / "orders.parquet"

    assert orders_metadata["path"].value == str(written)
    assert orders_metadata["bytes_written"].value == written.stat().st_size
    assert orders_metadata["dagster/storage_kind"].value == "parquet"


def test_the_manager_emits_no_column_schema(
    orders_metadata: dict[str, dg.MetadataValue],
) -> None:
    """`dagster/column_schema` describes the data, not the write, so the asset definition emits it."""
    assert "dagster/column_schema" not in orders_metadata


def test_the_manager_writes_no_sample_and_runs_no_statistics_pass(
    orders_metadata: dict[str, dg.MetadataValue],
) -> None:
    """Nothing beyond what varied this run, so no data sample and no statistics pass."""
    assert set(orders_metadata) == {"path", "bytes_written", "dagster/storage_kind"}


def test_an_unwritable_dtype_raises_before_the_write(tmp_path: Path) -> None:
    """Left to polars, `pl.Object` fails as a `ComputeError` from inside the writer. The manager gets there first, names the column, and leaves nothing behind."""

    @dg.asset(name="orders")
    def unwritable() -> pl.DataFrame:
        return pl.DataFrame(
            {"order_id": ["a"], "payload": pl.Series([object()], dtype=pl.Object)}
        )

    with pytest.raises(UnwritableDtypeError) as raised:
        _materialize(tmp_path, unwritable)

    assert "Column 'payload' (Object)" in str(raised.value)
    assert "drop it" in str(raised.value)
    assert "order_id" not in str(raised.value)
    assert not list(tmp_path.rglob("*.parquet"))


def test_the_error_reads_as_plural_for_several_columns(tmp_path: Path) -> None:
    """The message is the only thing a user sees of this error, so it agrees in number."""

    @dg.asset(name="orders")
    def unwritable() -> pl.DataFrame:
        return pl.DataFrame(
            {
                "payload": pl.Series([object()], dtype=pl.Object),
                "sidecar": pl.Series([object()], dtype=pl.Object),
            }
        )

    with pytest.raises(UnwritableDtypeError) as raised:
        _materialize(tmp_path, unwritable)

    assert "Columns 'payload' (Object), 'sidecar' (Object)" in str(raised.value)
    assert "drop them" in str(raised.value)


def test_an_output_that_is_not_a_frame_says_so(tmp_path: Path) -> None:
    """An asset that forgot its `-> None` annotation reaches the manager; it should not die on an `AttributeError` two frames down."""

    @dg.asset(name="orders")
    def not_a_frame():
        return None

    with pytest.raises(dg.DagsterInvariantViolationError, match="polars frames"):
        _materialize(tmp_path, not_a_frame)


@pytest.mark.parametrize(
    "base_dir", ["s3://bucket/prefix", "gs://bucket/prefix", "az://container/prefix"]
)
def test_a_cloud_uri_is_accepted_as_base_dir(base_dir: str) -> None:
    """Cloud support rests entirely on universal-pathlib, so the scheme has to survive path resolution."""
    manager = DataframelyParquetIOManager(base_dir=base_dir).create_io_manager(
        dg.build_init_resource_context()
    )

    # `_get_path` is private upstream API, and the only seam that answers "where would
    # this land" without reaching the network, which no test may do. Pin-and-assert
    # suite lands with #16.
    path = manager._get_path(dg.build_output_context(asset_key=dg.AssetKey(["orders"])))

    assert str(path) == f"{base_dir}/orders.parquet"
