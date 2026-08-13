"""Tests for `DataframelyCSVIOManager` and the codecs it writes through.

Everything here is asserted through `dg.materialize` in-process against `tmp_path`, as for parquet. The two managers share one implementation, so the behaviour that has no format in it is exercised once, on parquet. What is here is CSV's own: the five encodings, the schema the read needs, the refusals that keep Polars from panicking, and the handful of shared assertions whose expected value is the format.

Most assets carry `schema_metadata` on a plain `@dg.asset`. The manager reads the carrier off definition metadata and does not care how it got there, so putting it there by hand keeps these tests about the manager. One test at the end runs the decorator instead, to prove the two halves meet.
"""

import datetime as dt
import logging
import warnings
from decimal import Decimal
from pathlib import Path
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl
import pytest
from polars.testing import assert_frame_equal, assert_series_equal

import dagster_dataframely as dd
from tests import scenario

_CODEC_COLUMNS = ("took", "tags", "coords", "address", "payload")


class Shapes(dy.Schema):
    """One column per dtype a CSV cell cannot hold, beside three a read gets wrong without a schema.

    Its own schema rather than `scenario.Orders`, which carries three of the five codecs but no `Struct` and no `Array`. Widening the shared schema for two columns would rewrite the expected check list and Columns tab of every other suite.
    """

    key = dy.String(primary_key=True)
    took = dy.Duration(nullable=True)
    tags = dy.List(dy.String(), nullable=True)
    coords = dy.Array(dy.Float64(), 2, nullable=True)
    address = dy.Struct({"city": dy.String(nullable=True)}, nullable=True)
    payload = dy.Binary(nullable=True)
    # Not encoded, but inference reads a Decimal back as a float, an Enum as a string, and
    # an empty string as a null. All three are the schema's work on the read path.
    amount = dy.Decimal(10, 2, nullable=True)
    status = dy.Enum(["new", "paid"], nullable=True)
    note = dy.String(nullable=True)


# One row of everything CSV is bad at, one row of nulls, one row of empty-but-not-null.
_SHAPES = pl.DataFrame(
    {
        "key": ["awkward", "null", "empty"],
        "took": pl.Series(
            [dt.timedelta(hours=2, microseconds=3), None, dt.timedelta(0)],
            dtype=pl.Duration("us"),
        ),
        "tags": pl.Series([["a,b", 'say "hi"'], None, []], dtype=pl.List(pl.String)),
        "coords": pl.Series(
            [[1.5, -2.5], None, [0.0, 0.0]], dtype=pl.Array(pl.Float64, 2)
        ),
        "address": pl.Series(
            [{"city": "Rome, IT"}, None, {"city": None}],
            dtype=pl.Struct({"city": pl.String}),
        ),
        "payload": pl.Series([b'\x00,\x01"', None, b""], dtype=pl.Binary),
        "amount": pl.Series(
            [Decimal("1.50"), None, Decimal("0.00")], dtype=pl.Decimal(10, 2)
        ),
        "status": pl.Series(["new", None, "paid"], dtype=pl.Enum(["new", "paid"])),
        "note": pl.Series(
            ['has,comma and "quotes"\nand a newline', None, ""], dtype=pl.String
        ),
    }
)


@dg.asset(name="shapes", metadata=dd.wiring.schema_metadata(Shapes))
def _shapes() -> pl.DataFrame:
    return _SHAPES


def _materialize(
    tmp_path: Path,
    *assets: dg.AssetsDefinition,
    instance: dg.DagsterInstance | None = None,
    partition_key: str | None = None,
    selection: str | None = None,
) -> dg.ExecuteInProcessResult:
    """Runs `assets` with the manager rooted at `tmp_path`."""
    return dg.materialize(
        list(assets),
        resources={"io_manager": dd.DataframelyCSVIOManager(base_dir=str(tmp_path))},
        instance=instance,
        partition_key=partition_key,
        selection=selection,
    )


def _round_trip(
    tmp_path: Path,
    source: dg.AssetsDefinition,
    name: str,
    instance: dg.DagsterInstance | None = None,
) -> pl.DataFrame:
    """Writes `source` and reads it back through a downstream asset, which is the only way in."""
    read_back: list[pl.DataFrame] = []

    @dg.asset(name=f"{name}_copy", ins={name: dg.AssetIn()})
    def copy(**frames: pl.DataFrame) -> None:
        read_back.append(frames[name])

    assert _materialize(tmp_path, source, copy, instance=instance).success
    return read_back[0]


@pytest.fixture
def round_tripped(tmp_path: Path) -> pl.DataFrame:
    """`_SHAPES` written and read back once, for the tests that only inspect the result."""
    return _round_trip(tmp_path, _shapes, "shapes")


@pytest.fixture
def shapes_metadata(tmp_path: Path) -> dict[str, dg.MetadataValue]:
    """Materializes `shapes` once and returns the metadata on its materialization."""
    result = _materialize(tmp_path, _shapes)
    (event,) = result.get_asset_materialization_events()
    return dict(event.step_materialization_data.materialization.metadata)


@pytest.fixture
def run_log(tmp_path: Path) -> list[str]:
    """Every message logged by one run that writes `_SHAPES` and reads it back."""
    with dg.DagsterInstance.ephemeral() as instance:
        # `caplog` never sees these. `context.log` writes to the event log, not the stdlib
        # logger tree, and the event log is where a user reads them anyway.
        _round_trip(tmp_path, _shapes, "shapes", instance=instance)
        run_id = next(iter(instance.get_runs())).run_id
        return [entry.user_message for entry in instance.all_logs(run_id)]


def test_the_specimen_carries_the_dtypes_its_schema_declares() -> None:
    """Restated rather than derived, so a specimen that drifts fails here instead of failing every round trip below for the wrong reason."""
    assert dict(_SHAPES.schema) == {
        name: column.dtype for name, column in Shapes.columns().items()
    }


def test_a_written_frame_reads_back_equal(round_tripped: pl.DataFrame) -> None:
    """The whole job: every dtype survives, including the three the schema alone rescues and the empty strings inference would return as nulls."""
    assert_frame_equal(round_tripped, _SHAPES)


def test_a_lazy_annotation_reads_back_a_scan_that_decodes_on_collect(
    tmp_path: Path,
) -> None:
    """Going lazy changes nothing about where CSV's read gets its dtypes: `scan_csv` takes the same `schema_overrides`, and every codec is a pure expression builder over `with_columns`, which behaves identically on both frame types (#52)."""
    read_back: list[object] = []

    @dg.asset(name="shapes_copy")
    def copy(shapes: pl.LazyFrame) -> None:
        read_back.append(shapes)

    assert _materialize(tmp_path, _shapes, copy).success
    (scan,) = read_back
    assert isinstance(scan, pl.LazyFrame)
    # The decode sits on top of a scan node rather than on a frame already read.
    assert "Csv SCAN" in scan.explain()
    assert_frame_equal(scan.collect(), _SHAPES)


def test_the_lazy_decode_resolves_no_schema_of_its_own(tmp_path: Path) -> None:
    """`LazyFrame.columns` would answer which columns to decode by resolving the plan's schema, and Polars warns about it once per read. The decode asks `collect_schema()` instead, which is the same question without the warning."""
    read_back: list[object] = []

    @dg.asset(name="shapes_copy")
    def copy(shapes: pl.LazyFrame) -> None:
        read_back.append(shapes)

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        assert _materialize(tmp_path, _shapes, copy).success

    assert not [
        warning
        for warning in caught
        if issubclass(warning.category, pl.exceptions.PerformanceWarning)
    ]


@pytest.mark.parametrize("column", _CODEC_COLUMNS)
def test_each_codec_round_trips_through_commas_quotes_and_nulls(
    round_tripped: pl.DataFrame, column: str
) -> None:
    """One test name per codec, so a broken inverse says which dtype broke rather than which frame."""
    assert_series_equal(round_tripped[column], _SHAPES[column])


def test_the_file_on_disk_is_ordinary_csv(tmp_path: Path) -> None:
    """Read with every column as text: what a spreadsheet or a `cut` would see, and the only place the encodings themselves are visible."""
    _materialize(tmp_path, _shapes)
    raw = pl.read_csv(tmp_path / "shapes.csv", infer_schema=False)

    assert raw.columns == _SHAPES.columns
    assert raw["took"][0] == "7200000003"  # microseconds, the schema's own time unit
    assert raw["payload"][0] == "ACwBIg=="
    assert raw["tags"][0] == '{"tags":["a,b","say \\"hi\\""]}'
    assert raw["coords"][0] == '{"coords":[1.5,-2.5]}'
    # A struct needs no wrapper, so its cell holds the object a reader expects.
    assert raw["address"][0] == '{"city":"Rome, IT"}'
    # A null encodes to an empty cell in every column, whatever the codec.
    assert all(raw[column][1] is None for column in _CODEC_COLUMNS)


def test_the_encoded_columns_are_named_at_encode_time(run_log: list[str]) -> None:
    """The log is the codec's only home: materialization metadata is barred by the varied-this-run rule, and definition metadata cannot know which manager will bind."""
    (encoded,) = [message for message in run_log if message.startswith("Encoded")]

    assert "5 column(s)" in encoded
    assert all(f"'{column}'" in encoded for column in _CODEC_COLUMNS)
    assert "Duration(time_unit='us') as integer" in encoded
    assert "Binary as base64" in encoded
    assert "List(String) as JSON" in encoded


def test_the_decoded_columns_are_named_at_read_time(run_log: list[str]) -> None:
    """The read is where a user finds out the text they saw on disk was an encoding. Present tense, because a lazy read has queued the decode rather than run it and the line is the same one."""
    (decoded,) = [message for message in run_log if message.startswith("Decoding")]

    assert "5 column(s)" in decoded
    assert all(f"'{column}'" in decoded for column in _CODEC_COLUMNS)


@dg.asset(name="shapes", metadata=dd.wiring.schema_metadata(Shapes))
def _lazy_shapes() -> pl.LazyFrame:
    """`_SHAPES` handed over as a plan, for the writes that stream it."""
    return _SHAPES.lazy()


def test_a_lazy_output_writes_the_bytes_an_eager_one_does(tmp_path: Path) -> None:
    """A lazy output streams to storage here too, and every codec survives the change of engine: each one is an expression over `with_columns`, which behaves the same on a plan as on a frame (#54).

    Byte equality rather than a round trip, because the round trip would pass on a file whose quoting, null spelling or duration ticks had all quietly changed together.
    """
    assert _materialize(tmp_path / "lazy", _lazy_shapes).success
    assert _materialize(tmp_path / "eager", _shapes).success

    written = (tmp_path / "lazy" / "shapes.csv").read_bytes()
    assert written == (tmp_path / "eager" / "shapes.csv").read_bytes()


def test_a_lazy_output_still_names_the_columns_it_encoded(tmp_path: Path) -> None:
    """The log is the codec's only home on both paths. A sink that skipped the encode would write a file nothing can read back, and a sink that encoded silently would write one nobody knows to."""
    with dg.DagsterInstance.ephemeral() as instance:
        result = _materialize(tmp_path, _lazy_shapes, instance=instance)
        messages = [entry.user_message for entry in instance.all_logs(result.run_id)]

    assert result.success
    (encoded,) = [message for message in messages if message.startswith("Encoded")]
    assert "5 column(s)" in encoded
    assert all(f"'{column}'" in encoded for column in _CODEC_COLUMNS)


def test_the_csv_sink_runs_the_streaming_engine(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Covered per format rather than once on the base class, because the engine is named at each format's own call and byte equality above holds whichever engine ran.

    Counted as a set rather than a list, because Polars reaches its own `sink_csv` again on the way through and the number of times it does is its business, not this package's.
    """
    engines: list[object] = []
    sink = pl.LazyFrame.sink_csv

    def spy(frame: pl.LazyFrame, path: Path, **kwargs: Any) -> Any:
        engines.append(kwargs.get("engine"))
        return sink(frame, path, **kwargs)

    monkeypatch.setattr(pl.LazyFrame, "sink_csv", spy)

    assert _materialize(tmp_path, _lazy_shapes).success
    assert set(engines) == {"streaming"}


def _warnings(tmp_path: Path, asset: dg.AssetsDefinition) -> list[str]:
    """Materializes `asset` and returns everything it logged at warning level."""
    with dg.DagsterInstance.ephemeral() as instance:
        result = _materialize(tmp_path, asset, instance=instance)
        assert result.success
        return [
            entry.user_message
            for entry in instance.all_logs(result.run_id)
            if entry.level == logging.WARNING
        ]


def test_encoding_with_no_schema_at_all_warns(tmp_path: Path) -> None:
    """An encoding no read can undo is the silent failure this package exists to make visible, so it is a warning rather than an info line."""

    @dg.asset(name="plain")
    def plain() -> pl.DataFrame:
        return pl.DataFrame({"tags": pl.Series([["x"]], dtype=pl.List(pl.String))})

    assert any(
        "No schema on this asset declares 'tags'" in message
        for message in _warnings(tmp_path, plain)
    )


def test_encoding_a_column_the_schema_leaves_out_warns(tmp_path: Path) -> None:
    """Carrying a schema is not the same as being covered by it. The read decodes what the schema declares, so a column beside it is encoded and never decoded, and the warning names that column rather than the asset."""

    @dg.asset(name="extra", metadata=dd.wiring.schema_metadata(Shapes))
    def extra() -> pl.DataFrame:
        return _SHAPES.with_columns(
            pl.Series("aside", [["x"], [], None], dtype=pl.List(pl.String))
        )

    (warned,) = [
        message
        for message in _warnings(tmp_path, extra)
        if message.startswith("Encoded")
    ]

    assert "No schema on this asset declares 'aside'" in warned
    # The columns it does declare are not named as undecodable, only as encoded.
    assert "declares 'tags'" not in warned


def test_without_a_schema_an_encoded_column_comes_back_as_text(tmp_path: Path) -> None:
    """The manager does not guess. With no carrier the read is an ordinary inferred CSV read, which is what CSV was before any of this."""

    @dg.asset(name="plain")
    def plain() -> pl.DataFrame:
        return pl.DataFrame({"tags": pl.Series([["x"]], dtype=pl.List(pl.String))})

    assert _round_trip(tmp_path, plain, "plain")["tags"].to_list() == ['{"tags":["x"]}']


@pytest.mark.parametrize(
    ("column", "values", "dtype"),
    [
        ("anything", [object()], pl.Object),
        ("nested_payload", [[b"\x00"]], pl.List(pl.Binary)),
        (
            "nested_took",
            [{"d": dt.timedelta(hours=1)}],
            pl.Struct({"d": pl.Duration("us")}),
        ),
    ],
)
def test_a_dtype_with_no_codec_is_refused_before_the_write(
    tmp_path: Path, column: str, values: list[object], dtype: pl.DataType
) -> None:
    """`Binary` and `Duration` are encodable at the top level and unreachable below one: Polars panics writing binary to JSON, and writes a nested duration as ISO-8601 its own reader rejects. A Rust panic cannot be caught, so the manager gets there first."""

    @dg.asset(name="shapes")
    def unwritable() -> pl.DataFrame:
        return pl.DataFrame({"key": ["a"], column: pl.Series(values, dtype=dtype)})

    with pytest.raises(dd.errors.UnwritableDtypeError) as raised:
        _materialize(tmp_path, unwritable)

    assert f"'{column}'" in str(raised.value)
    assert "written to .csv" in str(raised.value)
    assert "key" not in str(raised.value)
    assert not list(tmp_path.rglob("*.csv"))


def test_a_frame_its_schema_does_not_describe_is_refused_before_the_write(
    tmp_path: Path,
) -> None:
    """The declaration is part of what gets written, because the decode reads every column back into the dtype it names.

    A `Duration('ns')` under a declared `Duration('us')` is the case that would otherwise land quietly: the file holds a tick count, the unit is nowhere in it, and the frame read back is a thousand times too long with nothing failing.
    """

    @dg.asset(name="shapes", metadata=dd.wiring.schema_metadata(Shapes))
    def nanoseconds() -> pl.DataFrame:
        return _SHAPES.with_columns(pl.col("took").cast(pl.Duration("ns")))

    with pytest.raises(dd.errors.SchemaShapeError) as raised:
        _materialize(tmp_path, nanoseconds)

    assert (
        "'took' (expected Duration(time_unit='us'), got Duration(time_unit='ns'))"
        in str(raised.value)
    )
    assert not list(tmp_path.rglob("*.csv"))


def test_the_manager_emits_path_bytes_written_and_storage_kind(
    tmp_path: Path, shapes_metadata: dict[str, dg.MetadataValue]
) -> None:
    """The same three keys as parquet, so what a materialization records does not depend on the format."""
    written = tmp_path / "shapes.csv"

    assert shapes_metadata["path"].value == str(written)
    assert shapes_metadata["bytes_written"].value == written.stat().st_size
    assert shapes_metadata["dagster/storage_kind"].value == "csv"


def test_the_manager_emits_no_column_schema_and_nothing_else(
    shapes_metadata: dict[str, dg.MetadataValue],
) -> None:
    """Reading the schema on both paths is not a licence to emit it. It describes the data, not the write, so the asset definition still owns it."""
    assert set(shapes_metadata) == {"path", "bytes_written", "dagster/storage_kind"}


@pytest.mark.parametrize(
    "base_dir", ["s3://bucket/prefix", "gs://bucket/prefix", "az://container/prefix"]
)
def test_a_cloud_uri_is_accepted_as_base_dir(base_dir: str) -> None:
    """Cloud support rests entirely on universal-pathlib, so the scheme has to survive path resolution."""
    manager = dd.DataframelyCSVIOManager(base_dir=base_dir).create_io_manager(
        dg.build_init_resource_context()
    )

    # `_get_path` is private upstream API, and the only route that answers "where would
    # this land" without reaching the network, which no test may do.
    path = manager._get_path(dg.build_output_context(asset_key=dg.AssetKey(["orders"])))

    assert str(path) == f"{base_dir}/orders.csv"


def test_the_schema_is_recovered_for_every_partition(tmp_path: Path) -> None:
    """The carrier hangs off the asset rather than off a partition, so each of the reads a fan-in assembles decodes against it.

    Parquet covers the layout on the argument that its hooks are partition-blind. This manager's read hook is the one that stopped being blind, so what it recovers per key is covered here rather than inherited.
    """
    days = dg.StaticPartitionsDefinition(["2026-01-01", "2026-01-02"])

    @dg.asset(
        name="by_day", partitions_def=days, metadata=dd.wiring.schema_metadata(Shapes)
    )
    def by_day() -> pl.DataFrame:
        return _SHAPES

    read_back: dict[str, pl.DataFrame] = {}

    @dg.asset(name="rollup")
    def rollup(by_day: dict[str, pl.DataFrame]) -> None:
        read_back.update(by_day)

    for day in days.get_partition_keys():
        assert _materialize(tmp_path, by_day, partition_key=day).success
    assert _materialize(tmp_path, by_day, rollup, selection="rollup").success

    assert set(read_back) == set(days.get_partition_keys())
    assert all(frame.equals(_SHAPES) for frame in read_back.values())


def test_the_door_puts_the_schema_where_the_manager_reads_it(tmp_path: Path) -> None:
    """The two halves meet. `dataframely_asset` writes the carrier onto the out and the manager reads it back off the same asset, so a `Duration` declared once survives storage without the engineer naming a format anywhere."""

    @dd.dataframely_asset(schema=scenario.Orders)
    def orders() -> pl.DataFrame:
        return scenario.clean_orders()

    assert_frame_equal(_round_trip(tmp_path, orders, "orders"), scenario.clean_orders())


def test_a_quarantine_decodes_the_same_columns_the_valid_table_does(
    tmp_path: Path,
) -> None:
    """The quarantine carries the same carrier, so the columns it inherits from the schema come back as themselves rather than as text.

    The rule columns beside them are the schema's own reserved namespace and no schema declares them, so they ride through the read on inference, which is what `String` wanted anyway.
    """

    @dd.dataframely_asset(schema=scenario.Orders, quarantine=dg.AssetOut())
    def orders() -> pl.DataFrame:
        return scenario.mixed_orders()

    read_back = _round_trip(tmp_path, orders, "orders_quarantine")
    _, failure = scenario.Orders.filter(scenario.mixed_orders())
    rejected = failure.invalid()

    assert read_back["fulfilled_in"].dtype == pl.Duration("us")
    assert read_back["payload"].dtype == pl.Binary
    assert read_back["tags"].dtype == pl.List(pl.String)
    assert_frame_equal(
        read_back.select(list(scenario.POLARS_SCHEMA)),
        rejected,
        check_row_order=False,
    )
    assert read_back["dy_rule__amount__min"].dtype == pl.String
