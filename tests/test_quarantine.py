"""What a quarantine is called, where it goes, and who puts it there.

`quarantine_path` and `build_quarantine_spec` are pure, so most of this file needs no run. The writers are not. The delegation tests are the only ones in the suite that care which IO manager is behind the asset.

Delegation is asserted against two managers (ADR-0006). The claim is that nothing in the package knows where the rows go, and one manager cannot show that. `PolarsParquetIOManager` is a `UPathIOManager` and `DuckDBPolarsIOManager` is a `DbIOManager`: one from each base class Dagster ships.

The multi-partition spelling is asserted here against a literal and pinned against `UPathIOManager`'s own path in `test_upstream_characterization.py`. This file says what the rule is; that one says whose rule it is.
"""

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from upath import UPath

from dagster_dataframely import build_quarantine_spec, dy_asset
from dagster_dataframely.errors import NothingSurvivedError
from dagster_dataframely.wiring import (
    file_writer,
    process,
    quarantine_frame,
    quarantine_path,
)
from tests.scenario import (
    WAREHOUSE_SCHEMA,
    Orders,
    cooccurring_orders,
    hopeless_orders,
    mixed_orders,
    storage,
    tables,
    warehouse,
)

_COLUMN_SCHEMA_KEY = "dagster/column_schema"
_ORDERS = dg.AssetKey(["orders"])
_DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])
_GRID = dg.MultiPartitionsDefinition(
    {"region": dg.StaticPartitionsDefinition(["eu", "us"]), "day": _DAYS}
)


def _invalid_rows() -> pl.DataFrame:
    """Build the frame a quarantine holds: invalid rows plus a rule column each."""
    _, failure = Orders.filter(mixed_orders())
    return quarantine_frame(Orders, failure)


# --- the path ---
def test_the_leaf_carries_the_suffix_so_a_shared_root_cannot_overwrite(tmp_path: Path):
    """`UPathIOManager` writes `orders.parquet` under the same quarantine_dir, so the suffix keeps the two side by side instead of one over the other. The delegating writer puts the same suffix on the asset key, so both routes name one thing."""
    path = quarantine_path(_ORDERS, tmp_path)

    assert path == UPath(tmp_path) / "orders_quarantine.parquet"


def test_a_path_is_a_upath_whatever_the_root_arrived_as(tmp_path: Path):
    """The decorator resolves a quarantine_dir to a `UPath`, but a hand-wirer reaching for the same rule holds whatever their config gave them."""
    paths = [
        quarantine_path(_ORDERS, quarantine_dir)
        for quarantine_dir in (tmp_path, str(tmp_path), UPath(tmp_path))
    ]

    assert all(isinstance(path, UPath) for path in paths)
    assert len(set(paths)) == 1


def test_a_key_prefix_becomes_directories_and_only_the_name_is_suffixed(tmp_path: Path):
    path = quarantine_path(dg.AssetKey(["sales", "orders"]), tmp_path)

    assert path == UPath(tmp_path) / "sales" / "orders_quarantine.parquet"


def test_a_partition_becomes_a_file_under_the_leaf(tmp_path: Path):
    """A directory rather than a longer file name, so a backfill of one partition rewrites one file."""
    path = quarantine_path(_ORDERS, tmp_path, "2026-01-02")

    assert path == UPath(tmp_path) / "orders_quarantine" / "2026-01-02.parquet"


def test_a_multi_partition_key_is_spelled_in_dimension_name_order(tmp_path: Path):
    """`day` before `region` because `d` sorts before `r`, never because of the order the dimensions were declared or the key was built."""
    key = dg.MultiPartitionKey({"region": "eu", "day": "2026-01-02"})

    path = quarantine_path(_ORDERS, tmp_path, key)

    assert path == UPath(tmp_path) / "orders_quarantine" / "2026-01-02" / "eu.parquet"


@pytest.mark.parametrize(
    ("partition_key", "tail"),
    [
        (None, "orders_quarantine.parquet"),
        ("2026-01-02", "orders_quarantine/2026-01-02.parquet"),
        (
            dg.MultiPartitionKey({"region": "eu", "day": "2026-01-02"}),
            "orders_quarantine/2026-01-02/eu.parquet",
        ),
    ],
    ids=["unpartitioned", "partitioned", "multi_partitioned"],
)
def test_a_cloud_root_is_the_same_rule(partition_key: str | None, tail: str):
    """One rule and one `UPath`, so nothing about the path changes when the quarantine_dir moves to object storage. Every form, because a `/` in the tail is where a scheme-aware path could behave differently."""
    path = quarantine_path(
        dg.AssetKey(["sales", "orders"]), "s3://bucket/warehouse", partition_key
    )

    assert str(path) == f"s3://bucket/warehouse/sales/{tail}"
    assert path.protocol == "s3"


@pytest.mark.parametrize(
    ("partition_key", "tail"),
    [
        ("/etc/passwd", "%2Fetc/passwd"),
        ("../../etc/passwd", "%2E%2E/%2E%2E/etc/passwd"),
    ],
    ids=["absolute", "traversal"],
)
def test_a_partition_key_cannot_climb_out_of_the_root(
    tmp_path: Path, partition_key: str, tail: str
):
    """A partition key can come from data, and both of these resolve outside the quarantine_dir unescaped: `pathlib` drops the left side of a join when the right side is absolute, and the OS walks `..` upward at write time.

    `FilesystemIOManager`'s pair of escapes, not the `UPathIOManager` base class's one, as upstream documents for a hierarchical filesystem.
    """
    path = quarantine_path(_ORDERS, tmp_path, partition_key)

    assert path == UPath(tmp_path) / f"orders_quarantine/{tail}.parquet"
    assert path.resolve().is_relative_to(UPath(tmp_path).resolve())


def test_a_partition_key_keeps_dots_that_are_not_a_segment(tmp_path: Path):
    """Only a whole `..` segment climbs. Escaping `my..backup` would rename a partition somebody chose."""
    path = quarantine_path(_ORDERS, tmp_path, "my..backup")

    assert path == UPath(tmp_path) / "orders_quarantine" / "my..backup.parquet"


# --- the spec ---
@dg.asset(key_prefix="sales", partitions_def=_DAYS)
def orders() -> None:
    """Customer orders."""


def test_the_spec_is_keyed_as_the_file_is_named():
    """The spec and the path are one rule, so a downstream dependency on the spec resolves to the file `quarantine_path` writes."""
    spec = build_quarantine_spec(Orders, orders)

    assert spec.key == dg.AssetKey(["sales", "orders_quarantine"])
    assert spec.key.path[-1] == quarantine_path(orders.key, "/warehouse").stem


def test_the_spec_depends_on_the_asset_the_invalid_rows_came_from():
    (dep,) = build_quarantine_spec(Orders, orders).deps

    assert dep.asset_key == dg.AssetKey(["sales", "orders"])


def test_the_spec_takes_the_partitions_off_the_definition():
    """Read, not passed, so the spec cannot disagree with the asset whose invalid rows it holds."""
    assert build_quarantine_spec(Orders, orders).partitions_def == _DAYS


def test_the_description_names_the_asset_the_rows_came_from():
    description = build_quarantine_spec(Orders, orders).description

    assert description is not None
    assert "sales/orders" in description


def test_the_columns_tab_states_no_constraint():
    """These rows broke the constraints, so a `not null` on a column full of nulls would be false about every row."""
    table = build_quarantine_spec(Orders, orders).metadata[_COLUMN_SCHEMA_KEY]

    assert isinstance(table, dg.TableSchema)
    assert table.constraints == dg.TableConstraints(other=[])
    assert all(
        column.constraints == dg.TableColumnConstraints() for column in table.columns
    )


def test_the_columns_tab_carries_a_column_for_every_rule():
    table = build_quarantine_spec(Orders, orders).metadata[_COLUMN_SCHEMA_KEY]
    names = [column.name for column in table.columns]

    assert names[: len(Orders.columns())] == list(Orders.columns())
    assert "dy_rule__amount__min" in names
    assert "dy_rule__paid_orders_have_amount" in names


@pytest.mark.parametrize(
    "form",
    [dg.AssetKey(["sales", "orders"]), ["sales", "orders"]],
    ids=["asset_key", "sequence"],
)
def test_a_key_form_is_keyed_exactly_as_the_definition_form(
    form: dg.AssetKey | Sequence[str],
):
    """A generated or foreign asset has no definition to read, and the answer must be the same."""
    assert build_quarantine_spec(Orders, form).key == dg.AssetKey(
        ["sales", "orders_quarantine"]
    )


def test_a_bare_string_is_a_single_key_part():
    assert build_quarantine_spec(Orders, "orders").key == dg.AssetKey(
        ["orders_quarantine"]
    )


@pytest.mark.parametrize(
    "form",
    [dg.AssetKey(["orders"]), ["orders"], "orders"],
    ids=["asset_key", "sequence", "string"],
)
def test_a_key_form_is_unpartitioned_unless_told_otherwise(
    form: dg.AssetKey | Sequence[str],
):
    """A key carries no partitions, so nothing can be inferred and guessing would state something false."""
    assert build_quarantine_spec(Orders, form).partitions_def is None


@pytest.mark.parametrize(
    "form",
    [dg.AssetKey(["orders"]), ["orders"], "orders"],
    ids=["asset_key", "sequence", "string"],
)
def test_a_key_form_takes_the_partitions_it_is_given(
    form: dg.AssetKey | Sequence[str],
):
    assert build_quarantine_spec(Orders, form, partitions_def=_DAYS).partitions_def == (
        _DAYS
    )


def test_a_definition_and_a_partitions_def_together_raise():
    """Two sources of one fact, and nothing decides which wins. Raising at call time is the only answer that cannot drift."""
    with pytest.raises(dg.DagsterInvariantViolationError) as raised:
        build_quarantine_spec(Orders, orders, partitions_def=_DAYS)

    assert "partitions_def" in str(raised.value)
    assert "sales/orders" in str(raised.value)


# --- the file and the graph together ---
def _loaded(
    tmp_path: Path,
    *,
    partitions_def: dg.PartitionsDefinition | None = None,
    partition_key: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Write a real quarantine frame where the rule says, then read it back downstream.

    The quarantine_dir is the manager's `base_dir`, the arrangement all three of these tests prove. Nothing in the run knows about this package: the spec supplies a key and `PolarsParquetIOManager` resolves it to the file already there.

    Returns
    -------
    What the downstream asset loaded, and what was written for it to load.
    """
    invalid = _invalid_rows()
    path = quarantine_path(_ORDERS, tmp_path, partition_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as file:
        invalid.write_parquet(file)
    loaded: dict[str, pl.DataFrame] = {}

    @dg.asset(partitions_def=partitions_def)
    def audit(orders_quarantine: pl.DataFrame) -> None:
        loaded["rows"] = orders_quarantine

    result = dg.materialize(
        [audit, build_quarantine_spec(Orders, "orders", partitions_def=partitions_def)],
        partition_key=partition_key,
        resources=storage(tmp_path),
    )

    assert result.success
    return loaded["rows"], invalid


def test_a_downstream_asset_loads_the_file_with_no_help_from_this_package(
    tmp_path: Path,
):
    """Every column survives, rule columns included, so a reader depends on the file itself, not a projection of it."""
    loaded, written = _loaded(tmp_path)

    assert_frame_equal(loaded, written)


def test_a_partitioned_downstream_asset_loads_the_partition_it_asked_for(
    tmp_path: Path,
):
    """One file per partition under the leaf, so a consumer of one partition reads one file."""
    loaded, written = _loaded(
        tmp_path, partitions_def=_DAYS, partition_key="2026-01-02"
    )

    assert_frame_equal(loaded, written)


def test_a_multi_partitioned_file_lands_where_the_manager_looks(tmp_path: Path):
    """The spelling asserted against a literal above, asserted here against the manager that has to find it."""
    loaded, written = _loaded(
        tmp_path,
        partitions_def=_GRID,
        partition_key=dg.MultiPartitionKey({"region": "eu", "day": "2026-01-02"}),
    )

    assert_frame_equal(loaded, written)


# --- the delegating writer ---
_DAY = "2026-08-01"
_ONE_DAY = dg.StaticPartitionsDefinition([_DAY])

_PARTITION_EXPR = {"partition_expr": "ordered_at"}
"""What `DbIOManager` needs to delete a partition before rewriting it, and what a user already declares for their own partitioned table. The borrowed context copies definition metadata, so it comes along; nothing here reads it."""

_REJECTING = [
    pytest.param(mixed_orders, 3, False, id="partial"),
    pytest.param(hopeless_orders, 2, True, id="nothing survived"),
]
"""The two outcomes with failing rows and somewhere to put them. The third has no writer, so nothing to place."""


def _delegating(
    frame: Callable[[], pl.DataFrame],
    *,
    partitioned: bool,
    partition_expr: bool = True,
) -> dg.AssetsDefinition:
    """Declare the asset the delegation tests run.

    Prefixed with the warehouse schema, because `DbIOManager` addresses a table as `<schema>.<name>` off the key's last part and reads the schema off its own configuration.
    """

    @dy_asset(
        Orders,
        name="orders",
        key_prefix=WAREHOUSE_SCHEMA,
        quarantine=True,
        partitions_def=_ONE_DAY if partitioned else None,
        metadata=_PARTITION_EXPR if partition_expr else None,
    )
    def orders() -> pl.DataFrame:
        return frame()

    return orders


def _run(
    asset: dg.AssetsDefinition,
    resources: dict[str, Any],
    *,
    partitioned: bool,
    aborts: bool,
) -> None:
    """Materialize the asset, tolerating the abort one of the outcomes raises."""
    result = dg.materialize(
        [asset],
        resources=resources,
        partition_key=_DAY if partitioned else None,
        raise_on_error=False,
    )

    assert result.success is not aborts


@pytest.mark.parametrize(("frame", "rejected", "aborts"), _REJECTING)
@pytest.mark.parametrize("partitioned", [False, True], ids=["whole", "partitioned"])
def test_a_filesystem_manager_puts_the_quarantine_beside_the_table(
    tmp_path: Path,
    frame: Callable[[], pl.DataFrame],
    rejected: int,
    aborts: bool,
    partitioned: bool,
):
    """A parquet file beside the table, with no configuration. The path is the manager's own answer to the quarantine's asset key, partition layout included."""
    _run(
        _delegating(frame, partitioned=partitioned),
        storage(tmp_path),
        partitioned=partitioned,
        aborts=aborts,
    )
    leaf = (
        f"orders_quarantine/{_DAY}.parquet"
        if partitioned
        else "orders_quarantine.parquet"
    )

    assert pl.read_parquet(tmp_path / WAREHOUSE_SCHEMA / leaf).height == rejected


@pytest.mark.parametrize(("frame", "rejected", "aborts"), _REJECTING)
@pytest.mark.parametrize("partitioned", [False, True], ids=["whole", "partitioned"])
def test_a_database_manager_puts_the_quarantine_in_a_table_beside_it(
    tmp_path: Path,
    frame: Callable[[], pl.DataFrame],
    rejected: int,
    aborts: bool,
    partitioned: bool,
):
    """The case a quarantine_dir cannot express. A warehouse stores tables, so the invalid rows belong in one beside the table they came from, not in a file elsewhere.

    The partitioned runs also forward `partition_expr` off the definition metadata. Without it `DbIOManager` refuses, as the test below asserts.
    """
    _run(
        _delegating(frame, partitioned=partitioned),
        warehouse(tmp_path),
        partitioned=partitioned,
        aborts=aborts,
    )
    written = tables(tmp_path)

    assert "orders_quarantine" in written
    assert written["orders_quarantine"].height == rejected
    assert "dy_rule__amount__min" in written["orders_quarantine"].columns


def test_the_quarantine_lands_even_though_the_run_dies(tmp_path: Path):
    """ADR-0004 promised the rows survive a run that fails, and delegation keeps that promise: the writer is called inside the asset body, before anything raises."""
    with pytest.raises(NothingSurvivedError):
        dg.materialize(
            [_delegating(hopeless_orders, partitioned=False)],
            resources=storage(tmp_path),
        )

    assert (tmp_path / WAREHOUSE_SCHEMA / "orders_quarantine.parquet").exists()


def test_a_partitioned_database_asset_fails_with_the_managers_own_error(tmp_path: Path):
    """`partition_expr` is `DbIOManager`'s requirement, not this package's, and the user already declares it for their own partitioned table. The failure stays as upstream words it; a guard here would state the requirement twice."""
    result = dg.materialize(
        [_delegating(mixed_orders, partitioned=True, partition_expr=False)],
        resources=warehouse(tmp_path),
        partition_key=_DAY,
        raise_on_error=False,
    )
    (failure,) = result.get_step_failure_events()

    assert not result.success
    assert "partition_expr" in str(failure.step_failure_data.error)


def test_the_address_is_the_key_the_manager_resolved(tmp_path: Path):
    """The run reports the asset key, not a path. The manager's own `path` or `Query` goes nowhere, because the borrowed context is not a real output, so the package says the one thing it knows (ADR-0006)."""
    result = dg.materialize(
        [_delegating(mixed_orders, partitioned=False)], resources=storage(tmp_path)
    )
    (event,) = result.get_asset_materialization_events()
    metadata = event.step_materialization_data.materialization.metadata

    assert metadata["dataframely/quarantine_address"] == dg.MetadataValue.text(
        f"{WAREHOUSE_SCHEMA}/orders_quarantine"
    )


def test_the_managers_own_metadata_does_not_reach_the_materialization(tmp_path: Path):
    """The risk the copy carries. A copied context reaches the same `add_output_metadata` the real output does, and `dagster-polars` calls it with the path and row count of whatever it wrote.

    `add_output_metadata` rebinds the mapping on the object it was called on, so the quarantine's numbers land on the copy and the step reads the original. The filter leaves 3 valid rows against 1 held back, so a leak would show as a row count of 1.
    """
    result = dg.materialize(
        [_delegating(cooccurring_orders, partitioned=False)],
        resources=storage(tmp_path),
    )
    (event,) = result.get_asset_materialization_events()
    metadata = event.step_materialization_data.materialization.metadata

    assert result.success
    assert metadata["dagster/row_count"] == dg.MetadataValue.int(3)
    assert "orders_quarantine" not in str(metadata.get("path", ""))


# --- the file writer ---
def test_the_file_writer_writes_where_the_path_rule_says(tmp_path: Path):
    """Asserted through `process` with no run: no context, no manager, no instance."""
    events = list(
        process(
            Orders,
            mixed_orders(),
            valid_key=_ORDERS,
            quarantine_writer=file_writer(_ORDERS, tmp_path),
        )
    )
    (materialization,) = [e for e in events if isinstance(e, dg.MaterializeResult)]
    path = tmp_path / "orders_quarantine.parquet"

    assert (materialization.metadata or {})["dataframely/quarantine_address"] == str(
        path
    )
    assert_frame_equal(pl.read_parquet(path), _invalid_rows())


def test_the_file_writer_creates_the_directories_it_needs(tmp_path: Path):
    """A prefix is a directory on disk, and nothing else in the run will have made it."""
    key = dg.AssetKey(["sales", "eu", "orders"])

    file_writer(key, tmp_path)(_invalid_rows())

    assert (tmp_path / "sales" / "eu" / "orders_quarantine.parquet").exists()


def test_the_file_writer_puts_a_partition_under_the_leaf(tmp_path: Path):
    file_writer(_ORDERS, tmp_path, "2026-01-02")(_invalid_rows())

    assert (tmp_path / "orders_quarantine" / "2026-01-02.parquet").exists()


def test_the_file_writer_leaves_nothing_behind_when_nothing_calls_it(tmp_path: Path):
    """Built before any row fails and called only if one does, so a writer that made its directory eagerly would leave an empty one on every clean run."""
    file_writer(dg.AssetKey(["sales", "orders"]), tmp_path)

    assert list(tmp_path.iterdir()) == []
