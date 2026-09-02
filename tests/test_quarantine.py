"""What a quarantine is called, and where it goes when no IO manager places it.

Both functions are pure, so most of this file needs no run. The three that do run cover the fallback end to end: a root equal to a `UPathIOManager`'s `base_dir` puts the file where that manager already looks, so a downstream asset reads it with no help from this package. Delegation (ADR-0006) is the usual path and belongs to #106; nothing here asserts it.

The multi-partition spelling is asserted here against a literal and pinned against `UPathIOManager`'s own path in `test_upstream_characterization.py`. Two assertions rather than one: this file says what the rule is, that one says whose rule it is.
"""

from collections.abc import Sequence
from pathlib import Path

import dagster as dg
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from upath import UPath

from dagster_dataframely import build_quarantine_spec
from dagster_dataframely.wiring import quarantine_frame, quarantine_path
from tests.scenario import Orders, mixed_orders, storage

_COLUMN_SCHEMA_KEY = "dagster/column_schema"
_ORDERS = dg.AssetKey(["orders"])
_DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])
_GRID = dg.MultiPartitionsDefinition(
    {"region": dg.StaticPartitionsDefinition(["eu", "us"]), "day": _DAYS}
)


def _invalid_rows() -> pl.DataFrame:
    """The frame a quarantine actually holds: invalid rows plus a rule column each."""
    _, failure = Orders.filter(mixed_orders())
    return quarantine_frame(Orders, failure)


# --- the path ---
def test_the_leaf_carries_the_suffix_so_a_shared_root_cannot_overwrite(tmp_path: Path):
    """The whole point of `_quarantine` on the leaf. `UPathIOManager` writes `orders.parquet` under the same root, so the two sit beside each other rather than one over the other. The delegating writer puts the same suffix on the asset key, so both routes name one thing."""
    path = quarantine_path(_ORDERS, tmp_path)

    assert path == UPath(tmp_path) / "orders_quarantine.parquet"


def test_a_path_is_a_upath_whatever_the_root_arrived_as(tmp_path: Path):
    """The decorator resolves a root to a `UPath`, but a hand-wirer reaching for the same rule holds whatever their config gave them."""
    paths = [
        quarantine_path(_ORDERS, root)
        for root in (tmp_path, str(tmp_path), UPath(tmp_path))
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
    """One rule and one `UPath`, so nothing about the path changes when the root moves to object storage. Every form, because a `/` in the tail is exactly where a scheme-aware path could have behaved differently."""
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
    """A partition key can come from data, and both of these resolve outside the root unescaped: `pathlib` drops the left side of a join when the right side is absolute, and the OS walks `..` upward at write time.

    `FilesystemIOManager`'s pair of escapes rather than the `UPathIOManager` base class's one, which is upstream's own documented advice for a hierarchical filesystem.
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
    """Read rather than passed, so the spec cannot disagree with the asset whose invalid rows it holds."""
    assert build_quarantine_spec(Orders, orders).partitions_def == _DAYS


def test_the_description_names_the_asset_the_rows_came_from():
    description = build_quarantine_spec(Orders, orders).description

    assert description is not None
    assert "sales/orders" in description


def test_the_columns_tab_states_no_constraint():
    """These rows are here for breaking the constraints, so a `not null` on a column full of nulls would be false about every row in the table."""
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
    """For a generated or foreign asset there is no definition to read, and the answer has to be the same one."""
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
    """Two sources of one fact, and nothing decides which wins. Raising at call time is the only answer that cannot silently drift."""
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

    The root is the manager's `base_dir`, which is the arrangement all three of these tests exist to prove. Nothing in the run knows about this package: the spec supplies a key and `PolarsParquetIOManager` resolves it to the file already sitting there.

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
    """What the fallback buys when it is the one in use. Every column survives, rule columns included, so what a reader depends on is the file itself rather than a projection of it."""
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
