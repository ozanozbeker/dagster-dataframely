"""The error rows' path, spec and key, and the two writers.

The delegation tests run on a `UPathIOManager` and a `DbIOManager`, because one IO manager cannot show that `build_io_manager_writer` works with any IO manager (ADR-0006).
`test_upstream_characterization.py` checks the multi-partition path format against `UPathIOManager`'s own.
"""

import datetime as dt
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, NamedTuple

import dagster as dg
import dataframely as dy
import duckdb
import polars as pl
import pytest

# A code location's repository, which only a launched run has.
from dagster._core.definitions.reconstruct import ReconstructableRepository

# The error a context property raises when there is no step.
from dagster._core.errors import DagsterInvalidPropertyError
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_polars import PolarsParquetIOManager
from polars.testing import assert_frame_equal
from upath import UPath

import dagster_dataframely as dd
from dagster_dataframely import _error_rows, build_error_rows_spec
from dagster_dataframely.errors import ErrorRowsKeyCollisionError, NoValidRowsError
from dagster_dataframely.wiring import (
    ErrorRowsWriter,
    build_asset_check_specs,
    build_file_writer,
    get_error_rows,
    get_error_rows_path,
    stream_results,
    validate_error_rows_key,
)
from tests.scenario import (
    WAREHOUSE_SCHEMA,
    OrdersSchema,
    clean_orders,
    cooccurring_orders,
    materializations,
    mixed_orders,
    no_valid_orders,
    storage,
    tables,
    warehouse,
)

_COLUMN_SCHEMA_KEY = "dagster/column_schema"
_ORDERS = dg.AssetKey(["orders"])
_TABLE_KEY = dg.AssetKey([WAREHOUSE_SCHEMA, "orders"])
_ERROR_ROWS_KEY = dg.AssetKey([WAREHOUSE_SCHEMA, "orders__error_rows"])
_DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])
_GRID = dg.MultiPartitionsDefinition({
    "region": dg.StaticPartitionsDefinition(["eu", "us"]),
    "day": _DAYS,
})


def _invalid_rows() -> pl.DataFrame:
    """Return the error rows for `mixed_orders`."""
    _, failure = OrdersSchema.filter(mixed_orders())
    return get_error_rows(OrdersSchema, failure)


class TestPath:
    """The path `get_error_rows_path` returns."""

    def test_the_leaf_carries_the_suffix_so_a_shared_root_cannot_overwrite(
        self, tmp_path: Path
    ):
        """`UPathIOManager` can write the asset's own `orders.parquet` to the same directory."""
        path = get_error_rows_path(_ORDERS, tmp_path)

        assert path == UPath(tmp_path) / "orders__error_rows.parquet"

    def test_a_path_is_a_upath_whatever_the_root_arrived_as(self, tmp_path: Path):
        paths = [
            get_error_rows_path(_ORDERS, error_rows_dir)
            for error_rows_dir in (tmp_path, str(tmp_path), UPath(tmp_path))
        ]

        assert all(isinstance(path, UPath) for path in paths)
        assert len(set(paths)) == 1

    def test_a_key_prefix_becomes_directories_and_only_the_name_is_suffixed(
        self, tmp_path: Path
    ):
        path = get_error_rows_path(dg.AssetKey(["sales", "orders"]), tmp_path)

        assert path == UPath(tmp_path) / "sales" / "orders__error_rows.parquet"

    def test_an_asset_name_carrying_a_dot_keeps_it(self, tmp_path: Path):
        """`Path.with_suffix` would turn `orders.v2` into `orders.parquet`, which another asset writes."""
        path = get_error_rows_path(dg.AssetKey(["orders.v2"]), tmp_path)

        assert path == UPath(tmp_path) / "orders.v2__error_rows.parquet"

    def test_a_partition_becomes_a_file_under_the_leaf(self, tmp_path: Path):
        path = get_error_rows_path(_ORDERS, tmp_path, "2026-01-02")

        assert path == UPath(tmp_path) / "orders__error_rows" / "2026-01-02.parquet"

    def test_a_multi_partition_key_is_formatted_in_dimension_name_order(
        self, tmp_path: Path
    ):
        key = dg.MultiPartitionKey({"region": "eu", "day": "2026-01-02"})

        path = get_error_rows_path(_ORDERS, tmp_path, key)

        assert (
            path == UPath(tmp_path) / "orders__error_rows" / "2026-01-02" / "eu.parquet"
        )

    @pytest.mark.parametrize(
        ("partition_key", "tail"),
        [
            (None, "orders__error_rows.parquet"),
            ("2026-01-02", "orders__error_rows/2026-01-02.parquet"),
            (
                dg.MultiPartitionKey({"region": "eu", "day": "2026-01-02"}),
                "orders__error_rows/2026-01-02/eu.parquet",
            ),
        ],
        ids=["unpartitioned", "partitioned", "multi_partitioned"],
    )
    def test_a_cloud_root_is_the_same_rule(self, partition_key: str | None, tail: str):
        """Each partition form, because a `/` in the tail is where an `s3://` path could differ."""
        path = get_error_rows_path(
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
        self, tmp_path: Path, partition_key: str, tail: str
    ):
        """A partition key can come from data."""
        path = get_error_rows_path(_ORDERS, tmp_path, partition_key)

        assert path == UPath(tmp_path) / f"orders__error_rows/{tail}.parquet"
        assert path.resolve().is_relative_to(UPath(tmp_path).resolve())

    @pytest.mark.parametrize(
        "error_rows_key",
        [dg.AssetKey(["audit", "orders"]), ["audit", "orders"]],
        ids=["asset_key", "sequence"],
    )
    def test_an_error_rows_key_replaces_the_suffixed_key(
        self, tmp_path: Path, error_rows_key: dg.AssetKey | Sequence[str]
    ):
        path = get_error_rows_path(
            _ORDERS, tmp_path, "2026-01-02", error_rows_key=error_rows_key
        )

        assert path == UPath(tmp_path) / "audit" / "orders" / "2026-01-02.parquet"

    def test_a_bare_string_error_rows_key_is_a_single_key_part(self, tmp_path: Path):
        path = get_error_rows_path(_ORDERS, tmp_path, error_rows_key="rejected_orders")

        assert path == UPath(tmp_path) / "rejected_orders.parquet"


# `get_error_rows_path` keeps a `..` inside a name, as in `my..backup`; `test_upstream_characterization.py` pins it.


@dg.asset(key_prefix="sales", partitions_def=_DAYS)
def orders() -> None:
    """Customer orders."""


@dg.asset(group_name="sales")
def grouped_orders() -> None:
    """Customer orders in the `sales` group."""


@dg.asset(
    owners=["team:marketing"],
    tags={"tier": "gold", "layer": "silver"},
    kinds={"polars", "parquet"},
)
def owned_orders() -> None:
    """Customer orders with owners, tags and kinds."""


class TestSpec:
    """The spec `build_error_rows_spec` returns."""

    def test_the_spec_is_keyed_as_the_file_is_named(self):
        spec = build_error_rows_spec(OrdersSchema, orders)

        assert spec.key == dg.AssetKey(["sales", "orders__error_rows"])
        # Each against the literal, because `_add_error_rows_suffix` builds both and comparing them would pass for any `_LEAF_SUFFIX`.
        assert (
            get_error_rows_path(orders.key, "/warehouse").stem == "orders__error_rows"
        )

    def test_the_spec_depends_on_the_asset_the_invalid_rows_came_from(self):
        (dep,) = build_error_rows_spec(OrdersSchema, orders).deps

        assert dep.asset_key == dg.AssetKey(["sales", "orders"])

    @pytest.mark.parametrize(
        "asset",
        [orders, dg.AssetKey(["sales", "orders"])],
        ids=["definition", "asset_key"],
    )
    def test_the_spec_has_the_polars_kind(
        self, asset: dg.AssetsDefinition | dg.AssetKey
    ):
        assert build_error_rows_spec(OrdersSchema, asset).kinds == {"polars"}

    def test_merge_attributes_adds_a_storage_kind_beside_polars(self):
        spec = build_error_rows_spec(OrdersSchema, orders).merge_attributes(
            kinds={"duckdb"}
        )

        assert spec.kinds == {"polars", "duckdb"}

    def test_the_spec_has_no_code_version_when_the_asset_has_one(self):
        @dd.asset(OrdersSchema, filter=True)
        def versioned() -> pl.DataFrame:
            return pl.DataFrame()

        assert versioned.get_asset_spec().code_version is not None
        assert build_error_rows_spec(OrdersSchema, versioned).code_version is None

    def test_the_spec_takes_the_partitions_off_the_definition(self):
        assert build_error_rows_spec(OrdersSchema, orders).partitions_def == _DAYS

    def test_the_spec_takes_the_group_off_the_definition(self):
        assert build_error_rows_spec(OrdersSchema, grouped_orders).group_name == "sales"

    def test_a_group_name_overrides_the_definitions_group(self):
        spec = build_error_rows_spec(
            OrdersSchema, grouped_orders, group_name="error_rows"
        )

        assert spec.group_name == "error_rows"

    def test_the_error_rows_is_in_its_parents_group_in_the_asset_graph(self):
        spec = build_error_rows_spec(OrdersSchema, grouped_orders)
        graph = dg.Definitions(assets=[grouped_orders, spec]).resolve_asset_graph()

        assert graph.get(spec.key).group_name == "sales"

    def test_the_spec_takes_the_owners_and_tags_off_the_definition(self):
        spec = build_error_rows_spec(OrdersSchema, owned_orders)

        assert spec.owners == ["team:marketing"]
        assert spec.tags == {
            "tier": "gold",
            "layer": "silver",
            "dagster/kind/polars": "",
        }

    def test_the_spec_keeps_none_of_the_definitions_other_kinds(self):
        spec = build_error_rows_spec(OrdersSchema, owned_orders)

        assert spec.kinds == {"polars"}
        assert "dagster/kind/parquet" not in spec.tags

    def test_owners_replaces_the_definitions_owners(self):
        spec = build_error_rows_spec(OrdersSchema, owned_orders, owners=["team:data"])

        assert spec.owners == ["team:data"]

    def test_tags_replaces_the_definitions_tags(self):
        spec = build_error_rows_spec(
            OrdersSchema, owned_orders, tags={"tier": "silver"}
        )

        assert spec.tags == {"tier": "silver", "dagster/kind/polars": ""}

    def test_empty_owners_and_tags_clear_the_definitions(self):
        spec = build_error_rows_spec(OrdersSchema, owned_orders, owners=[], tags={})

        assert spec.owners == []
        assert spec.tags == {"dagster/kind/polars": ""}

    def test_the_error_rows_have_their_parents_owners_and_tags_in_the_asset_graph(
        self,
    ):
        spec = build_error_rows_spec(OrdersSchema, owned_orders)
        graph = dg.Definitions(assets=[owned_orders, spec]).resolve_asset_graph()
        node = graph.get(spec.key)

        assert node.owners == ["team:marketing"]
        assert node.tags["tier"] == "gold"
        assert node.kinds == {"polars"}

    def test_the_description_names_the_asset_the_rows_came_from(self):
        description = build_error_rows_spec(OrdersSchema, orders).description

        assert description is not None
        assert "sales/orders" in description

    def test_the_columns_tab_states_no_constraint(self):
        table = build_error_rows_spec(OrdersSchema, orders).metadata[_COLUMN_SCHEMA_KEY]

        assert isinstance(table, dg.TableSchema)
        assert table.constraints == dg.TableConstraints(other=[])
        assert all(
            column.constraints == dg.TableColumnConstraints()
            for column in table.columns
        )

    def test_the_columns_tab_carries_a_column_for_every_rule(self):
        table = build_error_rows_spec(OrdersSchema, orders).metadata[_COLUMN_SCHEMA_KEY]
        names = [column.name for column in table.columns]

        assert names[: len(OrdersSchema.columns())] == list(OrdersSchema.columns())
        assert "dy_rule__amount__min" in names
        assert "dy_rule__paid_orders_have_amount" in names

    def test_the_rule_columns_are_written_in_the_order_the_columns_tab_declares(self):
        """The written rule columns match the Columns tab and the check specs in order."""
        written = [
            name for name in _invalid_rows().columns if name.startswith("dy_rule__")
        ]
        table = build_error_rows_spec(OrdersSchema, orders).metadata[_COLUMN_SCHEMA_KEY]
        assert isinstance(table, dg.TableSchema)

        declared = [
            column.name
            for column in table.columns
            if column.name.startswith("dy_rule__")
        ]
        checks = [
            spec.name
            for spec in build_asset_check_specs(OrdersSchema, asset=_ORDERS)
            if spec.name.startswith("dy_rule__")
        ]

        assert written == declared
        assert written == checks

    @pytest.mark.parametrize(
        "form",
        [dg.AssetKey(["sales", "orders"]), ["sales", "orders"]],
        ids=["asset_key", "sequence"],
    )
    def test_a_key_form_is_keyed_exactly_as_the_definition_form(
        self,
        form: dg.AssetKey | Sequence[str],
    ):
        assert build_error_rows_spec(OrdersSchema, form).key == dg.AssetKey([
            "sales",
            "orders__error_rows",
        ])

    def test_a_bare_string_is_a_single_key_part(self):
        assert build_error_rows_spec(OrdersSchema, "orders").key == dg.AssetKey([
            "orders__error_rows"
        ])

    @pytest.mark.parametrize(
        "form",
        [dg.AssetKey(["orders"]), ["orders"], "orders"],
        ids=["asset_key", "sequence", "string"],
    )
    def test_a_key_form_is_unpartitioned_unless_told_otherwise(
        self,
        form: dg.AssetKey | Sequence[str],
    ):
        assert build_error_rows_spec(OrdersSchema, form).partitions_def is None

    @pytest.mark.parametrize(
        "form",
        [dg.AssetKey(["orders"]), ["orders"], "orders"],
        ids=["asset_key", "sequence", "string"],
    )
    def test_a_key_form_takes_the_partitions_it_is_given(
        self,
        form: dg.AssetKey | Sequence[str],
    ):
        assert build_error_rows_spec(
            OrdersSchema, form, partitions_def=_DAYS
        ).partitions_def == (_DAYS)

    @pytest.mark.parametrize(
        "form",
        [dg.AssetKey(["orders"]), ["orders"], "orders"],
        ids=["asset_key", "sequence", "string"],
    )
    def test_a_key_form_takes_the_group_it_is_given(
        self, form: dg.AssetKey | Sequence[str]
    ):
        assert (
            build_error_rows_spec(OrdersSchema, form, group_name="silver").group_name
            == "silver"
        )

    def test_a_key_form_with_no_group_is_in_the_default_group(self):
        spec = build_error_rows_spec(OrdersSchema, "orders")
        graph = dg.Definitions(assets=[spec]).resolve_asset_graph()

        assert graph.get(spec.key).group_name == "default"

    def test_a_key_form_has_no_owners_or_tags_by_default(self):
        spec = build_error_rows_spec(OrdersSchema, "orders")

        assert spec.owners == []
        assert spec.tags == {"dagster/kind/polars": ""}

    def test_a_key_form_takes_the_owners_and_tags_it_is_given(self):
        spec = build_error_rows_spec(
            OrdersSchema, "orders", owners=["team:data"], tags={"tier": "gold"}
        )

        assert spec.owners == ["team:data"]
        assert spec.tags["tier"] == "gold"

    def test_a_hand_wired_definition_gives_the_spec_its_own_io_manager(self):
        """A downstream asset then loads the rows through the IO manager that wrote them."""

        @dg.asset(io_manager_key="warehouse")
        def wired() -> None: ...

        spec = build_error_rows_spec(OrdersSchema, wired)

        assert spec.key == dg.AssetKey(["wired__error_rows"])
        assert spec.metadata["dagster/io_manager_key"] == "warehouse"

    def test_the_spec_keeps_the_assets_metadata_but_not_where_its_rows_go(self):
        """The writer passes the asset's metadata to the IO manager, so a load needs the same."""

        @dd.asset(OrdersSchema, filter=True, metadata={"schema": "sales"})
        def kept() -> pl.DataFrame:
            return pl.DataFrame()

        metadata = build_error_rows_spec(OrdersSchema, kept).metadata

        assert metadata["schema"] == "sales"
        assert (
            metadata[_COLUMN_SCHEMA_KEY]
            == (
                build_error_rows_spec(OrdersSchema, "kept").metadata[_COLUMN_SCHEMA_KEY]
            )
        )
        assert not any(name.startswith("dataframely/") for name in metadata)

    def test_a_definition_and_a_partitions_def_together_raise(self):
        with pytest.raises(dg.DagsterInvariantViolationError) as raised:
            build_error_rows_spec(OrdersSchema, orders, partitions_def=_DAYS)

        assert "partitions_def" in str(raised.value)
        assert "sales/orders" in str(raised.value)


def _loaded(
    tmp_path: Path,
    *,
    partitions_def: dg.PartitionsDefinition | None = None,
    partition_key: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Write an error-rows file at `get_error_rows_path`, then return what a downstream asset loads from it and the rows it holds."""
    invalid = _invalid_rows()
    path = get_error_rows_path(_ORDERS, tmp_path, partition_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as file:
        invalid.write_parquet(file)
    loaded: dict[str, pl.DataFrame] = {}

    @dg.asset(partitions_def=partitions_def)
    def audit(orders__error_rows: pl.DataFrame) -> None:
        loaded["rows"] = orders__error_rows

    result = dg.materialize(
        [
            audit,
            build_error_rows_spec(
                OrdersSchema, "orders", partitions_def=partitions_def
            ),
        ],
        partition_key=partition_key,
        resources=storage(tmp_path),
    )

    assert result.success
    return loaded["rows"], invalid


class TestDownstreamAsset:
    """A downstream asset that loads the error-rows file through the asset graph."""

    def test_a_downstream_asset_loads_the_file_with_no_help_from_this_package(
        self,
        tmp_path: Path,
    ):
        loaded, written = _loaded(tmp_path)

        assert_frame_equal(loaded, written)

    def test_a_partitioned_downstream_asset_loads_the_partition_it_asked_for(
        self,
        tmp_path: Path,
    ):
        loaded, written = _loaded(
            tmp_path, partitions_def=_DAYS, partition_key="2026-01-02"
        )

        assert_frame_equal(loaded, written)

    def test_a_multi_partitioned_file_lands_where_the_manager_looks(
        self, tmp_path: Path
    ):
        loaded, written = _loaded(
            tmp_path,
            partitions_def=_GRID,
            partition_key=dg.MultiPartitionKey({"region": "eu", "day": "2026-01-02"}),
        )

        assert_frame_equal(loaded, written)


_DAY = "2026-08-01"
_ONE_DAY = dg.StaticPartitionsDefinition([_DAY])

_PARTITION_EXPR = {"partition_expr": "ordered_at"}
"""The metadata `DbIOManager` needs to rewrite a partition."""

_WITH_INVALID_ROWS = [
    pytest.param(mixed_orders, 3, False, id="partial"),
    pytest.param(no_valid_orders, 2, True, id="no valid rows"),
]
"""The two outcomes that write invalid rows."""


def _delegating(
    frame: Callable[[], pl.DataFrame],
    *,
    partitioned: bool,
    partition_expr: bool = True,
    **overrides: Any,
) -> dg.AssetsDefinition:
    """Declare the asset the delegation tests run, with `overrides` passed to `dd.asset`."""

    @dd.asset(
        OrdersSchema,
        name="orders",
        key_prefix=WAREHOUSE_SCHEMA,
        filter=True,
        partitions_def=_ONE_DAY if partitioned else None,
        metadata=_PARTITION_EXPR if partition_expr else None,
        **overrides,
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
    """Materialize the asset, and assert it fails exactly when `aborts` is true."""
    result = dg.materialize(
        [asset],
        resources=resources,
        partition_key=_DAY if partitioned else None,
        raise_on_error=False,
    )

    assert result.success is not aborts


class TestDelegatingWriter:
    """The writer `build_io_manager_writer` returns, which writes through the asset's IO manager."""

    @pytest.mark.parametrize(("frame", "invalid", "aborts"), _WITH_INVALID_ROWS)
    @pytest.mark.parametrize("partitioned", [False, True], ids=["whole", "partitioned"])
    def test_a_filesystem_manager_puts_the_error_rows_beside_the_table(
        self,
        tmp_path: Path,
        frame: Callable[[], pl.DataFrame],
        invalid: int,
        aborts: bool,
        partitioned: bool,
    ):
        _run(
            _delegating(frame, partitioned=partitioned),
            storage(tmp_path),
            partitioned=partitioned,
            aborts=aborts,
        )
        leaf = (
            f"orders__error_rows/{_DAY}.parquet"
            if partitioned
            else "orders__error_rows.parquet"
        )

        assert pl.read_parquet(tmp_path / WAREHOUSE_SCHEMA / leaf).height == invalid

    @pytest.mark.parametrize(("frame", "invalid", "aborts"), _WITH_INVALID_ROWS)
    @pytest.mark.parametrize("partitioned", [False, True], ids=["whole", "partitioned"])
    def test_a_database_manager_puts_the_error_rows_in_a_table_beside_it(
        self,
        tmp_path: Path,
        frame: Callable[[], pl.DataFrame],
        invalid: int,
        aborts: bool,
        partitioned: bool,
    ):
        """The partitioned runs also check that `DbIOManager` receives `partition_expr` from the definition metadata."""
        _run(
            _delegating(frame, partitioned=partitioned),
            warehouse(tmp_path),
            partitioned=partitioned,
            aborts=aborts,
        )
        written = tables(tmp_path)

        assert "orders__error_rows" in written
        assert written["orders__error_rows"].height == invalid
        assert "dy_rule__amount__min" in written["orders__error_rows"].columns

    def test_a_clean_rerun_empties_the_error_rows_file(self, tmp_path: Path):
        path = tmp_path / WAREHOUSE_SCHEMA / "orders__error_rows.parquet"
        _run(
            _delegating(mixed_orders, partitioned=False),
            storage(tmp_path),
            partitioned=False,
            aborts=False,
        )
        dirty = pl.read_parquet(path)

        _run(
            _delegating(clean_orders, partitioned=False),
            storage(tmp_path),
            partitioned=False,
            aborts=False,
        )
        clean = pl.read_parquet(path)

        assert clean.is_empty()
        assert clean.schema == dirty.schema

    def test_a_clean_rerun_empties_the_error_rows_table(self, tmp_path: Path):
        _run(
            _delegating(mixed_orders, partitioned=False),
            warehouse(tmp_path),
            partitioned=False,
            aborts=False,
        )
        dirty = tables(tmp_path)["orders__error_rows"]

        _run(
            _delegating(clean_orders, partitioned=False),
            warehouse(tmp_path),
            partitioned=False,
            aborts=False,
        )
        clean = tables(tmp_path)["orders__error_rows"]

        assert clean.height == 0
        assert (clean.columns, clean.types) == (dirty.columns, dirty.types)

    def test_a_run_delegates_without_reading_the_error_rows_dir(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        unread = tmp_path / "unread"
        monkeypatch.setenv("DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR", str(unread))

        _run(
            _delegating(mixed_orders, partitioned=False),
            storage(tmp_path),
            partitioned=False,
            aborts=False,
        )

        assert not unread.exists()
        assert (tmp_path / WAREHOUSE_SCHEMA / "orders__error_rows.parquet").exists()

    def test_the_step_probe_does_not_widen_to_the_whole_writer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """A `DagsterInvalidPropertyError` from `build_io_manager_writer` fails the run instead of switching to `build_file_writer`."""
        error_rows_dir = tmp_path / "filtering"
        monkeypatch.setenv("DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR", str(error_rows_dir))

        def raises(_context: dg.AssetExecutionContext) -> ErrorRowsWriter:
            not_the_step = "asset_partitions_time_window"
            raise DagsterInvalidPropertyError(not_the_step)

        monkeypatch.setattr(_error_rows, "build_io_manager_writer", raises)

        result = dg.materialize(
            [_delegating(mixed_orders, partitioned=False)],
            resources=storage(tmp_path),
            raise_on_error=False,
        )

        assert not result.success
        assert not error_rows_dir.exists()

    def test_the_error_rows_lands_even_though_the_run_dies(self, tmp_path: Path):
        with pytest.raises(NoValidRowsError):
            dg.materialize(
                [_delegating(no_valid_orders, partitioned=False)],
                resources=storage(tmp_path),
            )

        assert (tmp_path / WAREHOUSE_SCHEMA / "orders__error_rows.parquet").exists()

    def test_a_partitioned_database_asset_fails_with_the_managers_own_error(
        self, tmp_path: Path
    ):
        result = dg.materialize(
            [_delegating(mixed_orders, partitioned=True, partition_expr=False)],
            resources=warehouse(tmp_path),
            partition_key=_DAY,
            raise_on_error=False,
        )
        (failure,) = result.get_step_failure_events()

        assert not result.success
        assert "partition_expr" in str(failure.step_failure_data.error)

    def test_the_address_is_the_key_the_manager_resolved(self, tmp_path: Path):
        """The address is the error rows' asset key, not the IO manager's path (ADR-0006)."""
        result = dg.materialize(
            [_delegating(mixed_orders, partitioned=False)], resources=storage(tmp_path)
        )
        metadata = materializations(result)[_TABLE_KEY].metadata

        assert metadata["dataframely/error_rows_address"] == dg.MetadataValue.text(
            f"{WAREHOUSE_SCHEMA}/orders__error_rows"
        )

    def test_the_managers_own_metadata_does_not_reach_the_materialization(
        self, tmp_path: Path
    ):
        """`cooccurring_orders` has 3 valid rows and 1 invalid, so a row count of 1 would mean the error rows' metadata replaced the asset's."""
        result = dg.materialize(
            [_delegating(cooccurring_orders, partitioned=False)],
            resources=storage(tmp_path),
        )
        metadata = materializations(result)[_TABLE_KEY].metadata

        assert result.success
        assert metadata["dagster/row_count"] == dg.MetadataValue.int(3)
        assert "orders__error_rows" not in str(metadata.get("path", ""))


_AUDIT_KEY = ["audit", "orders__error_rows"]
"""An error-rows key under another prefix than the asset's."""


def _address(result: dg.ExecuteInProcessResult) -> dg.MetadataValue[Any]:
    """Return the error-rows address of the table's materialization."""
    return materializations(result)[_TABLE_KEY].metadata[
        "dataframely/error_rows_address"
    ]


def _parquet_under_warehouse(tmp_path: Path) -> dict[str, Any]:
    """Build resources whose parquet IO manager has a key other than `io_manager`."""
    return {"warehouse": PolarsParquetIOManager(base_dir=str(tmp_path))}


def _parquet_with_database_audit(tmp_path: Path) -> dict[str, Any]:
    """Build resources with a parquet `io_manager` and a database IO manager under `audit`."""
    return {**storage(tmp_path), "audit": warehouse(tmp_path)["io_manager"]}


class TestOverrides:
    """A run of an asset that sets `error_rows_key` or `error_rows_io_manager_key`."""

    def test_the_error_rows_key_sets_where_the_assets_io_manager_writes(
        self, tmp_path: Path
    ):
        result = dg.materialize(
            [_delegating(mixed_orders, partitioned=False, error_rows_key=_AUDIT_KEY)],
            resources=storage(tmp_path),
        )

        assert (
            pl.read_parquet(tmp_path / "audit" / "orders__error_rows.parquet").height
            == 3
        )
        assert not (tmp_path / WAREHOUSE_SCHEMA / "orders__error_rows.parquet").exists()
        assert _address(result) == dg.MetadataValue.text("audit/orders__error_rows")

    @pytest.mark.parametrize("partitioned", [False, True], ids=["whole", "partitioned"])
    def test_the_error_rows_io_manager_key_names_the_io_manager_that_writes_them(
        self, tmp_path: Path, partitioned: bool
    ):
        """A `DbIOManager` reads its connection from the context, so this also checks the context has its config and not the asset's IO manager's."""
        resources = _parquet_with_database_audit(tmp_path)

        _run(
            _delegating(
                mixed_orders, partitioned=partitioned, error_rows_io_manager_key="audit"
            ),
            resources,
            partitioned=partitioned,
            aborts=False,
        )

        assert tables(tmp_path)["orders__error_rows"].height == 3
        assert list(tmp_path.rglob("orders__error_rows*")) == []

    @pytest.mark.parametrize(
        ("metadata", "configured", "schema"),
        [
            ({}, {}, "audit"),
            ({}, {"schema": "configured"}, "configured"),
            ({"schema": "sales"}, {"schema": "configured"}, "sales"),
        ],
        ids=["key_prefix", "manager_config", "get_column_schema_metadata"],
    )
    def test_a_database_reads_the_schema_from_metadata_then_config_then_key_prefix(
        self,
        tmp_path: Path,
        metadata: dict[str, str],
        configured: dict[str, Any],
        schema: str,
    ):
        """`error_rows_key` sets the schema only when neither the metadata nor the IO manager's config does."""
        database = tmp_path / "errors.duckdb"
        audit = DuckDBPolarsIOManager(database=str(database), **configured)

        @dd.asset(
            OrdersSchema,
            name="orders",
            filter=True,
            metadata=metadata,
            error_rows_key=_AUDIT_KEY,
            error_rows_io_manager_key="audit",
        )
        def orders() -> pl.DataFrame:
            return mixed_orders()

        dg.materialize([orders], resources={**storage(tmp_path), "audit": audit})

        with duckdb.connect(str(database)) as connection:
            written = connection.sql(
                "select table_schema from information_schema.tables "
                "where table_name = 'orders__error_rows'"
            ).fetchall()
        assert written == [(schema,)]


_EVERY_WRITE = [pytest.param(clean_orders, 0, False, id="clean"), *_WITH_INVALID_ROWS]
"""Every outcome that writes the error rows, including the empty write of a clean run."""


_IN_PROCESS_EXECUTOR: dict[str, Any] = {"execution": {"config": {"in_process": {}}}}


def _error_rows_materializations(
    events: Sequence[dg.DagsterEvent],
) -> list[dg.AssetMaterialization]:
    """Return the materializations among a run's events for the error-rows key."""
    return [
        event.step_materialization_data.materialization
        for event in events
        if event.asset_key == _ERROR_ROWS_KEY
    ]


class TestMaterialization:
    """The materialization a run records for the error rows."""

    @pytest.mark.parametrize(("frame", "invalid", "aborts"), _EVERY_WRITE)
    def test_every_write_records_the_error_rows_row_count(
        self,
        tmp_path: Path,
        frame: Callable[[], pl.DataFrame],
        invalid: int,
        aborts: bool,
    ):
        asset = _delegating(frame, partitioned=False)

        result = dg.materialize(
            [asset, build_error_rows_spec(OrdersSchema, asset)],
            resources=storage(tmp_path),
            raise_on_error=False,
        )
        (materialization,) = _error_rows_materializations(
            result.get_asset_materialization_events()
        )

        assert result.success is not aborts
        assert materialization.metadata["dagster/row_count"] == dg.MetadataValue.int(
            invalid
        )

    def test_a_partitioned_write_records_its_partition_and_the_partitions_row_count(
        self, tmp_path: Path
    ):
        asset = _delegating(mixed_orders, partitioned=True)

        result = dg.materialize(
            [asset, build_error_rows_spec(OrdersSchema, asset)],
            resources=storage(tmp_path),
            partition_key=_DAY,
        )
        (materialization,) = _error_rows_materializations(
            result.get_asset_materialization_events()
        )

        assert materialization.partition == _DAY
        assert materialization.metadata[
            "dagster/partition_row_count"
        ] == dg.MetadataValue.int(3)
        assert "dagster/row_count" not in materialization.metadata

    @pytest.mark.parametrize(
        ("overrides", "resources", "entry"),
        [
            ({}, storage, "path"),
            ({}, warehouse, "dagster/table_name"),
            (
                {"error_rows_io_manager_key": "audit"},
                _parquet_with_database_audit,
                "dagster/table_name",
            ),
        ],
        ids=["filesystem", "database", "error_rows_io_manager_key"],
    )
    def test_the_event_has_the_metadata_the_manager_emitted_for_the_write(
        self,
        tmp_path: Path,
        overrides: dict[str, Any],
        resources: Callable[[Path], dict[str, Any]],
        entry: str,
    ):
        asset = _delegating(mixed_orders, partitioned=False, **overrides)

        result = dg.materialize(
            [asset, build_error_rows_spec(OrdersSchema, asset)],
            resources=resources(tmp_path),
        )
        (materialization,) = _error_rows_materializations(
            result.get_asset_materialization_events()
        )

        assert "orders__error_rows" in str(materialization.metadata[entry].value)

    def test_an_eager_asset_downstream_of_the_spec_is_requested_after_a_write(
        self, tmp_path: Path
    ):
        asset = _delegating(mixed_orders, partitioned=False)
        spec = build_error_rows_spec(OrdersSchema, asset)

        @dg.asset(deps=[spec], automation_condition=dg.AutomationCondition.eager())
        def triage() -> None: ...

        defs = dg.Definitions(assets=[asset, spec, triage])
        instance = dg.DagsterInstance.ephemeral()
        before = dg.evaluate_automation_conditions(defs=defs, instance=instance)
        dg.materialize([asset, spec], instance=instance, resources=storage(tmp_path))
        after = dg.evaluate_automation_conditions(
            defs=defs, instance=instance, cursor=before.cursor
        )

        assert before.get_num_requested(triage.key) == 0
        assert after.get_num_requested(triage.key) == 1

    @pytest.mark.parametrize(
        ("location", "count"), [("with_spec", 1), ("without_spec", 0)]
    )
    def test_a_run_from_a_code_location_records_the_error_rows_only_with_the_spec(
        self, location: str, count: int
    ):
        job = ReconstructableRepository.for_module(
            "tests.code_locations", location
        ).get_reconstructable_job("__ASSET_JOB")

        with dg.instance_for_test() as instance:
            result = dg.execute_job(job, instance, run_config=_IN_PROCESS_EXECUTOR)

            assert result.success
            assert (
                len(
                    _error_rows_materializations(
                        result.get_asset_materialization_events()
                    )
                )
                == count
            )

    def test_an_in_process_run_records_the_error_rows_without_the_spec(
        self, tmp_path: Path
    ):
        result = dg.materialize(
            [_delegating(mixed_orders, partitioned=False)], resources=storage(tmp_path)
        )

        assert (
            len(_error_rows_materializations(result.get_asset_materialization_events()))
            == 1
        )


class _LedgerSchema(dy.Schema):
    """A schema with no `Duration` column, because Polars loads DuckDB's INTERVAL only with an unstable environment variable set."""

    order_id = dy.String(primary_key=True)
    amount = dy.Float64(min=0.0)
    ordered_at = dy.Datetime()


def _ledger() -> pl.DataFrame:
    """One valid row and two that fail `amount|min`."""
    return pl.DataFrame({
        "order_id": ["ORD-1", "ORD-2", "ORD-3"],
        "amount": [10.0, -1.0, -2.0],
        "ordered_at": [dt.datetime(2026, 8, 1, 12)] * 3,  # noqa: DTZ001 - the schema declares no time zone
    })


_LOADS = [
    pytest.param({}, storage, None, id="default"),
    pytest.param(
        {"io_manager_key": "warehouse"},
        _parquet_under_warehouse,
        None,
        id="the_assets_io_manager",
    ),
    pytest.param(
        {"metadata": {"schema": "sales"}},
        warehouse,
        None,
        id="get_column_schema_metadata",
    ),
    pytest.param(
        # A cast, so the partition's date matches the rows' 12:00 timestamps.
        {
            "metadata": {"partition_expr": "CAST(ordered_at AS DATE)"},
            "partitions_def": _ONE_DAY,
        },
        warehouse,
        _DAY,
        id="partitioned_database",
    ),
    pytest.param(
        {"error_rows_key": _AUDIT_KEY, "error_rows_io_manager_key": "audit"},
        _parquet_with_database_audit,
        None,
        id="both_overrides",
    ),
]
"""Each set of `dd.asset` arguments that changes where or how the writer writes, with the resources and partition its run needs."""


class TestLoadedErrorRows:
    """The error rows a downstream asset loads through `build_error_rows_spec(schema, definition)` after a run wrote them."""

    @pytest.mark.parametrize(("arguments", "resources", "partition_key"), _LOADS)
    def test_a_downstream_asset_loads_the_rows_from_where_the_writer_wrote_them(
        self,
        tmp_path: Path,
        arguments: dict[str, Any],
        resources: Callable[[Path], dict[str, Any]],
        partition_key: str | None,
    ):
        loaded: dict[str, pl.DataFrame] = {}

        @dd.asset(
            _LedgerSchema,
            name="orders",
            key_prefix=WAREHOUSE_SCHEMA,
            filter=True,
            **arguments,
        )
        def orders() -> pl.DataFrame:
            return _ledger()

        spec = build_error_rows_spec(_LedgerSchema, orders)

        @dg.asset(
            ins={"rows": dg.AssetIn(spec.key)}, partitions_def=orders.partitions_def
        )
        def audit(rows: pl.DataFrame) -> None:
            loaded["rows"] = rows

        bound = resources(tmp_path)
        dg.materialize([orders], resources=bound, partition_key=partition_key)
        dg.materialize([audit, spec], resources=bound, partition_key=partition_key)

        assert loaded["rows"]["order_id"].to_list() == ["ORD-2", "ORD-3"]
        assert "dy_rule__amount__min" in loaded["rows"].columns


class TestFileWriter:
    """The writer `build_file_writer` returns."""

    def test_the_file_writer_writes_where_the_path_rule_says(self, tmp_path: Path):
        events = list(
            stream_results(
                OrdersSchema,
                mixed_orders(),
                asset_key=_ORDERS,
                error_rows_writer=build_file_writer(_ORDERS, tmp_path),
            )
        )
        (materialization,) = [e for e in events if isinstance(e, dg.MaterializeResult)]
        path = tmp_path / "orders__error_rows.parquet"

        assert (materialization.metadata or {})[
            "dataframely/error_rows_address"
        ] == str(path)
        assert_frame_equal(pl.read_parquet(path), _invalid_rows())

    def test_the_file_writer_creates_the_directories_it_needs(self, tmp_path: Path):
        key = dg.AssetKey(["sales", "eu", "orders"])

        build_file_writer(key, tmp_path)(_invalid_rows())

        assert (tmp_path / "sales" / "eu" / "orders__error_rows.parquet").exists()

    def test_the_file_writer_puts_a_partition_under_the_leaf(self, tmp_path: Path):
        build_file_writer(_ORDERS, tmp_path, "2026-01-02")(_invalid_rows())

        assert (tmp_path / "orders__error_rows" / "2026-01-02.parquet").exists()

    def test_the_file_writer_writes_under_the_error_rows_key(self, tmp_path: Path):
        address = build_file_writer(
            _ORDERS, tmp_path, error_rows_key=["audit", "orders"]
        )(_invalid_rows())

        assert address == str(tmp_path / "audit" / "orders.parquet")
        assert_frame_equal(pl.read_parquet(address), _invalid_rows())

    def test_the_file_writer_leaves_nothing_behind_when_nothing_calls_it(
        self, tmp_path: Path
    ):
        build_file_writer(dg.AssetKey(["sales", "orders"]), tmp_path)

        assert list(tmp_path.iterdir()) == []


class _Location(NamedTuple):
    """A stand-in context with the `repository_def` that only a run launched from a code location has (ADR-0007)."""

    asset_key: dg.AssetKey
    repository_def: dg.RepositoryDefinition


def _collider() -> dg.AssetsDefinition:
    """Declare an asset with the error rows' key."""

    @dg.asset(name="orders__error_rows", key_prefix=WAREHOUSE_SCHEMA)
    def orders__error_rows() -> pl.DataFrame:
        return pl.DataFrame({"sentinel": [1]})

    return orders__error_rows


class TestReservedKey:
    """The error rows' key, which `validate_error_rows_key` reserves."""

    def test_a_run_raises_when_another_asset_owns_the_error_rows_key(
        self, tmp_path: Path
    ):
        with pytest.raises(ErrorRowsKeyCollisionError) as raised:
            dg.materialize(
                [_delegating(mixed_orders, partitioned=False), _collider()],
                resources=storage(tmp_path),
            )
        message = str(raised.value)

        assert f"'{WAREHOUSE_SCHEMA}/orders'" in message
        assert f"'{WAREHOUSE_SCHEMA}/orders__error_rows'" in message
        assert "Rename that asset" in message
        assert (
            f"set `error_rows_key` on '{WAREHOUSE_SCHEMA}/orders' to another key"
            in message
        )

    def test_the_collision_error_is_raised_before_the_body_runs(self, tmp_path: Path):
        ran: list[int] = []

        @dd.asset(OrdersSchema, name="orders", key_prefix=WAREHOUSE_SCHEMA, filter=True)
        def orders() -> pl.DataFrame:
            ran.append(1)
            return mixed_orders()

        with pytest.raises(ErrorRowsKeyCollisionError):
            dg.materialize([orders, _collider()], resources=storage(tmp_path))

        assert ran == []

    def test_a_run_raises_when_another_asset_owns_the_error_rows_key_it_set(
        self, tmp_path: Path
    ):
        ran: list[int] = []

        @dd.asset(
            OrdersSchema,
            name="orders",
            key_prefix=WAREHOUSE_SCHEMA,
            filter=True,
            error_rows_key=_AUDIT_KEY,
        )
        def orders() -> pl.DataFrame:
            ran.append(1)
            return mixed_orders()

        @dg.asset(key=_AUDIT_KEY)
        def audit_rows() -> pl.DataFrame:
            return pl.DataFrame({"sentinel": [1]})

        with pytest.raises(ErrorRowsKeyCollisionError) as raised:
            dg.materialize([orders, audit_rows], resources=storage(tmp_path))

        assert "'audit/orders__error_rows'" in str(raised.value)
        assert ran == []

    def test_an_error_rows_key_equal_to_the_assets_own_key_raises(self, tmp_path: Path):
        """Otherwise the error rows and the table would replace each other."""
        own = _delegating(
            mixed_orders, partitioned=False, error_rows_key=[WAREHOUSE_SCHEMA, "orders"]
        )

        with pytest.raises(ErrorRowsKeyCollisionError):
            dg.materialize([own], resources=storage(tmp_path))

    def test_a_run_with_no_invalid_rows_fails_on_the_key_too(self, tmp_path: Path):
        with pytest.raises(ErrorRowsKeyCollisionError):
            dg.materialize(
                [_delegating(clean_orders, partitioned=False), _collider()],
                resources=storage(tmp_path),
            )

    def test_the_whole_code_location_is_read_when_the_run_has_one(self):
        """A run launched from a code location checks every key in it, not only its job's."""
        asset = _delegating(mixed_orders, partitioned=False)
        defs = dg.Definitions(assets=[asset, _collider()])

        with pytest.raises(ErrorRowsKeyCollisionError):
            validate_error_rows_key(
                _Location(asset.key, defs.get_repository_def())  # pyrefly: ignore[bad-argument-type]
            )

    def test_the_error_rows_key_is_the_key_checked(self):
        """`_collider` owns the default key, so only the key passed collides."""
        asset = _delegating(mixed_orders, partitioned=False)
        defs = dg.Definitions(assets=[asset, _collider()])
        location = _Location(asset.key, defs.get_repository_def())

        validate_error_rows_key(
            location,  # pyrefly: ignore[bad-argument-type]
            error_rows_key=_AUDIT_KEY,
        )
        with pytest.raises(ErrorRowsKeyCollisionError):
            validate_error_rows_key(
                location,  # pyrefly: ignore[bad-argument-type]
                error_rows_key=[WAREHOUSE_SCHEMA, "orders__error_rows"],
            )

    def test_a_error_rows_spec_stands_for_the_error_rows_rather_than_competing(self):
        """`build_error_rows_spec` has the error rows' key but is not executable, so it does not collide."""
        asset = _delegating(mixed_orders, partitioned=False)
        defs = dg.Definitions(
            assets=[asset, build_error_rows_spec(OrdersSchema, asset)]
        )
        repository = defs.get_repository_def()

        validate_error_rows_key(
            _Location(asset.key, repository)  # pyrefly: ignore[bad-argument-type]
        )

        assert (
            dg.AssetKey([WAREHOUSE_SCHEMA, "orders__error_rows"])
            not in repository.asset_graph.executable_asset_keys
        )
