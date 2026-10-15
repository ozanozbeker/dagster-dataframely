"""The error rows' path, spec and key, and the two writers.

The delegation tests run on a `UPathIOManager` and a `DbIOManager`, because one IO manager cannot show that `delegating_writer` works with any IO manager (ADR-0006).
`test_upstream_characterization.py` checks the multi-partition path format against `UPathIOManager`'s own.
"""

from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any, NamedTuple

import dagster as dg
import polars as pl
import pytest

# The error a context property raises when there is no step.
from dagster._core.errors import DagsterInvalidPropertyError
from polars.testing import assert_frame_equal
from upath import UPath

import dagster_dataframely as dd
from dagster_dataframely import _error_rows, error_rows_spec
from dagster_dataframely.errors import ErrorRowsKeyCollisionError, NoValidRowsError
from dagster_dataframely.wiring import (
    ErrorRowsWriter,
    asset_check_specs,
    error_rows_frame,
    error_rows_path,
    file_writer,
    validate_error_rows_key,
    validation_results,
)
from tests.scenario import (
    WAREHOUSE_SCHEMA,
    Orders,
    clean_orders,
    cooccurring_orders,
    mixed_orders,
    no_valid_orders,
    storage,
    tables,
    warehouse,
)

_COLUMN_SCHEMA_KEY = "dagster/column_schema"
_ORDERS = dg.AssetKey(["orders"])
_DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])
_GRID = dg.MultiPartitionsDefinition({
    "region": dg.StaticPartitionsDefinition(["eu", "us"]),
    "day": _DAYS,
})


def _invalid_rows() -> pl.DataFrame:
    """Return the error rows for `mixed_orders`."""
    _, failure = Orders.filter(mixed_orders())
    return error_rows_frame(Orders, failure)


class TestPath:
    """The path `error_rows_path` returns."""

    def test_the_leaf_carries_the_suffix_so_a_shared_root_cannot_overwrite(
        self, tmp_path: Path
    ):
        """`UPathIOManager` can write the asset's own `orders.parquet` to the same directory."""
        path = error_rows_path(_ORDERS, tmp_path)

        assert path == UPath(tmp_path) / "orders_error_rows.parquet"

    def test_a_path_is_a_upath_whatever_the_root_arrived_as(self, tmp_path: Path):
        paths = [
            error_rows_path(_ORDERS, error_rows_dir)
            for error_rows_dir in (tmp_path, str(tmp_path), UPath(tmp_path))
        ]

        assert all(isinstance(path, UPath) for path in paths)
        assert len(set(paths)) == 1

    def test_a_key_prefix_becomes_directories_and_only_the_name_is_suffixed(
        self, tmp_path: Path
    ):
        path = error_rows_path(dg.AssetKey(["sales", "orders"]), tmp_path)

        assert path == UPath(tmp_path) / "sales" / "orders_error_rows.parquet"

    def test_an_asset_name_carrying_a_dot_keeps_it(self, tmp_path: Path):
        """`Path.with_suffix` would turn `orders.v2` into `orders.parquet`, which another asset writes."""
        path = error_rows_path(dg.AssetKey(["orders.v2"]), tmp_path)

        assert path == UPath(tmp_path) / "orders.v2_error_rows.parquet"

    def test_a_partition_becomes_a_file_under_the_leaf(self, tmp_path: Path):
        path = error_rows_path(_ORDERS, tmp_path, "2026-01-02")

        assert path == UPath(tmp_path) / "orders_error_rows" / "2026-01-02.parquet"

    def test_a_multi_partition_key_is_formatted_in_dimension_name_order(
        self, tmp_path: Path
    ):
        key = dg.MultiPartitionKey({"region": "eu", "day": "2026-01-02"})

        path = error_rows_path(_ORDERS, tmp_path, key)

        assert (
            path == UPath(tmp_path) / "orders_error_rows" / "2026-01-02" / "eu.parquet"
        )

    @pytest.mark.parametrize(
        ("partition_key", "tail"),
        [
            (None, "orders_error_rows.parquet"),
            ("2026-01-02", "orders_error_rows/2026-01-02.parquet"),
            (
                dg.MultiPartitionKey({"region": "eu", "day": "2026-01-02"}),
                "orders_error_rows/2026-01-02/eu.parquet",
            ),
        ],
        ids=["unpartitioned", "partitioned", "multi_partitioned"],
    )
    def test_a_cloud_root_is_the_same_rule(self, partition_key: str | None, tail: str):
        """Each partition form, because a `/` in the tail is where an `s3://` path could differ."""
        path = error_rows_path(
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
        path = error_rows_path(_ORDERS, tmp_path, partition_key)

        assert path == UPath(tmp_path) / f"orders_error_rows/{tail}.parquet"
        assert path.resolve().is_relative_to(UPath(tmp_path).resolve())


# `error_rows_path` keeps a `..` inside a name, as in `my..backup`; `test_upstream_characterization.py` pins it.


@dg.asset(key_prefix="sales", partitions_def=_DAYS)
def orders() -> None:
    """Customer orders."""


@dg.asset(group_name="sales")
def grouped_orders() -> None:
    """Customer orders in the `sales` group."""


class TestSpec:
    """The spec `error_rows_spec` returns."""

    def test_the_spec_is_keyed_as_the_file_is_named(self):
        spec = error_rows_spec(Orders, orders)

        assert spec.key == dg.AssetKey(["sales", "orders_error_rows"])
        # Each against the literal, because `_suffixed` builds both and comparing them would pass for any `_LEAF_SUFFIX`.
        assert error_rows_path(orders.key, "/warehouse").stem == "orders_error_rows"

    def test_the_spec_depends_on_the_asset_the_invalid_rows_came_from(self):
        (dep,) = error_rows_spec(Orders, orders).deps

        assert dep.asset_key == dg.AssetKey(["sales", "orders"])

    def test_the_spec_takes_the_partitions_off_the_definition(self):
        assert error_rows_spec(Orders, orders).partitions_def == _DAYS

    def test_the_spec_takes_the_group_off_the_definition(self):
        assert error_rows_spec(Orders, grouped_orders).group_name == "sales"

    def test_a_group_name_overrides_the_definitions_group(self):
        spec = error_rows_spec(Orders, grouped_orders, group_name="error_rows")

        assert spec.group_name == "error_rows"

    def test_the_error_rows_is_in_its_parents_group_in_the_asset_graph(self):
        spec = error_rows_spec(Orders, grouped_orders)
        graph = dg.Definitions(assets=[grouped_orders, spec]).resolve_asset_graph()

        assert graph.get(spec.key).group_name == "sales"

    def test_the_description_names_the_asset_the_rows_came_from(self):
        description = error_rows_spec(Orders, orders).description

        assert description is not None
        assert "sales/orders" in description

    def test_the_columns_tab_states_no_constraint(self):
        table = error_rows_spec(Orders, orders).metadata[_COLUMN_SCHEMA_KEY]

        assert isinstance(table, dg.TableSchema)
        assert table.constraints == dg.TableConstraints(other=[])
        assert all(
            column.constraints == dg.TableColumnConstraints()
            for column in table.columns
        )

    def test_the_columns_tab_carries_a_column_for_every_rule(self):
        table = error_rows_spec(Orders, orders).metadata[_COLUMN_SCHEMA_KEY]
        names = [column.name for column in table.columns]

        assert names[: len(Orders.columns())] == list(Orders.columns())
        assert "dy_rule__amount__min" in names
        assert "dy_rule__paid_orders_have_amount" in names

    def test_the_rule_columns_are_written_in_the_order_the_columns_tab_declares(self):
        """The written rule columns match the Columns tab and the check specs in order."""
        written = [
            name for name in _invalid_rows().columns if name.startswith("dy_rule__")
        ]
        table = error_rows_spec(Orders, orders).metadata[_COLUMN_SCHEMA_KEY]
        assert isinstance(table, dg.TableSchema)

        declared = [
            column.name
            for column in table.columns
            if column.name.startswith("dy_rule__")
        ]
        checks = [
            spec.name
            for spec in asset_check_specs(Orders, asset=_ORDERS)
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
        assert error_rows_spec(Orders, form).key == dg.AssetKey([
            "sales",
            "orders_error_rows",
        ])

    def test_a_bare_string_is_a_single_key_part(self):
        assert error_rows_spec(Orders, "orders").key == dg.AssetKey([
            "orders_error_rows"
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
        assert error_rows_spec(Orders, form).partitions_def is None

    @pytest.mark.parametrize(
        "form",
        [dg.AssetKey(["orders"]), ["orders"], "orders"],
        ids=["asset_key", "sequence", "string"],
    )
    def test_a_key_form_takes_the_partitions_it_is_given(
        self,
        form: dg.AssetKey | Sequence[str],
    ):
        assert error_rows_spec(Orders, form, partitions_def=_DAYS).partitions_def == (
            _DAYS
        )

    @pytest.mark.parametrize(
        "form",
        [dg.AssetKey(["orders"]), ["orders"], "orders"],
        ids=["asset_key", "sequence", "string"],
    )
    def test_a_key_form_takes_the_group_it_is_given(
        self, form: dg.AssetKey | Sequence[str]
    ):
        assert error_rows_spec(Orders, form, group_name="silver").group_name == "silver"

    def test_a_key_form_with_no_group_is_in_the_default_group(self):
        spec = error_rows_spec(Orders, "orders")
        graph = dg.Definitions(assets=[spec]).resolve_asset_graph()

        assert graph.get(spec.key).group_name == "default"

    def test_a_definition_and_a_partitions_def_together_raise(self):
        with pytest.raises(dg.DagsterInvariantViolationError) as raised:
            error_rows_spec(Orders, orders, partitions_def=_DAYS)

        assert "partitions_def" in str(raised.value)
        assert "sales/orders" in str(raised.value)


def _loaded(
    tmp_path: Path,
    *,
    partitions_def: dg.PartitionsDefinition | None = None,
    partition_key: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Write an error-rows file at `error_rows_path`, then return what a downstream asset loads from it and the rows it holds."""
    invalid = _invalid_rows()
    path = error_rows_path(_ORDERS, tmp_path, partition_key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("wb") as file:
        invalid.write_parquet(file)
    loaded: dict[str, pl.DataFrame] = {}

    @dg.asset(partitions_def=partitions_def)
    def audit(orders_error_rows: pl.DataFrame) -> None:
        loaded["rows"] = orders_error_rows

    result = dg.materialize(
        [audit, error_rows_spec(Orders, "orders", partitions_def=partitions_def)],
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
) -> dg.AssetsDefinition:
    """Declare the asset the delegation tests run."""

    @dd.asset(
        Orders,
        name="orders",
        key_prefix=WAREHOUSE_SCHEMA,
        filter=True,
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
    """Materialize the asset, and assert it fails exactly when `aborts` is true."""
    result = dg.materialize(
        [asset],
        resources=resources,
        partition_key=_DAY if partitioned else None,
        raise_on_error=False,
    )

    assert result.success is not aborts


class TestDelegatingWriter:
    """The writer `delegating_writer` returns, which writes through the asset's IO manager."""

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
            f"orders_error_rows/{_DAY}.parquet"
            if partitioned
            else "orders_error_rows.parquet"
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

        assert "orders_error_rows" in written
        assert written["orders_error_rows"].height == invalid
        assert "dy_rule__amount__min" in written["orders_error_rows"].columns

    def test_a_clean_rerun_empties_the_error_rows_file(self, tmp_path: Path):
        path = tmp_path / WAREHOUSE_SCHEMA / "orders_error_rows.parquet"
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
        dirty = tables(tmp_path)["orders_error_rows"]

        _run(
            _delegating(clean_orders, partitioned=False),
            warehouse(tmp_path),
            partitioned=False,
            aborts=False,
        )
        clean = tables(tmp_path)["orders_error_rows"]

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
        assert (tmp_path / WAREHOUSE_SCHEMA / "orders_error_rows.parquet").exists()

    def test_the_step_probe_does_not_widen_to_the_whole_writer(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ):
        """A `DagsterInvalidPropertyError` from `delegating_writer` fails the run instead of switching to `file_writer`."""
        error_rows_dir = tmp_path / "filtering"
        monkeypatch.setenv("DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR", str(error_rows_dir))

        def raises(_context: dg.AssetExecutionContext) -> ErrorRowsWriter:
            not_the_step = "asset_partitions_time_window"
            raise DagsterInvalidPropertyError(not_the_step)

        monkeypatch.setattr(_error_rows, "delegating_writer", raises)

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

        assert (tmp_path / WAREHOUSE_SCHEMA / "orders_error_rows.parquet").exists()

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
        (event,) = result.get_asset_materialization_events()
        metadata = event.step_materialization_data.materialization.metadata

        assert metadata["dataframely/error_rows_address"] == dg.MetadataValue.text(
            f"{WAREHOUSE_SCHEMA}/orders_error_rows"
        )

    def test_the_managers_own_metadata_does_not_reach_the_materialization(
        self, tmp_path: Path
    ):
        """`cooccurring_orders` has 3 valid rows and 1 invalid, so a row count of 1 would mean the error rows' metadata replaced the asset's."""
        result = dg.materialize(
            [_delegating(cooccurring_orders, partitioned=False)],
            resources=storage(tmp_path),
        )
        (event,) = result.get_asset_materialization_events()
        metadata = event.step_materialization_data.materialization.metadata

        assert result.success
        assert metadata["dagster/row_count"] == dg.MetadataValue.int(3)
        assert "orders_error_rows" not in str(metadata.get("path", ""))


class TestFileWriter:
    """The writer `file_writer` returns."""

    def test_the_file_writer_writes_where_the_path_rule_says(self, tmp_path: Path):
        events = list(
            validation_results(
                Orders,
                mixed_orders(),
                valid_key=_ORDERS,
                error_rows_writer=file_writer(_ORDERS, tmp_path),
            )
        )
        (materialization,) = [e for e in events if isinstance(e, dg.MaterializeResult)]
        path = tmp_path / "orders_error_rows.parquet"

        assert (materialization.metadata or {})[
            "dataframely/error_rows_address"
        ] == str(path)
        assert_frame_equal(pl.read_parquet(path), _invalid_rows())

    def test_the_file_writer_creates_the_directories_it_needs(self, tmp_path: Path):
        key = dg.AssetKey(["sales", "eu", "orders"])

        file_writer(key, tmp_path)(_invalid_rows())

        assert (tmp_path / "sales" / "eu" / "orders_error_rows.parquet").exists()

    def test_the_file_writer_puts_a_partition_under_the_leaf(self, tmp_path: Path):
        file_writer(_ORDERS, tmp_path, "2026-01-02")(_invalid_rows())

        assert (tmp_path / "orders_error_rows" / "2026-01-02.parquet").exists()

    def test_the_file_writer_leaves_nothing_behind_when_nothing_calls_it(
        self, tmp_path: Path
    ):
        file_writer(dg.AssetKey(["sales", "orders"]), tmp_path)

        assert list(tmp_path.iterdir()) == []


class _Location(NamedTuple):
    """A stand-in context with the `repository_def` that only a run launched from a code location has (ADR-0007)."""

    asset_key: dg.AssetKey
    repository_def: dg.RepositoryDefinition


def _collider() -> dg.AssetsDefinition:
    """Declare an asset with the error rows' key."""

    @dg.asset(name="orders_error_rows", key_prefix=WAREHOUSE_SCHEMA)
    def orders_error_rows() -> pl.DataFrame:
        return pl.DataFrame({"sentinel": [1]})

    return orders_error_rows


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
        assert f"'{WAREHOUSE_SCHEMA}/orders_error_rows'" in message
        assert "Rename that asset" in message

    def test_the_collision_error_is_raised_before_the_body_runs(self, tmp_path: Path):
        ran: list[int] = []

        @dd.asset(Orders, name="orders", key_prefix=WAREHOUSE_SCHEMA, filter=True)
        def orders() -> pl.DataFrame:
            ran.append(1)
            return mixed_orders()

        with pytest.raises(ErrorRowsKeyCollisionError):
            dg.materialize([orders, _collider()], resources=storage(tmp_path))

        assert ran == []

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

    def test_a_error_rows_spec_stands_for_the_error_rows_rather_than_competing(self):
        """`error_rows_spec` has the error rows' key but is not executable, so it does not collide."""
        asset = _delegating(mixed_orders, partitioned=False)
        defs = dg.Definitions(assets=[asset, error_rows_spec(Orders, asset)])
        repository = defs.get_repository_def()

        validate_error_rows_key(
            _Location(asset.key, repository)  # pyrefly: ignore[bad-argument-type]
        )

        assert (
            dg.AssetKey([WAREHOUSE_SCHEMA, "orders_error_rows"])
            not in repository.asset_graph.executable_asset_keys
        )
