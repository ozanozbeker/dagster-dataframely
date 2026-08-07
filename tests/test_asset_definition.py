"""Definition-time behaviour of `@dataframely_asset`, asserted with no execution.

The seam is the `AssetsDefinition` the decorator returns. Everything a user sees before the asset has ever run is on it: the keys, the check specs, and the definition metadata that fills the Columns tab.
"""

import datetime as dt
import inspect

import dagster as dg
import dataframely as dy
import polars as pl
import pytest
from dagster._config.field_utils import Shape
from dagster._core.definitions.metadata.metadata_value import ObjectMetadataValue
from dagster._core.remote_representation.external_data import RepositorySnap
from dagster._serdes import deserialize_value, serialize_value

from dagster_dataframely import (
    CheckNameCollisionError,
    CollectionNotSupportedError,
    DagsterDataframelyError,
    ReservedColumnError,
    dataframely_asset,
)
from tests.scenario import Orders

_COLUMN_SCHEMA_KEY = "dagster/column_schema"
_SCHEMA_CARRIER_KEY = "dagster_dataframely/schema"

# Every rule `Orders` declares, in the order dataframely reports them. Spelled out rather than derived so that a rule silently disappearing is a failure here.
_RULES = [
    "paid_orders_have_amount",
    "line_numbers_are_dense",
    "primary_key",
    "order_id|nullability",
    "order_id|regex",
    "line_no|nullability",
    "line_no|min",
    "email|nullability",
    "email|check__lowercase",
    "amount|nullability",
    "amount|min",
    "tracking_id|unique",
    "quantity|nullability",
    "quantity|min",
    "status|nullability",
    "ordered_at|nullability",
    "tags|inner_nullability",
    "note|check",
]


@dataframely_asset(schema=Orders, group_name="sales")
def orders() -> pl.DataFrame:
    """Doc line that becomes the asset description."""
    return pl.DataFrame()


def _specs_by_name(asset: dg.AssetsDefinition) -> dict[str, dg.AssetCheckSpec]:
    return {spec.name: spec for spec in asset.check_specs}


def test_the_door_produces_a_multi_asset_with_one_out():
    assert orders.keys == {dg.AssetKey(["orders"])}
    assert orders.group_names_by_key == {dg.AssetKey(["orders"]): "sales"}


def test_the_out_is_not_required():
    """The gate and the abort paths both end the step without yielding the output."""
    (spec,) = orders.specs
    assert spec.skippable


def test_a_key_prefix_carries_the_checks_with_it():
    """The key is built once and handed to both surfaces, so the checks cannot lag it."""

    @dataframely_asset(schema=Orders, key_prefix="sales")
    def prefixed() -> pl.DataFrame:
        return pl.DataFrame()

    key = dg.AssetKey(["sales", "prefixed"])

    assert prefixed.keys == {key}
    assert {spec.asset_key for spec in prefixed.check_specs} == {key}


def test_a_sequence_key_prefix_nests():
    @dataframely_asset(schema=Orders, key_prefix=["warehouse", "sales"])
    def nested() -> pl.DataFrame:
        return pl.DataFrame()

    assert nested.keys == {dg.AssetKey(["warehouse", "sales", "nested"])}


def test_name_overrides_the_function_name():
    """A private-looking function can back a public asset key."""

    @dataframely_asset(schema=Orders, name="orders", key_prefix="sales")
    def _orders_impl() -> pl.DataFrame:
        return pl.DataFrame()

    assert _orders_impl.keys == {dg.AssetKey(["sales", "orders"])}


def test_upstream_dependencies_bind_as_ordinary_parameters():
    """`functools.wraps` is what carries the signature through the wrapper."""

    @dataframely_asset(schema=Orders)
    def downstream(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return raw_orders

    assert downstream.keys_by_input_name == {"raw_orders": dg.AssetKey(["raw_orders"])}


@dg.success_hook
def _notify(context: dg.HookContext) -> None:
    pass


class _Warehouse(dg.ConfigurableResource[None]):
    dsn: str


def test_every_forwarded_multi_asset_parameter_reaches_the_definition():
    """Thirteen of the fifteen at once, so a parameter that forwards only in isolation still fails here. `name` and `pool` have their own tests: one changes the asset key rather than landing unchanged, and the other cannot share an op with `backfill_policy`."""
    partitions = dg.StaticPartitionsDefinition(["a", "b"])
    retry = dg.RetryPolicy(max_retries=2)
    backfill = dg.BackfillPolicy.single_run()

    @dataframely_asset(
        schema=Orders,
        ins={"raw": dg.AssetIn(key=dg.AssetKey(["upstream_frame"]))},
        deps=["upstream"],
        description="Explicit description.",
        config_schema={"threshold": int},
        required_resource_keys={"warehouse"},
        partitions_def=partitions,
        hooks={_notify},
        backfill_policy=backfill,
        op_tags={"team": "data"},
        resource_defs={"warehouse": _Warehouse(dsn="postgres://")},
        group_name="sales",
        retry_policy=retry,
        code_version="v1",
    )
    def forwarded(raw: pl.DataFrame) -> pl.DataFrame:
        return raw

    (spec,) = forwarded.specs

    # A `deps` entry becomes an input too, so this reads one key rather than the mapping.
    assert forwarded.keys_by_input_name["raw"] == dg.AssetKey(["upstream_frame"])
    assert {dep.asset_key for dep in spec.deps} == {
        dg.AssetKey(["upstream"]),
        dg.AssetKey(["upstream_frame"]),
    }
    assert spec.description == "Explicit description."
    config_type = forwarded.op.config_schema.config_type
    assert isinstance(config_type, Shape)
    assert sorted(config_type.fields) == ["threshold"]
    assert forwarded.op.required_resource_keys == frozenset({"warehouse"})
    assert spec.partitions_def == partitions
    assert {hook.name for hook in forwarded.hook_defs} == {"_notify"}
    assert forwarded.backfill_policy == backfill
    assert forwarded.op.tags["team"] == "data"
    assert sorted(forwarded.resource_defs) == ["warehouse"]
    assert spec.group_name == "sales"
    assert forwarded.op.retry_policy == retry
    assert spec.code_version == "v1"


def test_pool_reaches_the_underlying_op():
    """Separate because a pool and a `backfill_policy` cannot both sit on one op."""

    @dataframely_asset(schema=Orders, pool="limited")
    def pooled() -> pl.DataFrame:
        return pl.DataFrame()

    assert pooled.op.pool == "limited"


def test_asset_level_parameters_reach_the_out():
    """All seven at once. `@dg.multi_asset` has no per-out vocabulary, so these land on the `AssetOut` rather than being forwarded. Same names as `@dg.asset` uses, because the door is designed for one table."""
    condition = dg.AutomationCondition.eager()
    freshness = dg.FreshnessPolicy.time_window(fail_window=dt.timedelta(hours=24))

    @dataframely_asset(
        schema=Orders,
        io_manager_key="warehouse",
        metadata={"sla_hours": 4},
        tags={"layer": "silver"},
        owners=["team:data"],
        kinds={"parquet"},
        automation_condition=condition,
        freshness_policy=freshness,
    )
    def routed() -> pl.DataFrame:
        return pl.DataFrame()

    (spec,) = routed.specs
    key = dg.AssetKey(["routed"])

    assert routed.node_def.output_dict["routed"].io_manager_key == "warehouse"
    assert routed.metadata_by_key[key]["sla_hours"] == 4
    assert spec.tags["layer"] == "silver"
    assert list(spec.owners) == ["team:data"]
    assert spec.kinds == {"parquet"}
    assert spec.automation_condition == condition
    assert spec.freshness_policy == freshness


def test_user_metadata_cannot_displace_the_packages_own():
    """The Columns tab and the schema carrier are what the decorator is for, so a colliding user key loses rather than silently breaking the IO manager's read path."""

    @dataframely_asset(
        schema=Orders,
        metadata={
            _COLUMN_SCHEMA_KEY: "mine",
            _SCHEMA_CARRIER_KEY: "mine",
            "own": "kept",
        },
    )
    def collides() -> pl.DataFrame:
        return pl.DataFrame()

    definition_metadata = collides.metadata_by_key[dg.AssetKey(["collides"])]

    assert definition_metadata["own"] == "kept"
    assert isinstance(definition_metadata[_COLUMN_SCHEMA_KEY], dg.TableSchema)
    assert definition_metadata[_SCHEMA_CARRIER_KEY].instance is Orders


# --- checks ---
def test_there_is_one_check_per_rule_plus_the_gate():
    """Specs come off the schema, so a clean run still reports on every rule."""
    expected = {"dy_schema__dtypes"} | {
        f"dy_rule__{rule.replace('|', '__')}" for rule in _RULES
    }

    assert set(_specs_by_name(orders)) == expected


def test_only_the_gate_check_is_blocking():
    specs = _specs_by_name(orders)

    assert specs["dy_schema__dtypes"].blocking
    assert not any(
        spec.blocking for name, spec in specs.items() if name != "dy_schema__dtypes"
    )


def test_a_rules_docstring_becomes_its_check_description():
    specs = _specs_by_name(orders)

    assert (
        specs["dy_rule__paid_orders_have_amount"].description
        == "Paid orders must carry a positive amount."
    )


def test_a_rule_without_a_docstring_falls_back_to_its_name():
    """The rendered-constraint rung of the ladder arrives with the renderer (#20)."""
    specs = _specs_by_name(orders)

    assert specs["dy_rule__line_numbers_are_dense"].description == (
        "line_numbers_are_dense"
    )
    assert specs["dy_rule__amount__min"].description == "amount|min"


def test_the_gate_check_names_the_schema():
    assert _specs_by_name(orders)["dy_schema__dtypes"].description == (
        "Columns and dtypes match Orders."
    )


# --- definition metadata ---
def _columns() -> dict[str, dg.TableColumn]:
    table_schema = orders.metadata_by_key[dg.AssetKey(["orders"])][_COLUMN_SCHEMA_KEY]
    return {column.name: column for column in table_schema.columns}


def test_the_columns_tab_is_populated_before_first_materialization():
    """Dtype, description and nullability, read off the schema."""
    amount = _columns()["amount"]

    assert amount.type == "Decimal(precision=10, scale=2)"
    assert amount.description == "Line total in account currency."
    assert not amount.constraints.nullable


def test_column_metadata_becomes_tags():
    """`Column.metadata` is stored by dataframely and never read, so column tags are its only destination. Values are stringified because Dagster's tags are `Mapping[str, str]` and it rejects anything else at definition time."""
    assert _columns()["amount"].tags == {"owner": "finance", "pii": "False"}


def test_a_column_without_metadata_carries_no_tags():
    assert not _columns()["quantity"].tags


def test_a_nullable_column_says_so():
    assert _columns()["note"].constraints.nullable


def test_every_schema_column_reaches_the_catalog_in_order():
    assert list(_columns()) == list(Orders.columns())


def test_no_pill_or_table_constraint_is_emitted_yet():
    """Pills and the table-level primary key arrive with the renderer (#20)."""
    table_schema = orders.metadata_by_key[dg.AssetKey(["orders"])][_COLUMN_SCHEMA_KEY]

    assert table_schema.constraints == dg.TableConstraints(other=[])
    assert all(column.constraints.other == [] for column in table_schema.columns)


def test_a_unique_column_says_so():
    """`tracking_id` declares `unique=True`, which dataframely enforces with its own rule and therefore its own check. The catalog has to agree with the check."""
    assert _columns()["tracking_id"].constraints.unique
    assert "dy_rule__tracking_id__unique" in _specs_by_name(orders)


def test_a_primary_key_column_never_claims_to_be_unique():
    """dataframely keeps the two flags independent: a key member gets a composite `as_struct(...).is_unique()` rule and `column.unique` stays `False`. Deriving `unique` from `primary_key` would assert a per-column uniqueness that nothing enforces."""
    assert not _columns()["order_id"].constraints.unique
    assert not _columns()["line_no"].constraints.unique


def test_the_schema_carrier_holds_the_live_class_under_an_explicit_label():
    """Deriving the label instead would yield the metaclass name, `SchemaMeta`."""
    carrier = orders.metadata_by_key[dg.AssetKey(["orders"])][_SCHEMA_CARRIER_KEY]

    assert carrier.value == "Orders"
    assert carrier.instance is Orders


# --- definition-time errors ---
def test_a_user_column_in_the_reserved_namespace_raises():
    class Reserved(dy.Schema):
        dy_flag = dy.Bool()
        order_id = dy.String()

    with pytest.raises(ReservedColumnError) as raised:

        @dataframely_asset(schema=Reserved)
        def reserved() -> pl.DataFrame:
            return pl.DataFrame()

    assert "Column 'dy_flag' of Reserved uses" in str(raised.value)
    assert "Rename it." in str(raised.value)
    assert "order_id" not in str(raised.value)


def test_the_reserved_column_error_reads_as_plural_for_several_columns():
    """The message is the only thing a user sees of this error, so it agrees in number."""

    class Reserved(dy.Schema):
        dy_flag = dy.Bool()
        dy_rule__amount__min = dy.Bool()

    with pytest.raises(ReservedColumnError) as raised:

        @dataframely_asset(schema=Reserved)
        def reserved() -> pl.DataFrame:
            return pl.DataFrame()

    assert "Columns 'dy_flag', 'dy_rule__amount__min' of Reserved use" in str(
        raised.value
    )
    assert "Rename them." in str(raised.value)


def test_two_rules_colliding_after_the_rewrite_raise_and_name_both():
    class Colliding(dy.Schema):
        order_id = dy.String(nullable=False)

        @dy.rule()
        def order_id__nullability(cls) -> pl.Expr:
            return cls.order_id.col.is_not_null()

    with pytest.raises(CheckNameCollisionError) as raised:

        @dataframely_asset(schema=Colliding)
        def colliding() -> pl.DataFrame:
            return pl.DataFrame()

    message = str(raised.value)

    assert "order_id__nullability" in message
    assert "order_id|nullability" in message
    assert "dy_rule__order_id__nullability" in message


def test_a_collection_is_refused_at_the_boundary():
    class OrderBook(dy.Collection):
        orders: dy.LazyFrame[Orders]

    with pytest.raises(CollectionNotSupportedError) as raised:
        dataframely_asset(schema=OrderBook)  # pyrefly: ignore[bad-argument-type]

    assert "OrderBook" in str(raised.value)


def test_a_non_schema_argument_is_left_to_fail_however_it_fails():
    """The Collection guard exists because `dy.Collection` is the plausible wrong reach. It was deliberately not generalised into a type check on `schema=`."""
    with pytest.raises(Exception) as raised:  # noqa: PT011 - breadth is the point

        @dataframely_asset(schema=42)  # pyrefly: ignore[bad-argument-type]
        def nonsense() -> pl.DataFrame:
            return pl.DataFrame()

    assert not isinstance(raised.value, DagsterDataframelyError)


# --- the door's own contract with dagster ---
# Parameters `dg.multi_asset` has that the door deliberately does not forward.
_NOT_FORWARDED = {
    "outs",  # door-owned: the door builds the out from the schema
    "check_specs",  # door-owned: derived from the schema, never contested
    "specs",  # door-owned: the alternative spelling of `outs`
    "can_subset",  # deliberately absent (#4): a subset executes but saves nothing
    "internal_asset_deps",  # nothing to wire: the door emits a single out
}
_DOOR_OWNED = {"schema", "key_prefix"}

# Not forwarded to `multi_asset`, which has no per-out vocabulary. These land on the good `dg.AssetOut` instead, which is why they are absent from its signature.
_ASSET_LEVEL = {
    "io_manager_key",
    "metadata",
    "tags",
    "owners",
    "kinds",
    "automation_condition",
    "freshness_policy",
}

# `@dg.multi_asset` is the mechanism, but `@dg.asset` is the vocabulary: this decorator is designed for a single table. Parameters `dg.asset` has that the door deliberately does not, each for a reason that is not "nobody thought about it".
_NOT_ON_THE_DOOR = {
    "check_specs",  # door-owned: derived from the schema, never contested
    "key",  # door-owned: `key_prefix` plus `name` already say it, once
    "output_required",  # door-owned: the gate and abort paths must be able to skip
    "dagster_type",  # ruled out (#3): runs before the IO manager, no severity dial
    "is_virtual",  # a virtual asset has no compute, so there is no transform
    "io_manager_def",  # not settable per out; the forwarded `resource_defs` covers it
}


def test_the_door_speaks_dg_assets_vocabulary():
    """The interface is designed for one table, so anything `@dg.asset` can say about an asset should be sayable here under the same name.

    Asserted in both directions, like the `multi_asset` pin: nothing the door offers has vanished from `dg.asset`, and nothing `dg.asset` gains is silently missing here.
    """
    door = set(inspect.signature(dataframely_asset).parameters) - {"schema"}
    upstream = set(inspect.signature(dg.asset).parameters) - {"compute_fn", "kwargs"}

    assert door <= upstream, f"no longer on dg.asset: {door - upstream}"
    assert upstream - door == _NOT_ON_THE_DOOR, (
        f"new on dg.asset, unconsidered here: {upstream - door - _NOT_ON_THE_DOOR}"
    )


def test_the_forwarded_parameter_list_matches_multi_assets_signature():
    """Asserted in both directions, so the curated list neither breaks silently nor silently lags a new Dagster feature (#15, pin-and-assert obligation 4)."""
    forwarded = (
        set(inspect.signature(dataframely_asset).parameters)
        - _DOOR_OWNED
        - _ASSET_LEVEL
    )
    # `kwargs` is `multi_asset`'s varargs catch-all, not a parameter to forward.
    upstream = set(inspect.signature(dg.multi_asset).parameters) - {"kwargs"}

    assert forwarded <= upstream, f"no longer on multi_asset: {forwarded - upstream}"
    assert upstream - forwarded == _NOT_FORWARDED


def test_the_surfaces_the_package_owns_are_not_parameters():
    """Statically unpassable, so no runtime guard is needed."""
    parameters = set(inspect.signature(dataframely_asset).parameters)

    assert parameters.isdisjoint({"outs", "check_specs", "specs", "can_subset"})


def test_the_code_location_snapshot_degrades_the_schema_carrier():
    """The carrier holds a live class, which cannot be serialized. Dagster must drop the instance rather than refuse to build the snapshot (#15, behavioural pin).

    `RepositorySnap` and `serialize_value` are private paths; they are what a code location actually runs on load, and there is no public equivalent.
    """
    definitions = dg.Definitions(assets=[orders])
    snapshot = RepositorySnap.from_def(definitions.get_repository_def())

    restored = deserialize_value(serialize_value(snapshot), RepositorySnap)
    (node,) = [
        n for n in restored.asset_nodes if n.asset_key == dg.AssetKey(["orders"])
    ]
    carrier = node.metadata[_SCHEMA_CARRIER_KEY]

    assert isinstance(carrier, ObjectMetadataValue)
    assert carrier.value == "Orders"
    assert carrier.instance is None
