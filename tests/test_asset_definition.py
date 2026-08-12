"""Definition-time behaviour of `@dataframely_asset`, asserted with no execution.

The seam is the `AssetsDefinition` the decorator returns. Everything a user sees before the asset has ever run is on it: the keys, the check specs, and the definition metadata that fills the Columns tab.
"""

import datetime as dt
import inspect
from typing import Any

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
    InvalidSettingError,
    QuarantineSettingError,
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
    "email|max_length",
    "amount|nullability",
    "amount|min",
    "tracking_id|unique",
    "quantity|nullability",
    "quantity|min",
    "status|nullability",
    "ordered_at|nullability",
    "tags|max_length",
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
    """The shape check and the abort paths both end the step without yielding the output."""
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
    """Twelve of the fourteen at once, so a parameter that forwards only in isolation still fails here. `name` and `pool` have their own tests: one changes the asset key rather than landing unchanged, and the other cannot share an op with `backfill_policy`."""
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
    assert forwarded.op.retry_policy == retry
    assert spec.code_version == "v1"


def test_pool_reaches_the_underlying_op():
    """Separate because a pool and a `backfill_policy` cannot both sit on one op."""

    @dataframely_asset(schema=Orders, pool="limited")
    def pooled() -> pl.DataFrame:
        return pl.DataFrame()

    assert pooled.op.pool == "limited"


def test_asset_level_parameters_reach_the_out():
    """All eight at once. Seven land on the `AssetOut` because `@dg.multi_asset` has no per-out vocabulary for them; `group_name` lands there because Dagster refuses it on the `multi_asset` as soon as an out names one, which is what leaves the quarantine free to claim its own. Same names as `@dg.asset` uses, because the door is designed for one table."""
    condition = dg.AutomationCondition.eager()
    freshness = dg.FreshnessPolicy.time_window(fail_window=dt.timedelta(hours=24))

    @dataframely_asset(
        schema=Orders,
        io_manager_key="warehouse",
        group_name="sales",
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
    assert spec.group_name == "sales"
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
    """Specs come off the schema, so a clean run still reports on every rule.

    Also the whole of "no rule value appears in any check name": a name is the rule's own name rewritten and nothing else, so tightening `min` leaves the check where it was rather than orphaning its history.
    """
    expected = {"dy_schema__dtypes"} | {
        f"dy_rule__{rule.replace('|', '__')}" for rule in _RULES
    }

    assert set(_specs_by_name(orders)) == expected


def test_only_the_shape_check_is_blocking():
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


def test_a_check_without_a_docstring_describes_the_constraint_itself():
    """The middle rung. A check list reads flat, so the column is named: a bare `>= 0.00` says nothing about which column it bounds."""
    specs = _specs_by_name(orders)

    assert specs["dy_rule__amount__min"].description == "amount >= 0.00"
    assert specs["dy_rule__email__max_length"].description == (
        "email length <= 254 bytes"
    )
    assert specs["dy_rule__order_id__nullability"].description == "order_id not null"


def test_the_primary_key_check_describes_the_whole_key():
    """It is one rule over a struct of every key column, so its description says so rather than naming one of them."""
    assert _specs_by_name(orders)["dy_rule__primary_key"].description == (
        "PK: order_id, line_no"
    )


def test_a_rule_with_neither_a_docstring_nor_a_constraint_falls_back_to_its_name():
    """A `@dy.rule()` body is an arbitrary expression, so there is nothing structured to render and the name is all that is left."""
    assert _specs_by_name(orders)["dy_rule__line_numbers_are_dense"].description == (
        "line_numbers_are_dense"
    )


def test_no_check_description_is_blank():
    """The ladder's whole point: every rung is reachable and the last one always holds."""
    assert all(spec.description for spec in orders.check_specs)


def test_the_shape_check_names_the_schema():
    assert _specs_by_name(orders)["dy_schema__dtypes"].description == (
        "Columns and dtypes match Orders."
    )


# --- check granularity ---
_RULE_BEARING_COLUMNS = {rule.split("|")[0] for rule in _RULES if "|" in rule}

# The rules no single column owns: both `@dy.rule()` bodies and the composite key.
_MULTI_COLUMN_RULES = [rule for rule in _RULES if "|" not in rule]


@dataframely_asset(schema=Orders, name="by_column", check_granularity="column")
def by_column() -> pl.DataFrame:
    return pl.DataFrame()


@dataframely_asset(schema=Orders, name="by_schema", check_granularity="schema")
def by_schema() -> pl.DataFrame:
    return pl.DataFrame()


def test_column_granularity_collapses_to_one_check_per_rule_bearing_column():
    """A 40-column schema contributes around 120 checks at `rule` granularity, which is a check list nobody reads."""
    assert set(_specs_by_name(by_column)) == {
        "dy_schema__dtypes",
        "dy_schema__rules",
    } | {f"dy_col__{column}" for column in _RULE_BEARING_COLUMNS}


def test_a_column_carrying_no_rule_carries_no_check():
    """`fulfilled_in` and `payload` are nullable with no constraints, so a check for either would report on nothing."""
    assert not {"dy_col__fulfilled_in", "dy_col__payload"} & set(
        _specs_by_name(by_column)
    )


def test_a_ten_field_struct_is_ten_checks_by_rule_and_one_by_column():
    """dataframely emits one `inner_<field>_nullability` rule per struct field, so a wide struct is where the check list gets unreadable fastest."""

    class Addresses(dy.Schema):
        address_id = dy.String(primary_key=True)
        address = dy.Struct(
            {f"field_{n}": dy.String(nullable=False) for n in range(10)}, nullable=True
        )

    @dataframely_asset(schema=Addresses, name="by_rule")
    def by_rule() -> pl.DataFrame:
        return pl.DataFrame()

    @dataframely_asset(schema=Addresses, name="collapsed", check_granularity="column")
    def collapsed() -> pl.DataFrame:
        return pl.DataFrame()

    struct_checks = [
        name
        for name in _specs_by_name(by_rule)
        if name.startswith("dy_rule__address__")
    ]

    assert len(struct_checks) == 10
    assert set(_specs_by_name(collapsed)) == {
        "dy_schema__dtypes",
        "dy_schema__rules",
        "dy_col__address_id",
        "dy_col__address",
    }


def test_multi_column_rules_bucket_into_the_schema_check_by_default():
    """They belong to no column, so at column granularity they have no rule set of their own to land in."""
    specs = _specs_by_name(by_column)

    assert "dy_schema__rules" in specs
    assert not {f"dy_rule__{rule}" for rule in _MULTI_COLUMN_RULES} & set(specs)


def test_per_rule_gives_each_multi_column_rule_a_check_of_its_own():
    """The setting is for a schema whose cross-column rules are the ones worth their own history."""

    @dataframely_asset(
        schema=Orders,
        name="per_rule",
        check_granularity="column",
        multi_column_rules="per_rule",
    )
    def per_rule() -> pl.DataFrame:
        return pl.DataFrame()

    specs = _specs_by_name(per_rule)

    assert {f"dy_rule__{rule}" for rule in _MULTI_COLUMN_RULES} <= set(specs)
    assert "dy_schema__rules" not in specs


def test_the_multi_column_bucket_cannot_be_collided_with_by_a_user_column():
    """It is `dy_schema__rules` rather than `dy_col__schema` precisely because a column named `schema` is a column somebody has."""

    class Tables(dy.Schema):
        table_id = dy.String(primary_key=True)
        schema = dy.String(nullable=False)

    @dataframely_asset(schema=Tables, name="tables", check_granularity="column")
    def tables() -> pl.DataFrame:
        return pl.DataFrame()

    specs = _specs_by_name(tables)

    assert "dy_col__schema" in specs
    assert "dy_schema__rules" in specs
    assert specs["dy_col__schema"] != specs["dy_schema__rules"]


def test_schema_granularity_leaves_one_rules_check_beside_the_gate():
    assert set(_specs_by_name(by_schema)) == {"dy_schema__dtypes", "dy_schema__rules"}


@pytest.mark.parametrize("granularity", ["rule", "column", "schema"])
def test_a_schema_with_no_rules_gets_the_shape_check_and_nothing_else(granularity: Any):
    """The shape check still holds the shape. A rules check with no rules in it would pass forever and say nothing, at any granularity."""

    class Blob(dy.Schema):
        payload = dy.String(nullable=True)

    @dataframely_asset(
        schema=Blob, name=f"blob_{granularity}", check_granularity=granularity
    )
    def blob() -> pl.DataFrame:
        return pl.DataFrame()

    assert set(_specs_by_name(blob)) == {"dy_schema__dtypes"}


@pytest.mark.parametrize("asset", [orders, by_column, by_schema])
def test_the_shape_check_is_present_and_blocking_at_every_granularity(
    asset: dg.AssetsDefinition,
):
    """It is not a rule, so it never joins a rule set, and a wrong-shaped frame has to stop the run whatever the check list looks like."""
    specs = _specs_by_name(asset)

    assert specs["dy_schema__dtypes"].blocking
    assert not any(
        spec.blocking for name, spec in specs.items() if name != "dy_schema__dtypes"
    )


def test_a_collapsed_check_names_the_rules_it_reports_for():
    """The description is where the collapsed detail goes: the check name says `email`, and the constraints it stands for say the rest."""
    specs = _specs_by_name(by_column)

    assert specs["dy_col__email"].description == (
        "Every rule on email: not null, lowercase, length <= 254 bytes."
    )
    assert specs["dy_schema__rules"].description == (
        "Every rule of Orders that no single column owns: paid_orders_have_amount, "
        "line_numbers_are_dense, primary_key."
    )
    assert _specs_by_name(by_schema)["dy_schema__rules"].description == (
        "Every validation rule of Orders."
    )


def test_a_granularity_outside_the_vocabulary_raises_at_the_door():
    """Definition time, so a misconfiguration never reaches a run.

    The value arrives through an untyped name because the literal is already a static error, and the runtime guard is what a user without a type checker gets.
    """
    wrong: Any = "per_column"

    with pytest.raises(InvalidSettingError) as raised:

        @dataframely_asset(schema=Orders, name="misconfigured", check_granularity=wrong)
        def _misconfigured() -> pl.DataFrame:
            return pl.DataFrame()

    assert "check_granularity" in str(raised.value)


def test_the_environment_variable_sets_the_house_granularity(
    monkeypatch: pytest.MonkeyPatch,
):
    """One export in a code location's environment, and every asset in it collapses."""
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_CHECK_GRANULARITY", "schema")

    @dataframely_asset(schema=Orders, name="house_style")
    def house_style() -> pl.DataFrame:
        return pl.DataFrame()

    assert set(_specs_by_name(house_style)) == {"dy_schema__dtypes", "dy_schema__rules"}


# --- definition metadata ---
def _catalog(asset: dg.AssetsDefinition, key: dg.AssetKey) -> dg.TableSchema:
    """The Columns tab a data consumer opens."""
    return asset.metadata_by_key[key][_COLUMN_SCHEMA_KEY]


def _columns_of(
    asset: dg.AssetsDefinition, key: dg.AssetKey
) -> dict[str, dg.TableColumn]:
    return {column.name: column for column in _catalog(asset, key).columns}


def _columns() -> dict[str, dg.TableColumn]:
    return _columns_of(orders, dg.AssetKey(["orders"]))


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


def test_a_unique_column_says_so():
    """`tracking_id` declares `unique=True`, which dataframely enforces with its own rule and therefore its own check. The catalog has to agree with the check."""
    assert _columns()["tracking_id"].constraints.unique
    assert "dy_rule__tracking_id__unique" in _specs_by_name(orders)


def test_a_primary_key_column_never_claims_to_be_unique():
    """dataframely keeps the two flags independent: a key member gets a composite `as_struct(...).is_unique()` rule and `column.unique` stays `False`. Deriving `unique` from `primary_key` would assert a per-column uniqueness that nothing enforces."""
    assert not _columns()["order_id"].constraints.unique
    assert not _columns()["line_no"].constraints.unique


# --- constraint pills ---
class Measurements(dy.Schema):
    """The constraint shapes `Orders` has no natural column for.

    Local to this file rather than added to the shared scenario: none of them changes what a runtime or IO-manager test sees, and each is here only to pin one arm of the pill renderer.
    """

    reading_at = dy.Datetime(resolution="1h")
    grade = dy.Int32(is_in=[1, 2, 3])
    depth = dy.Int32(min=0, max=100)
    ratio = dy.Float64(min_exclusive=0.0, max_exclusive=1.0)
    corners = dy.Array(dy.Int32(min=0), 2)
    label = dy.String(
        min_length=2,
        # Two lambdas, so dataframely disambiguates them with a counter rather than a name.
        check=[lambda expr: expr != "", lambda expr: expr == expr.str.strip_chars()],
    )


@dataframely_asset(schema=Measurements)
def measurements() -> pl.DataFrame:
    return pl.DataFrame()


def _measured() -> dict[str, dg.TableColumn]:
    return _columns_of(measurements, dg.AssetKey(["measurements"]))


def test_a_bound_reads_as_an_operator_rather_than_as_a_check_name():
    """A consumer should see what the bound is, not merely that one exists."""
    assert _columns()["line_no"].constraints.other == [">= 1"]
    assert _measured()["depth"].constraints.other == [">= 0", "<= 100"]
    assert _measured()["ratio"].constraints.other[:2] == ["> 0.0", "< 1.0"]


def test_a_length_bound_states_the_unit_it_counts():
    """`min_length` and `max_length` are spelled the same on both column families and mean different things, so the renderer dispatches on the column type. A bare `length <= 254` would be wrong for any multibyte text."""
    assert _columns()["email"].constraints.other == [
        "lowercase",
        "length <= 254 bytes",
    ]
    assert _columns()["tags"].constraints.other == [
        "length <= 5 elements",
        "elements not null",
    ]
    assert _measured()["label"].constraints.other[-1] == "length >= 2 bytes"


def test_the_remaining_constraint_shapes_render_their_value():
    assert _columns()["order_id"].constraints.other == [r"matches ^ORD-\d+$"]
    assert _measured()["reading_at"].constraints.other == ["aligned to 1h"]
    assert _measured()["grade"].constraints.other == ["in (1, 2, 3)"]


def test_a_named_check_renders_its_key_and_an_anonymous_one_says_it_is_one():
    """The nudge to name a check: `custom check` is all an unnamed lambda leaves to render, and dataframely's counter suffix is not a name anybody wrote."""
    assert "lowercase" in _columns()["email"].constraints.other
    assert _columns()["note"].constraints.other == ["custom check"]
    assert _measured()["label"].constraints.other[:2] == [
        "custom check",
        "custom check",
    ]


def test_a_nested_columns_rules_render_as_constraints_on_its_elements():
    """A `List` or an `Array` runs its inner column's rules over the elements, so the renderer recurses into it rather than falling back to `inner_min` on a surface that is supposed to read as operators."""
    assert _measured()["corners"].constraints.other == [
        "elements not null",
        "elements >= 0",
    ]


def test_a_constraint_dagster_models_first_class_is_not_repeated_as_a_pill():
    """`nullable` and `unique` have their own fields on `TableColumnConstraints`, so a pill saying the same thing would double every column's constraint list."""
    assert _columns()["quantity"].constraints.other == [">= 1"]
    assert _columns()["tracking_id"].constraints.other == []


def test_a_constraint_left_at_its_dataframely_default_renders_no_pill():
    """`allow_inf` and `allow_nan` default to `False`, so every float column carries an `inf` and a `nan` rule nobody asked for. Setting either flag `True` removes its rule rather than changing it, so the pill could never say anything but the default and says nothing at all. The checks still exist and still report."""
    assert _measured()["ratio"].constraints.other == ["> 0.0", "< 1.0"]
    assert {"dy_rule__ratio__inf", "dy_rule__ratio__nan"} <= set(
        _specs_by_name(measurements)
    )


def test_sibling_rules_render_in_one_voice_on_the_constraint_surface():
    """`paid_orders_have_amount` carries a docstring and `line_numbers_are_dense` does not. A docstring reaching this surface would put two sibling rules in different registers for a reason invisible from the UI, so the name is the constant here and the docstring reaches only the check description."""
    assert _catalog(orders, dg.AssetKey(["orders"])).constraints.other == [
        "paid_orders_have_amount",
        "line_numbers_are_dense",
        "PK: order_id, line_no",
    ]


def test_the_primary_key_is_stated_once_at_table_level():
    """dataframely models it as one rule over a struct of every key column, and stating it once is what distinguishes a composite key from two independent single-column ones."""
    table_schema = _catalog(orders, dg.AssetKey(["orders"]))

    assert "PK: order_id, line_no" in table_schema.constraints.other
    assert not any(
        "PK" in pill
        for column in table_schema.columns
        for pill in column.constraints.other
    )


class Ledger(dy.Schema):
    """A key member that also declares `unique=True`, which `Orders` has nowhere to put.

    dataframely keeps the two flags independent, so `entry_id` carries both rules: the composite `primary_key` over the pair, and its own `entry_id|unique` over itself.
    """

    entry_id = dy.String(primary_key=True, unique=True)
    posted_at = dy.Datetime(primary_key=True)


@dataframely_asset(schema=Ledger)
def ledger() -> pl.DataFrame:
    return pl.DataFrame()


def test_a_key_member_reads_not_null_and_claims_uniqueness_only_where_it_declared_it():
    """dataframely forbids a nullable key column, so `not null` is free. `unique` is not: on a composite key only the tuple is unique, and `{"a": ["x", "x"], "b": [1, 2]}` passes."""
    columns = _columns_of(ledger, dg.AssetKey(["ledger"]))

    assert not columns["entry_id"].constraints.nullable
    assert not columns["posted_at"].constraints.nullable
    assert columns["entry_id"].constraints.unique
    assert not columns["posted_at"].constraints.unique
    assert "dy_rule__entry_id__unique" in _specs_by_name(ledger)


def test_a_schema_with_no_primary_key_states_no_table_constraint():
    assert _catalog(measurements, dg.AssetKey(["measurements"])).constraints == (
        dg.TableConstraints(other=[])
    )


def test_the_schema_carrier_holds_the_live_class_under_an_explicit_label():
    """Deriving the label instead would yield the metaclass name, `SchemaMeta`."""
    carrier = orders.metadata_by_key[dg.AssetKey(["orders"])][_SCHEMA_CARRIER_KEY]

    assert carrier.value == "Orders"
    assert carrier.instance is Orders


# --- the quarantine ---
@dataframely_asset(schema=Orders, quarantine=dg.AssetOut(), group_name="sales")
def quarantined() -> pl.DataFrame:
    return pl.DataFrame()


_QUARANTINE_KEY = dg.AssetKey(["quarantined_quarantine"])


def test_declaring_a_quarantine_adds_a_second_out():
    """Its presence alone is the failure policy. There is no flag to disagree with it."""
    assert quarantined.keys == {dg.AssetKey(["quarantined"]), _QUARANTINE_KEY}


def test_the_quarantine_out_is_not_required_either():
    """The clean run skips it, and the nothing-survived path skips the good one."""
    assert all(spec.skippable for spec in quarantined.specs)


def test_the_quarantine_key_is_a_sibling_that_inherits_the_prefix():
    """The two land next to each other with no configuration."""

    @dataframely_asset(schema=Orders, key_prefix="sales", quarantine=dg.AssetOut())
    def prefixed_quarantine() -> pl.DataFrame:
        return pl.DataFrame()

    assert prefixed_quarantine.keys == {
        dg.AssetKey(["sales", "prefixed_quarantine"]),
        dg.AssetKey(["sales", "prefixed_quarantine_quarantine"]),
    }


def test_an_explicit_key_overrides_the_sibling_completely():
    """The sensitive-data case, at zero cost: a different key, group, owner set and IO manager."""

    @dataframely_asset(
        schema=Orders,
        key_prefix="sales",
        group_name="sales",
        io_manager_key="warehouse",
        quarantine=dg.AssetOut(
            key=dg.AssetKey(["restricted", "orders"]),
            group_name="restricted",
            owners=["team:security"],
            io_manager_key="vault",
        ),
    )
    def routed_quarantine() -> pl.DataFrame:
        return pl.DataFrame()

    key = dg.AssetKey(["restricted", "orders"])
    spec = {s.key: s for s in routed_quarantine.specs}[key]

    assert key in routed_quarantine.keys
    assert spec.group_name == "restricted"
    assert list(spec.owners) == ["team:security"]
    assert (
        routed_quarantine.node_def.output_dict[
            "routed_quarantine_quarantine"
        ].io_manager_key
        == "vault"
    )


def test_the_quarantine_inherits_the_io_manager_key_when_it_names_none():
    """One declaration stores both tables together, and routing the rejected rows elsewhere stays a one-word change."""

    @dataframely_asset(
        schema=Orders, io_manager_key="warehouse", quarantine=dg.AssetOut()
    )
    def shared_storage() -> pl.DataFrame:
        return pl.DataFrame()

    outputs = shared_storage.node_def.output_dict

    assert outputs["shared_storage"].io_manager_key == "warehouse"
    assert outputs["shared_storage_quarantine"].io_manager_key == "warehouse"


def test_the_quarantine_inherits_the_group_when_it_names_none():
    """Dagster refuses `group_name` on the `multi_asset` as soon as an out names one, so the door sets it per out and inherits it here explicitly."""
    assert quarantined.group_names_by_key == {
        dg.AssetKey(["quarantined"]): "sales",
        _QUARANTINE_KEY: "sales",
    }


def test_the_quarantine_inherits_the_partitioning():
    """It lives on the `multi_asset`, so a partitioned asset cannot produce an unpartitioned pile of bad rows."""
    partitions = dg.StaticPartitionsDefinition(["a", "b"])

    @dataframely_asset(
        schema=Orders, partitions_def=partitions, quarantine=dg.AssetOut()
    )
    def partitioned() -> pl.DataFrame:
        return pl.DataFrame()

    assert all(spec.partitions_def == partitions for spec in partitioned.specs)


@pytest.mark.parametrize(
    "setting",
    [
        dg.AssetOut(automation_condition=dg.AutomationCondition.eager()),
        dg.AssetOut(
            freshness_policy=dg.FreshnessPolicy.time_window(
                fail_window=dt.timedelta(hours=24)
            )
        ),
        dg.AssetOut(code_version="v1"),
    ],
    ids=["automation_condition", "freshness_policy", "code_version"],
)
def test_a_setting_that_cannot_differ_between_the_outs_raises_at_definition_time(
    setting: dg.AssetOut,
):
    """`can_subset` is absent, so one step always produces both tables. A condition or a policy that differs between them cannot be true of either, and inheriting silently would discard something the engineer wrote."""
    with pytest.raises(QuarantineSettingError) as raised:

        @dataframely_asset(schema=Orders, quarantine=setting)
        def contested() -> pl.DataFrame:
            return pl.DataFrame()

    assert "One step always produces both tables" in str(raised.value)


def test_the_doors_own_schedule_and_freshness_policy_stay_on_the_good_out():
    """The `AssetOut` cannot contest these, but the door's own values do not reach the quarantine either.

    A freshness policy there would fail forever on a healthy pipeline, because a clean run skips the quarantine by design. A condition there would request a step the good asset's condition already requests, since neither out can execute alone.
    """
    condition = dg.AutomationCondition.eager()
    freshness = dg.FreshnessPolicy.time_window(fail_window=dt.timedelta(hours=24))

    @dataframely_asset(
        schema=Orders,
        automation_condition=condition,
        freshness_policy=freshness,
        quarantine=dg.AssetOut(),
    )
    def scheduled() -> pl.DataFrame:
        return pl.DataFrame()

    specs = {spec.key: spec for spec in scheduled.specs}
    good = specs[dg.AssetKey(["scheduled"])]
    bad = specs[dg.AssetKey(["scheduled_quarantine"])]

    assert good.automation_condition == condition
    assert good.freshness_policy == freshness
    assert bad.automation_condition is None
    assert bad.freshness_policy is None


def test_the_quarantine_setting_error_names_the_setting_and_the_door():
    with pytest.raises(QuarantineSettingError) as raised:

        @dataframely_asset(schema=Orders, quarantine=dg.AssetOut(code_version="v1"))
        def contested() -> pl.DataFrame:
            return pl.DataFrame()

    assert "code_version" in str(raised.value)
    assert "dataframely_asset" in str(raised.value)


def _quarantine_columns() -> dict[str, dg.TableColumn]:
    return _columns_of(quarantined, _QUARANTINE_KEY)


def test_the_quarantine_mirrors_the_schemas_columns_keeping_dtype_and_prose():
    columns = _quarantine_columns()
    amount = columns["amount"]

    assert list(columns)[: len(Orders.columns())] == list(Orders.columns())
    assert amount.type == "Decimal(precision=10, scale=2)"
    assert amount.description == "Line total in account currency."
    assert amount.tags == {"owner": "finance", "pii": "False"}


def test_the_quarantine_mirror_carries_no_constraint():
    """These rows are here precisely because they violate them. The primary key above all: the rejected rows are exactly where a duplicate key ends up."""
    assert all(
        column.constraints == dg.TableColumnConstraints()
        for column in _quarantine_columns().values()
    )
    assert _catalog(quarantined, _QUARANTINE_KEY).constraints == dg.TableConstraints(
        other=[]
    )


def test_the_quarantine_declares_one_string_outcome_column_per_rule():
    """Named byte-identically to the asset checks, so an engineer carries the string across by eye."""
    columns = _quarantine_columns()
    outcomes = {name: columns[name] for name in columns if name.startswith("dy_rule__")}
    expected = {f"dy_rule__{rule.replace('|', '__')}" for rule in _RULES}

    assert set(outcomes) == expected
    assert expected <= set(_specs_by_name(quarantined))
    assert all(column.type == "String" for column in outcomes.values())


def test_each_outcome_column_is_described_as_the_outcome_of_its_rule():
    assert _quarantine_columns()["dy_rule__amount__min"].description == (
        "Outcome of rule 'amount|min': 'valid' / 'invalid' / 'unknown'."
    )


def test_the_quarantine_carries_the_schema_carrier_too():
    """The carrier is a dtype lookup by name, not a claim that these rows conform. A quarantine frame carries every column the schema declares at the dtype it declares, so an IO manager that needs the schema to read a file back reads this table exactly as it reads the good one."""
    assert (
        quarantined.metadata_by_key[_QUARANTINE_KEY][_SCHEMA_CARRIER_KEY].instance
        is Orders
    )


def test_no_quarantine_means_one_out():
    assert orders.keys == {dg.AssetKey(["orders"])}


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
    "outs",  # door-owned: the door builds both outs from the schema
    "check_specs",  # door-owned: derived from the schema, never contested
    "specs",  # door-owned: the alternative spelling of `outs`
    "can_subset",  # deliberately absent (#4): a subset executes but saves nothing
    "internal_asset_deps",  # nothing to wire: neither out feeds the other
    # Refused by `multi_asset` outright when any out names one, so it lands on the
    # outs, which is what leaves the quarantine free to claim a group of its own.
    "group_name",
}
_DOOR_OWNED = {
    "schema",
    "key_prefix",
    "quarantine",
    "check_granularity",
    "multi_column_rules",
    "max_failure_samples",
    "statistics",
    "row_sample",
    "temp_dir",
}

# The door-owned parameters with no `@dg.asset` counterpart at all. `key_prefix` is not
# one of them: it is `dg.asset` vocabulary that the door happens to own the meaning of.
_NO_DG_ASSET_COUNTERPART = {
    "schema",
    "quarantine",
    "check_granularity",
    "multi_column_rules",
    "max_failure_samples",
    "statistics",
    "row_sample",
    "temp_dir",
}

# Not forwarded to `multi_asset`, which has no per-out vocabulary. These land on the good `dg.AssetOut` instead. Seven of them are absent from `multi_asset`'s signature entirely; `group_name` is the exception, and is here because Dagster refuses it on the `multi_asset` as soon as an out names one.
_ASSET_LEVEL = {
    "io_manager_key",
    "group_name",
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
    "output_required",  # door-owned: the shape check and abort paths must be able to skip
    "dagster_type",  # ruled out (#3): runs before the IO manager, no severity dial
    "is_virtual",  # a virtual asset has no compute, so there is no transform
    "io_manager_def",  # not settable per out; the forwarded `resource_defs` covers it
}


def test_the_door_speaks_dg_assets_vocabulary():
    """The interface is designed for one table, so anything `@dg.asset` can say about an asset should be sayable here under the same name.

    Asserted in both directions, like the `multi_asset` pin: nothing the door offers has vanished from `dg.asset`, and nothing `dg.asset` gains is silently missing here.
    """
    door = (
        set(inspect.signature(dataframely_asset).parameters) - _NO_DG_ASSET_COUNTERPART
    )
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
