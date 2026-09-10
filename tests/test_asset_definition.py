"""Definition-time behaviour of `@dy_asset`, asserted without running anything.

Every test reads the `AssetsDefinition` the decorator returns. It carries everything a user sees before the first run: the keys, the check specs, and the definition metadata that fills the Columns tab.
"""

import datetime as dt
import inspect
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl
import pytest
from dagster._config.field_utils import Shape

from dagster_dataframely import dy_asset
from dagster_dataframely.errors import (
    CheckNameCollisionError,
    CollectionNotSupportedError,
    DagsterDataframelyError,
    InvalidSettingError,
    ReservedColumnError,
)
from tests.scenario import Orders

_COLUMN_SCHEMA_KEY = "dagster/column_schema"

# Every rule `Orders` declares, in the order Dataframely reports them. Spelled out rather than derived, so a rule that silently disappears fails here.
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


@dy_asset(Orders, group_name="sales")
def orders() -> pl.DataFrame:
    """The decorated function's own docstring, which `Orders`'s outranks."""
    return pl.DataFrame()


def _specs_by_name(asset: dg.AssetsDefinition) -> dict[str, dg.AssetCheckSpec]:
    return {spec.name: spec for spec in asset.check_specs}


def test_the_decorator_produces_one_asset():
    assert orders.keys == {dg.AssetKey(["orders"])}
    assert orders.group_names_by_key == {dg.AssetKey(["orders"]): "sales"}


def test_the_output_is_not_required():
    """The column-schema check and both failures that write nothing end the step without yielding the output."""
    (spec,) = orders.specs
    assert spec.skippable


def test_a_key_prefix_carries_the_checks_with_it():
    """The key is built once and handed to both the asset and its check specs, so the checks cannot lag it."""

    @dy_asset(Orders, key_prefix="sales")
    def prefixed() -> pl.DataFrame:
        return pl.DataFrame()

    key = dg.AssetKey(["sales", "prefixed"])

    assert prefixed.keys == {key}
    assert {spec.asset_key for spec in prefixed.check_specs} == {key}


def test_a_sequence_key_prefix_nests():
    @dy_asset(Orders, key_prefix=["warehouse", "sales"])
    def nested() -> pl.DataFrame:
        return pl.DataFrame()

    assert nested.keys == {dg.AssetKey(["warehouse", "sales", "nested"])}


def test_name_overrides_the_function_name():
    """A private-looking function can back a public asset key."""

    @dy_asset(Orders, name="orders", key_prefix="sales")
    def _orders_impl() -> pl.DataFrame:
        return pl.DataFrame()

    assert _orders_impl.keys == {dg.AssetKey(["sales", "orders"])}


def test_upstream_dependencies_bind_as_ordinary_parameters():
    """`functools.wraps` carries the signature through the wrapper."""

    @dy_asset(Orders)
    def downstream(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return raw_orders

    assert downstream.keys_by_input_name == {"raw_orders": dg.AssetKey(["raw_orders"])}


@dg.success_hook
def _notify(context: dg.HookContext) -> None:
    pass


class _Warehouse(dg.ConfigurableResource[None]):
    dsn: str


def test_every_forwarded_dg_asset_parameter_reaches_the_definition():
    """Twelve at once, so a parameter that forwards only in isolation still fails here. `name` and `pool` have their own tests: `name` changes the asset key instead of landing unchanged, and `pool` cannot share an op with `backfill_policy`."""
    partitions = dg.StaticPartitionsDefinition(["a", "b"])
    retry = dg.RetryPolicy(max_retries=2)
    backfill = dg.BackfillPolicy.single_run()

    @dy_asset(
        Orders,
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

    @dy_asset(Orders, pool="limited")
    def pooled() -> pl.DataFrame:
        return pl.DataFrame()

    assert pooled.op.pool == "limited"


def test_the_asset_shaping_parameters_reach_the_definition():
    """The eight that describe the table, not the op. Same names as `@dg.asset`, forwarded unchanged."""
    condition = dg.AutomationCondition.eager()
    freshness = dg.FreshnessPolicy.time_window(fail_window=dt.timedelta(hours=24))

    @dy_asset(
        Orders,
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

    assert routed.node_def.output_dict["result"].io_manager_key == "warehouse"
    assert spec.group_name == "sales"
    assert routed.metadata_by_key[key]["sla_hours"] == 4
    assert spec.tags["layer"] == "silver"
    assert list(spec.owners) == ["team:data"]
    assert spec.kinds == {"parquet"}
    assert spec.automation_condition == condition
    assert spec.freshness_policy == freshness


def test_user_metadata_cannot_displace_the_packages_own():
    """The decorator exists to fill the Columns tab, so a colliding user key loses."""

    @dy_asset(
        Orders,
        metadata={_COLUMN_SCHEMA_KEY: "mine", "own": "kept"},
    )
    def collides() -> pl.DataFrame:
        return pl.DataFrame()

    definition_metadata = collides.metadata_by_key[dg.AssetKey(["collides"])]

    assert definition_metadata["own"] == "kept"
    assert isinstance(definition_metadata[_COLUMN_SCHEMA_KEY], dg.TableSchema)


# --- the description ---
class _Documented(dy.Schema):
    """Postal codes, one row per code."""

    code = dy.String(primary_key=True)


class _Undocumented(dy.Schema):
    code = dy.String(primary_key=True)


class _Blank(dy.Schema):
    """ """

    code = dy.String(primary_key=True)


def test_the_schema_docstring_fills_a_description_the_decorator_was_not_given():
    @dy_asset(_Documented)
    def postal_codes() -> pl.DataFrame:
        """The decorated function's own docstring, which the schema outranks."""
        return pl.DataFrame()

    (spec,) = postal_codes.specs
    assert spec.description == "Postal codes, one row per code."


def test_an_explicit_description_outranks_the_schema_docstring():
    @dy_asset(_Documented, description="Said at the call site.")
    def explicit() -> pl.DataFrame:
        """The decorated function's own docstring."""
        return pl.DataFrame()

    (spec,) = explicit.specs
    assert spec.description == "Said at the call site."


def test_a_schema_without_a_docstring_leaves_dagsters_own_fallback_standing():
    """The last fallback is Dagster's, not the package's. With nothing to fill the gap, the decorated function's docstring lands as it always did."""

    @dy_asset(_Undocumented)
    def undocumented() -> pl.DataFrame:
        """The decorated function's own docstring."""
        return pl.DataFrame()

    (spec,) = undocumented.specs
    assert spec.description == "The decorated function's own docstring."


def test_the_base_schemas_docstring_never_reaches_an_asset():
    """`inspect.getdoc` walks the MRO, so reading the docstring through it would describe every undocumented schema as a base class."""

    assert dy.Schema.__doc__ is not None

    @dy_asset(_Undocumented, name="inherits_nothing")
    def inherits_nothing() -> pl.DataFrame:
        return pl.DataFrame()

    (spec,) = inherits_nothing.specs
    assert spec.description is None


def test_an_empty_description_counts_as_absent_too():
    """Both sources share one emptiness rule. Neither can say "no description at all": Dagster's fallback takes over the moment the package has nothing."""

    @dy_asset(_Documented, description="")
    def empty() -> pl.DataFrame:
        """The decorated function's own docstring."""
        return pl.DataFrame()

    (spec,) = empty.specs
    assert spec.description == "Postal codes, one row per code."


def test_a_whitespace_only_schema_docstring_counts_as_absent():
    @dy_asset(_Blank)
    def blank() -> pl.DataFrame:
        """The decorated function's own docstring."""
        return pl.DataFrame()

    (spec,) = blank.specs
    assert spec.description == "The decorated function's own docstring."


def test_a_multi_line_schema_docstring_arrives_dedented():
    """Raw `__doc__` keeps its source indentation, which the catalog renders as a code block."""

    @dy_asset(Orders)
    def dedented() -> pl.DataFrame:
        return pl.DataFrame()

    (spec,) = dedented.specs
    assert spec.description is not None
    assert spec.description.startswith("Customer orders, one row per order line.\n\n")
    assert not any(line.startswith(" ") for line in spec.description.splitlines()), (
        spec.description
    )
    assert spec.description == spec.description.strip()


# --- checks ---
def test_there_is_one_check_per_rule_plus_the_gate():
    """Specs come off the schema, so a clean run still reports on every rule.

    This also covers "no rule value appears in any check name": a check name is the rule name rewritten and nothing else, so tightening `min` leaves the check in place rather than orphaning its history.
    """
    expected = {"dy_schema__columns"} | {
        f"dy_rule__{rule.replace('|', '__')}" for rule in _RULES
    }

    assert set(_specs_by_name(orders)) == expected


def test_only_the_column_schema_check_is_blocking():
    specs = _specs_by_name(orders)

    assert specs["dy_schema__columns"].blocking
    assert not any(
        spec.blocking for name, spec in specs.items() if name != "dy_schema__columns"
    )


def test_a_rules_docstring_becomes_its_check_description():
    specs = _specs_by_name(orders)

    assert (
        specs["dy_rule__paid_orders_have_amount"].description
        == "Paid orders must carry a positive amount."
    )


def test_a_check_without_a_docstring_describes_the_constraint_itself():
    """The middle fallback. A check list reads flat, so the description names the column: a bare `>= 0.00` says nothing about which column it bounds."""
    specs = _specs_by_name(orders)

    assert specs["dy_rule__amount__min"].description == "amount >= 0.00"
    assert specs["dy_rule__email__max_length"].description == (
        "email length <= 254 bytes"
    )
    assert specs["dy_rule__order_id__nullability"].description == "order_id not null"


def test_the_primary_key_check_describes_the_whole_key():
    """The primary key is one rule over a struct of every key column, so the description names the whole key, not one column."""
    assert _specs_by_name(orders)["dy_rule__primary_key"].description == (
        "PK: order_id, line_no"
    )


def test_a_rule_with_neither_a_docstring_nor_a_constraint_falls_back_to_its_name():
    """A `@dy.rule()` body is an arbitrary expression, so nothing structured is left to render but the name."""
    assert _specs_by_name(orders)["dy_rule__line_numbers_are_dense"].description == (
        "line_numbers_are_dense"
    )


def test_no_check_description_is_blank():
    """Every fallback step is reachable and the last one always holds."""
    assert all(spec.description for spec in orders.check_specs)


def test_the_column_schema_check_names_the_schema():
    assert _specs_by_name(orders)["dy_schema__columns"].description == (
        "Columns and dtypes match Orders."
    )


# --- check granularity ---
_RULE_BEARING_COLUMNS = {rule.split("|")[0] for rule in _RULES if "|" in rule}

# The rules no single column owns: both `@dy.rule()` bodies and the composite key.
_MULTI_COLUMN_RULES = [rule for rule in _RULES if "|" not in rule]


@dy_asset(Orders, name="by_column", check_granularity="column")
def by_column() -> pl.DataFrame:
    return pl.DataFrame()


@dy_asset(Orders, name="by_schema", check_granularity="schema")
def by_schema() -> pl.DataFrame:
    return pl.DataFrame()


def test_column_granularity_collapses_to_one_check_per_rule_bearing_column():
    """A 40-column schema contributes around 120 checks at `rule` granularity. Nobody reads that list."""
    assert set(_specs_by_name(by_column)) == {
        "dy_schema__columns",
        "dy_schema__rules",
    } | {f"dy_col__{column}" for column in _RULE_BEARING_COLUMNS}


def test_a_column_carrying_no_rule_carries_no_check():
    """`fulfilled_in` and `payload` are nullable with no constraints, so a check for either would report on nothing."""
    assert not {"dy_col__fulfilled_in", "dy_col__payload"} & set(
        _specs_by_name(by_column)
    )


def test_a_ten_field_struct_is_ten_checks_by_rule_and_one_by_column():
    """Dataframely emits one `inner_<field>_nullability` rule per struct field, so a wide struct makes the check list unreadable fastest."""

    class Addresses(dy.Schema):
        address_id = dy.String(primary_key=True)
        address = dy.Struct(
            {f"field_{n}": dy.String(nullable=False) for n in range(10)}, nullable=True
        )

    @dy_asset(Addresses, name="by_rule")
    def by_rule() -> pl.DataFrame:
        return pl.DataFrame()

    @dy_asset(Addresses, name="collapsed", check_granularity="column")
    def collapsed() -> pl.DataFrame:
        return pl.DataFrame()

    struct_checks = [
        name
        for name in _specs_by_name(by_rule)
        if name.startswith("dy_rule__address__")
    ]

    assert len(struct_checks) == 10
    assert set(_specs_by_name(collapsed)) == {
        "dy_schema__columns",
        "dy_schema__rules",
        "dy_col__address_id",
        "dy_col__address",
    }


def test_multi_column_rules_collapse_into_the_schema_check_by_default():
    """They belong to no column, so at column granularity they have no rule set of their own to land in."""
    specs = _specs_by_name(by_column)

    assert "dy_schema__rules" in specs
    assert not {f"dy_rule__{rule}" for rule in _MULTI_COLUMN_RULES} & set(specs)


def test_per_rule_gives_each_multi_column_rule_a_check_of_its_own():
    """The setting serves a schema whose cross-column rules deserve their own history."""

    @dy_asset(
        Orders,
        name="per_rule",
        check_granularity="column",
        multi_column_rules="per_rule",
    )
    def per_rule() -> pl.DataFrame:
        return pl.DataFrame()

    specs = _specs_by_name(per_rule)

    assert {f"dy_rule__{rule}" for rule in _MULTI_COLUMN_RULES} <= set(specs)
    assert "dy_schema__rules" not in specs


def test_the_multi_column_rule_set_cannot_be_collided_with_by_a_user_column():
    """The name is `dy_schema__rules` rather than `dy_col__schema` because somebody has a column named `schema`."""

    class Tables(dy.Schema):
        table_id = dy.String(primary_key=True)
        schema = dy.String(nullable=False)

    @dy_asset(Tables, name="tables", check_granularity="column")
    def tables() -> pl.DataFrame:
        return pl.DataFrame()

    specs = _specs_by_name(tables)

    assert "dy_col__schema" in specs
    assert "dy_schema__rules" in specs
    assert specs["dy_col__schema"] != specs["dy_schema__rules"]


def test_schema_granularity_leaves_one_rules_check_beside_the_gate():
    assert set(_specs_by_name(by_schema)) == {"dy_schema__columns", "dy_schema__rules"}


@pytest.mark.parametrize("granularity", ["rule", "column", "schema"])
def test_a_schema_with_no_rules_gets_the_column_schema_check_and_nothing_else(
    granularity: Any,
):
    """The column-schema check still holds. A rules check with no rules in it would pass forever and say nothing, at any granularity."""

    class Blob(dy.Schema):
        payload = dy.String(nullable=True)

    @dy_asset(Blob, name=f"blob_{granularity}", check_granularity=granularity)
    def blob() -> pl.DataFrame:
        return pl.DataFrame()

    assert set(_specs_by_name(blob)) == {"dy_schema__columns"}


@pytest.mark.parametrize("asset", [orders, by_column, by_schema])
def test_the_column_schema_check_is_present_and_blocking_at_every_granularity(
    asset: dg.AssetsDefinition,
):
    """The column-schema check is not a rule, so it never joins a rule set. A frame whose columns do not match must stop the run whatever the check list looks like."""
    specs = _specs_by_name(asset)

    assert specs["dy_schema__columns"].blocking
    assert not any(
        spec.blocking for name, spec in specs.items() if name != "dy_schema__columns"
    )


def test_a_collapsed_check_names_the_rules_it_reports_for():
    """The collapsed detail goes into the description: the check name says `email`, and the constraints it stands for say the rest."""
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


def test_a_granularity_outside_the_vocabulary_raises_at_definition_time():
    """Definition time, so a misconfiguration never reaches a run.

    The value arrives through an untyped name because the literal is already a static error. The runtime guard serves a user without a type checker.
    """
    wrong: Any = "per_column"

    with pytest.raises(InvalidSettingError) as raised:

        @dy_asset(Orders, name="misconfigured", check_granularity=wrong)
        def _misconfigured() -> pl.DataFrame:
            return pl.DataFrame()

    assert "check_granularity" in str(raised.value)


@pytest.mark.parametrize("quarantine", [False, True], ids=["plain", "quarantined"])
def test_a_malformed_quarantine_dir_raises_at_definition_time(
    monkeypatch: pytest.MonkeyPatch, quarantine: bool
):
    """`DAGSTER_DATAFRAMELY_QUARANTINE_DIR=${SCRATCH}` in a deployment whose `SCRATCH` never got set arrives empty.

    The decorator drops the value it resolves, because the directory a call writes under is read where the rows are written (#115). It resolves anyway for this refusal, so a variable written wrong is reported where it was written. Both declarations, because a malformed variable is malformed whether or not the asset declares a quarantine.
    """
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_QUARANTINE_DIR", "   ")

    with pytest.raises(InvalidSettingError) as raised:

        @dy_asset(Orders, name="misconfigured", quarantine=quarantine)
        def _misconfigured() -> pl.DataFrame:
            return pl.DataFrame()

    assert "quarantine_dir" in str(raised.value)


def test_the_environment_variable_sets_the_house_granularity(
    monkeypatch: pytest.MonkeyPatch,
):
    """One export in a code location's environment sets the granularity for every asset in it."""
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_CHECK_GRANULARITY", "schema")

    @dy_asset(Orders, name="house_style")
    def house_style() -> pl.DataFrame:
        return pl.DataFrame()

    assert set(_specs_by_name(house_style)) == {
        "dy_schema__columns",
        "dy_schema__rules",
    }


# --- definition metadata ---
def _catalog(asset: dg.AssetsDefinition, key: dg.AssetKey) -> dg.TableSchema:
    """Read the Columns tab a data consumer opens."""
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
    """Dataframely stores `Column.metadata` and never reads it, so column tags are the only place it reaches. Values are stringified because Dagster's tags are `Mapping[str, str]` and it rejects anything else at definition time."""
    assert _columns()["amount"].tags == {"owner": "finance", "pii": "False"}


def test_a_column_without_metadata_carries_no_tags():
    assert not _columns()["quantity"].tags


def test_a_nullable_column_says_so():
    assert _columns()["note"].constraints.nullable


def test_every_schema_column_reaches_the_catalog_in_order():
    assert list(_columns()) == list(Orders.columns())


def test_a_unique_column_says_so():
    """`tracking_id` declares `unique=True`. Dataframely enforces that with its own rule, so it has its own check, and the catalog has to agree with the check."""
    assert _columns()["tracking_id"].constraints.unique
    assert "dy_rule__tracking_id__unique" in _specs_by_name(orders)


def test_a_primary_key_column_never_claims_to_be_unique():
    """Dataframely keeps the two flags independent: a key member gets a composite `as_struct(...).is_unique()` rule and `column.unique` stays `False`. Deriving `unique` from `primary_key` would assert a per-column uniqueness that nothing enforces."""
    assert not _columns()["order_id"].constraints.unique
    assert not _columns()["line_no"].constraints.unique


# --- column constraints ---
class Measurements(dy.Schema):
    """The constraint kinds `Orders` has no natural column for.

    Local to this file rather than the shared scenario: none of them changes what a runtime or IO-manager test sees, and each covers one arm of the constraint renderer.
    """

    reading_at = dy.Datetime(resolution="1h")
    grade = dy.Int32(is_in=[1, 2, 3])
    depth = dy.Int32(min=0, max=100)
    ratio = dy.Float64(min_exclusive=0.0, max_exclusive=1.0)
    corners = dy.Array(dy.Int32(min=0), 2)
    label = dy.String(
        min_length=2,
        # Two lambdas, so Dataframely disambiguates them with a counter rather than a name.
        check=[lambda expr: expr != "", lambda expr: expr == expr.str.strip_chars()],
    )


@dy_asset(Measurements)
def measurements() -> pl.DataFrame:
    return pl.DataFrame()


def _measured() -> dict[str, dg.TableColumn]:
    return _columns_of(measurements, dg.AssetKey(["measurements"]))


def test_a_bound_reads_as_an_operator_rather_than_as_a_check_name():
    """A consumer should see what the bound is, not only that one exists."""
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


def test_the_remaining_constraint_kinds_render_their_value():
    assert _columns()["order_id"].constraints.other == [r"matches ^ORD-\d+$"]
    assert _measured()["reading_at"].constraints.other == ["aligned to 1h"]
    assert _measured()["grade"].constraints.other == ["in (1, 2, 3)"]


def test_a_named_check_renders_its_key_and_an_anonymous_one_says_it_is_one():
    """An unnamed lambda leaves only `custom check` to render, and Dataframely's counter suffix is not a name anybody wrote. That is the nudge to name a check."""
    assert "lowercase" in _columns()["email"].constraints.other
    assert _columns()["note"].constraints.other == ["custom check"]
    assert _measured()["label"].constraints.other[:2] == [
        "custom check",
        "custom check",
    ]


def test_a_nested_columns_rules_render_as_constraints_on_its_elements():
    """A `List` or an `Array` runs its inner column's rules over the elements, so the renderer recurses into it. Falling back to `inner_min` would break a constraint list meant to read as operators."""
    assert _measured()["corners"].constraints.other == [
        "elements not null",
        "elements >= 0",
    ]


def test_a_constraint_dagster_models_first_class_is_not_repeated():
    """`nullable` and `unique` have their own fields on `TableColumnConstraints`, so a constraint saying the same thing would double every column's constraint list."""
    assert _columns()["quantity"].constraints.other == [">= 1"]
    assert _columns()["tracking_id"].constraints.other == []


def test_a_constraint_left_at_its_dataframely_default_renders_nothing():
    """`allow_inf` and `allow_nan` default to `False`, so every float column carries an `inf` and a `nan` rule nobody asked for. Setting either flag `True` removes its rule rather than changing it, so the constraint could only ever state the default. It says nothing instead. The checks still exist and still report."""
    assert _measured()["ratio"].constraints.other == ["> 0.0", "< 1.0"]
    assert {"dy_rule__ratio__inf", "dy_rule__ratio__nan"} <= set(
        _specs_by_name(measurements)
    )


def test_sibling_rules_render_in_one_voice_on_the_constraint_surface():
    """`paid_orders_have_amount` carries a docstring and `line_numbers_are_dense` does not. A docstring on the Columns tab would put two sibling rules in different registers for a reason invisible from the UI. So the name is the constant here, and the docstring reaches only the check description."""
    assert _catalog(orders, dg.AssetKey(["orders"])).constraints.other == [
        "paid_orders_have_amount",
        "line_numbers_are_dense",
        "PK: order_id, line_no",
    ]


def test_the_primary_key_is_stated_once_at_table_level():
    """Dataframely models the key as one rule over a struct of every key column. Stating it once distinguishes a composite key from two independent single-column keys."""
    table_schema = _catalog(orders, dg.AssetKey(["orders"]))

    assert "PK: order_id, line_no" in table_schema.constraints.other
    assert not any(
        "PK" in constraint
        for column in table_schema.columns
        for constraint in column.constraints.other
    )


class Ledger(dy.Schema):
    """A key member that also declares `unique=True`, which `Orders` has nowhere to put.

    Dataframely keeps the two flags independent, so `entry_id` carries both rules: the composite `primary_key` over the pair, and its own `entry_id|unique` over itself.
    """

    entry_id = dy.String(primary_key=True, unique=True)
    posted_at = dy.Datetime(primary_key=True)


@dy_asset(Ledger)
def ledger() -> pl.DataFrame:
    return pl.DataFrame()


def test_a_key_member_reads_not_null_and_claims_uniqueness_only_where_it_declared_it():
    """Dataframely forbids a nullable key column, so `not null` is free. `unique` is not: on a composite key only the tuple is unique, and `{"a": ["x", "x"], "b": [1, 2]}` passes."""
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


# --- the quarantine ---
def test_a_quarantine_adds_nothing_to_the_graph():
    """A quarantine is evidence of a run, not a `dg.AssetOut`. The rows go to the asset's own IO manager under a suffixed key, so nothing in the definition changes and nothing new appears in the lineage (ADR-0004, ADR-0006).

    `build_quarantine_spec` gives a quarantine a node, and the user declares it.
    """

    @dy_asset(Orders, quarantine=True)
    def kept() -> pl.DataFrame:
        return pl.DataFrame()

    @dy_asset(Orders, quarantine=False)
    def refused() -> pl.DataFrame:
        return pl.DataFrame()

    assert kept.keys == {dg.AssetKey(["kept"])}
    assert len(kept.node_def.output_dict) == len(refused.node_def.output_dict)
    assert {spec.name for spec in kept.check_specs} == {
        spec.name for spec in refused.check_specs
    }


def test_a_quarantine_needs_no_resource_declared():
    """The manager is borrowed off the step, never read off `context.resources`. Dagster validates a declared resource at bind time, so every direct invocation would have to supply a manager it never uses."""

    @dy_asset(Orders, quarantine=True)
    def kept() -> pl.DataFrame:
        return pl.DataFrame()

    assert kept.op.required_resource_keys == frozenset()


# --- definition-time errors ---
def test_a_user_column_in_the_reserved_namespace_raises():
    class Reserved(dy.Schema):
        dy_flag = dy.Bool()
        order_id = dy.String()

    with pytest.raises(ReservedColumnError) as raised:

        @dy_asset(Reserved)
        def reserved() -> pl.DataFrame:
            return pl.DataFrame()

    assert "Column 'dy_flag' of Reserved uses" in str(raised.value)
    assert "Rename it." in str(raised.value)
    assert "order_id" not in str(raised.value)


def test_the_reserved_column_error_reads_as_plural_for_several_columns():
    """The message is all a user sees of this error, so it agrees in number."""

    class Reserved(dy.Schema):
        dy_flag = dy.Bool()
        dy_rule__amount__min = dy.Bool()

    with pytest.raises(ReservedColumnError) as raised:

        @dy_asset(Reserved)
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

        @dy_asset(Colliding)
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
        dy_asset(OrderBook)  # pyrefly: ignore[bad-argument-type]

    assert "OrderBook" in str(raised.value)


def test_a_non_schema_argument_is_left_to_fail_however_it_fails():
    """The Collection guard exists because `dy.Collection` is the plausible wrong reach. Generalising it into a type check on `schema=` was rejected."""
    with pytest.raises(Exception) as raised:  # noqa: PT011 - breadth is the point

        @dy_asset(42)  # pyrefly: ignore[bad-argument-type]
        def nonsense() -> pl.DataFrame:
            return pl.DataFrame()

    assert not isinstance(raised.value, DagsterDataframelyError)


# --- the underlying op ---
def _shipments(prefix: str, *, quarantine: bool = False) -> dg.AssetsDefinition:
    """Build the asset that used to collide with itself under a second prefix (#70)."""

    @dy_asset(Orders, key_prefix=prefix, name="shipments", quarantine=quarantine)
    def shipments() -> pl.DataFrame:
        return pl.DataFrame()

    return shipments


def test_the_op_takes_its_name_from_the_key_exactly_as_dg_asset_takes_its_own():
    """An op name has to be unique across a code location, and the asset name alone is not. Only the prefix distinguishes two assets that share a name.

    Asserted against a live `@dg.asset` rather than a spelled-out string, so the parity holds through an upstream change to how the identifier is built.
    """

    @dy_asset(Orders, key_prefix=["warehouse", "sales"], name="shipments")
    def attached() -> pl.DataFrame:
        return pl.DataFrame()

    @dg.asset(key_prefix=["warehouse", "sales"], name="shipments")
    def plain() -> None: ...

    assert attached.op.name == plain.op.name


@pytest.mark.parametrize("quarantine", [False, True], ids=["bare", "quarantined"])
def test_two_assets_sharing_a_name_under_different_prefixes_coexist(quarantine: bool):
    """Dagster tolerates a repeated op name only where the two definitions compare equal. Two of these never do, because every check output name embeds its own asset key. The collision was always fatal, not sometimes."""
    definitions = dg.Definitions(
        assets=[
            _shipments("alpha", quarantine=quarantine),
            _shipments("beta", quarantine=quarantine),
        ]
    )

    job = definitions.resolve_implicit_global_asset_job_def()

    assert {"alpha__shipments", "beta__shipments"} <= {
        node.name for node in job.graph.nodes
    }


def test_a_downstream_asset_binds_to_the_node_that_owns_its_key():
    """The collision's second face, and the expensive one. In a graph of any size it shows up later than the name clash, as a dependency wired to the wrong node.

    `dependency_structure` is undocumented but not private, and the only place the resolved edge reads as a node name. `graph.dependencies` holds the same edge wrapped in a `BlockingAssetChecksDependencyDefinition`, which the column-schema check puts there and which adds nothing here.
    """

    @dg.asset(ins={"upstream": dg.AssetIn(key=dg.AssetKey(["beta", "shipments"]))})
    def consumer(upstream: pl.DataFrame) -> None: ...

    definitions = dg.Definitions(
        assets=[_shipments("alpha"), _shipments("beta"), consumer]
    )

    job = definitions.resolve_implicit_global_asset_job_def()
    upstream_outputs = (
        job.graph.dependency_structure.input_to_upstream_outputs_for_node("consumer")
    )

    assert {
        handle.node_name for handles in upstream_outputs.values() for handle in handles
    } == {"beta__shipments"}


# --- the decorator's own contract with dagster ---
# The decorator-owned parameters with no `@dg.asset` counterpart. `key_prefix` is not one of them: it is `dg.asset` vocabulary whose meaning the decorator owns.
_NO_DG_ASSET_COUNTERPART = {
    "schema",
    "quarantine",
    "check_granularity",
    "multi_column_rules",
    "max_failure_samples",
    "statistics",
    "row_sample",
}

# Parameters `dg.asset` has that the decorator leaves out on purpose, each with its reason.
_NOT_ON_THE_DECORATOR = {
    "check_specs",  # decorator-owned: derived from the schema, never contested
    "key",  # decorator-owned: `key_prefix` plus `name` already say it, once
    "output_required",  # decorator-owned: the column-schema check and the failures that write nothing must be able to skip
    "dagster_type",  # ruled out (#3): runs before the IO manager, no severity dial
    "is_virtual",  # a virtual asset has no compute, so there is nothing to decorate
    "io_manager_def",  # the forwarded `resource_defs` covers it, keyed rather than positional
}


def test_the_decorator_speaks_dg_assets_vocabulary():
    """`dg.asset` is both the mechanism and the vocabulary, so anything it can say about an asset is sayable here under the same name.

    Asserted in both directions (#15): nothing the decorator offers has vanished from `dg.asset`, and nothing `dg.asset` gains is missing here without a line above saying why.
    """
    decorator = set(inspect.signature(dy_asset).parameters) - _NO_DG_ASSET_COUNTERPART
    upstream = set(inspect.signature(dg.asset).parameters) - {"compute_fn", "kwargs"}

    assert decorator <= upstream, f"no longer on dg.asset: {decorator - upstream}"
    assert upstream - decorator == _NOT_ON_THE_DECORATOR, (
        f"new on dg.asset, unconsidered here: {upstream - decorator - _NOT_ON_THE_DECORATOR}"
    )


def test_the_schema_is_the_one_positional_parameter_and_is_required():
    """The schema is the reason the decorator exists, so it reads as `@dy_asset(Orders, ...)` rather than as one keyword among thirty. No default can make it optional."""
    parameters = inspect.signature(dy_asset).parameters
    positional = [
        name
        for name, parameter in parameters.items()
        if parameter.kind is inspect.Parameter.POSITIONAL_ONLY
    ]

    assert positional == ["schema"]
    assert parameters["schema"].default is inspect.Parameter.empty
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for name, parameter in parameters.items()
        if name != "schema"
    )


def test_the_surfaces_the_package_owns_are_not_parameters():
    """Statically unpassable, so no runtime guard is needed."""
    parameters = set(inspect.signature(dy_asset).parameters)

    assert parameters.isdisjoint({"check_specs", "key", "output_required"})
