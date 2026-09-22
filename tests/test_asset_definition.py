"""Definition-time behaviour of `dd.asset`, read from the `AssetsDefinition` it returns without running anything."""

import datetime as dt
import inspect
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl
import pytest
from dagster._config.field_utils import Shape

import dagster_dataframely as dd
from dagster_dataframely.errors import (
    CollectionNotSupportedError,
    DagsterDataframelyError,
    InvalidColumnNameError,
    InvalidSettingError,
    ReservedColumnError,
)
from tests.scenario import Orders

_COLUMN_SCHEMA_KEY = "dagster/column_schema"

# Every rule of `Orders` in Dataframely's order, listed rather than derived so a rule that disappears fails a test.
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


@dd.asset(Orders, group_name="sales")
def orders() -> pl.DataFrame:
    """The decorated function's own docstring."""
    return pl.DataFrame()


def _specs_by_name(asset: dg.AssetsDefinition) -> dict[str, dg.AssetCheckSpec]:
    return {spec.name: spec for spec in asset.check_specs}


def test_the_decorator_produces_one_asset():
    assert orders.keys == {dg.AssetKey(["orders"])}
    assert orders.group_names_by_key == {dg.AssetKey(["orders"]): "sales"}


def test_the_output_is_not_required():
    (spec,) = orders.specs
    assert spec.skippable


def test_a_key_prefix_applies_to_the_asset_and_its_checks():
    @dd.asset(Orders, key_prefix="sales")
    def prefixed() -> pl.DataFrame:
        return pl.DataFrame()

    key = dg.AssetKey(["sales", "prefixed"])

    assert prefixed.keys == {key}
    assert {spec.asset_key for spec in prefixed.check_specs} == {key}


def test_a_sequence_key_prefix_nests():
    @dd.asset(Orders, key_prefix=["warehouse", "sales"])
    def nested() -> pl.DataFrame:
        return pl.DataFrame()

    assert nested.keys == {dg.AssetKey(["warehouse", "sales", "nested"])}


def test_name_overrides_the_function_name():
    @dd.asset(Orders, name="orders", key_prefix="sales")
    def _orders_impl() -> pl.DataFrame:
        return pl.DataFrame()

    assert _orders_impl.keys == {dg.AssetKey(["sales", "orders"])}


def test_upstream_dependencies_bind_as_ordinary_parameters():
    @dd.asset(Orders)
    def downstream(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return raw_orders

    assert downstream.keys_by_input_name == {"raw_orders": dg.AssetKey(["raw_orders"])}


@dg.success_hook
def _notify(context: dg.HookContext) -> None:
    pass


class _Warehouse(dg.ConfigurableResource[None]):
    dsn: str


def test_every_forwarded_dg_asset_parameter_appears_in_the_definition():
    """The decorator forwards all twelve at once, so a parameter it forwards only on its own still fails."""
    partitions = dg.StaticPartitionsDefinition(["a", "b"])
    retry = dg.RetryPolicy(max_retries=2)
    backfill = dg.BackfillPolicy.single_run()

    @dd.asset(
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

    # A `deps` entry is an input too, so this reads one key, not the whole mapping.
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


def test_pool_is_passed_to_the_underlying_op():
    # Separate, because one op cannot have both a pool and a `backfill_policy`.
    @dd.asset(Orders, pool="limited")
    def pooled() -> pl.DataFrame:
        return pl.DataFrame()

    assert pooled.op.pool == "limited"


def test_the_parameters_that_describe_the_table_appear_in_the_definition():
    condition = dg.AutomationCondition.eager()
    freshness = dg.FreshnessPolicy.time_window(fail_window=dt.timedelta(hours=24))

    @dd.asset(
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
    @dd.asset(
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
    @dd.asset(_Documented)
    def postal_codes() -> pl.DataFrame:
        """The decorated function's own docstring."""
        return pl.DataFrame()

    (spec,) = postal_codes.specs
    assert spec.description == "Postal codes, one row per code."


def test_an_explicit_description_takes_precedence_over_the_schema_docstring():
    @dd.asset(_Documented, description="Said at the call site.")
    def explicit() -> pl.DataFrame:
        """The decorated function's own docstring."""
        return pl.DataFrame()

    (spec,) = explicit.specs
    assert spec.description == "Said at the call site."


def test_a_schema_without_a_docstring_uses_the_decorated_functions_docstring():
    @dd.asset(_Undocumented)
    def undocumented() -> pl.DataFrame:
        """The decorated function's own docstring."""
        return pl.DataFrame()

    (spec,) = undocumented.specs
    assert spec.description == "The decorated function's own docstring."


def test_an_undocumented_schema_does_not_inherit_the_base_schemas_docstring():
    # `inspect.getdoc` would return this docstring for a schema without its own.
    assert dy.Schema.__doc__ is not None

    @dd.asset(_Undocumented, name="inherits_nothing")
    def inherits_nothing() -> pl.DataFrame:
        return pl.DataFrame()

    (spec,) = inherits_nothing.specs
    assert spec.description is None


def test_an_empty_description_counts_as_absent_too():
    @dd.asset(_Documented, description="")
    def empty() -> pl.DataFrame:
        """The decorated function's own docstring."""
        return pl.DataFrame()

    (spec,) = empty.specs
    assert spec.description == "Postal codes, one row per code."


def test_a_whitespace_only_schema_docstring_counts_as_absent():
    @dd.asset(_Blank)
    def blank() -> pl.DataFrame:
        """The decorated function's own docstring."""
        return pl.DataFrame()

    (spec,) = blank.specs
    assert spec.description == "The decorated function's own docstring."


def test_a_multi_line_schema_docstring_is_dedented():
    @dd.asset(Orders)
    def dedented() -> pl.DataFrame:
        return pl.DataFrame()

    (spec,) = dedented.specs
    assert spec.description is not None
    assert spec.description.startswith("Customer orders, one row per order line.\n\n")
    # The catalog renders an indented line as a code block.
    assert not any(line.startswith(" ") for line in spec.description.splitlines()), (
        spec.description
    )
    assert spec.description == spec.description.strip()


# --- checks ---
def test_there_is_one_check_per_rule_plus_the_column_schema_check():
    """Check names contain no rule value, so changing a bound does not start a new check history."""
    expected = ["dy_schema__columns"] + [
        f"dy_rule__{rule.replace('|', '__')}" for rule in _RULES
    ]

    assert list(_specs_by_name(orders)) == expected


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
    specs = _specs_by_name(orders)

    assert specs["dy_rule__amount__min"].description == "amount >= 0.00"
    assert specs["dy_rule__email__max_length"].description == (
        "email length <= 254 bytes"
    )
    assert specs["dy_rule__order_id__nullability"].description == "order_id not null"


def test_the_primary_key_check_describes_the_whole_key():
    assert _specs_by_name(orders)["dy_rule__primary_key"].description == (
        "PK: order_id, line_no"
    )


def test_a_rule_with_neither_a_docstring_nor_a_constraint_falls_back_to_its_name():
    assert _specs_by_name(orders)["dy_rule__line_numbers_are_dense"].description == (
        "line_numbers_are_dense"
    )


def test_no_check_description_is_blank():
    assert all(spec.description for spec in orders.check_specs)


def test_the_column_schema_check_names_the_schema():
    assert _specs_by_name(orders)["dy_schema__columns"].description == (
        "Columns and dtypes match Orders."
    )


# --- check granularity ---
_COLUMNS_WITH_RULES = {rule.split("|")[0] for rule in _RULES if "|" in rule}

_SCHEMA_LEVEL_RULES = [rule for rule in _RULES if "|" not in rule]


@dd.asset(Orders, name="by_column", check_granularity="column")
def by_column() -> pl.DataFrame:
    return pl.DataFrame()


@dd.asset(Orders, name="by_schema", check_granularity="schema")
def by_schema() -> pl.DataFrame:
    return pl.DataFrame()


def test_column_granularity_collapses_the_rules_into_one_check_per_column_with_rules():
    assert set(_specs_by_name(by_column)) == {
        "dy_schema__columns",
        "dy_schema__rules",
    } | {f"dy_col__{column}" for column in _COLUMNS_WITH_RULES}


def test_a_column_without_rules_has_no_check():
    assert not {"dy_col__fulfilled_in", "dy_col__payload"} & set(
        _specs_by_name(by_column)
    )


def test_a_ten_field_struct_is_ten_checks_by_rule_and_one_by_column():
    class Addresses(dy.Schema):
        address_id = dy.String(primary_key=True)
        address = dy.Struct(
            {f"field_{n}": dy.String(nullable=False) for n in range(10)}, nullable=True
        )

    @dd.asset(Addresses, name="by_rule")
    def by_rule() -> pl.DataFrame:
        return pl.DataFrame()

    @dd.asset(Addresses, name="collapsed", check_granularity="column")
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


def test_schema_rules_collapse_into_the_schema_check_by_default():
    specs = _specs_by_name(by_column)

    assert "dy_schema__rules" in specs
    assert not {f"dy_rule__{rule}" for rule in _SCHEMA_LEVEL_RULES} & set(specs)


def test_per_rule_gives_each_schema_rule_a_check_of_its_own():
    @dd.asset(
        Orders,
        name="per_rule",
        check_granularity="column",
        schema_rules="per_rule",
    )
    def per_rule() -> pl.DataFrame:
        return pl.DataFrame()

    specs = _specs_by_name(per_rule)

    assert {f"dy_rule__{rule}" for rule in _SCHEMA_LEVEL_RULES} <= set(specs)
    assert "dy_schema__rules" not in specs


def test_the_schema_rule_set_cannot_be_collided_with_by_a_user_column():
    class Tables(dy.Schema):
        table_id = dy.String(primary_key=True)
        schema = dy.String(nullable=False)

    @dd.asset(Tables, name="tables", check_granularity="column")
    def tables() -> pl.DataFrame:
        return pl.DataFrame()

    specs = _specs_by_name(tables)

    assert "dy_col__schema" in specs
    assert "dy_schema__rules" in specs
    assert specs["dy_col__schema"] != specs["dy_schema__rules"]


def test_schema_granularity_leaves_one_rules_check_beside_the_column_schema_check():
    assert set(_specs_by_name(by_schema)) == {"dy_schema__columns", "dy_schema__rules"}


@pytest.mark.parametrize("granularity", ["rule", "column", "schema"])
def test_a_schema_with_no_rules_gets_the_column_schema_check_and_nothing_else(
    granularity: Any,
):
    class Blob(dy.Schema):
        payload = dy.String(nullable=True)

    @dd.asset(Blob, name=f"blob_{granularity}", check_granularity=granularity)
    def blob() -> pl.DataFrame:
        return pl.DataFrame()

    assert set(_specs_by_name(blob)) == {"dy_schema__columns"}


@pytest.mark.parametrize("asset", [orders, by_column, by_schema])
def test_the_column_schema_check_is_present_and_blocking_at_every_granularity(
    asset: dg.AssetsDefinition,
):
    specs = _specs_by_name(asset)

    assert specs["dy_schema__columns"].blocking
    assert not any(
        spec.blocking for name, spec in specs.items() if name != "dy_schema__columns"
    )


def test_a_collapsed_check_names_the_rules_it_reports_for():
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


def test_a_granularity_outside_the_allowed_values_raises_at_definition_time():
    # Typed `Any`, because a type checker rejects the literal.
    wrong: Any = "per_column"

    with pytest.raises(InvalidSettingError) as raised:

        @dd.asset(Orders, name="misconfigured", check_granularity=wrong)
        def _misconfigured() -> pl.DataFrame:
            return pl.DataFrame()

    assert "check_granularity" in str(raised.value)


@pytest.mark.parametrize("quarantine", [False, True], ids=["plain", "quarantined"])
def test_a_malformed_quarantine_dir_raises_at_definition_time(
    monkeypatch: pytest.MonkeyPatch, quarantine: bool
):
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_QUARANTINE_DIR", "   ")

    with pytest.raises(InvalidSettingError) as raised:

        @dd.asset(Orders, name="misconfigured", quarantine=quarantine)
        def _misconfigured() -> pl.DataFrame:
            return pl.DataFrame()

    assert "quarantine_dir" in str(raised.value)


def test_the_environment_variable_sets_a_default_granularity_for_the_code_location(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("DAGSTER_DATAFRAMELY_CHECK_GRANULARITY", "schema")

    @dd.asset(Orders, name="code_location_default")
    def code_location_default() -> pl.DataFrame:
        return pl.DataFrame()

    assert set(_specs_by_name(code_location_default)) == {
        "dy_schema__columns",
        "dy_schema__rules",
    }


# --- definition metadata ---
def _catalog(asset: dg.AssetsDefinition, key: dg.AssetKey) -> dg.TableSchema:
    """Return the table schema that fills the Columns tab."""
    return asset.metadata_by_key[key][_COLUMN_SCHEMA_KEY]


def _columns_of(
    asset: dg.AssetsDefinition, key: dg.AssetKey
) -> dict[str, dg.TableColumn]:
    return {column.name: column for column in _catalog(asset, key).columns}


def _columns() -> dict[str, dg.TableColumn]:
    return _columns_of(orders, dg.AssetKey(["orders"]))


def test_the_columns_tab_is_populated_before_first_materialization():
    amount = _columns()["amount"]

    assert amount.type == "Decimal(precision=10, scale=2)"
    assert amount.description == "Line total in account currency."
    assert not amount.constraints.nullable


def test_column_metadata_becomes_tags():
    """Values become strings, because Dagster's tags are `Mapping[str, str]`."""
    assert _columns()["amount"].tags == {"owner": "finance", "pii": "False"}


def test_a_column_without_metadata_has_no_tags():
    assert not _columns()["quantity"].tags


def test_a_nullable_column_is_marked_nullable():
    assert _columns()["note"].constraints.nullable


def test_every_schema_column_appears_in_the_catalog_in_order():
    assert list(_columns()) == list(Orders.columns())


def test_a_unique_column_is_marked_unique():
    assert _columns()["tracking_id"].constraints.unique
    assert "dy_rule__tracking_id__unique" in _specs_by_name(orders)


def test_a_primary_key_column_is_not_marked_unique():
    """On a composite key, only the combination of columns is unique."""
    assert not _columns()["order_id"].constraints.unique
    assert not _columns()["line_no"].constraints.unique


# --- column constraints ---
class Measurements(dy.Schema):
    """The constraint kinds that `Orders` has no column for."""

    reading_at = dy.Datetime(resolution="1h")
    grade = dy.Int32(is_in=[1, 2, 3])
    depth = dy.Int32(min=0, max=100)
    ratio = dy.Float64(min_exclusive=0.0, max_exclusive=1.0)
    corners = dy.Array(dy.Int32(min=0), 2)
    # A `Struct`, which the renderer does not recurse into.
    box = dy.Struct({"width": dy.Int64(min=1)})
    label = dy.String(
        min_length=2,
        # Two lambdas, so Dataframely names them with a counter.
        check=[lambda expr: expr != "", lambda expr: expr == expr.str.strip_chars()],
    )


@dd.asset(Measurements)
def measurements() -> pl.DataFrame:
    return pl.DataFrame()


def _measured() -> dict[str, dg.TableColumn]:
    return _columns_of(measurements, dg.AssetKey(["measurements"]))


def test_a_bound_reads_as_an_operator_rather_than_as_a_check_name():
    assert _columns()["line_no"].constraints.other == [">= 1"]
    assert _measured()["depth"].constraints.other == [">= 0", "<= 100"]
    assert _measured()["ratio"].constraints.other[:2] == ["> 0.0", "< 1.0"]


def test_a_length_bound_states_the_unit_it_counts():
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


def test_a_named_check_renders_its_key_and_an_anonymous_one_renders_as_custom_check():
    assert "lowercase" in _columns()["email"].constraints.other
    assert _columns()["note"].constraints.other == ["custom check"]
    assert _measured()["label"].constraints.other[:2] == [
        "custom check",
        "custom check",
    ]


def test_a_nested_columns_rules_render_as_constraints_on_its_elements():
    assert _measured()["corners"].constraints.other == [
        "elements not null",
        "elements >= 0",
    ]


def test_a_struct_fields_rules_fall_back_to_their_own_names():
    assert _measured()["box"].constraints.other == [
        "inner_width_nullability",
        "inner_width_min",
    ]


def test_a_constraint_dagster_models_first_class_is_not_repeated():
    """`nullable` and `unique` have their own fields on `dg.TableColumnConstraints`, so `other` leaves them out."""
    assert _columns()["quantity"].constraints.other == [">= 1"]
    assert _columns()["tracking_id"].constraints.other == []


def test_a_constraint_left_at_its_dataframely_default_renders_nothing():
    """Every float column has `inf` and `nan` rules, because `allow_inf` and `allow_nan` default to `False`."""
    assert _measured()["ratio"].constraints.other == ["> 0.0", "< 1.0"]
    assert {"dy_rule__ratio__inf", "dy_rule__ratio__nan"} <= set(
        _specs_by_name(measurements)
    )


def test_the_three_rules_no_column_row_states_are_stated_by_a_collapsed_check():
    @dd.asset(Measurements, name="measured_by_column", check_granularity="column")
    def collapsed() -> pl.DataFrame:
        return pl.DataFrame()

    assert _specs_by_name(collapsed)["dy_col__ratio"].description == (
        "Every rule on ratio: not null, > 0.0, < 1.0, not infinite, not NaN."
    )
    assert _specs_by_name(by_column)["dy_col__tracking_id"].description == (
        "Every rule on tracking_id: unique."
    )


def test_a_rules_docstring_does_not_appear_in_the_table_constraints():
    """`paid_orders_have_amount` has a docstring, and the table constraint shows only its name."""
    assert _catalog(orders, dg.AssetKey(["orders"])).constraints.other == [
        "paid_orders_have_amount",
        "line_numbers_are_dense",
        "PK: order_id, line_no",
    ]


def test_the_primary_key_is_stated_once_at_table_level():
    table_schema = _catalog(orders, dg.AssetKey(["orders"]))

    assert "PK: order_id, line_no" in table_schema.constraints.other
    assert not any(
        "PK" in constraint
        for column in table_schema.columns
        for constraint in column.constraints.other
    )


class Ledger(dy.Schema):
    """A schema with a composite key whose `entry_id` also declares `unique=True`."""

    entry_id = dy.String(primary_key=True, unique=True)
    posted_at = dy.Datetime(primary_key=True)


@dd.asset(Ledger)
def ledger() -> pl.DataFrame:
    return pl.DataFrame()


def test_a_primary_key_column_is_not_null_and_unique_only_if_it_declares_unique():
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
    """The quarantine is not an asset (ADR-0004, ADR-0006)."""

    @dd.asset(Orders, quarantine=True)
    def kept() -> pl.DataFrame:
        return pl.DataFrame()

    @dd.asset(Orders, quarantine=False)
    def plain() -> pl.DataFrame:
        return pl.DataFrame()

    assert kept.keys == {dg.AssetKey(["kept"])}
    assert len(kept.node_def.output_dict) == len(plain.node_def.output_dict)
    assert {spec.name for spec in kept.check_specs} == {
        spec.name for spec in plain.check_specs
    }


def test_a_quarantine_needs_no_resource_declared():
    """The IO manager comes from the step, so a direct invocation does not have to pass one."""

    @dd.asset(Orders, quarantine=True)
    def kept() -> pl.DataFrame:
        return pl.DataFrame()

    assert kept.op.required_resource_keys == frozenset()


# --- definition-time errors ---
# `test_reserved_namespace.py` tests the singular messages; these test the plural.
def test_the_reserved_column_error_reads_as_plural_for_several_columns():
    class Reserved(dy.Schema):
        dy_flag = dy.Bool()
        dy_rule__amount__min = dy.Bool()

    with pytest.raises(ReservedColumnError) as raised:

        @dd.asset(Reserved)
        def reserved() -> pl.DataFrame:
            return pl.DataFrame()

    assert "Columns 'dy_flag', 'dy_rule__amount__min' of Reserved use" in str(
        raised.value
    )
    assert "Rename them." in str(raised.value)


def test_the_invalid_column_name_error_reads_as_plural_for_several_columns():
    class InvalidNames(dy.Schema):
        total = dy.Int64(alias="Order Total")
        net = dy.Int64(alias="Net/Total")

    with pytest.raises(InvalidColumnNameError) as raised:

        @dd.asset(InvalidNames)
        def invalid_names() -> pl.DataFrame:
            return pl.DataFrame()

    assert (
        "Columns 'Order Total', 'Net/Total' of InvalidNames contain characters outside"
        in str(raised.value)
    )
    assert "Rename them, or change the `alias=` that sets the name." in str(
        raised.value
    )


def test_a_collection_raises_at_decoration_time():
    class OrderBook(dy.Collection):
        orders: dy.LazyFrame[Orders]

    with pytest.raises(CollectionNotSupportedError) as raised:
        dd.asset(OrderBook)  # pyrefly: ignore[bad-argument-type]

    assert "OrderBook" in str(raised.value)


def test_a_non_schema_argument_is_left_to_fail_however_it_fails():
    """Only `dy.Collection` has a guard, because it is the one plausible wrong argument."""
    with pytest.raises(Exception) as raised:  # noqa: PT011 - breadth is the point

        @dd.asset(42)  # pyrefly: ignore[bad-argument-type]
        def nonsense() -> pl.DataFrame:
            return pl.DataFrame()

    assert not isinstance(raised.value, DagsterDataframelyError)


# --- the underlying op ---
def _shipments(prefix: str, *, quarantine: bool = False) -> dg.AssetsDefinition:
    """Return an asset named `shipments` under `prefix`."""

    @dd.asset(Orders, key_prefix=prefix, name="shipments", quarantine=quarantine)
    def shipments() -> pl.DataFrame:
        return pl.DataFrame()

    return shipments


def test_the_op_takes_its_name_from_the_key_exactly_as_dg_asset_takes_its_own():
    @dd.asset(Orders, key_prefix=["warehouse", "sales"], name="shipments")
    def attached() -> pl.DataFrame:
        return pl.DataFrame()

    # A real `@dg.asset`, so the test still passes if Dagster changes how it names an op.
    @dg.asset(key_prefix=["warehouse", "sales"], name="shipments")
    def plain() -> None: ...

    assert attached.op.name == plain.op.name


@pytest.mark.parametrize("quarantine", [False, True], ids=["bare", "quarantined"])
def test_two_assets_sharing_a_name_under_different_prefixes_coexist(quarantine: bool):
    """Dagster allows a repeated op name only for equal definitions, and these differ because each check output name contains the asset key."""
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
    @dg.asset(ins={"upstream": dg.AssetIn(key=dg.AssetKey(["beta", "shipments"]))})
    def consumer(upstream: pl.DataFrame) -> None: ...

    definitions = dg.Definitions(
        assets=[_shipments("alpha"), _shipments("beta"), consumer]
    )

    job = definitions.resolve_implicit_global_asset_job_def()
    # Undocumented but public, and the only API that returns the edge as a node name.
    upstream_outputs = (
        job.graph.dependency_structure.input_to_upstream_outputs_for_node("consumer")
    )

    assert {
        handle.node_name for handles in upstream_outputs.values() for handle in handles
    } == {"beta__shipments"}


# --- the decorator's own contract with dagster ---
_NO_DG_ASSET_COUNTERPART = {
    "schema",
    "quarantine",
    "check_granularity",
    "schema_rules",
    "max_failure_samples",
    "statistics",
    "row_sample",
}

_NOT_ON_THE_DECORATOR = {
    "check_specs",  # derived from the schema
    "key",  # set by `key_prefix` and `name`
    "output_required",  # always `False`, so the step can end without the output
    "dagster_type",  # ruled out (#3): runs before the IO manager and has no severity
    "is_virtual",  # a virtual asset has no function to decorate
    "io_manager_def",  # `resource_defs` covers it
}


def test_the_decorator_takes_dg_assets_parameters_under_the_same_names():
    """A new `dg.asset` parameter fails this test until the decorator forwards it or a set above lists it."""
    decorator = set(inspect.signature(dd.asset).parameters) - _NO_DG_ASSET_COUNTERPART
    upstream = set(inspect.signature(dg.asset).parameters) - {"compute_fn", "kwargs"}

    assert decorator <= upstream, f"no longer on dg.asset: {decorator - upstream}"
    assert upstream - decorator == _NOT_ON_THE_DECORATOR, (
        f"new on dg.asset, unconsidered here: {upstream - decorator - _NOT_ON_THE_DECORATOR}"
    )


def test_the_schema_is_the_one_positional_parameter_and_is_required():
    parameters = inspect.signature(dd.asset).parameters
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


def test_check_specs_key_and_output_required_are_not_parameters():
    parameters = set(inspect.signature(dd.asset).parameters)

    assert parameters.isdisjoint({"check_specs", "key", "output_required"})
