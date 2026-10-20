"""Definition-time behaviour of `dd.asset`, read from the `AssetsDefinition` it returns without running anything."""

import datetime as dt
import inspect
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl
import pytest
from dagster._config.field_utils import Shape

import dagster_dataframely as dd
from dagster_dataframely._rules import describe_rules
from dagster_dataframely.errors import (
    CollectionNotSupportedError,
    DagsterDataframelyError,
    InvalidColumnNameError,
    InvalidSettingError,
    ReservedColumnError,
)
from dagster_dataframely.wiring import get_code_version
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


def _expression_text(rule: str, schema: type[dy.Schema] = Orders) -> str:
    return str(describe_rules(schema)[rule].expr)


class TestAssetSpec:
    """The asset's spec, and the keys of its checks and its inputs."""

    def test_the_decorator_produces_one_asset(self):
        assert orders.keys == {dg.AssetKey(["orders"])}
        assert orders.group_names_by_key == {dg.AssetKey(["orders"]): "sales"}

    def test_the_output_is_not_required(self):
        (spec,) = orders.specs
        assert spec.skippable

    def test_a_key_prefix_applies_to_the_asset_and_its_checks(self):
        @dd.asset(Orders, key_prefix="sales")
        def prefixed() -> pl.DataFrame:
            return pl.DataFrame()

        key = dg.AssetKey(["sales", "prefixed"])

        assert prefixed.keys == {key}
        assert {spec.asset_key for spec in prefixed.check_specs} == {key}

    def test_a_sequence_key_prefix_nests(self):
        @dd.asset(Orders, key_prefix=["warehouse", "sales"])
        def nested() -> pl.DataFrame:
            return pl.DataFrame()

        assert nested.keys == {dg.AssetKey(["warehouse", "sales", "nested"])}

    def test_name_overrides_the_function_name(self):
        @dd.asset(Orders, name="orders", key_prefix="sales")
        def _orders_impl() -> pl.DataFrame:
            return pl.DataFrame()

        assert _orders_impl.keys == {dg.AssetKey(["sales", "orders"])}

    def test_upstream_dependencies_bind_as_ordinary_parameters(self):
        @dd.asset(Orders)
        def downstream(raw_orders: pl.DataFrame) -> pl.DataFrame:
            return raw_orders

        assert downstream.keys_by_input_name == {
            "raw_orders": dg.AssetKey(["raw_orders"])
        }

    @pytest.mark.parametrize(
        "kinds", [None, set(), {"polars"}], ids=["none", "empty", "polars"]
    )
    def test_an_asset_without_other_kinds_has_only_the_polars_kind(
        self, kinds: set[str] | None
    ):
        @dd.asset(Orders, kinds=kinds)
        def kinded() -> pl.DataFrame:
            return pl.DataFrame()

        (spec,) = kinded.specs

        assert spec.kinds == {"polars"}
        assert spec.tags["dagster/kind/polars"] == ""


@dg.success_hook
def _notify(context: dg.HookContext) -> None:
    pass


class _Warehouse(dg.ConfigurableResource[None]):
    dsn: str


class TestForwardedParameters:
    """The `dg.asset` parameters that `dd.asset` forwards to the definition."""

    def test_every_forwarded_dg_asset_parameter_appears_in_the_definition(self):
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

    def test_pool_is_passed_to_the_underlying_op(self):
        # Separate, because one op cannot have both a pool and a `backfill_policy`.
        @dd.asset(Orders, pool="limited")
        def pooled() -> pl.DataFrame:
            return pl.DataFrame()

        assert pooled.op.pool == "limited"

    def test_the_parameters_that_describe_the_table_appear_in_the_definition(self):
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
        assert spec.kinds == {"polars", "parquet"}
        assert spec.automation_condition == condition
        assert spec.freshness_policy == freshness

    def test_user_metadata_cannot_displace_the_packages_own(self):
        @dd.asset(
            Orders,
            metadata={_COLUMN_SCHEMA_KEY: "mine", "own": "kept"},
        )
        def collides() -> pl.DataFrame:
            return pl.DataFrame()

        definition_metadata = collides.metadata_by_key[dg.AssetKey(["collides"])]

        assert definition_metadata["own"] == "kept"
        assert isinstance(definition_metadata[_COLUMN_SCHEMA_KEY], dg.TableSchema)


class _Documented(dy.Schema):
    """Postal codes, one row per code."""

    code = dy.String(primary_key=True)


class _Undocumented(dy.Schema):
    code = dy.String(primary_key=True)


class _Blank(dy.Schema):
    """ """

    code = dy.String(primary_key=True)


class TestDescription:
    """The asset's description."""

    def test_the_schema_docstring_fills_a_description_the_decorator_was_not_given(self):
        @dd.asset(_Documented)
        def postal_codes() -> pl.DataFrame:
            """The decorated function's own docstring."""
            return pl.DataFrame()

        (spec,) = postal_codes.specs
        assert spec.description == "Postal codes, one row per code."

    def test_an_explicit_description_takes_precedence_over_the_schema_docstring(self):
        @dd.asset(_Documented, description="Said at the call site.")
        def explicit() -> pl.DataFrame:
            """The decorated function's own docstring."""
            return pl.DataFrame()

        (spec,) = explicit.specs
        assert spec.description == "Said at the call site."

    def test_a_schema_without_a_docstring_uses_the_decorated_functions_docstring(self):
        @dd.asset(_Undocumented)
        def undocumented() -> pl.DataFrame:
            """The decorated function's own docstring."""
            return pl.DataFrame()

        (spec,) = undocumented.specs
        assert spec.description == "The decorated function's own docstring."

    def test_an_undocumented_schema_does_not_inherit_the_base_schemas_docstring(self):
        # `inspect.getdoc` would return this docstring for a schema without its own.
        assert dy.Schema.__doc__ is not None

        @dd.asset(_Undocumented, name="inherits_nothing")
        def inherits_nothing() -> pl.DataFrame:
            return pl.DataFrame()

        (spec,) = inherits_nothing.specs
        assert spec.description is None

    def test_an_empty_description_counts_as_absent_too(self):
        @dd.asset(_Documented, description="")
        def empty() -> pl.DataFrame:
            """The decorated function's own docstring."""
            return pl.DataFrame()

        (spec,) = empty.specs
        assert spec.description == "Postal codes, one row per code."

    def test_a_whitespace_only_schema_docstring_counts_as_absent(self):
        @dd.asset(_Blank)
        def blank() -> pl.DataFrame:
            """The decorated function's own docstring."""
            return pl.DataFrame()

        (spec,) = blank.specs
        assert spec.description == "The decorated function's own docstring."

    def test_a_multi_line_schema_docstring_is_dedented(self):
        @dd.asset(Orders)
        def dedented() -> pl.DataFrame:
            return pl.DataFrame()

        (spec,) = dedented.specs
        assert spec.description is not None
        assert spec.description.startswith(
            "Customer orders, one row per order line.\n\n"
        )
        # The catalog renders an indented line as a code block.
        assert not any(
            line.startswith(" ") for line in spec.description.splitlines()
        ), spec.description
        assert spec.description == spec.description.strip()


_ROOT = Path(__file__).parent.parent

# `str(pl.Expr)` prints only the first two and the last of these sixteen values.
_LETTERS = [chr(code) for code in range(ord("a"), ord("q"))]


class _Priced(dy.Schema):
    amount = dy.Float64(min=0.0)


class _PricedFromOne(dy.Schema):
    amount = dy.Float64(min=1.0)


class _Redocumented(dy.Schema):
    """The columns of `_Priced`, with a docstring."""

    amount = dy.Float64(min=0.0)


class _Warned(dy.Schema):
    amount = dy.Float64(
        min=0.0, metadata={"dagster_dataframely": {"severity": {"min": "warn"}}}
    )


class _Lettered(dy.Schema):
    code = dy.String(check=lambda column: column.is_in(_LETTERS))


class _Relettered(dy.Schema):
    code = dy.String(
        check=lambda column: column.is_in([*_LETTERS[:7], "z", *_LETTERS[8:]])
    )


class _Mapped(dy.Schema):
    code = dy.String(
        check=lambda column: column.map_elements(
            lambda value: value[0] == "A", return_dtype=pl.Boolean
        )
    )


class _Raising(dy.Schema):
    value = dy.Int64()

    @dy.rule()
    def raising(cls) -> pl.Expr:
        """Documented, so its check description does not show the expression."""
        raise RuntimeError


class TestCodeVersion:
    """The asset's code version."""

    def test_an_asset_without_a_code_version_gets_a_sha1_hexdigest(self):
        (spec,) = orders.specs

        assert spec.code_version is not None
        assert re.fullmatch(r"[0-9a-f]{40}", spec.code_version)

    def test_the_wiring_function_returns_the_code_version_the_decorator_sets(self):
        (spec,) = orders.specs

        assert get_code_version(Orders) == spec.code_version

    def test_a_changed_column_constraint_changes_the_code_version(self):
        assert get_code_version(_Priced) != get_code_version(_PricedFromOne)

    def test_a_changed_value_the_expression_text_elides_changes_the_code_version(
        self,
    ):
        assert _expression_text("code|check", _Lettered) == _expression_text(
            "code|check", _Relettered
        )
        assert get_code_version(_Lettered) != get_code_version(_Relettered)

    def test_a_changed_severity_changes_the_code_version(self):
        assert get_code_version(_Priced) != get_code_version(_Warned)

    def test_a_changed_docstring_leaves_the_code_version_unchanged(self):
        assert get_code_version(_Priced) == get_code_version(_Redocumented)

    def test_deriving_the_code_version_calls_every_rule_function(self):
        with pytest.raises(RuntimeError):

            @dd.asset(_Raising)
            def raising() -> pl.DataFrame:
                return pl.DataFrame()

    def test_a_python_udf_rule_builds_without_cloudpickle(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setitem(sys.modules, "cloudpickle", None)

        @dd.asset(_Mapped)
        def mapped() -> pl.DataFrame:
            return pl.DataFrame()

        (spec,) = mapped.specs
        assert spec.code_version is not None
        assert re.fullmatch(r"[0-9a-f]{40}", spec.code_version)

    def test_two_processes_derive_the_same_code_version(self):
        """Two processes derive the same code version for a schema with a lambda `check=`."""
        script = "from tests.scenario import Orders; import dagster_dataframely as dd; print(dd.wiring.get_code_version(Orders))"
        versions = {
            subprocess.run(  # noqa: S603 - the script is a literal
                [sys.executable, "-c", script],
                capture_output=True,
                check=True,
                cwd=_ROOT,
                text=True,
            ).stdout.strip()
            for _ in range(2)
        }

        assert versions == {get_code_version(Orders)}


_COLUMNS_WITH_RULES = {rule.split("|")[0] for rule in _RULES if "|" in rule}


_SCHEMA_LEVEL_RULES = [rule for rule in _RULES if "|" not in rule]


@dd.asset(Orders, name="by_column", check_granularity="column")
def by_column() -> pl.DataFrame:
    return pl.DataFrame()


class TestCheckSpecs:
    """The check specs, at every `check_granularity`."""

    def test_there_is_one_check_per_rule_plus_the_column_schema_check(self):
        """Check names contain no rule value, so changing a bound does not start a new check history."""
        expected = ["dy_schema__columns"] + [
            f"dy_rule__{rule.replace('|', '__')}" for rule in _RULES
        ]

        assert list(_specs_by_name(orders)) == expected

    def test_every_check_is_blocking_because_every_rule_is_error(self):
        """`test_severity_runs.py` covers a schema that declares `"warn"`."""
        assert all(spec.blocking for spec in _specs_by_name(orders).values())

    def test_a_rules_docstring_becomes_its_check_description(self):
        specs = _specs_by_name(orders)

        assert (
            specs["dy_rule__paid_orders_have_amount"].description
            == "Paid orders must carry a positive amount."
        )

    def test_a_check_without_a_docstring_describes_the_constraint_itself(self):
        specs = _specs_by_name(orders)

        assert specs["dy_rule__amount__min"].description == "amount >= 0.00"
        assert specs["dy_rule__email__max_length"].description == (
            "email length <= 254 bytes"
        )
        assert (
            specs["dy_rule__order_id__nullability"].description == "order_id not null"
        )

    def test_the_primary_key_check_describes_the_whole_key(self):
        assert _specs_by_name(orders)["dy_rule__primary_key"].description == (
            "PK: order_id, line_no"
        )

    def test_a_rule_with_neither_a_docstring_nor_a_constraint_shows_its_name_and_expression(
        self,
    ):
        spec = _specs_by_name(orders)["dy_rule__line_numbers_are_dense"]

        assert spec.description == (
            f"line_numbers_are_dense `{_expression_text('line_numbers_are_dense')}`"
        )

    def test_a_check_argument_rule_shows_its_key_and_expression(self):
        specs = _specs_by_name(orders)

        assert specs["dy_rule__email__check__lowercase"].description == (
            f"email lowercase `{_expression_text('email|check__lowercase')}`"
        )
        assert specs["dy_rule__note__check"].description == (
            f"note custom check `{_expression_text('note|check')}`"
        )

    def test_an_element_check_rule_shows_its_key_and_expression(self):
        class Tagged(dy.Schema):
            tags = dy.List(
                dy.String(check={"short": lambda expr: expr.str.len_chars() < 5})
            )

        @dd.asset(Tagged)
        def tagged() -> pl.DataFrame:
            return pl.DataFrame()

        spec = _specs_by_name(tagged)["dy_rule__tags__inner_check__short"]

        assert spec.description == (
            f"tags elements short `{_expression_text('tags|inner_check__short', Tagged)}`"
        )

    def test_a_documented_rule_shows_its_docstring_without_building_its_expression(
        self,
    ):
        class Unbuilt(dy.Schema):
            value = dy.Int64()

            @dy.rule()
            def unbuilt(cls) -> pl.Expr:
                """Values stay unbuilt."""
                raise AssertionError

        # Set, because deriving a code version builds every expression.
        @dd.asset(Unbuilt, code_version="v1")
        def unbuilt() -> pl.DataFrame:
            return pl.DataFrame()

        assert _specs_by_name(unbuilt)["dy_rule__unbuilt"].description == (
            "Values stay unbuilt."
        )

    def test_an_expression_with_backticks_gets_a_longer_delimiter(self):
        class Ticked(dy.Schema):
            value = dy.String(check={"ticked": lambda expr: expr != "``"})

        @dd.asset(Ticked)
        def ticked() -> pl.DataFrame:
            return pl.DataFrame()

        expr = _expression_text("value|check__ticked", Ticked)
        assert "``" in expr
        assert _specs_by_name(ticked)["dy_rule__value__check__ticked"].description == (
            f"value ticked ```{expr}```"
        )

    def test_no_check_description_is_blank(self):
        assert all(spec.description for spec in orders.check_specs)

    def test_the_column_schema_check_names_the_schema(self):
        assert _specs_by_name(orders)["dy_schema__columns"].description == (
            "Columns and dtypes match Orders."
        )

    def test_column_granularity_collapses_the_rules_into_one_check_per_column_with_rules(
        self,
    ):
        assert set(_specs_by_name(by_column)) == (
            {"dy_schema__columns"}
            | {f"dy_col__{column}" for column in _COLUMNS_WITH_RULES}
            | {f"dy_rule__{rule}" for rule in _SCHEMA_LEVEL_RULES}
        )

    def test_a_column_without_rules_has_no_check(self):
        assert not {"dy_col__fulfilled_in", "dy_col__payload"} & set(
            _specs_by_name(by_column)
        )

    def test_a_ten_field_struct_is_ten_checks_by_rule_and_one_by_column(self):
        class Addresses(dy.Schema):
            address_id = dy.String(primary_key=True)
            address = dy.Struct(
                {f"field_{n}": dy.String(nullable=False) for n in range(10)},
                nullable=True,
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
            "dy_rule__primary_key",
            "dy_col__address_id",
            "dy_col__address",
        }

    def test_column_granularity_keeps_one_check_per_schema_level_rule(self):
        """A `@dy.rule()` or the primary key is written by the user, so it keeps its own history."""
        specs = _specs_by_name(by_column)

        assert {f"dy_rule__{rule}" for rule in _SCHEMA_LEVEL_RULES} <= set(specs)
        assert not any(name.startswith("dy_schema__r") for name in specs)

    @pytest.mark.parametrize("granularity", ["rule", "column"])
    def test_a_schema_with_no_rules_gets_the_column_schema_check_and_nothing_else(
        self,
        granularity: Any,
    ):
        class Blob(dy.Schema):
            payload = dy.String(nullable=True)

        @dd.asset(Blob, name=f"blob_{granularity}", check_granularity=granularity)
        def blob() -> pl.DataFrame:
            return pl.DataFrame()

        assert set(_specs_by_name(blob)) == {"dy_schema__columns"}

    @pytest.mark.parametrize("asset", [orders, by_column])
    def test_the_column_schema_check_is_present_and_blocking_at_every_granularity(
        self,
        asset: dg.AssetsDefinition,
    ):
        assert _specs_by_name(asset)["dy_schema__columns"].blocking

    def test_a_collapsed_check_names_the_rules_it_reports_for(self):
        specs = _specs_by_name(by_column)

        assert specs["dy_col__email"].description == (
            "Every rule on email: not null, "
            f"lowercase `{_expression_text('email|check__lowercase')}`, length <= 254 bytes."
        )

    def test_the_environment_variable_sets_a_default_granularity_for_the_code_location(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ):
        monkeypatch.setenv("DAGSTER_DATAFRAMELY_CHECK_GRANULARITY", "column")

        @dd.asset(Orders, name="code_location_default")
        def code_location_default() -> pl.DataFrame:
            return pl.DataFrame()

        assert set(_specs_by_name(code_location_default)) == set(
            _specs_by_name(by_column)
        )


def _catalog(asset: dg.AssetsDefinition, key: dg.AssetKey) -> dg.TableSchema:
    """Return the table schema that fills the Columns tab."""
    return asset.metadata_by_key[key][_COLUMN_SCHEMA_KEY]


def _columns_of(
    asset: dg.AssetsDefinition, key: dg.AssetKey
) -> dict[str, dg.TableColumn]:
    return {column.name: column for column in _catalog(asset, key).columns}


def _columns() -> dict[str, dg.TableColumn]:
    return _columns_of(orders, dg.AssetKey(["orders"]))


class TestColumnsTab:
    """The columns the Columns tab lists, from the table schema in the definition metadata."""

    def test_the_columns_tab_is_populated_before_first_materialization(self):
        amount = _columns()["amount"]

        assert amount.type == "Decimal(precision=10, scale=2)"
        assert amount.description == "Line total in account currency."
        assert not amount.constraints.nullable

    def test_column_metadata_becomes_tags(self):
        """Values become strings, because Dagster's tags are `Mapping[str, str]`."""
        assert _columns()["amount"].tags == {"owner": "finance", "pii": "False"}

    def test_a_column_without_metadata_has_no_tags(self):
        assert not _columns()["quantity"].tags

    def test_a_nullable_column_is_marked_nullable(self):
        assert _columns()["note"].constraints.nullable

    def test_every_schema_column_appears_in_the_catalog_in_order(self):
        assert list(_columns()) == list(Orders.columns())

    def test_a_unique_column_is_marked_unique(self):
        assert _columns()["tracking_id"].constraints.unique
        assert "dy_rule__tracking_id__unique" in _specs_by_name(orders)

    def test_a_primary_key_column_is_not_marked_unique(self):
        """On a composite key, only the combination of columns is unique."""
        assert not _columns()["order_id"].constraints.unique
        assert not _columns()["line_no"].constraints.unique


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


class Ledger(dy.Schema):
    """A schema with a composite key whose `entry_id` also declares `unique=True`."""

    entry_id = dy.String(primary_key=True, unique=True)
    posted_at = dy.Datetime(primary_key=True)


@dd.asset(Ledger)
def ledger() -> pl.DataFrame:
    return pl.DataFrame()


class TestColumnConstraints:
    """The constraints the Columns tab states for each column and for the table."""

    def test_a_bound_reads_as_an_operator_rather_than_as_a_check_name(self):
        assert _columns()["line_no"].constraints.other == [">= 1"]
        assert _measured()["depth"].constraints.other == [">= 0", "<= 100"]
        assert _measured()["ratio"].constraints.other[:2] == ["> 0.0", "< 1.0"]

    def test_a_length_bound_states_the_unit_it_counts(self):
        assert _columns()["email"].constraints.other == [
            "lowercase",
            "length <= 254 bytes",
        ]
        assert _columns()["tags"].constraints.other == [
            "length <= 5 elements",
            "elements not null",
        ]
        assert _measured()["label"].constraints.other[-1] == "length >= 2 bytes"

    def test_the_remaining_constraint_kinds_render_their_value(self):
        assert _columns()["order_id"].constraints.other == [r"matches ^ORD-\d+$"]
        assert _measured()["reading_at"].constraints.other == ["aligned to 1h"]
        assert _measured()["grade"].constraints.other == ["in (1, 2, 3)"]

    def test_a_named_check_renders_its_key_and_an_anonymous_one_renders_as_custom_check(
        self,
    ):
        assert "lowercase" in _columns()["email"].constraints.other
        assert _columns()["note"].constraints.other == ["custom check"]
        assert _measured()["label"].constraints.other[:2] == [
            "custom check",
            "custom check",
        ]

    def test_a_nested_columns_rules_render_as_constraints_on_its_elements(self):
        assert _measured()["corners"].constraints.other == [
            "elements not null",
            "elements >= 0",
        ]

    def test_a_struct_fields_rules_fall_back_to_their_own_names(self):
        assert _measured()["box"].constraints.other == [
            "inner_width_nullability",
            "inner_width_min",
        ]

    def test_a_constraint_dagster_models_first_class_is_not_repeated(self):
        """`nullable` and `unique` have their own fields on `dg.TableColumnConstraints`, so `other` leaves them out."""
        assert _columns()["quantity"].constraints.other == [">= 1"]
        assert _columns()["tracking_id"].constraints.other == []

    def test_a_constraint_left_at_its_dataframely_default_renders_nothing(self):
        """Every float column has `inf` and `nan` rules, because `allow_inf` and `allow_nan` default to `False`."""
        assert _measured()["ratio"].constraints.other == ["> 0.0", "< 1.0"]
        assert {"dy_rule__ratio__inf", "dy_rule__ratio__nan"} <= set(
            _specs_by_name(measurements)
        )

    def test_the_three_rules_no_column_row_states_are_stated_by_a_collapsed_check(self):
        @dd.asset(Measurements, name="measured_by_column", check_granularity="column")
        def collapsed() -> pl.DataFrame:
            return pl.DataFrame()

        assert _specs_by_name(collapsed)["dy_col__ratio"].description == (
            "Every rule on ratio: not null, > 0.0, < 1.0, not infinite, not NaN."
        )
        assert _specs_by_name(by_column)["dy_col__tracking_id"].description == (
            "Every rule on tracking_id: unique."
        )

    def test_a_rules_docstring_does_not_appear_in_the_table_constraints(self):
        """`paid_orders_have_amount` has a docstring, and the table constraint shows only its name."""
        assert _catalog(orders, dg.AssetKey(["orders"])).constraints.other == [
            "paid_orders_have_amount",
            "line_numbers_are_dense",
            "PK: order_id, line_no",
        ]

    def test_the_primary_key_is_stated_once_at_table_level(self):
        table_schema = _catalog(orders, dg.AssetKey(["orders"]))

        assert "PK: order_id, line_no" in table_schema.constraints.other
        assert not any(
            "PK" in constraint
            for column in table_schema.columns
            for constraint in column.constraints.other
        )

    def test_a_primary_key_column_is_not_null_and_unique_only_if_it_declares_unique(
        self,
    ):
        columns = _columns_of(ledger, dg.AssetKey(["ledger"]))

        assert not columns["entry_id"].constraints.nullable
        assert not columns["posted_at"].constraints.nullable
        assert columns["entry_id"].constraints.unique
        assert not columns["posted_at"].constraints.unique
        assert "dy_rule__entry_id__unique" in _specs_by_name(ledger)

    def test_a_schema_with_no_primary_key_states_no_table_constraint(self):
        assert _catalog(measurements, dg.AssetKey(["measurements"])).constraints == (
            dg.TableConstraints(other=[])
        )


class TestErrorRows:
    """The asset graph and resources of an asset with `filter=True`."""

    def test_a_error_rows_adds_nothing_to_the_graph(self):
        """The error rows are not an executable asset (ADR-0004, ADR-0006)."""

        @dd.asset(Orders, filter=True)
        def kept() -> pl.DataFrame:
            return pl.DataFrame()

        @dd.asset(Orders, filter=False)
        def plain() -> pl.DataFrame:
            return pl.DataFrame()

        assert kept.keys == {dg.AssetKey(["kept"])}
        assert len(kept.node_def.output_dict) == len(plain.node_def.output_dict)
        assert {spec.name for spec in kept.check_specs} == {
            spec.name for spec in plain.check_specs
        }

    def test_a_error_rows_needs_no_resource_declared(self):
        """The IO manager comes from the step, so a direct invocation does not have to pass one."""

        @dd.asset(Orders, filter=True)
        def kept() -> pl.DataFrame:
            return pl.DataFrame()

        assert kept.op.required_resource_keys == frozenset()

    @pytest.mark.parametrize(
        ("overrides", "error_rows_key", "error_rows_io_manager_key"),
        [
            ({}, ["kept__error_rows"], "io_manager"),
            ({"io_manager_key": "warehouse"}, ["kept__error_rows"], "warehouse"),
            (
                {
                    "error_rows_key": ["audit", "kept"],
                    "error_rows_io_manager_key": "audit",
                },
                ["audit", "kept"],
                "audit",
            ),
        ],
        ids=["default", "the_assets_io_manager", "overridden"],
    )
    def test_the_definition_records_where_a_run_writes_the_error_rows(
        self,
        overrides: dict[str, Any],
        error_rows_key: list[str],
        error_rows_io_manager_key: str,
    ):
        """`build_error_rows_spec` reads both entries, so the spec loads the rows from where the writer wrote them."""

        @dd.asset(Orders, filter=True, **overrides)
        def kept() -> pl.DataFrame:
            return pl.DataFrame()

        metadata = kept.get_asset_spec().metadata

        assert metadata["dataframely/error_rows_key"] == dg.MetadataValue.asset(
            dg.AssetKey(error_rows_key)
        )
        assert metadata["dataframely/error_rows_io_manager_key"] == (
            dg.MetadataValue.text(error_rows_io_manager_key)
        )

    def test_an_asset_without_filter_records_nothing_about_error_rows(self):
        assert not any(
            name.startswith("dataframely/error_rows")
            for name in orders.get_asset_spec().metadata
        )

    def test_the_error_rows_io_manager_is_a_required_resource(self):
        """A step's resources hold only the keys its op requires, so the writer finds the IO manager there."""

        @dd.asset(
            Orders,
            filter=True,
            error_rows_io_manager_key="audit",
            required_resource_keys={"warehouse"},
        )
        def kept() -> pl.DataFrame:
            return pl.DataFrame()

        assert kept.op.required_resource_keys == {"audit", "warehouse"}
        with pytest.raises(dg.DagsterInvalidDefinitionError) as raised:
            dg.Definitions.validate_loadable(dg.Definitions(assets=[kept]))
        assert "resource with key 'audit' required by op 'kept'" in str(raised.value)


class TestDefinitionTimeErrors:
    """The errors `dd.asset` raises while it builds the definition."""

    # `test_reserved_namespace.py` tests the singular messages; these test the plural.
    def test_the_reserved_column_error_reads_as_plural_for_several_columns(self):
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

    def test_the_invalid_column_name_error_reads_as_plural_for_several_columns(self):
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

    def test_a_collection_raises_at_decoration_time(self):
        class OrderBook(dy.Collection):
            orders: dy.LazyFrame[Orders]

        with pytest.raises(CollectionNotSupportedError) as raised:
            dd.asset(OrderBook)  # pyrefly: ignore[bad-argument-type]

        assert "OrderBook" in str(raised.value)
        assert "then pass them to `dd.build_collection_checks`" in str(raised.value)

    def test_a_non_schema_argument_is_left_to_fail_however_it_fails(self):
        """Only `dy.Collection` has a guard, because it is the one plausible wrong argument."""
        with pytest.raises(Exception) as raised:  # noqa: PT011 - breadth is the point

            @dd.asset(42)  # pyrefly: ignore[bad-argument-type]
            def nonsense() -> pl.DataFrame:
                return pl.DataFrame()

        assert not isinstance(raised.value, DagsterDataframelyError)

    def test_a_granularity_outside_the_allowed_values_raises_at_definition_time(self):
        # Typed `Any`, because a type checker rejects the literal.
        wrong: Any = "per_column"

        with pytest.raises(InvalidSettingError) as raised:

            @dd.asset(Orders, name="misconfigured", check_granularity=wrong)
            def _misconfigured() -> pl.DataFrame:
                return pl.DataFrame()

        assert "check_granularity" in str(raised.value)

    @pytest.mark.parametrize(
        ("overrides", "named"),
        [
            ({"error_rows_key": ["audit", "orders__error_rows"]}, "`error_rows_key`."),
            ({"error_rows_io_manager_key": "audit"}, "`error_rows_io_manager_key`."),
            (
                {"error_rows_key": "audit", "error_rows_io_manager_key": "audit"},
                "`error_rows_key` and `error_rows_io_manager_key`.",
            ),
        ],
        ids=["key", "io_manager_key", "both"],
    )
    def test_an_error_rows_override_without_filter_raises_at_definition_time(
        self, overrides: dict[str, Any], named: str
    ):
        with pytest.raises(dg.DagsterInvalidDefinitionError) as raised:

            @dd.asset(Orders, name="unfiltered", **overrides)
            def _unfiltered() -> pl.DataFrame:
                return pl.DataFrame()

        assert str(raised.value) == (
            f"Only an asset with `filter=True` writes error rows. Pass `filter=True`, or remove {named}"
        )

    @pytest.mark.parametrize(
        "filtering", [False, True], ids=["plain", "check_and_filter"]
    )
    def test_a_malformed_error_rows_dir_raises_at_definition_time(
        self, monkeypatch: pytest.MonkeyPatch, filtering: bool
    ):
        monkeypatch.setenv("DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR", "   ")

        with pytest.raises(InvalidSettingError) as raised:

            @dd.asset(Orders, name="misconfigured", filter=filtering)
            def _misconfigured() -> pl.DataFrame:
                return pl.DataFrame()

        assert "error_rows_dir" in str(raised.value)


def _shipments(prefix: str, *, filtering: bool = False) -> dg.AssetsDefinition:
    """Return an asset named `shipments` under `prefix`."""

    @dd.asset(Orders, key_prefix=prefix, name="shipments", filter=filtering)
    def shipments() -> pl.DataFrame:
        return pl.DataFrame()

    return shipments


class TestUnderlyingOp:
    """The op under the asset."""

    def test_the_op_takes_its_name_from_the_key_exactly_as_dg_asset_takes_its_own(self):
        @dd.asset(Orders, key_prefix=["warehouse", "sales"], name="shipments")
        def attached() -> pl.DataFrame:
            return pl.DataFrame()

        # A real `@dg.asset`, so the test still passes if Dagster changes how it names an op.
        @dg.asset(key_prefix=["warehouse", "sales"], name="shipments")
        def plain() -> None: ...

        assert attached.op.name == plain.op.name

    @pytest.mark.parametrize(
        "filtering", [False, True], ids=["bare", "check_and_filter"]
    )
    def test_two_assets_sharing_a_name_under_different_prefixes_coexist(
        self, filtering: bool
    ):
        """Dagster allows a repeated op name only for equal definitions, and these differ because each check output name contains the asset key."""
        definitions = dg.Definitions(
            assets=[
                _shipments("alpha", filtering=filtering),
                _shipments("beta", filtering=filtering),
            ]
        )

        job = definitions.resolve_implicit_global_asset_job_def()

        assert {"alpha__shipments", "beta__shipments"} <= {
            node.name for node in job.graph.nodes
        }

    def test_a_downstream_asset_binds_to_the_node_that_owns_its_key(self):
        @dg.asset(ins={"upstream": dg.AssetIn(key=dg.AssetKey(["beta", "shipments"]))})
        def consumer(upstream: pl.DataFrame) -> None: ...

        definitions = dg.Definitions(
            assets=[_shipments("alpha"), _shipments("beta"), consumer]
        )

        job = definitions.resolve_implicit_global_asset_job_def()
        # Undocumented but public, and the only API that returns the edge as a node name.
        upstream_outputs = (
            job.graph.dependency_structure.input_to_upstream_outputs_for_node(
                "consumer"
            )
        )

        assert {
            handle.node_name
            for handles in upstream_outputs.values()
            for handle in handles
        } == {"beta__shipments"}


_NO_DG_ASSET_COUNTERPART = {
    "schema",
    "filter",
    "error_rows_key",
    "error_rows_io_manager_key",
    "check_granularity",
    "max_failure_examples",
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


class TestSignature:
    """The decorator's parameters, compared with `dg.asset`'s."""

    def test_the_decorator_takes_dg_assets_parameters_under_the_same_names(self):
        """A new `dg.asset` parameter fails this test until the decorator forwards it or a set above lists it."""
        decorator = (
            set(inspect.signature(dd.asset).parameters) - _NO_DG_ASSET_COUNTERPART
        )
        upstream = set(inspect.signature(dg.asset).parameters) - {
            "compute_fn",
            "kwargs",
        }

        assert decorator <= upstream, f"no longer on dg.asset: {decorator - upstream}"
        assert upstream - decorator == _NOT_ON_THE_DECORATOR, (
            f"new on dg.asset, unconsidered here: {upstream - decorator - _NOT_ON_THE_DECORATOR}"
        )

    def test_the_schema_is_the_one_positional_parameter_and_is_required(self):
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

    def test_check_specs_key_and_output_required_are_not_parameters(self):
        parameters = set(inspect.signature(dd.asset).parameters)

        assert parameters.isdisjoint({"check_specs", "key", "output_required"})
