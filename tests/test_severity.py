"""A schema declares each rule's severity, and every public function that takes a schema raises on a declaration it cannot read."""

from collections.abc import Callable
from typing import ClassVar

import dagster as dg
import dataframely as dy
import polars as pl
import pytest

import dagster_dataframely as dd
from dagster_dataframely._rules import describe_rules
from dagster_dataframely.errors import InvalidSeverityError
from tests.scenario import SCHEMA_TAKERS, OrdersSchema


def _resolve_severities(schema: type[dy.Schema]) -> dict[str, str]:
    return {name: rule.severity for name, rule in describe_rules(schema).items()}


class DeclaredSchema(dy.Schema):
    order_id = dy.String(
        primary_key=True,
        regex=r"^ORD-\d+$",
        metadata={"dagster_dataframely": {"severity": {"regex": "warn"}}},
    )
    email = dy.String(
        nullable=True,
        max_length=20,
        check={
            "lowercase": lambda col: col == col.str.to_lowercase(),
            "trimmed": lambda col: col == col.str.strip_chars(),
        },
        metadata={"dagster_dataframely": {"severity": {"check": "warn"}}},
    )
    amount = dy.Float64(
        nullable=False,
        min=0.0,
        metadata={
            "dagster_dataframely": {
                "severity": {
                    "nullable": "warn",
                    "allow_inf": "warn",
                    "allow_nan": "warn",
                }
            }
        },
    )
    note = dy.String(
        nullable=False,
        max_length=5,
        metadata={"dagster_dataframely": {"severity": "warn"}, "owner": "sales"},
    )

    __dagster_dataframely__: ClassVar = {"severity": {"positive": "warn"}}

    @dy.rule()
    def positive(cls) -> pl.Expr:
        return cls.amount.col > 0


class Inheriting(DeclaredSchema):
    pass


class AllSchemaLevelSchema(dy.Schema):
    order_id = dy.String(primary_key=True)
    amount = dy.Float64(nullable=False)

    __dagster_dataframely__: ClassVar = {"severity": "warn"}

    @dy.rule()
    def positive(cls) -> pl.Expr:
        return cls.amount.col > 0


class TestDeclaredSeverity:
    """`describe_rules` reads each rule's severity from the schema."""

    def test_an_undeclared_rule_is_error(self):
        assert set(_resolve_severities(OrdersSchema).values()) == {"error"}

    def test_a_single_value_covers_every_rule_on_the_column(self):
        severities = _resolve_severities(DeclaredSchema)

        assert severities["note|nullability"] == "warn"
        assert severities["note|max_length"] == "warn"

    def test_a_mapping_sets_only_the_rules_it_names(self):
        severities = _resolve_severities(DeclaredSchema)

        assert severities["order_id|regex"] == "warn"
        assert severities["order_id|nullability"] == "error"

    def test_an_argument_name_is_read_as_the_rule_it_generates(self):
        severities = _resolve_severities(DeclaredSchema)

        assert severities["amount|nullability"] == "warn"
        assert severities["amount|inf"] == "warn"
        assert severities["amount|nan"] == "warn"
        assert severities["amount|min"] == "error"

    def test_check_names_every_rule_its_dict_generates(self):
        severities = _resolve_severities(DeclaredSchema)

        assert severities["email|check__lowercase"] == "warn"
        assert severities["email|check__trimmed"] == "warn"
        assert severities["email|max_length"] == "error"

    def test_a_schema_level_rule_is_declared_on_the_class(self):
        severities = _resolve_severities(DeclaredSchema)

        assert severities["positive"] == "warn"
        assert severities["primary_key"] == "error"

    def test_a_single_value_on_the_class_covers_every_schema_level_rule(self):
        severities = _resolve_severities(AllSchemaLevelSchema)

        assert severities["primary_key"] == "warn"
        assert severities["positive"] == "warn"
        assert severities["amount|nullability"] == "error"

    def test_a_subclass_has_its_parents_declarations(self):
        assert _resolve_severities(Inheriting) == _resolve_severities(DeclaredSchema)


class TestColumnTags:
    """The tags of each column in the table schema `dd.wiring.get_table_schema` returns for `DeclaredSchema`."""

    def test_the_columns_tab_omits_the_declaration_and_keeps_other_tags(self):
        columns = {
            column.name: column
            for column in dd.wiring.get_table_schema(DeclaredSchema).columns
        }

        assert columns["note"].tags == {"owner": "sales"}
        assert not columns["order_id"].tags


class UnknownKeySchema(dy.Schema):
    amount = dy.Int64(min=1, metadata={"dagster_dataframely": {"severty": "warn"}})


class NotAMappingSchema(dy.Schema):
    amount = dy.Int64(min=1, metadata={"dagster_dataframely": "warn"})


class UnknownValueSchema(dy.Schema):
    amount = dy.Int64(min=1, metadata={"dagster_dataframely": {"severity": "warning"}})


class EnumValueSchema(dy.Schema):
    amount = dy.Int64(
        min=1,
        metadata={"dagster_dataframely": {"severity": dg.AssetCheckSeverity.WARN}},
    )


class EnumInMappingSchema(dy.Schema):
    amount = dy.Int64(
        min=1,
        metadata={
            "dagster_dataframely": {"severity": {"min": dg.AssetCheckSeverity.WARN}}
        },
    )


class UnknownRuleSchema(dy.Schema):
    amount = dy.Int64(
        nullable=False,
        min=1,
        metadata={"dagster_dataframely": {"severity": {"maximum": "warn"}}},
    )


class NoRulesSchema(dy.Schema):
    note = dy.String(
        nullable=True, metadata={"dagster_dataframely": {"severity": "warn"}}
    )


class ClassNamesColumnRuleSchema(dy.Schema):
    amount = dy.Int64(min=1)

    __dagster_dataframely__: ClassVar = {"severity": {"amount|min": "warn"}}


class ClassNamesNoColumnSchema(dy.Schema):
    order_id = dy.String(primary_key=True)

    __dagster_dataframely__: ClassVar = {"severity": {"nope|min": "warn"}}


class ClassUnknownRuleSchema(dy.Schema):
    order_id = dy.String(primary_key=True)

    __dagster_dataframely__: ClassVar = {"severity": {"positive": "warn"}}


class ClassUnknownKeySchema(dy.Schema):
    order_id = dy.String(primary_key=True)

    __dagster_dataframely__: ClassVar = {"severty": {"primary_key": "warn"}}


_CASES = [
    pytest.param(
        UnknownKeySchema,
        ["metadata of column 'amount' in UnknownKeySchema", "'severty'", "'severity'"],
        id="unknown key",
    ),
    pytest.param(NotAMappingSchema, ["column 'amount'", "a str"], id="not a mapping"),
    pytest.param(
        UnknownValueSchema, ["'warning'", "'error' or 'warn'"], id="unknown value"
    ),
    pytest.param(EnumValueSchema, ["Write the string 'warn'"], id="AssetCheckSeverity"),
    pytest.param(
        EnumInMappingSchema,
        ["Write the string 'warn'"],
        id="AssetCheckSeverity in a mapping",
    ),
    pytest.param(
        UnknownRuleSchema,
        ["'maximum'", "rules of column 'amount' are 'nullability', 'min'"],
        id="unknown rule",
    ),
    pytest.param(
        NoRulesSchema, ["column 'note'", "has no rules"], id="column with no rules"
    ),
    pytest.param(
        ClassNamesColumnRuleSchema,
        ["'amount|min'", "metadata of column 'amount'"],
        id="column rule on the class",
    ),
    pytest.param(
        ClassNamesNoColumnSchema,
        ["'nope|min'", "not a schema-level rule of ClassNamesNoColumnSchema"],
        id="column rule of a column that does not exist",
    ),
    pytest.param(
        ClassUnknownRuleSchema,
        [
            "'positive'",
            "schema-level rules of ClassUnknownRuleSchema are 'primary_key'",
        ],
        id="unknown schema-level rule",
    ),
    pytest.param(
        ClassUnknownKeySchema,
        ["__dagster_dataframely__", "'severty'"],
        id="unknown key on the class",
    ),
]


@pytest.mark.parametrize("taker", list(SCHEMA_TAKERS.values()), ids=list(SCHEMA_TAKERS))
@pytest.mark.parametrize(("schema", "fragments"), _CASES)
def test_a_declaration_the_package_cannot_read_raises(
    taker: Callable[[type[dy.Schema]], object],
    schema: type[dy.Schema],
    fragments: list[str],
) -> None:
    with pytest.raises(InvalidSeverityError) as raised:
        taker(schema)

    message = str(raised.value)
    assert all(fragment in message for fragment in fragments), message


def test_an_explicit_error_repeats_the_default():
    class ExplicitSchema(dy.Schema):
        amount = dy.Int64(
            min=1, metadata={"dagster_dataframely": {"severity": "error"}}
        )

    assert _resolve_severities(ExplicitSchema) == {
        "amount|nullability": "error",
        "amount|min": "error",
    }
