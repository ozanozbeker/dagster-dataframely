"""Throwaway UI probe for wayfinder ticket #10.

Renders the same logical schema through every candidate mechanism so a human can
compare Dagster UI surfaces side by side. Nothing here is package design; it is a
set of controlled specimens.

Groups (asset `group_name`, alphabetical so they sort in the UI):
  a_prior_art  - what dagster-pandera and dagster-polars[patito] actually look like
  b_bucket     - the same dataframely schema in definition / materialization / both
  c_pills      - three candidate renderings of dataframely rules as constraint pills
  d_checks     - per-rule asset checks + a quarantine sibling asset
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any

import dagster as dg
import dataframely as dy
import pandas as pd
import pandera as pa
import patito as pt
import polars as pl
from dagster._core.definitions.metadata.table import (
    TableColumn,
    TableColumnConstraints,
    TableConstraints,
    TableSchema,
)
from dagster_pandera import pandera_schema_to_dagster_type
from dagster_polars import PolarsParquetIOManager
from dagster_polars.patito import patito_model_to_dagster_type

BASE = Path(__file__).parent
STORAGE = BASE / "storage"
STORAGE.mkdir(exist_ok=True)


# ---------------------------------------------------------------------------
# The specimen schema, expressed once per library.
#
# Mixed-case column names (OrderID, CustomerID) are deliberate: research #5
# found the Columns view lowercases names whenever BOTH buckets carry a schema.
# ---------------------------------------------------------------------------


class OrdersSchema(dy.Schema):
    """A customer order, as dataframely sees it."""

    OrderID = dy.String(
        primary_key=True,
        description="Merchant-facing order identifier.",
    )
    CustomerID = dy.Integer(
        nullable=False,
        min=1,
        description="Foreign key to customers.CustomerID.",
        metadata={"pii": "false", "owner": "growth-team"},
    )
    amount = dy.Float64(
        nullable=False,
        min=0,
        max=10_000,
        description="Order total, in USD.",
        metadata={"unit": "USD", "owner": "finance"},
    )
    email = dy.String(
        nullable=True,
        regex=r"^[^@]+@[^@]+\.[a-z]{2,}$",
        description="Contact email at time of order.",
        metadata={"pii": "true"},
    )
    status = dy.String(
        nullable=False,
        check={
            "is_a_known_status": lambda col: col.is_in(
                ["placed", "shipped", "cancelled"]
            )
        },
        description="Fulfilment status.",
    )

    @dy.rule()
    def cancelled_orders_are_zero_amount() -> pl.Expr:
        return (pl.col("status") != "cancelled") | (pl.col("amount") == 0)


ORDERS_PANDERA = pa.DataFrameSchema(
    name="OrdersSchema",
    columns={
        "OrderID": pa.Column(
            str, unique=True, description="Merchant-facing order identifier."
        ),
        "CustomerID": pa.Column(
            int,
            pa.Check.greater_than_or_equal_to(1),
            description="Foreign key to customers.",
        ),
        "amount": pa.Column(
            float,
            [
                pa.Check.greater_than_or_equal_to(0),
                pa.Check.less_than_or_equal_to(10_000),
            ],
            description="Order total, in USD.",
        ),
        "email": pa.Column(
            str, nullable=True, description="Contact email at time of order."
        ),
        "status": pa.Column(
            str,
            pa.Check.isin(["placed", "shipped", "cancelled"]),
            description="Fulfilment status.",
        ),
    },
    checks=[pa.Check(lambda df: len(df) > 0, description="Table is non-empty.")],
)


class OrdersPatito(pt.Model):
    """A customer order, as patito sees it."""

    OrderID: str = pt.Field(
        unique=True, description="Merchant-facing order identifier."
    )
    CustomerID: int = pt.Field(ge=1, description="Foreign key to customers.")
    amount: float = pt.Field(ge=0, le=10_000, description="Order total, in USD.")
    email: str | None = pt.Field(
        default=None, description="Contact email at time of order."
    )
    status: str = pt.Field(
        constraints=[pl.col("status").is_in(["placed", "shipped", "cancelled"])],
        description="Fulfilment status.",
    )


# ---------------------------------------------------------------------------
# Candidate translation: dataframely Schema -> Dagster TableSchema.
#
# `pill_style` is the live question in group c: what string, if any, goes in
# TableColumnConstraints.other. The UI truncates each at 30 chars.
# ---------------------------------------------------------------------------

# Rules dataframely emits per column that Dagster already models first-class,
# so they must not be duplicated into `other`.
FIRST_CLASS_RULES = {"nullability", "unique"}


def _human_constraint(rule_name: str, column: dy.Column) -> str:
    """Pandera-style operator strings, derived from the column's own attributes."""
    match rule_name:
        case "min":
            return f">= {getattr(column, 'min', '?')}"
        case "max":
            return f"<= {getattr(column, 'max', '?')}"
        case "min_exclusive":
            return f"> {getattr(column, 'min_exclusive', '?')}"
        case "max_exclusive":
            return f"< {getattr(column, 'max_exclusive', '?')}"
        case "regex":
            return f"matches {getattr(column, 'regex', '?')}"
        case "is_in":
            return f"in {getattr(column, 'is_in', '?')}"
        case "inf":
            return "not infinite"
        case "nan":
            return "not NaN"
        case "primary_key":
            return "primary key"
        case _:
            return rule_name


def dy_schema_to_table_schema(
    schema: type[dy.Schema],
    pill_style: str = "rule_names",
) -> TableSchema:
    """Translate a dataframely Schema into a Dagster TableSchema.

    pill_style:
      "rule_names" - `dy_rule__amount__min`, the same string as the asset check (#8/#9)
      "human"      - `>= 0`, the shape dagster-pandera ships
      "raw_expr"   - the polars expression repr, to exercise the 30-char truncation
      "none"       - nullable/unique only, the shape dagster-polars[patito] ships
    """
    columns: list[TableColumn] = []

    for name, column in schema.columns().items():
        rule_names = [
            rule
            for rule in column.validation_rules(pl.col(name))
            if rule not in FIRST_CLASS_RULES
        ]

        match pill_style:
            case "none":
                other = []
            case "rule_names":
                other = [f"dy_rule__{name}__{rule}" for rule in rule_names]
            case "human":
                other = [_human_constraint(rule, column) for rule in rule_names]
            case "raw_expr":
                exprs = column.validation_rules(pl.col(name))
                other = [str(exprs[rule]) for rule in rule_names]
            case _:
                raise ValueError(pill_style)

        columns.append(
            TableColumn(
                name=name,
                type=str(column.dtype),
                description=column.description,
                constraints=TableColumnConstraints(
                    nullable=column.nullable,
                    unique=column.unique or column.primary_key,
                    other=other,
                ),
                tags={k: str(v) for k, v in (column.metadata or {}).items()},
            )
        )

    # Schema-level rules (@dy.rule and the composite primary key) have no column
    # to hang off, so they become table-level constraints.
    schema_level = [
        r for r in schema._validation_rules(with_cast=False) if "|" not in r
    ]
    match pill_style:
        case "none":
            table_other = []
        case "rule_names":
            table_other = [f"dy_rule__{r}" for r in schema_level]
        case _:
            table_other = [
                _human_constraint(r, None) if r == "primary_key" else r
                for r in schema_level
            ]  # type: ignore[arg-type]

    return TableSchema(columns=columns, constraints=TableConstraints(other=table_other))


def quarantine_table_schema(schema: type[dy.Schema]) -> TableSchema:
    """The package-owned quarantine frame from #9: mirrored columns + one String
    rule column per rule, named identically to the asset check.
    """
    mirrored = [
        TableColumn(
            name=name,
            type=str(column.dtype),
            description=column.description,
            # No constraints: these rows are here precisely because they violate them.
            constraints=TableColumnConstraints(nullable=True),
            tags={k: str(v) for k, v in (column.metadata or {}).items()},
        )
        for name, column in schema.columns().items()
    ]
    rule_columns = [
        TableColumn(
            name=f"dy_rule__{rule.replace('|', '__')}",
            type="String",
            description=f"Outcome of rule {rule!r}: 'valid' / 'invalid' / 'unknown'.",
        )
        for rule in schema._validation_rules(with_cast=False)  # noqa: SLF001
    ]
    return TableSchema(columns=mirrored + rule_columns)


def sample_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "OrderID": ["A-1", "A-2", "A-3", "A-4"],
            "CustomerID": [1, 2, 3, 4],
            "amount": [12.5, 0.0, 99.0, 7.25],
            "email": ["a@example.com", None, "c@example.com", "d@example.com"],
            "status": ["placed", "cancelled", "shipped", "placed"],
        }
    )


DEFINITION_SCHEMA = dy_schema_to_table_schema(OrdersSchema, "rule_names")


# ---------------------------------------------------------------------------
# a_prior_art
# ---------------------------------------------------------------------------


@dg.asset(
    group_name="a_prior_art",
    dagster_type=pandera_schema_to_dagster_type(ORDERS_PANDERA),
    io_manager_key="fs_io_manager",
    description="dagster-pandera: schema travels as a DagsterType. Look at the Lineage sidebar Type accordion.",
)
def pandera_orders() -> pd.DataFrame:
    return sample_frame().to_pandas()


@dg.asset(
    group_name="a_prior_art",
    dagster_type=patito_model_to_dagster_type(OrdersPatito),
    io_manager_key="polars_io_manager",
    description="dagster-polars[patito]: DagsterType metadata AND a dagster-polars materialization schema. The archive's 'both' world.",
)
def patito_orders() -> pt.DataFrame[OrdersPatito]:
    return pt.DataFrame[OrdersPatito](sample_frame().to_pandas())


# ---------------------------------------------------------------------------
# b_bucket - the Q1/Q2 comparison
# ---------------------------------------------------------------------------


@dg.asset(
    group_name="b_bucket",
    io_manager_key="fs_io_manager",
    metadata={"dagster/column_schema": DEFINITION_SCHEMA},
    description="DEFINITION bucket only. The Q1 answer. Note: visible in the Columns tab even before it has ever run.",
)
def bucket_definition_only() -> pl.DataFrame:
    return sample_frame()


@dg.asset(
    group_name="b_bucket",
    io_manager_key="fs_io_manager",
    description="MATERIALIZATION bucket only, via context.add_asset_metadata(). Nothing in the UI until it runs.",
)
def bucket_materialization_only(context) -> pl.DataFrame:
    context.add_asset_metadata({"dagster/column_schema": DEFINITION_SCHEMA})
    return sample_frame()


@dg.asset(
    group_name="b_bucket",
    io_manager_key="fs_io_manager",
    metadata={"dagster/column_schema": DEFINITION_SCHEMA},
    description="BOTH buckets, identical schema. Watch the column names: OrderID/CustomerID should render lowercased.",
)
def bucket_both(context) -> pl.DataFrame:
    context.add_asset_metadata({"dagster/column_schema": DEFINITION_SCHEMA})
    return sample_frame()


@dg.asset(
    group_name="b_bucket",
    io_manager_key="polars_io_manager",
    metadata={"dagster/column_schema": DEFINITION_SCHEMA},
    description=(
        "THE REALISTIC CASE. Our definition schema + a real dagster-polars IO manager, "
        "which emits its own constraint-free materialization schema. This is what a user "
        "pairing this package with dagster-polars actually gets."
    ),
)
def bucket_defn_plus_polars_iom() -> pl.DataFrame:
    return sample_frame()


# ---------------------------------------------------------------------------
# c_pills - what goes in TableColumnConstraints.other
# ---------------------------------------------------------------------------


def _pill_asset(style: str, note: str) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"pills_{style}",
        group_name="c_pills",
        io_manager_key="fs_io_manager",
        metadata={
            "dagster/column_schema": dy_schema_to_table_schema(OrdersSchema, style)
        },
        description=note,
    )
    def _asset() -> pl.DataFrame:
        return sample_frame()

    return _asset


pills_rule_names = _pill_asset(
    "rule_names",
    "Pills are the asset-check names: dy_rule__amount__min. Same string in the check list (#9 symmetry).",
)
pills_human = _pill_asset(
    "human",
    "Pills read as operators: '>= 0', '<= 10000'. The shape dagster-pandera ships.",
)
pills_raw_expr = _pill_asset(
    "raw_expr",
    "Pills are raw polars expressions. Exercises the 30-char truncation (MAX_CONSTRAINT_TAG_CHARS).",
)
pills_none = _pill_asset(
    "none",
    "No pills at all - nullable/unique only. The shape dagster-polars[patito] ships.",
)


# ---------------------------------------------------------------------------
# d_checks - per-rule checks (#8) + quarantine sibling (#9)
# ---------------------------------------------------------------------------

RULE_TO_CHECK = {
    rule: f"dy_rule__{rule.replace('|', '__')}"
    for rule in OrdersSchema._validation_rules(with_cast=False)  # noqa: SLF001
}

CHECK_SPECS = [
    dg.AssetCheckSpec(
        name=check_name,
        asset="orders",
        description=f"dataframely rule {rule!r}.",
        blocking=False,
    )
    for rule, check_name in RULE_TO_CHECK.items()
] + [
    dg.AssetCheckSpec(
        name="dy_schema__dtypes",
        asset="orders",
        description="Declared dtypes and columns are present, per #7. Blocking; aborts before any row check.",
        blocking=True,
    )
]


@dg.multi_asset(
    group_name="d_checks",
    outs={
        "orders": dg.AssetOut(
            io_manager_key="fs_io_manager",
            metadata={"dagster/column_schema": DEFINITION_SCHEMA},
            description="The good half. Per-rule checks below; two deliberately fail with WARN.",
        ),
        "orders_quarantine": dg.AssetOut(
            key=dg.AssetKey(["orders_quarantine"]),
            io_manager_key="fs_io_manager",
            metadata={"dagster/column_schema": quarantine_table_schema(OrdersSchema)},
            description=(
                "The quarantine half from #9: mirrored user columns plus one String "
                "dy_rule__* column per rule. Compare these column names against the check names."
            ),
        ),
    },
    check_specs=CHECK_SPECS,
)
def orders_with_quarantine():
    good = sample_frame()

    # A hand-built stand-in for FailureInfo.details(): original columns + one
    # String column per rule.
    bad = (
        pl.DataFrame(
            {
                "OrderID": ["A-9", "A-10"],
                "CustomerID": [0, 11],
                "amount": [-3.0, 25_000.0],
                "email": ["not-an-email", "z@example.com"],
                "status": ["placed", "shipped"],
            }
        )
        .with_columns(
            [pl.lit("valid").alias(check_name) for check_name in RULE_TO_CHECK.values()]
        )
        .with_columns(
            pl.lit("invalid").alias("dy_rule__CustomerID__min"),
            pl.lit("invalid").alias("dy_rule__amount__min"),
            pl.lit("invalid").alias("dy_rule__email__regex"),
        )
    )

    yield dg.Output(good, output_name="orders")
    yield dg.Output(bad, output_name="orders_quarantine")

    yield dg.AssetCheckResult(
        check_name="dy_schema__dtypes",
        asset_key=dg.AssetKey("orders"),
        passed=True,
        metadata={"columns_checked": len(OrdersSchema.columns())},
    )

    failing = {
        "dy_rule__CustomerID__min": 1,
        "dy_rule__amount__min": 1,
        "dy_rule__email__regex": 1,
    }
    for rule, check_name in RULE_TO_CHECK.items():
        failures = failing.get(check_name, 0)
        yield dg.AssetCheckResult(
            check_name=check_name,
            asset_key=dg.AssetKey("orders"),
            passed=failures == 0,
            severity=dg.AssetCheckSeverity.WARN,
            metadata={
                "dataframely rule": rule,
                "rows failing": failures,
                "quarantine column": f"{check_name} == 'invalid'",
            },
        )


defs = dg.Definitions(
    assets=[
        pandera_orders,
        patito_orders,
        bucket_definition_only,
        bucket_materialization_only,
        bucket_both,
        bucket_defn_plus_polars_iom,
        pills_rule_names,
        pills_human,
        pills_raw_expr,
        pills_none,
        orders_with_quarantine,
    ],
    resources={
        "fs_io_manager": dg.FilesystemIOManager(base_dir=str(STORAGE / "fs")),
        "polars_io_manager": PolarsParquetIOManager(base_dir=str(STORAGE / "polars")),
    },
)
