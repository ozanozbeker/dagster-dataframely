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

import datetime as dt
import decimal
import re
from pathlib import Path
from typing import Any

import dagster as dg
import dataframely as dy
import pandas as pd
import pandera as pa
import patito as pt
import polars as pl
from dagster._core.definitions.metadata.metadata_value import ObjectMetadataValue
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
    def cancelled_orders_are_zero_amount(self) -> pl.Expr:
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


# ---------------------------------------------------------------------------
# A richer schema, used only by the groups below. It deliberately contains one
# instance of every case that makes rule text hard:
#
#   - a long regex, which cannot fit a constraint pill
#   - a @dy.rule() WITH a docstring and one WITHOUT
#   - a named check={...} and an anonymous check=lambda
#   - is_in with several values
#   - a composite primary key
# ---------------------------------------------------------------------------


class OrdersRich(dy.Schema):
    """A customer order, with every hard-to-render rule shape."""

    order_id = dy.String(primary_key=True, description="Merchant-facing order id.")
    customer_id = dy.Integer(primary_key=True, description="FK to customers.")
    email = dy.String(
        regex=r"^[^@]+@[^@]+\.[a-z]{2,}$",
        max_length=254,
        description="Contact email at time of order.",
        metadata={"pii": "true"},
    )
    amount = dy.Float64(
        min=0, max=10_000, description="Order total, USD.", metadata={"unit": "USD"}
    )
    priority = dy.Integer(is_in=[1, 2, 3], nullable=True, description="1 = highest.")
    sku = dy.String(
        min_length=8,
        max_length=8,
        unique=True,
        check={"upper_only": lambda col: col == col.str.to_uppercase()},
        description="Stock keeping unit.",
    )
    region = dy.String(
        check=lambda col: col.str.len_bytes() == 2, description="ISO-3166-2 region."
    )

    @dy.rule()
    def cancelled_orders_are_zero_amount(self) -> pl.Expr:
        """A cancelled order must carry a zero amount."""
        return (pl.col("amount") >= 0) | (pl.col("priority") == 1)

    @dy.rule()
    def priority_orders_ship_domestic(self) -> pl.Expr:
        return (pl.col("priority") != 1) | (pl.col("region") == "US")


PILL_CAP = 26


def _truncated(text: str) -> str:
    """What the Columns tab actually shows before you hover."""
    return text if len(text) <= PILL_CAP else text[: PILL_CAP - 1] + "…"


def _rule_docstring(schema: type[dy.Schema], rule_name: str) -> str | None:
    factory = getattr(schema, rule_name, None)
    doc = getattr(getattr(factory, "validation_fn", None), "__doc__", None)
    return doc.strip().splitlines()[0] if doc else None


def render_unified(schema: type[dy.Schema], rule_name: str) -> str:
    """One renderer, one fallback ladder: constraint -> docstring -> rule name.

    The same string is used on every surface; the check surface prefixes the
    column name, because a bare '>= 0' means nothing in a flat list.
    """
    if rule_name == "primary_key":
        keys = ", ".join(n for n, c in schema.columns().items() if c.primary_key)
        return f"primary key ({keys})"
    if "|" not in rule_name:
        return _rule_docstring(schema, rule_name) or rule_name

    name, kind = rule_name.split("|", 1)
    col = schema.columns()[name]
    match kind:
        case "nullability":
            return "not null"
        case "unique":
            return "unique"
        case "min":
            return f">= {col.min}"
        case "max":
            return f"<= {col.max}"
        case "min_exclusive":
            return f"> {col.min_exclusive}"
        case "max_exclusive":
            return f"< {col.max_exclusive}"
        case "min_length":
            return f"length >= {col.min_length}"
        case "max_length":
            return f"length <= {col.max_length}"
        case "regex":
            return f"matches {col.regex}"
        case "is_in":
            return "in (" + ", ".join(str(v) for v in col.is_in) + ")"
        case "check":
            return "custom check"
        case _ if kind.startswith("check__"):
            return kind.removeprefix("check__")
        case _:
            return kind


def render_pill_terse(schema: type[dy.Schema], rule_name: str) -> str:
    """Per-surface variant: the pill deliberately degrades to fit PILL_CAP.

    Anything whose value cannot fit is named rather than shown, and the value
    moves to the asset check, which has room for it.
    """
    if "|" in rule_name:
        name, kind = rule_name.split("|", 1)
        col = schema.columns()[name]
        match kind:
            case "regex":
                return "matches regex"
            case "is_in":
                return f"in {len(col.is_in)} values"
            case "check":
                return "custom check"
            case _ if kind.startswith("check__"):
                return "check: " + kind.removeprefix("check__")
    return render_unified(schema, rule_name)


def render_check_description(
    schema: type[dy.Schema], rule_name: str, *, prose: bool
) -> str:
    """What goes in AssetCheckSpec(description=...)."""
    if "|" not in rule_name:
        return render_unified(schema, rule_name)
    name, _ = rule_name.split("|", 1)
    body = render_unified(schema, rule_name)
    if not prose:
        return f"{name} {body}"
    return f"Every row must satisfy: {name} {body}."


def ruletext_table_schema(schema: type[dy.Schema], *, terse_pills: bool) -> TableSchema:
    render_pill = render_pill_terse if terse_pills else render_unified
    columns = []
    for name, column in schema.columns().items():
        other = [
            render_pill(schema, f"{name}|{rule}")
            for rule in column.validation_rules(pl.col(name))
            if rule not in FIRST_CLASS_RULES
        ]
        if column.primary_key:
            other.insert(0, "primary key")
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
    table_level = [
        render_pill(schema, rule)
        for rule in schema._validation_rules(with_cast=False)  # noqa: SLF001
        if "|" not in rule
    ]
    return TableSchema(columns=columns, constraints=TableConstraints(other=table_level))


RICH_RULES = list(OrdersRich._validation_rules(with_cast=False))  # noqa: SLF001
RICH_EXPRS = OrdersRich._validation_rules(with_cast=False)  # noqa: SLF001


def rich_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "order_id": ["A-1", "A-2", "A-3"],
            "customer_id": [1, 2, 3],
            "email": ["a@example.com", "b@example.com", "c@example.com"],
            "amount": [12.5, 0.0, 99.0],
            "priority": [1, 2, None],
            "sku": ["ABCD-123", "EFGH-456", "IJKL-789"],
            "region": ["US", "CA", "US"],
        }
    )


def _ruletext_asset(
    name: str, *, terse_pills: bool, prose_descriptions: bool, note: str
) -> dg.AssetsDefinition:
    check_specs = [
        dg.AssetCheckSpec(
            name=f"dy_rule__{rule.replace('|', '__')}",
            asset=name,
            description=render_check_description(
                OrdersRich, rule, prose=prose_descriptions
            ),
            blocking=False,
        )
        for rule in RICH_RULES
    ]

    @dg.multi_asset(
        outs={
            name: dg.AssetOut(
                io_manager_key="fs_io_manager",
                metadata={
                    "dagster/column_schema": ruletext_table_schema(
                        OrdersRich, terse_pills=terse_pills
                    )
                },
                description=note,
            )
        },
        group_name="e_ruletext",
        check_specs=check_specs,
        name=name,
    )
    def _asset():
        yield dg.Output(rich_frame(), output_name=name)
        for rule in RICH_RULES:
            yield dg.AssetCheckResult(
                check_name=f"dy_rule__{rule.replace('|', '__')}",
                asset_key=dg.AssetKey(name),
                passed=rule != "email|regex",
                severity=dg.AssetCheckSeverity.WARN,
                metadata={
                    "dy_rule": rule,
                    "dy_rule__expr": str(RICH_EXPRS[rule].expr),
                    "rows failing": 1 if rule == "email|regex" else 0,
                },
            )

    return _asset


ruletext_unified = _ruletext_asset(
    "ruletext_unified",
    terse_pills=False,
    prose_descriptions=False,
    note=(
        "ONE RENDERER. The same string on every surface; the check list adds a "
        "column-name prefix. Watch email's regex pill truncate, and watch "
        "dy_rule__priority_orders_ship_domestic fall back to its own name because "
        "it has no docstring."
    ),
)

ruletext_per_surface = _ruletext_asset(
    "ruletext_per_surface",
    terse_pills=True,
    prose_descriptions=True,
    note=(
        "PER-SURFACE. Pills degrade on purpose to stay under the truncation cap "
        "('matches regex', 'in 3 values'); the value moves to the check description, "
        "which is also written as a sentence. Compare the Columns tab and the Checks "
        "tab against ruletext_unified."
    ),
)


# ---------------------------------------------------------------------------
# f_primarykey - dataframely models the PK as ONE schema-level rule
# (`as_struct("order_id","customer_id").is_unique()`), even for a single column.
# Dagster has no first-class PK concept: only nullable / unique / other.
# ---------------------------------------------------------------------------


def pk_table_schema(schema: type[dy.Schema], style: str) -> TableSchema:
    keys = [n for n, c in schema.columns().items() if c.primary_key]
    columns = []
    for name, column in schema.columns().items():
        other = []
        if column.primary_key and style in ("column", "both"):
            other.append("primary key")
        if column.primary_key and style in ("column_composite", "conditional"):
            other.append("primary key")
        # `unique` is TRUE only when the column itself is declared unique. A column
        # in a COMPOSITE primary key is not unique on its own - dataframely checks
        # `as_struct(...).is_unique()`, and {"a": ["x","x","y"], "b": [1,2,1]} passes.
        unique = column.unique if style == "top" else (column.unique or column.primary_key)
        columns.append(
            TableColumn(
                name=name,
                type=str(column.dtype),
                description=column.description,
                constraints=TableColumnConstraints(
                    nullable=column.nullable,
                    unique=unique,
                    other=other,
                ),
            )
        )
    table_other = []
    if style in ("table", "both"):
        table_other.append("primary key (" + ", ".join(keys) + ")")
    # The conditional rule: the table-level row appears only when it has something
    # the per-column pills cannot say, i.e. that the key spans more than one column.
    if style == "conditional" and len(keys) > 1:
        table_other.append("composite primary key (" + ", ".join(keys) + ")")
    # `top`: the key is stated ONCE, table-level, and nowhere else. PK columns carry
    # no pill - they already read `not null` (dataframely forbids a nullable PK), and
    # `unique` is deliberately NOT claimed for them.
    if style == "top":
        table_other.append("PK: " + ", ".join(keys))
    return TableSchema(columns=columns, constraints=TableConstraints(other=table_other))


class OrdersSinglePK(dy.Schema):
    """The same shape, but with a single-column primary key."""

    order_id = dy.String(primary_key=True, description="Merchant-facing order id.")
    customer_id = dy.Integer(description="FK to customers.")
    amount = dy.Float64(min=0, max=10_000, description="Order total, USD.")


def _pk_asset(
    style: str, note: str, schema: type[dy.Schema] = OrdersRich, suffix: str = ""
) -> dg.AssetsDefinition:
    @dg.asset(
        name=f"pk_{style}{suffix}",
        group_name="f_primarykey",
        io_manager_key="fs_io_manager",
        metadata={"dagster/column_schema": pk_table_schema(schema, style)},
        description=note,
    )
    def _asset() -> pl.DataFrame:
        return rich_frame().select(list(schema.columns()))

    return _asset


pk_table = _pk_asset(
    "table",
    "PK as a TABLE-LEVEL constraint only: 'primary key (order_id, customer_id)'. Mirrors how dataframely models it.",
)
pk_column = _pk_asset(
    "column",
    "PK as a per-column PILL only. Composite-ness is implied by two columns carrying it, never stated.",
)
pk_both = _pk_asset(
    "both", "PK on BOTH surfaces. Is the duplication redundant or reassuring?"
)
pk_conditional_composite = _pk_asset(
    "conditional",
    "THE CONDITIONAL RULE, composite branch. Every PK column gets a 'primary key' pill, "
    "AND the table-level row says 'composite primary key (order_id, customer_id)' because "
    "that is the one thing the pills cannot say.",
    suffix="_composite",
)
pk_top_composite = _pk_asset(
    "top",
    "THE KEY STATED ONCE, at the top: 'PK: order_id, customer_id'. No per-column pill. "
    "PK columns already read `not null` (dataframely forbids a nullable PK), and they are "
    "deliberately NOT marked `unique` - only the tuple is unique.",
    suffix="_composite",
)
pk_top_single = _pk_asset(
    "top",
    "Same rule, single-column key: 'PK: order_id'. Here the column IS genuinely unique, "
    "but the pill is still not drawn - the top row says it.",
    schema=OrdersSinglePK,
    suffix="_single",
)
pk_conditional_single = _pk_asset(
    "conditional",
    "THE CONDITIONAL RULE, single branch. One 'primary key' pill on order_id and NO "
    "table-level row at all - it would only restate the pill.",
    schema=OrdersSinglePK,
    suffix="_single",
)


# ---------------------------------------------------------------------------
# g_extras - the surfaces #14 added: the schema-carrier row, the kind badge,
# IO-manager facts, and the four markdown statistics tables.
# ---------------------------------------------------------------------------


def _md(df: pl.DataFrame) -> dg.MetadataValue:
    with pl.Config(
        tbl_formatting="ASCII_MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=-1,
    ):
        return dg.MetadataValue.md(str(df))


def _stats_tables(df: pl.DataFrame) -> dict[str, dg.MetadataValue]:
    numeric = df.select(pl.selectors.numeric())
    string = df.select(pl.selectors.string())
    out: dict[str, dg.MetadataValue] = {}
    if numeric.width:
        out["dy_stats__numeric"] = _md(
            pl.DataFrame(
                [
                    {
                        "variable": c,
                        "count": numeric[c].len(),
                        "null_count": numeric[c].null_count(),
                        "mean": numeric[c].mean(),
                        "std": numeric[c].std(),
                        "min": numeric[c].min(),
                        "p50": numeric[c].median(),
                        "max": numeric[c].max(),
                    }
                    for c in numeric.columns
                ]
            )
        )
    if string.width:
        out["dy_stats__string"] = _md(
            pl.DataFrame(
                [
                    {
                        "variable": c,
                        "count": string[c].len(),
                        "null_count": string[c].null_count(),
                        "n_unique": string[c].n_unique(),
                        "min_len": string[c].str.len_bytes().min(),
                        "max_len": string[c].str.len_bytes().max(),
                        "n_empty": int((string[c].str.len_bytes() == 0).sum()),
                    }
                    for c in string.columns
                ]
            )
        )
    out["dy_stats__temporal"] = _md(
        pl.DataFrame(
            {
                "variable": ["placed_at", "elapsed"],
                "count": [3, 3],
                "null_count": [0, 1],
                "min": ["2024-01-01 00:00:00", None],
                "max": ["2024-01-09 00:00:00", None],
                "span": ["8d", "1m 30s"],
            }
        )
    )
    out["dy_stats__boolean"] = _md(
        pl.DataFrame(
            {
                "variable": ["is_gift"],
                "count": [3],
                "null_count": [0],
                "n_true": [1],
                "n_false": [2],
                "true_rate": [0.333],
            }
        )
    )
    return out


# Three candidate key-naming schemes. `stats` (bare) is already taken by
# dagster-polars' get_polars_metadata, so an unprefixed `stats/...` sits directly
# beside a key another integration owns.
KEY_SCHEMES = {
    "bare": ("stats/", "", "dagster_dataframely/schema"),
    "dy": ("dy/stats/", "dy/", "dagster_dataframely/schema"),
    "full": (
        "dagster_dataframely/stats/",
        "dagster_dataframely/",
        "dagster_dataframely/schema",
    ),
}


def _extras_asset(scheme: str) -> dg.AssetsDefinition:
    stats_prefix, fact_prefix, schema_key = KEY_SCHEMES[scheme]

    @dg.asset(
        name=f"extras_keys_{scheme}",
        group_name="g_extras",
        io_manager_key="fs_io_manager",
        metadata={
            # The live schema carrier from #14, now via ObjectMetadataValue - the
            # supported API for this. Renders as the bare class name; .instance
            # reaches the IO manager on both write and read. Passing the raw class
            # instead is deprecated and renders '[SchemaMeta] (unserializable)'.
            schema_key: ObjectMetadataValue(
                OrdersRich.__name__, instance=OrdersRich
            ),
            "dagster/column_schema": ruletext_table_schema(
                OrdersRich, terse_pills=False
            ),
            "dagster/storage_kind": "parquet",
        },
        kinds={"polars", "parquet"},
        description=(
            f"KEY NAMING: {scheme!r}. Four stats tables plus the schema carrier and "
            "the IO-manager facts. Open the Metadata accordion and judge the width "
            "of the key column against the other two extras_keys_* assets."
        ),
    )
    def _asset(context) -> pl.DataFrame:
        df = rich_frame()
        context.add_output_metadata(
            {
                "dagster/row_count": df.height,
                "path": f"storage/fs/extras_keys_{scheme}",
                f"{fact_prefix}bytes_written": 4096,
                f"{fact_prefix}parquet/compression": "zstd",
                **{
                    stats_prefix + family.removeprefix("dy_stats__"): value
                    for family, value in _stats_tables(df).items()
                },
            }
        )
        return df

    return _asset


@dg.asset(
    group_name="g_extras",
    io_manager_key="fs_io_manager",
    description=(
        "MARKDOWN RENDERING. The same table five ways, to isolate why the last row "
        "renders with boxes around its values. The polars source is clean GFM with "
        "zero non-ASCII characters, so the cause is in Dagster's renderer."
    ),
)
def extras_md_variants(context) -> pl.DataFrame:
    df = rich_frame()
    stats = pl.DataFrame(
        [
            {
                "variable": c,
                "count": df[c].len(),
                "null_count": df[c].null_count(),
                "n_unique": df[c].n_unique(),
            }
            for c in df.columns
        ]
    )
    with pl.Config(
        tbl_formatting="ASCII_MARKDOWN",
        tbl_hide_column_data_types=True,
        tbl_hide_dataframe_shape=True,
        tbl_rows=-1,
    ):
        raw = str(stats)

    context.add_output_metadata(
        {
            # 1. exactly what the probe emits today
            "md/as_is": dg.MetadataValue.md(raw),
            # 2. terminate the final row
            "md/trailing_newline": dg.MetadataValue.md(raw + "\n"),
            # 3. isolate the table as its own block
            "md/blank_lines": dg.MetadataValue.md("\n" + raw + "\n\n"),
            # 4. a heading above it, forcing a block boundary
            "md/with_heading": dg.MetadataValue.md("#### stats\n\n" + raw + "\n"),
            # 5. not markdown at all - Dagster's own table renderer, for contrast
            "md/table_value": dg.MetadataValue.table(
                records=[
                    dg.TableRecord(dict(zip(stats.columns, row)))
                    for row in stats.iter_rows()
                ]
            ),
        }
    )
    return df


STATS_FRAME = pl.DataFrame(
    {
        "amount": [12.5, 0.0, 99.0, None],
        "price": [decimal.Decimal("1.25"), decimal.Decimal("3.5"), decimal.Decimal("99.999"), None],
        "priority": [1, 2, 3, 1],
        "region": ["US", "CA", "US", ""],
        "sku": ["ABCD-123", "EFGH-4567", "IJKL-89", "MNOP-0"],
        "placed_at": [
            dt.datetime(2024, 1, 1),
            dt.datetime(2024, 1, 5),
            dt.datetime(2024, 1, 9),
            None,
        ],
        "elapsed": [
            dt.timedelta(days=8),
            dt.timedelta(minutes=1, seconds=30),
            dt.timedelta(hours=2, minutes=5),
            None,
        ],
        "is_gift": [True, False, False, None],
    },
    schema_overrides={"price": pl.Decimal(10, 3)},
)


def _human_duration(micros: int | None) -> str | None:
    """polars' own repr style: '8d', '1m 30s', '2h 5m'. Not reachable via the API."""
    if micros is None:
        return None
    sign, micros = ("-" if micros < 0 else ""), abs(micros)
    days, rem = divmod(micros, 86_400_000_000)
    hours, rem = divmod(rem, 3_600_000_000)
    minutes, rem = divmod(rem, 60_000_000)
    seconds, micros = divmod(rem, 1_000_000)
    parts = [
        f"{v}{u}"
        for v, u in ((days, "d"), (hours, "h"), (minutes, "m"), (seconds, "s"))
        if v
    ]
    if micros:
        parts.append(f"{micros}µs")
    return sign + (" ".join(parts[:2]) if parts else "0s")


def _cell(v: Any) -> Any:
    """Coerce a numeric stat to a TableRecord-legal cell, WITHOUT rounding.

    Decimal is the reason this is not optional: pl.selectors.numeric() picks up
    Decimal columns, Series.min()/max() return decimal.Decimal, and TableRecord
    rejects it. (Series.mean() already coerces to float; min/max do not.)

    Used for min/max/p25/p75 - values that exist in the data, so they are shown
    as they are. A very high-precision Decimal loses digits through float(); that
    is display-only and the exact value is still in the table.
    """
    if v is None or isinstance(v, bool | int):
        return v
    return float(v)


def _rounded(v: Any) -> Any:
    """As _cell, but rounded to <=4 dp. Only for COMPUTED statistics - mean, std,
    median, true_rate - which are derived, not values anyone stored.
    """
    c = _cell(v)
    return round(c, 4) if isinstance(c, float) else c


def _records(rows: list[dict]) -> dg.MetadataValue:
    return dg.MetadataValue.table(records=[dg.TableRecord(r) for r in rows])


def _native_stats(df: pl.DataFrame, *, human_duration: bool) -> dict[str, Any]:
    """Typed polars expressions throughout; strings only at the final display step."""
    out: dict[str, Any] = {}

    numeric = df.select(pl.selectors.numeric())
    if numeric.width:
        out["stats/numeric"] = _records(
            [
                {
                    "variable": c,
                    "count": numeric[c].len(),
                    "null_count": numeric[c].null_count(),
                    "mean": _rounded(numeric[c].mean()),
                    "std": _rounded(numeric[c].std()),
                    "min": _cell(numeric[c].min()),
                    "p50": _rounded(numeric[c].median()),
                    "max": _cell(numeric[c].max()),
                }
                for c in numeric.columns
            ]
        )

    string = df.select(pl.selectors.string())
    if string.width:
        out["stats/string"] = _records(
            [
                {
                    "variable": c,
                    "count": string[c].len(),
                    "null_count": string[c].null_count(),
                    "n_unique": string[c].n_unique(),
                    "min_len": string[c].str.len_bytes().min(),
                    "max_len": string[c].str.len_bytes().max(),
                    "n_empty": int((string[c].str.len_bytes() == 0).sum()),
                }
                for c in string.columns
            ]
        )

    temporal = df.select(pl.selectors.temporal())
    if temporal.width:
        rows = []
        for c in temporal.columns:
            s = temporal[c]
            lo, hi = s.min(), s.max()
            span = (hi - lo) if (lo is not None and hi is not None) else None
            def fmt(v: Any, *, dtype: Any = s.dtype) -> str | None:
                # min()/max() skip nulls, so v is None only for an all-null column.
                if v is None:
                    return None
                if dtype != pl.Duration:
                    return str(v)
                if human_duration:
                    return _human_duration(int(v.total_seconds() * 1_000_000))
                return pl.Series([v]).dt.to_string()[0]
            rows.append(
                {
                    "variable": c,
                    "count": s.len(),
                    "null_count": s.null_count(),
                    "min": fmt(lo),
                    "max": fmt(hi),
                    "span": (
                        _human_duration(int(span.total_seconds() * 1_000_000))
                        if (span is not None and human_duration)
                        else (None if span is None else str(span))
                    ),
                }
            )
        out["stats/temporal"] = _records(rows)

    boolean = df.select(pl.selectors.boolean())
    if boolean.width:
        out["stats/boolean"] = _records(
            [
                {
                    "variable": c,
                    "count": boolean[c].len(),
                    "null_count": boolean[c].null_count(),
                    "n_true": int(boolean[c].sum() or 0),
                    "n_false": int((~boolean[c]).sum() or 0),
                    "true_rate": _rounded(boolean[c].mean()),
                }
                for c in boolean.columns
            ]
        )
    return out


def _native_stats_asset(name: str, *, human_duration: bool, note: str):
    @dg.asset(name=name, group_name="g_extras", io_manager_key="fs_io_manager",
              description=note)
    def _asset(context) -> pl.DataFrame:
        context.add_output_metadata(
            {
                "dagster/row_count": STATS_FRAME.height,
                **_native_stats(STATS_FRAME, human_duration=human_duration),
            }
        )
        return STATS_FRAME

    return _asset


extras_stats_iso = _native_stats_asset(
    "extras_stats_iso",
    human_duration=False,
    note=(
        "ALL FOUR FAMILIES as MetadataValue.table. Durations via polars' own "
        "dt.to_string(): ISO-8601, 'P8D' / 'PT1M30S'. One rule, null-safe, no code "
        "of ours to test."
    ),
)
extras_stats_human = _native_stats_asset(
    "extras_stats_human",
    human_duration=True,
    note=(
        "IDENTICAL, except durations are formatted by us in polars' repr style: "
        "'8d' / '1m 30s' / '2h 5m'. Friendlier, but it is our code and our tests. "
        "Compare the temporal table against extras_stats_iso."
    ),
)


extras_keys_bare = _extras_asset("bare")
extras_keys_dy = _extras_asset("dy")
extras_keys_full = _extras_asset("full")


@dg.asset(
    group_name="g_extras",
    io_manager_key="fs_io_manager",
    metadata={
        "dagster/column_schema": ruletext_table_schema(OrdersRich, terse_pills=False)
    },
    description=(
        "ONE MERGED STATS TABLE instead of four. Same numbers, one accordion row, "
        "but the columns are heterogeneous by construction (mean beside min_len). "
        "Compare density against extras_four_stats_tables."
    ),
)
def extras_one_stats_table(context) -> pl.DataFrame:
    df = rich_frame()
    context.add_output_metadata(
        {
            "dagster/row_count": df.height,
            "dy_stats": _md(
                pl.DataFrame(
                    [
                        {
                            "variable": c,
                            "family": "numeric"
                            if df[c].dtype.is_numeric()
                            else "string",
                            "count": df[c].len(),
                            "null_count": df[c].null_count(),
                            "mean": str(df[c].mean())
                            if df[c].dtype.is_numeric()
                            else None,
                            "n_unique": str(df[c].n_unique())
                            if not df[c].dtype.is_numeric()
                            else None,
                        }
                        for c in df.columns
                    ]
                )
            ),
        }
    )
    return df


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
        ruletext_unified,
        ruletext_per_surface,
        pk_table,
        pk_column,
        pk_both,
        pk_conditional_composite,
        pk_conditional_single,
        pk_top_composite,
        pk_top_single,
        extras_md_variants,
        extras_stats_iso,
        extras_stats_human,
        extras_keys_bare,
        extras_keys_dy,
        extras_keys_full,
        extras_one_stats_table,
    ],
    resources={
        "fs_io_manager": dg.FilesystemIOManager(base_dir=str(STORAGE / "fs")),
        "polars_io_manager": PolarsParquetIOManager(base_dir=str(STORAGE / "polars")),
    },
)
