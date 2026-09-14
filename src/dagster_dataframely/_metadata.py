"""What the asset definition declares about its data, before it has run.

The asset body owns what the data is. The IO manager owns where and how it was written. A schema says what the data is, so it lives here and no IO manager has to emit it.
"""

import dagster as dg
import dataframely as dy

from dagster_dataframely._rendering import column_constraints, table_constraints
from dagster_dataframely._rules import described_rules, validate_namespace

_COLUMN_SCHEMA_KEY = "dagster/column_schema"


def _tags(column: dy.Column) -> dict[str, str] | None:
    """Render a column's free-form metadata as Dagster tags.

    Values are stringified because `TableColumn.tags` is `Mapping[str, str]` and Dagster rejects anything else at definition time. That is a display rendering, not a cast: no data is touched. Refusing would mean a `metadata={"pii": False}`, which Dataframely permits, could not be attached to an asset at all.
    """
    if not column.metadata:
        return None
    return {key: str(value) for key, value in column.metadata.items()}


def table_schema(schema: type[dy.Schema]) -> dg.TableSchema:
    """Project a schema onto Dagster's Columns tab.

    Dtype, description, tags, and every constraint the schema declares. Nullability and uniqueness go in Dagster's own two fields, the rest as constraints, and the primary key once at table level.

    `unique` is read from the column's own flag and never derived from `primary_key`. Dataframely keeps the two independent: a key member gets a composite `as_struct(...).is_unique()` rule and `column.unique` stays `False`. Deriving would claim a per-column uniqueness that nothing enforces.

    Tags come from `Column.metadata`, which Dataframely stores and never reads. It is the one Dataframely attribute with no other home here, and Dagster's column tags exist for free-form key/value annotation.

    Returns
    -------
    A table schema whose columns are in the schema's own order.

    Raises
    ------
    ReservedColumnError
        A user column sits inside the reserved namespace.
    UnnameableColumnError
        A user column is spelled in characters Dagster refuses in a name.
    CheckNameCollisionError
        Two rules rewrite to the same check name.
    """
    validate_namespace(schema)
    constraints: dict[str, list[str]] = column_constraints(schema)
    return dg.TableSchema(
        columns=[
            dg.TableColumn(
                name=name,
                type=str(column.dtype),
                description=column.description,
                constraints=dg.TableColumnConstraints(
                    nullable=column.nullable,
                    unique=column.unique,
                    other=constraints[name],
                ),
                tags=_tags(column),
            )
            for name, column in schema.columns().items()
        ],
        constraints=dg.TableConstraints(other=table_constraints(schema)),
    )


def _quarantine_table_schema(schema: type[dy.Schema]) -> dg.TableSchema:
    """Project the quarantine's column schema onto its own Columns tab.

    No constraints: these rows are here because they fail them, so a `not null` on a column full of nulls would state something false about every row, and a duplicate key is exactly what ends up here.

    Its own function rather than a flag on `table_schema`. The two comprehensions read alike, but every constraint the other one carries is a claim this table cannot make.

    The rule columns are `String` rather than the `Enum` Dataframely produces, because the cast happens before the write.

    Returns
    -------
    A table schema: the schema's columns in their own order, then the rules in theirs.
    """
    return dg.TableSchema(
        columns=[
            dg.TableColumn(
                name=name,
                type=str(column.dtype),
                description=column.description,
                tags=_tags(column),
            )
            for name, column in schema.columns().items()
        ]
        + [
            dg.TableColumn(
                name=rule.check_name,
                type="String",
                description=(
                    f"Whether the row is 'valid', 'invalid' or 'unknown' under rule '{rule.name}'."
                ),
            )
            for rule in described_rules(schema).values()
        ]
    )


def schema_metadata(schema: type[dy.Schema]) -> dict[str, dg.TableSchema]:
    """Build the definition metadata a schema-backed asset declares.

    One entry, the Columns tab. A mapping rather than the bare value because the decorator merges it over the user's `metadata`.

    Every refusal is `table_schema`'s, which is this function's whole body.

    Returns
    -------
    A mapping to hand to `dg.asset(metadata=...)`.

    Raises
    ------
    ReservedColumnError
        A user column sits inside the reserved namespace.
    UnnameableColumnError
        A user column is spelled in characters Dagster refuses in a name.
    CheckNameCollisionError
        Two rules rewrite to the same check name.
    """
    return {_COLUMN_SCHEMA_KEY: table_schema(schema)}


def quarantine_metadata(schema: type[dy.Schema]) -> dict[str, dg.TableSchema]:
    """Build the metadata a quarantine declares about its own column schema.

    Its own Columns tab, because every constraint the valid table states is one these rows break.

    Returns
    -------
    A mapping to hand to `dg.AssetSpec(metadata=...)`.
    """
    return {_COLUMN_SCHEMA_KEY: _quarantine_table_schema(schema)}
