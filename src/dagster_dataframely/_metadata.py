"""Definition metadata that fills the Columns tab."""

import dagster as dg
import dataframely as dy

from dagster_dataframely._rendering import column_constraints, table_constraints
from dagster_dataframely._rules import described_rules, validate_namespace

_COLUMN_SCHEMA_KEY = "dagster/column_schema"


def _tags(column: dy.Column) -> dict[str, str] | None:
    """Render a column's metadata as string tags, the only kind Dagster accepts."""
    if not column.metadata:
        return None
    return {key: str(value) for key, value in column.metadata.items()}


def table_schema(schema: type[dy.Schema]) -> dg.TableSchema:
    """Return the schema as the `dg.TableSchema` that Dagster's Columns tab shows.

    Each column has its dtype, description, nullability, uniqueness and column constraints. Its tags come from `dy.Column(metadata=...)`, with each value converted to a string. The primary key is a table constraint, so only `unique=True` marks a key column unique.

    Returns
    -------
    A table schema with the columns in the schema's order.

    Raises
    ------
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
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
    """Return the quarantine's column schema, without the constraints its rows fail."""
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
    """Return the definition metadata that fills an asset's Columns tab from the schema.

    Returns
    -------
    A one-entry mapping from `dagster/column_schema` to `table_schema(schema)`, to pass to `dg.asset(metadata=...)`.

    Raises
    ------
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
    """
    return {_COLUMN_SCHEMA_KEY: table_schema(schema)}


def quarantine_metadata(schema: type[dy.Schema]) -> dict[str, dg.TableSchema]:
    """Return the quarantine's definition metadata."""
    return {_COLUMN_SCHEMA_KEY: _quarantine_table_schema(schema)}
