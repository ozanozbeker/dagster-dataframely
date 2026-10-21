"""`get_table_schema` and `get_column_schema_metadata` build the metadata that fills the Columns tab."""

import dagster as dg
import dataframely as dy

from dagster_dataframely._rendering import (
    render_column_constraints,
    render_table_constraints,
)
from dagster_dataframely._rules import DECLARATION_KEY, validate_namespace

COLUMN_SCHEMA_KEY = "dagster/column_schema"


def get_column_tags(column: dy.Column) -> dict[str, str] | None:
    """Return a column's metadata as string tags, because Dagster accepts only strings."""
    tags: dict[str, str] = {
        key: str(value)
        for key, value in (column.metadata or {}).items()
        if key != DECLARATION_KEY
    }
    return tags or None


def get_table_schema(schema: type[dy.Schema]) -> dg.TableSchema:
    """Return the schema as the `dg.TableSchema` that Dagster's Columns tab shows.

    Each column has its dtype, description, nullability, uniqueness and column constraints. Its tags come from `dy.Column(metadata=...)`, with each value converted to a string, except the `dagster_dataframely` key, whose severities the checks show. The primary key is a table constraint, so only `unique=True` marks a key column unique.

    Parameters
    ----------
    schema
        The schema to render.

    Returns
    -------
    :
        A table schema with the columns in the schema's order.

    Raises
    ------
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
    InvalidSeverityError
        The schema declares a severity this package cannot read.

    Examples
    --------
    ```python
    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema

    dd.wiring.get_table_schema(OrdersSchema).columns[0]
    ```
    """
    validate_namespace(schema)
    constraints: dict[str, list[str]] = render_column_constraints(schema)
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
                tags=get_column_tags(column),
            )
            for name, column in schema.columns().items()
        ],
        constraints=dg.TableConstraints(other=render_table_constraints(schema)),
    )


def get_column_schema_metadata(schema: type[dy.Schema]) -> dict[str, dg.TableSchema]:
    """Return the metadata that fills an asset's Columns tab from the schema.

    Parameters
    ----------
    schema
        The schema to render.

    Returns
    -------
    :
        A one-entry mapping from `dagster/column_schema` to `get_table_schema(schema)`, to pass to `dg.asset(metadata=...)` and, in a run, to `context.add_asset_metadata`.

    Raises
    ------
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
    InvalidSeverityError
        The schema declares a severity this package cannot read.

    Examples
    --------
    ```python
    import dagster as dg
    import polars as pl

    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema


    @dg.asset(metadata=dd.wiring.get_column_schema_metadata(OrdersSchema))
    def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return raw_orders


    list(orders.metadata_by_key[orders.key])
    ```
    """
    return {COLUMN_SCHEMA_KEY: get_table_schema(schema)}
