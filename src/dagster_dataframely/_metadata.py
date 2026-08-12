"""What the asset definition declares about its data, before it has ever run.

The asset body owns what the data is, the IO manager owns where and how it was written. A schema is what the data is, so it lives here and the IO manager never emits it.
"""

from collections.abc import Mapping

import dagster as dg
import dataframely as dy
import polars as pl

# `@public` upstream, but absent from `dagster` and with no `MetadataValue.object()` factory. Covered by a characterization test (#16).
from dagster._core.definitions.metadata.metadata_value import (
    ObjectMetadataValue,
)

from dagster_dataframely._naming import check_name, validation_rules
from dagster_dataframely._rendering import column_constraints, table_constraints

_COLUMN_SCHEMA_KEY = "dagster/column_schema"
SCHEMA_CARRIER_KEY = "dagster_dataframely/schema"


def _tags(column: dy.Column) -> dict[str, str] | None:
    """Renders a column's free-form metadata as Dagster tags.

    Values are stringified because `TableColumn.tags` is `Mapping[str, str]` and Dagster rejects anything else at definition time. That is a display rendering, not a cast: no data is touched, and refusing instead would mean a `metadata={"pii": False}` dataframely explicitly permits could not be attached to an asset at all.
    """
    if not column.metadata:
        return None
    return {key: str(value) for key, value in column.metadata.items()}


def table_schema(schema: type[dy.Schema]) -> dg.TableSchema:
    """Projects a schema onto Dagster's Columns tab.

    Dtype, description, tags, and every constraint the schema declares: nullability and uniqueness in Dagster's own two fields, the rest as constraints, and the primary key once at table level.

    `unique` is read from the column's own flag and never derived from `primary_key`. dataframely keeps the two independent: a key member gets a composite `as_struct(...).is_unique()` rule and `column.unique` stays `False`, so deriving would claim a per-column uniqueness that nothing enforces.

    Tags come from `Column.metadata`, which dataframely stores and never reads. It is the one dataframely attribute with no other home here, and free-form key/value annotation is exactly what Dagster's column tags are for.

    Args:
        schema: The schema to project.

    Returns:
        A table schema whose columns are in the schema's own order.
    """
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


def quarantine_table_schema(schema: type[dy.Schema]) -> dg.TableSchema:
    """Projects the quarantine's shape onto its own Columns tab.

    The schema's columns mirrored, keeping dtype, description and tags but **no constraints**: these rows are here precisely because they violate them, so a `not null` constraint on a column full of nulls would state something false about every row in the table. The primary key above all, which is why it is stated table-level on the valid table and nowhere here: the invalid rows are exactly where a duplicate key ends up.

    Its own function rather than a flag on `table_schema`. The two comprehensions read alike, but every constraint the other one carries is a claim this table cannot make.

    Then one `String` column per rule, named exactly as that rule's asset check, so `dy_rule__amount__min` in the check list and `dy_rule__amount__min` in this table are the same string. `String` rather than the `Enum` dataframely produces, because the cast happens before the write.

    Args:
        schema: The schema whose invalid rows this table holds.

    Returns:
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
                name=check_name(rule),
                type="String",
                description=(
                    f"Outcome of rule '{rule}': 'valid' / 'invalid' / 'unknown'."
                ),
            )
            for rule in validation_rules(schema)
        ]
    )


def schema_metadata(
    schema: type[dy.Schema],
) -> dict[str, dg.TableSchema | ObjectMetadataValue]:
    """Builds the definition metadata a schema-backed asset declares.

    Two entries: the Columns tab, and the carrier that takes the live schema class to the IO manager on both the write and the read path. The carrier's label is passed explicitly because deriving it would yield the metaclass name, `SchemaMeta`.

    Args:
        schema: The schema being attached to the asset.

    Returns:
        A mapping to hand to `dg.AssetOut(metadata=...)`.

    Example:
        >>> import dagster as dg
        >>> import dataframely as dy
        >>> import dagster_dataframely as dd
        >>> class Orders(dy.Schema):
        ...     order_id = dy.String(primary_key=True)
        >>> out = dg.AssetOut(metadata=dd.schema_metadata(Orders))
    """
    return {
        _COLUMN_SCHEMA_KEY: table_schema(schema),
        # A raw class here is deprecated upstream, so the carrier is explicit.
        SCHEMA_CARRIER_KEY: ObjectMetadataValue(schema.__name__, instance=schema),
    }


def carried_schema(
    definition_metadata: Mapping[str, object],
) -> type[dy.Schema] | None:
    """Recovers the live schema class the carrier took to the IO manager.

    The other end of `schema_metadata`. It never re-imports and never reads the data, so what the manager holds is the class the definition declared and cannot drift from it.

    Everything absent returns `None` rather than raising, and one of those cases is relied on deliberately: `ObjectMetadataValue` keeps the instance only inside the process that built it, so a manager reading across a process boundary gets the label and no object. A CSV then reads back as an ordinary inferred CSV, which is a smaller failure than a run that cannot read its own input.

    Args:
        definition_metadata: An `OutputContext`'s, or an upstream output's on the read path.

    Returns:
        The schema, or `None` when the asset declares none or the object did not survive the trip.
    """
    carrier = definition_metadata.get(SCHEMA_CARRIER_KEY)
    if not isinstance(carrier, ObjectMetadataValue):
        return None
    schema = carrier.instance
    if isinstance(schema, type) and issubclass(schema, dy.Schema):
        return schema
    return None


def schema_dtypes(schema: type[dy.Schema]) -> dict[str, pl.DataType]:
    """Reads a schema as the polars dtypes it declares, one per column.

    What a reader that is not dataframely needs from a schema: the CSV manager decodes and reads against these, and nothing else on it.

    Args:
        schema: The schema to read.

    Returns:
        The declared dtypes, in the schema's own column order.
    """
    return {name: column.dtype for name, column in schema.columns().items()}


def _quarantine_metadata(
    schema: type[dy.Schema],
) -> dict[str, dg.TableSchema | ObjectMetadataValue]:
    """Builds the definition metadata the quarantine out declares.

    Its own Columns tab, because every constraint the valid table states is one these rows are here for breaking. Then the same carrier the valid out gets, because the two entries answer different questions and only the first of them is about conformance.

    The carrier was withheld at first, on the reading that a table with a rule column for every rule is not schema-shaped. What reads it settles the question: the CSV manager takes dtypes off it, one per name, and a quarantine frame carries every column the schema declares at the dtype it declares. The rule columns sit beside them, and a dtype lookup by name never asks about a name it was not given. So `fulfilled_in`, `payload` and `tags` decode out of a quarantine exactly as they decode out of the valid table, which is the parity a reader would expect and the earlier reading cost them.

    Args:
        schema: The schema whose invalid rows the out holds.

    Returns:
        A mapping to hand to `dg.AssetOut(metadata=...)`.
    """
    return {
        _COLUMN_SCHEMA_KEY: quarantine_table_schema(schema),
        SCHEMA_CARRIER_KEY: ObjectMetadataValue(schema.__name__, instance=schema),
    }
