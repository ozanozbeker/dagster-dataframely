"""What the asset definition declares about its data, before it has ever run.

The seam: the asset body owns what the data is, the IO manager owns where and how it landed. A schema is what the data is, so it lives here and the IO manager never emits it.
"""

import dagster as dg
import dataframely as dy

# `@public` upstream, but absent from `dagster` and with no `MetadataValue.object()` factory. Pinned by its own test (#16).
from dagster._core.definitions.metadata.metadata_value import (
    ObjectMetadataValue,
)

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

    Dtype, description, nullability, uniqueness and tags. Constraint pills and the table-level primary key arrive with the renderer (#20).

    `unique` is read from the column's own flag and never derived from `primary_key`. dataframely keeps the two independent: a key member gets a composite `as_struct(...).is_unique()` rule and `column.unique` stays `False`, so deriving would claim a per-column uniqueness that nothing enforces.

    Tags come from `Column.metadata`, which dataframely stores and never reads. It is the one dataframely attribute with no other home here, and free-form key/value annotation is exactly what Dagster's column tags are for.

    Args:
        schema: The schema to project.

    Returns:
        A table schema whose columns are in the schema's own order.
    """
    return dg.TableSchema(
        columns=[
            dg.TableColumn(
                name=name,
                type=str(column.dtype),
                description=column.description,
                constraints=dg.TableColumnConstraints(
                    nullable=column.nullable, unique=column.unique
                ),
                tags=_tags(column),
            )
            for name, column in schema.columns().items()
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
