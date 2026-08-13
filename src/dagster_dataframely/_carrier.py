"""The channel that takes the live schema class from the asset definition to the IO manager.

Both ends in one module, because a carrier written at one end and read at the other is a single decision and each half is meaningless without the other. `_metadata` writes it into what an asset declares; the CSV manager reads it back to decode a file that carries no dtypes of its own.
"""

from collections.abc import Mapping

import dataframely as dy
import polars as pl

# `@public` upstream, but absent from `dagster` and with no `MetadataValue.object()` factory. Covered by a characterization test (#16).
from dagster._core.definitions.metadata.metadata_value import (
    ObjectMetadataValue,
)

SCHEMA_CARRIER_KEY = "dagster_dataframely/schema"


def carrier(schema: type[dy.Schema]) -> ObjectMetadataValue:
    """Wraps a schema as the metadata value an asset's definition carries.

    A raw class is deprecated upstream, so the wrapper is explicit. The label is passed explicitly too, because deriving it would yield the metaclass name, `SchemaMeta`.

    Args:
        schema: The schema being attached to the asset.

    Returns:
        The value to file under `SCHEMA_CARRIER_KEY`.
    """
    return ObjectMetadataValue(schema.__name__, instance=schema)


def carried_schema(
    definition_metadata: Mapping[str, object],
) -> type[dy.Schema] | None:
    """Recovers the live schema class the carrier took to the IO manager.

    The other end of `carrier`. It never re-imports and never reads the data, so what the manager holds is the class the definition declared and cannot drift from it.

    Everything absent returns `None` rather than raising, and one of those cases is relied on deliberately: `ObjectMetadataValue` keeps the instance only inside the process that built it, so a manager reading across a process boundary gets the label and no object. A CSV then reads back as an ordinary inferred CSV, which is a smaller failure than a run that cannot read its own input.

    Args:
        definition_metadata: An `OutputContext`'s, or an upstream output's on the read path.

    Returns:
        The schema, or `None` when the asset declares none or the object did not survive the trip.
    """
    held = definition_metadata.get(SCHEMA_CARRIER_KEY)
    if not isinstance(held, ObjectMetadataValue):
        return None
    schema = held.instance
    if isinstance(schema, type) and issubclass(schema, dy.Schema):
        return schema
    return None


def schema_dtypes(schema: type[dy.Schema]) -> dict[str, pl.DataType]:
    """Reads a schema as the Polars dtypes it declares, one per column.

    What a reader that is not Dataframely needs from a schema: the CSV manager decodes and reads against these, and nothing else on it. Here rather than with the Columns tab because the carrier is what delivers the schema it reads.

    Args:
        schema: The schema to read.

    Returns:
        The declared dtypes, in the schema's own column order.
    """
    return {name: column.dtype for name, column in schema.columns().items()}
