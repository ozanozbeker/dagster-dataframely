"""The parts the decorator assembles, for a `@dg.asset` you wire yourself.

Reach for these when the decorator's shape is not the shape you need: a schema attached to an asset you did not declare, or a reporting arrangement the decorator does not offer. `dy_asset` calls the same functions internally, so a hand-wired asset and a decorated one report alike by construction rather than by agreement.

**A namespace, not an implementation.** Every name is defined in a private module and re-exported here. The file tree stays free to change while the import path stays put. `__init__.py` has the same shape for the same reason.

**Its own module rather than eleven more names in the root.** The root is the happy path, and so is the decorator. Somebody who never hand-wires should not have to read past `check_specs` and `quarantine_frame` to find it. Hand-wiring is supported, not recommended, and one name in the root instead of eleven is what that distinction looks like from outside. `errors` is the other module with a public name, for the same reason.

Examples
--------
```python
import dagster as dg
import dataframely as dy
import polars as pl

import dagster_dataframely as dd


class Orders(dy.Schema):
    order_id = dy.String(primary_key=True)


@dg.asset(
    metadata=dd.wiring.schema_metadata(Orders),
    check_specs=dd.wiring.check_specs(Orders, asset=dg.AssetKey(["orders"])),
    output_required=False,
)
def orders(context: dg.AssetExecutionContext) -> dd.wiring.AssetYield:
    yield from dd.wiring.process(
        Orders,
        pl.DataFrame({"order_id": ["a"]}),
        valid_key=context.asset_key,
        quarantine_writer=dd.wiring.delegating_writer(context),
    )
```
"""

from dagster_dataframely._checks import check_specs
from dagster_dataframely._metadata import schema_metadata, table_schema
from dagster_dataframely._naming import check_name
from dagster_dataframely._quarantine import (
    QuarantineWriter,
    delegating_writer,
    file_writer,
    quarantine_path,
)
from dagster_dataframely._runtime import AssetYield, process, quarantine_frame

__all__ = [
    "AssetYield",
    "QuarantineWriter",
    "check_name",
    "check_specs",
    "delegating_writer",
    "file_writer",
    "process",
    "quarantine_frame",
    "quarantine_path",
    "schema_metadata",
    "table_schema",
]
