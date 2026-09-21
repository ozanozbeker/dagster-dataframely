"""Where the demo's tables land.

This package ships no IO manager, so the demo brings one. `PolarsParquetIOManager` is the `UPathIOManager` half of ADR-0006, which is the half that addresses a table by file path.

`base_dir` is relative, so it resolves against the working directory and `dg dev` run from `demo/` lands the tables in `demo/storage/`.

One manager, bound as `io_manager`, and every quarantine goes through it too. The invalid rows travel through whatever the asset was already bound to, under the asset's own key with `_quarantine` on the end, so nothing here is configured for them.
"""

import dagster as dg
from dagster_polars import PolarsParquetIOManager


@dg.definitions
def resources() -> dg.Definitions:
    """Bind the one IO manager every asset in the demo writes through."""
    return dg.Definitions(
        resources={"io_manager": PolarsParquetIOManager(base_dir="storage")}
    )
