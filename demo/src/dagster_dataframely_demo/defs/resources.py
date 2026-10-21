"""The pipeline writes every table through the IO manager this module binds."""

import dagster as dg

from dagster_dataframely.data import ParquetIOManager


@dg.definitions
def resources() -> dg.Definitions:
    """Bind the Parquet IO manager the pipeline writes through."""
    return dg.Definitions(
        resources={"io_manager": ParquetIOManager(base_dir="storage")}
    )
