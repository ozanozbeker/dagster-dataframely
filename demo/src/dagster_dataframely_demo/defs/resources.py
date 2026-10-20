"""The pipeline writes every table through the IO manager this module defines."""

import dagster as dg
from dagster_polars import PolarsParquetIOManager


@dg.definitions
def resources() -> dg.Definitions:
    """Bind the Parquet IO manager the pipeline writes through."""
    return dg.Definitions(
        resources={"io_manager": PolarsParquetIOManager(base_dir="storage")}
    )
