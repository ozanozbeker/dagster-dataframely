"""The code locations a test launches a run from, because only such a run has a `repository_def` (ADR-0007)."""

import dagster as dg
import polars as pl

import dagster_dataframely as dd
from tests.scenario import WAREHOUSE_SCHEMA, Orders, mixed_orders


@dd.asset(Orders, name="orders", key_prefix=WAREHOUSE_SCHEMA, filter=True)
def orders() -> pl.DataFrame:
    return mixed_orders()


with_spec = dg.Definitions(assets=[orders, dd.build_error_rows_spec(Orders, orders)])
without_spec = dg.Definitions(assets=[orders])
