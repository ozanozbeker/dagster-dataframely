"""Attach a Dataframely schema to a Dagster asset: `dd.asset` fills the Columns tab, reports every rule as an asset check and sets what happens to the rows that fail."""

from dagster_dataframely import data, errors, testing, wiring
from dagster_dataframely._asset import asset
from dagster_dataframely._collection import build_collection_checks
from dagster_dataframely._error_rows import build_error_rows_spec
from dagster_dataframely._rule_checks import build_rule_checks
from dagster_dataframely._settings import Granularity

__all__ = [
    "Granularity",
    "asset",
    "build_collection_checks",
    "build_error_rows_spec",
    "build_rule_checks",
    "data",
    "errors",
    "testing",
    "wiring",
]
