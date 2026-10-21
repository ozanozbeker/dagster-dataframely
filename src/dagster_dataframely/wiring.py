"""`dd.wiring` exports the functions for a `@dg.asset` you wire yourself, and `dd.asset` calls most of them too.

Most users need only `dd.asset`, so this module holds these names instead of the root namespace.
"""

from dagster_dataframely._checks import build_asset_check_specs, evaluate_checks
from dagster_dataframely._code_version import get_code_version
from dagster_dataframely._error_rows import (
    ErrorRowsWriter,
    build_file_writer,
    build_io_manager_writer,
    get_error_rows,
    get_error_rows_key,
    get_error_rows_path,
    validate_error_rows_key,
)
from dagster_dataframely._metadata import get_column_schema_metadata, get_table_schema
from dagster_dataframely._naming import get_check_name_for_rule
from dagster_dataframely._runtime import stream_results

__all__ = [
    "ErrorRowsWriter",
    "build_asset_check_specs",
    "build_file_writer",
    "build_io_manager_writer",
    "evaluate_checks",
    "get_check_name_for_rule",
    "get_code_version",
    "get_column_schema_metadata",
    "get_error_rows",
    "get_error_rows_key",
    "get_error_rows_path",
    "get_table_schema",
    "stream_results",
    "validate_error_rows_key",
]
