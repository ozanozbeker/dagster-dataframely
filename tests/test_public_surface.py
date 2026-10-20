"""The public names of `dagster_dataframely`, `dd.data`, `dd.errors`, `dd.testing` and `dd.wiring`.

Each set lists its names rather than reading them from `__all__`, so every new public name needs a line in this file.
"""

import pkgutil

import dagster_dataframely as dd

_PUBLIC = {
    "asset",
    "build_collection_checks",
    "build_error_rows_spec",
    "data",
    "errors",
    "build_rule_checks",
    "testing",
    "wiring",
    # `Granularity` is at the root rather than in a namespace, because annotated code uses it often.
    "Granularity",
}

_WIRING = {
    "ErrorRowsWriter",
    "get_check_name_for_rule",
    "evaluate_checks",
    "build_asset_check_specs",
    "build_io_manager_writer",
    "build_file_writer",
    "stream_results",
    "get_error_rows",
    "get_error_rows_path",
    "get_code_version",
    "get_column_schema_metadata",
    "get_table_schema",
    "validate_error_rows_key",
}

_TESTING = {"DirectInvocationResult"}

_DATA = {"OrdersSchema", "marketplace_orders", "storefront_orders"}

# `dd.data` imports these to build its schema and frames; the set lists them so a new import fails a test.
_DATA_IMPORTS = {"dt", "dy", "pl", "Decimal"}

# `dd.testing` imports these; the set lists them so a new import fails a test.
_TESTING_IMPORTS = {"Iterable", "Self", "dataclass", "dg", "pl"}

# `dd.errors` imports these for its annotations; the set lists them so a third import fails a test.
_ERROR_IMPORTS = {"Mapping", "Sequence"}

_ERRORS = {
    "CheckNameCollisionError",
    "CollectionNotSupportedError",
    "DagsterDataframelyError",
    "FailurePropagationError",
    "InvalidMembersError",
    "InvalidSettingError",
    "InvalidSeverityError",
    "MaterializeResultFieldError",
    "MaterializeResultValueError",
    "NoValidRowsError",
    "ErrorRowsDirError",
    "ErrorRowsKeyCollisionError",
    "ReservedColumnError",
    "InvalidColumnNameError",
    "ColumnSchemaError",
    "ValidationAbortError",
}


def test_the_package_exports_exactly_the_public_surface():
    assert set(dd.__all__) == _PUBLIC


def test_the_error_module_exports_exactly_its_family():
    assert set(dd.errors.__all__) == _ERRORS


def test_the_testing_module_exports_exactly_the_record():
    assert set(dd.testing.__all__) == _TESTING


def test_the_wiring_module_exports_exactly_the_assembled_parts():
    assert set(dd.wiring.__all__) == _WIRING


def test_the_data_module_exports_exactly_the_example_data():
    assert set(dd.data.__all__) == _DATA


def test_nothing_public_leaks_past_any_export_list():
    """`__all__` controls only `import *`, so this also compares each module's attributes."""
    reachable = {name for name in vars(dd) if not name.startswith("_")}
    errors = {name for name in vars(dd.errors) if not name.startswith("_")}
    testing = {name for name in vars(dd.testing) if not name.startswith("_")}
    wiring = {name for name in vars(dd.wiring) if not name.startswith("_")}
    data = {name for name in vars(dd.data) if not name.startswith("_")}

    assert reachable == _PUBLIC
    assert data == _DATA | _DATA_IMPORTS
    assert errors == _ERRORS | _ERROR_IMPORTS
    assert testing == _TESTING | _TESTING_IMPORTS
    assert wiring == _WIRING


def test_only_data_errors_testing_and_wiring_have_public_module_names():
    """Every other module is private, so renaming or splitting one does not break an import."""
    public = [
        module.name
        for module in pkgutil.iter_modules(dd.__path__)
        if not module.name.startswith("_")
    ]

    assert sorted(public) == ["data", "errors", "testing", "wiring"]


def test_the_error_family_is_exported_whole():
    """`__subclasses__()` finds the family, so a new error the package does not export fails this test."""

    def descendants(error: type[Exception]) -> set[str]:
        return {error.__name__}.union(
            *(descendants(child) for child in error.__subclasses__())
        )

    assert descendants(dd.errors.DagsterDataframelyError) <= _ERRORS


# `.github/workflows/release.yml` checks that the wheel contains `py.typed`.
