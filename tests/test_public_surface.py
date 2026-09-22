"""The public names of `dagster_dataframely`, `dd.errors` and `dd.wiring`.

Each set lists its names rather than reading them from `__all__`, so every new public name needs a line in this file.
"""

import pkgutil

import dagster_dataframely as dd

_PUBLIC = {
    "asset",
    "quarantine_spec",
    "errors",
    "wiring",
    # At the root rather than in a namespace, because annotated code uses them often.
    "Granularity",
    "SchemaRules",
}

_WIRING = {
    "AssetYield",
    "QuarantineWriter",
    "check_name",
    "check_results",
    "check_specs",
    "delegating_writer",
    "file_writer",
    "validation_results",
    "quarantine_frame",
    "quarantine_path",
    "schema_metadata",
    "table_schema",
    "validate_quarantine_key",
}

# Imported by `dd.errors` for its annotations, and listed so a third import fails a test.
_ERROR_IMPORTS = {"Mapping", "Sequence"}

_ERRORS = {
    "CheckNameCollisionError",
    "CollectionNotSupportedError",
    "DagsterDataframelyError",
    "InvalidSettingError",
    "MaterializeResultFieldError",
    "MaterializeResultValueError",
    "NoValidRowsError",
    "QuarantineDirError",
    "QuarantineKeyCollisionError",
    "ReservedColumnError",
    "InvalidColumnNameError",
    "ColumnSchemaError",
    "ValidationAbortError",
}


def test_the_package_exports_exactly_the_public_surface():
    assert set(dd.__all__) == _PUBLIC


def test_the_error_module_exports_exactly_its_family():
    assert set(dd.errors.__all__) == _ERRORS


def test_the_wiring_module_exports_exactly_the_assembled_parts():
    assert set(dd.wiring.__all__) == _WIRING


def test_nothing_public_leaks_past_any_export_list():
    """`__all__` controls only `import *`, so this also compares each module's attributes."""
    reachable = {name for name in vars(dd) if not name.startswith("_")}
    errors = {name for name in vars(dd.errors) if not name.startswith("_")}
    wiring = {name for name in vars(dd.wiring) if not name.startswith("_")}

    assert reachable == _PUBLIC
    assert errors == _ERRORS | _ERROR_IMPORTS
    assert wiring == _WIRING


def test_only_errors_and_wiring_have_public_module_names():
    """Every other module is private, so renaming or splitting one does not break an import."""
    public = [
        module.name
        for module in pkgutil.iter_modules(dd.__path__)
        if not module.name.startswith("_")
    ]

    assert sorted(public) == ["errors", "wiring"]


def test_the_error_family_is_exported_whole():
    """`__subclasses__()` finds the family, so a new error the package does not export fails this test."""

    def descendants(error: type[Exception]) -> set[str]:
        return {error.__name__}.union(
            *(descendants(child) for child in error.__subclasses__())
        )

    assert descendants(dd.errors.DagsterDataframelyError) <= _ERRORS


# `.github/workflows/release.yml` checks that the wheel contains `py.typed`.
