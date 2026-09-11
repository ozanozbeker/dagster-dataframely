"""What `import dagster_dataframely` puts in reach, what it keeps private, and the marker that makes the annotations visible downstream.

The surface is spelled out here, not derived from `__all__`. A test that reads its expectation off the thing it tests agrees with every change. Adding a name to the package costs a line in this file, where the question "should a user see this?" gets asked.

`errors` and `wiring` are the only modules besides the root with public names. Nothing lands in any of the three namespaces that it did not choose to export.
"""

import pkgutil
import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

import dagster_dataframely as dd

_ROOT = Path(__file__).parent.parent

# The root: the happy path, and one name for each namespace that is not it.
_PUBLIC = {
    # The decorator, and the spec that puts its quarantine in the graph.
    "dy_asset",
    "build_quarantine_spec",
    # The two namespaces, rather than their twenty names.
    "errors",
    "wiring",
    # The types, so a caller can annotate what it passes the decorator. Not behind a
    # namespace of their own: a heavily annotated code base reaches for these
    # constantly, and two aliases do not earn the indirection.
    "Granularity",
    "MultiColumnRules",
}

# What `dd.wiring` puts in reach, for a `@dg.asset` a user assembles themselves.
_WIRING = {
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
    "validate_quarantine_key",
}

# What `dd.errors` imports to build its messages. Named so a third one is a decision made here, not a name that became reachable as `dd.errors.<it>`.
_ERROR_IMPORTS = {"Mapping", "Sequence"}

# What `dd.errors` puts in reach, spelled out for the reason the root's list is.
_ERRORS = {
    "CheckNameCollisionError",
    "CollectionNotSupportedError",
    "DagsterDataframelyError",
    "InvalidSettingError",
    "MaterializeResultFieldError",
    "MaterializeResultValueError",
    "NothingSurvivedError",
    "QuarantineDirError",
    "QuarantineKeyCollisionError",
    "ReservedColumnError",
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
    """The namespaces themselves, which `__all__` alone does not cover in either direction.

    A submodule with a public name, or a third-party name imported at the root, is reachable as `dd.<name>` whatever `__all__` says, and a user will come to depend on whatever is reachable. The same assertion catches the other side: a name left in `__all__` after its import moved away breaks `from dagster_dataframely import *`.

    `dd.errors` is held to the same standard, minus the two names it imports to build its messages. Those are listed, not eliminated. Eliminating them has two routes. One is `from __future__ import annotations` plus a `TYPE_CHECKING` block; Polars does this, and it would make this the one module in the package with stringified annotations. The other is spelling them `_Mapping` and `_Sequence` at six sites in the module a user reads tracebacks from. Neither is worth paying to hide a name nobody will type. Listing them keeps the guarantee that matters: a third import fails this test.

    `dd.wiring` imports nothing but what it re-exports, so it is held to the standard exactly.
    """
    reachable = {name for name in vars(dd) if not name.startswith("_")}
    errors = {name for name in vars(dd.errors) if not name.startswith("_")}
    wiring = {name for name in vars(dd.wiring) if not name.startswith("_")}

    assert reachable == _PUBLIC
    assert errors == _ERRORS | _ERROR_IMPORTS
    assert wiring == _WIRING


def test_only_errors_and_wiring_have_public_module_names():
    """One responsibility per module and no promise about any of them, so the tree stays free to change.

    Two exceptions, both bought for the same thing: a root namespace where the happy path is not outnumbered. Eleven error names and eleven wiring names would be twenty-two of a namespace of twenty-six, and a user reaches for neither set to get work done. Polars answered errors the same way and deprecated its own root re-exports in 1.0.0 to finish the move.

    Each costs the freedom to rename that one file. `errors` is a leaf holding one class per failure, so it has nothing to split along. `wiring` re-exports rather than defines, so everything behind it stays free to move.
    """
    public = [
        module.name
        for module in pkgutil.iter_modules(dd.__path__)
        if not module.name.startswith("_")
    ]

    assert sorted(public) == ["errors", "wiring"]


def test_the_error_family_is_exported_whole():
    """Catching `DagsterDataframelyError` is the point of the family, so a subclass a user cannot name is one they cannot catch on its own.

    Derived from the base, not listed, because the failure this guards against is a new error added to the package and forgotten in `errors.__all__`.
    """

    def descendants(error: type[Exception]) -> set[str]:
        return {error.__name__}.union(
            *(descendants(child) for child in error.__subclasses__())
        )

    assert descendants(dd.errors.DagsterDataframelyError) <= _ERRORS


def test_py_typed_ships_in_the_built_wheel(tmp_path: Path):
    """The marker makes the annotations visible to a type checker downstream. It is a file, not code, so only a build proves it made the trip.

    Built offline: uv resolves `uv_build` to its own bundled backend, so this needs no network and no warm cache.
    """
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is what builds the wheel")

    subprocess.run(  # noqa: S603 - every argument is this file's own literal
        [uv, "build", "--wheel", "--offline", "--out-dir", str(tmp_path)],
        cwd=_ROOT,
        check=True,
        capture_output=True,
    )
    (wheel,) = tmp_path.glob("*.whl")

    assert "dagster_dataframely/py.typed" in zipfile.ZipFile(wheel).namelist()
