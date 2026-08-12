"""The package's front matter: what `import dagster_dataframely` puts in reach, what it keeps private, and the marker that makes the annotations visible downstream.

The surface is spelled out here rather than derived from `__all__`, because a test that reads its expectation off the thing it is testing agrees with every change. Adding a name to the package is meant to cost a line in this file, which is where the question "should a user see this?" gets asked.

Privacy is one convention held in two places: `errors` and `wiring` are the only modules besides the root with public names, and nothing lands in any of the three namespaces that it did not choose to export.
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
    # The decorator.
    "dataframely_asset",
    # The IO managers.
    "DataframelyCSVIOManager",
    "DataframelyParquetIOManager",
    # The two namespaces, rather than their eighteen names.
    "errors",
    "wiring",
    # The types, so a caller can annotate what it passes the decorator. Here rather than
    # behind a namespace of their own: a heavily annotated code base reaches for these
    # constantly, and two aliases do not earn the indirection.
    "Granularity",
    "MultiColumnRules",
}

# What `dd.wiring` puts in reach, for a `@dg.multi_asset` a user assembles themselves.
_WIRING = {
    "AssetYield",
    "check_name",
    "check_specs",
    "process",
    "quarantine_frame",
    "quarantine_table_schema",
    "schema_metadata",
    "table_schema",
}

# What `dd.errors` imports to build its messages. Named rather than skipped, so a fourth one is a decision somebody makes here instead of a name that quietly became reachable as `dd.errors.<it>`.
_ERROR_IMPORTS = {"Mapping", "Sequence", "pl"}

# What `dd.errors` puts in reach, spelled out for the reason the root's list is.
_ERRORS = {
    "CheckNameCollisionError",
    "CollectionNotSupportedError",
    "DagsterDataframelyError",
    "InvalidSettingError",
    "NothingSurvivedError",
    "QuarantineSettingError",
    "ReservedColumnError",
    "SchemaShapeError",
    "UnwritableDtypeError",
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

    A submodule with a public name, or a third-party name imported at the root, is reachable as `dd.<name>` whatever `__all__` says, and reachable is what a user will come to depend on. The same assertion catches the other side: a name left in `__all__` after its import moved away is a `from dagster_dataframely import *` that fails.

    `dd.errors` is held to the same standard, minus the three names it imports to build its messages. Those are listed rather than eliminated: the alternatives are `from __future__ import annotations` plus a `TYPE_CHECKING` block, which is what polars does and which would make this the one module in the package with stringified annotations, or spelling them `_pl` and `_Mapping` at seven sites in the module a user reads tracebacks from. Neither is worth paying to hide a name nobody will type, and listing them keeps the guarantee that matters: a fourth import fails this test.

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

    Two exceptions, both bought for the same thing: a root namespace where the happy path is not outnumbered. Ten error names and eight wiring names would be three quarters of it, and neither set is what a user reaches for to get work done. polars answered the error half the same way and deprecated its own root re-exports in 1.0.0 to finish the move.

    What each costs is the freedom to rename that one file. `errors` is a leaf holding one class per failure, so it has nothing to split along, and `wiring` re-exports rather than defines, so everything behind it stays free to move.
    """
    public = [
        module.name
        for module in pkgutil.iter_modules(dd.__path__)
        if not module.name.startswith("_")
    ]

    assert sorted(public) == ["errors", "wiring"]


def test_the_error_family_is_exported_whole():
    """Catching `DagsterDataframelyError` is the point of the family, so a subclass a user cannot name is one they cannot catch on its own.

    Derived from the base rather than listed, because the failure this guards is a new error added to the package and forgotten in `errors.__all__`.
    """

    def descendants(error: type[Exception]) -> set[str]:
        return {error.__name__}.union(
            *(descendants(child) for child in error.__subclasses__())
        )

    assert descendants(dd.errors.DagsterDataframelyError) <= _ERRORS


def test_py_typed_ships_in_the_built_wheel(tmp_path: Path):
    """The marker is what makes the annotations visible to a type checker downstream, and it is a file rather than code, so nothing but a build proves it made the trip.

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
