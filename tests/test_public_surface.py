"""The package's front matter: what `import dagster_dataframely` puts in reach, what it keeps private, and the marker that makes the annotations visible downstream.

The surface is spelled out here rather than derived from `__all__`, because a test that reads its expectation off the thing it is testing agrees with every change. Adding a name to the package is meant to cost a line in this file, which is where the question "should a user see this?" gets asked.

Privacy is one convention held in two places: no module but the root has a public name, and nothing lands in the root's namespace that the root did not choose to export.
"""

import pkgutil
import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest

import dagster_dataframely as dd

_ROOT = Path(__file__).parent.parent

# The whole public surface, grouped as the spec groups it.
_PUBLIC = {
    # The door.
    "dataframely_asset",
    # The IO managers.
    "DataframelyCSVIOManager",
    "DataframelyParquetIOManager",
    # The kit: the door's own parts, for a wiring the door does not offer.
    "check_name",
    "check_specs",
    "process",
    "quarantine_frame",
    "quarantine_table_schema",
    "schema_metadata",
    "table_schema",
    # The errors.
    "CheckNameCollisionError",
    "CollectionNotSupportedError",
    "DagsterDataframelyError",
    "InvalidSettingError",
    "NothingSurvivedError",
    "QuarantineSettingError",
    "ReservedColumnError",
    "SchemaGateError",
    "UnwritableDtypeError",
    "ValidationAbortError",
    # The types, so a caller can annotate what it passes the door and what a fan-in hands back.
    "DataFramePartitions",
    "Granularity",
    "MultiColumnRules",
}


def test_the_package_exports_exactly_the_public_surface():
    assert set(dd.__all__) == _PUBLIC


def test_nothing_public_leaks_past_the_export_list():
    """The namespace itself, which `__all__` alone does not cover in either direction.

    A submodule with a public name, or a third-party name imported at the root, is reachable as `dd.<name>` whatever `__all__` says, and reachable is what a user will come to depend on. The same assertion catches the other side: a name left in `__all__` after its import moved away is a `from dagster_dataframely import *` that fails.
    """
    reachable = {name for name in vars(dd) if not name.startswith("_")}

    assert reachable == _PUBLIC


def test_every_module_but_the_root_is_private():
    """One responsibility per module and no promise about any of them: the root is the only import path this package supports."""
    public = [
        module.name
        for module in pkgutil.iter_modules(dd.__path__)
        if not module.name.startswith("_")
    ]

    assert public == []


def test_the_error_family_is_exported_whole():
    """Catching `DagsterDataframelyError` is the point of the family, so a subclass a user cannot name is one they cannot catch on its own.

    Derived from the base rather than listed, because the failure this guards is a new error added to the package and forgotten in the root.
    """

    def descendants(error: type[Exception]) -> set[str]:
        return {error.__name__}.union(
            *(descendants(child) for child in error.__subclasses__())
        )

    assert descendants(dd.DagsterDataframelyError) <= _PUBLIC


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
