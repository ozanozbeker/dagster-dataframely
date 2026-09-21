"""The code location's entry point, which `dg` looks for by name.

Everything is autoloaded out of `defs/`, so this file never grows.
"""

from pathlib import Path

from dagster import Definitions, definitions, load_from_defs_folder


@definitions
def defs() -> Definitions:
    """Load every module under `defs/` as one code location."""
    return load_from_defs_folder(path_within_project=Path(__file__).parent)
