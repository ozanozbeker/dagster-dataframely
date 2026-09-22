"""The code location's entry point, which `dg` loads by name."""

from pathlib import Path

import dagster as dg


@dg.definitions
def defs() -> dg.Definitions:
    """Load every module under `defs/` as one code location."""
    return dg.load_from_defs_folder(path_within_project=Path(__file__).parent)
