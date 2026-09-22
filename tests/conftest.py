"""The suite's one hook unsets the package's environment variables, so a developer's own settings do not change what the suite asserts.

It is `pytest_configure`, not an autouse fixture, because test modules decorate assets during collection, and the decorator resolves the settings then. A test that needs a variable sets it with `monkeypatch.setenv`.
"""

import os

_PREFIX = "DAGSTER_DATAFRAMELY_"


def pytest_configure() -> None:
    """Unset every `DAGSTER_DATAFRAMELY_*` variable."""
    for name in list(os.environ):
        if name.startswith(_PREFIX):
            del os.environ[name]
