"""What the suite does before it collects anything.

One hook, and it takes the package's own environment variables away. Every setting reads one, so a developer who exports `DAGSTER_DATAFRAMELY_STATISTICS` for their own work would otherwise change what the suite asserts, in whichever tests happen to read that setting. A test that wants a variable sets it through `monkeypatch.setenv`, which restores itself afterwards.

`pytest_configure` rather than an autouse fixture, because a fixture is too late. Several test modules decorate an asset at module level, and the decorator resolves the settings that shape a definition right there. That import happens during collection, before the first fixture runs, so a fixture would leave exactly the settings a definition is built from ambient.

Nothing else lives here. A schema is a class and a frame is a value, so the shared ones read more cheaply as module constants in `tests/scenario.py`.
"""

import os

_PREFIX = "DAGSTER_DATAFRAMELY_"


def pytest_configure() -> None:
    """Unset every `DAGSTER_DATAFRAMELY_*` variable the shell brought in.

    Listed before the loop starts, because the loop writes to the mapping it reads. Nothing restores them: the environment belongs to this process, and every test that wants one sets it.
    """
    for name in list(os.environ):
        if name.startswith(_PREFIX):
            del os.environ[name]
