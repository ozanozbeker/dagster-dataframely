"""The three-tier settings chain, asserted at the seam every knob resolves through.

`_Setting.resolve` is where the tiers meet, so precedence and validation are both testable without going near an asset. The two knobs the package ships are exercised through it, and a fake setting covers the one tier a shipped knob cannot reach: its own default is valid by construction.
"""

import importlib
import pkgutil
from typing import Any

import pytest

import dagster_dataframely
from dagster_dataframely import InvalidSettingError
from dagster_dataframely.settings import (
    CHECK_GRANULARITY,
    MULTI_COLUMN_RULES,
    _Setting,
)

_GRANULARITY_ENV = "DAGSTER_DATAFRAMELY_CHECK_GRANULARITY"

# The literal is already a static error, so the runtime guard is asserted through a name a type checker cannot narrow: it is what a user without one gets.
_WRONG: Any = "per_column"

# A setting whose package default is already outside its own vocabulary. The shipped knobs cannot be wrong in that tier, so this is the only way to assert the default is validated rather than trusted.
_BROKEN = _Setting[str](name="fake_setting", default="nonsense", allowed=("on", "off"))


def test_a_knob_nobody_touched_is_the_package_default():
    assert CHECK_GRANULARITY.resolve(None) == "rule"
    assert MULTI_COLUMN_RULES.resolve(None) == "schema"


def test_the_environment_variable_beats_the_package_default(
    monkeypatch: pytest.MonkeyPatch,
):
    """The house-style tier: set once by a platform engineer, for every asset in the code location."""
    monkeypatch.setenv(_GRANULARITY_ENV, "column")

    assert CHECK_GRANULARITY.resolve(None) == "column"


def test_the_argument_beats_the_environment_variable(monkeypatch: pytest.MonkeyPatch):
    """The override tier: the house style holds everywhere except where an asset says otherwise."""
    monkeypatch.setenv(_GRANULARITY_ENV, "column")

    assert CHECK_GRANULARITY.resolve("schema") == "schema"


def test_every_setting_names_its_environment_variable_after_itself():
    """`DAGSTER_DATAFRAMELY_*` is collision-proof and obviously machine surface, and it is derived rather than transcribed so the two cannot drift."""
    assert CHECK_GRANULARITY.env_var == _GRANULARITY_ENV
    assert MULTI_COLUMN_RULES.env_var == "DAGSTER_DATAFRAMELY_MULTI_COLUMN_RULES"


def test_a_value_outside_the_vocabulary_raises_from_the_argument_tier():
    with pytest.raises(InvalidSettingError) as raised:
        CHECK_GRANULARITY.resolve(_WRONG)

    assert "per_column" in str(raised.value)


def test_a_value_outside_the_vocabulary_raises_from_the_environment_tier(
    monkeypatch: pytest.MonkeyPatch,
):
    """A wrong value in a deployment's environment is the tier where a silent misconfiguration would spread furthest."""
    monkeypatch.setenv(_GRANULARITY_ENV, "per_column")

    with pytest.raises(InvalidSettingError) as raised:
        CHECK_GRANULARITY.resolve(None)

    assert _GRANULARITY_ENV in str(raised.value)


def test_a_value_outside_the_vocabulary_raises_from_the_default_tier():
    """The chain validates on resolve, so no tier is trusted, including the package's own."""
    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN.resolve(None)

    assert "nonsense" in str(raised.value)


def test_the_error_names_the_setting_the_value_and_the_tier_order():
    """Everything needed to find the typo without opening the package source: which knob, what it got, and every place it could have come from."""
    with pytest.raises(InvalidSettingError) as raised:
        CHECK_GRANULARITY.resolve(_WRONG)
    message = str(raised.value)

    assert "check_granularity" in message
    assert "per_column" in message
    assert "'rule', 'column', 'schema'" in message
    assert "package default" in message
    assert _GRANULARITY_ENV in message
    assert "argument" in message


def test_no_module_offers_a_global_default_setter():
    """There is deliberately no fourth tier.

    Dagster loads code locations lazily, so "has the default been set yet" would depend on an import order the user does not control: the same asset would derive different checks depending on which module happened to be imported first.
    """
    modules = [
        importlib.import_module(f"dagster_dataframely.{module.name}")
        for module in pkgutil.iter_modules(dagster_dataframely.__path__)
    ]

    setters = {
        f"{module.__name__}.{name}"
        for module in [dagster_dataframely, *modules]
        for name in dir(module)
        if name.startswith("set_default")
    }

    assert setters == set()
