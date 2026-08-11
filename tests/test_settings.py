"""The three-tier settings chain, asserted at the seam every knob resolves through.

`resolve` is where the tiers meet, so precedence and validation are both testable without going near an asset. The knobs the package ships are exercised through it, and a fake setting covers the one tier a shipped knob cannot reach: its own default is valid by construction.

A two-valued knob is here for the tier a vocabulary knob cannot exercise: the environment arrives as a string whatever the setting holds, so a flag is the one that has to parse it rather than match it.
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
    STATISTICS,
    _Setting,
)

_GRANULARITY_ENV = "DAGSTER_DATAFRAMELY_CHECK_GRANULARITY"
_STATISTICS_ENV = "DAGSTER_DATAFRAMELY_STATISTICS"

# The literal is already a static error, so the runtime guard is asserted through a name a type checker cannot narrow: it is what a user without one gets.
_WRONG: Any = "per_column"

# A setting whose package default is already outside its own vocabulary. The shipped knobs cannot be wrong in that tier, so this is the only way to assert the default is validated rather than trusted.
_BROKEN = _Setting[str](name="fake_setting", default="nonsense", allowed=("on", "off"))


def test_a_knob_nobody_touched_is_the_package_default():
    assert CHECK_GRANULARITY.resolve(None) == "rule"
    assert MULTI_COLUMN_RULES.resolve(None) == "schema"
    assert STATISTICS.resolve(None) is True


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
    assert STATISTICS.env_var == _STATISTICS_ENV


def test_a_flag_parses_the_environment_tier_rather_than_matching_it(
    monkeypatch: pytest.MonkeyPatch,
):
    """The one tier where a two-valued knob differs from a vocabulary one, asserted in both directions and in the casing a deployment is as likely to write."""
    monkeypatch.setenv(_STATISTICS_ENV, "false")
    assert STATISTICS.resolve(None) is False

    monkeypatch.setenv(_STATISTICS_ENV, "TRUE")
    assert STATISTICS.resolve(None) is True


def test_a_flag_turned_off_by_an_argument_is_off_rather_than_unset(
    monkeypatch: pytest.MonkeyPatch,
):
    """The case a truthiness test would get wrong, which is the one that matters: with the tier below saying on, the knob would be impossible to turn off."""
    monkeypatch.setenv(_STATISTICS_ENV, "true")

    assert STATISTICS.resolve(argument=False) is False


def test_a_flag_rejects_a_word_that_is_not_one_of_its_two(
    monkeypatch: pytest.MonkeyPatch,
):
    """`1` is the plausible wrong word, and the error has to be the same one a vocabulary knob raises: same setting, same allowed values, same tier order."""
    monkeypatch.setenv(_STATISTICS_ENV, "1")

    with pytest.raises(InvalidSettingError) as raised:
        STATISTICS.resolve(None)
    message = str(raised.value)

    assert "statistics" in message
    assert "'true', 'false'" in message
    assert _STATISTICS_ENV in message


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
