"""The three-tier settings chain, asserted at the seam every knob resolves through.

`resolve` is where the tiers meet, so precedence and validation are both testable without going near an asset. The knobs the package ships are exercised through it, and a fake setting covers the one tier a shipped knob cannot reach: its own default is valid by construction.

A two-valued knob is here for the tier a vocabulary knob cannot exercise: the environment arrives as a string whatever the setting holds, so a flag is the one that has to parse it rather than match it. A count is here for the tier neither of those can exercise: its vocabulary is a range, so it is the only shape with something to reject in a tier a type checker has already narrowed.
"""

import importlib
import pkgutil
from typing import Any

import pytest

import dagster_dataframely
from dagster_dataframely import InvalidSettingError
from dagster_dataframely._settings import (
    CHECK_GRANULARITY,
    MAX_FAILURE_SAMPLES,
    MULTI_COLUMN_RULES,
    ROW_SAMPLE,
    STATISTICS,
    _Count,
    _Flag,
    _Setting,
)

_GRANULARITY_ENV = "DAGSTER_DATAFRAMELY_CHECK_GRANULARITY"
_STATISTICS_ENV = "DAGSTER_DATAFRAMELY_STATISTICS"
_ROW_SAMPLE_ENV = "DAGSTER_DATAFRAMELY_ROW_SAMPLE"

# The literal is already a static error, so the runtime guard is asserted through a name a type checker cannot narrow: it is what a user without one gets.
_WRONG: Any = "per_column"

# The same, for the shape whose vocabulary a type checker narrows to `int`.
_FRACTION: Any = 2.5
_YES: Any = True

# The same, for the two-valued shape. It is the environment tier's own spelling, which is what makes it the word somebody writes into the argument by mistake.
_WORD: Any = "false"

# A setting whose package default is already outside its own vocabulary. The shipped knobs cannot be wrong in that tier, so this is the only way to assert the default is validated rather than trusted.
_BROKEN = _Setting[str](name="fake_setting", default="nonsense", allowed=("on", "off"))

# The same, one shape along: a count whose default is a number it does not accept.
_BROKEN_COUNT = _Count(name="fake_count", default=-1)

# And one along again: a flag whose default is the word for a value rather than the value.
_BROKEN_FLAG = _Flag(name="fake_flag", default=_WORD)


def test_a_knob_nobody_touched_is_the_package_default():
    assert CHECK_GRANULARITY.resolve(None) == "rule"
    assert MULTI_COLUMN_RULES.resolve(None) == "schema"
    assert STATISTICS.resolve(None) is True
    assert MAX_FAILURE_SAMPLES.resolve(None) == 5
    assert ROW_SAMPLE.resolve(None) == 5


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
    assert MAX_FAILURE_SAMPLES.env_var == "DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES"
    assert ROW_SAMPLE.env_var == _ROW_SAMPLE_ENV


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


def test_a_flag_rejects_a_word_from_the_argument_tier():
    """The one shape where an unvalidated argument is silently the *opposite* of what was written.

    `statistics="false"` is a non-empty string, so trusting the `bool | None` annotation resolves it to the word and turns the pass on. The environment tier spells the same instruction exactly that way, which is what makes the mistake reachable rather than hypothetical.
    """
    with pytest.raises(InvalidSettingError) as raised:
        STATISTICS.resolve(_WORD)
    message = str(raised.value)

    assert "statistics" in message
    assert "'false'" in message
    assert "argument" in message


def test_a_count_reads_the_environment_tier_as_a_number(
    monkeypatch: pytest.MonkeyPatch,
):
    """The other shape that has to parse rather than match, and the one where a wrong reading would be silent: `'0'` is a string every truthiness test calls true."""
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "3")
    assert ROW_SAMPLE.resolve(None) == 3

    monkeypatch.setenv(_ROW_SAMPLE_ENV, "0")
    assert ROW_SAMPLE.resolve(None) == 0


def test_a_count_turned_off_by_an_argument_is_off_rather_than_unset(
    monkeypatch: pytest.MonkeyPatch,
):
    """Zero is what turns a sample off, so the tier test cannot be truthiness: with the tier below saying 5, the knob would be impossible to turn off."""
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "5")

    assert ROW_SAMPLE.resolve(0) == 0


def test_a_count_rejects_a_negative_from_the_argument_tier():
    with pytest.raises(InvalidSettingError) as raised:
        MAX_FAILURE_SAMPLES.resolve(-1)
    message = str(raised.value)

    assert "max_failure_samples" in message
    assert "'-1'" in message
    assert "non-negative integers" in message
    assert "package default" in message
    assert "DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES" in message
    assert "argument" in message


def test_a_count_rejects_a_negative_from_the_environment_tier(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "-1")

    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(None)

    assert _ROW_SAMPLE_ENV in str(raised.value)


def test_a_count_rejects_a_word_the_environment_tier_cannot_read_as_a_number(
    monkeypatch: pytest.MonkeyPatch,
):
    """The same error a negative gets, so a deployment never has to tell two failures apart."""
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "five")

    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(None)
    message = str(raised.value)

    assert "row_sample" in message
    assert "'five'" in message
    assert "non-negative integers" in message


def test_a_count_rejects_a_fraction():
    """Half a row is not a row. The value arrives through an untyped name because the literal is already a static error, exactly like a value outside a vocabulary."""
    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(_FRACTION)

    assert "'2.5'" in str(raised.value)


def test_a_count_rejects_a_bool():
    """`True` is an `int` in Python, so a knob confused with `statistics` would otherwise resolve to one row and say nothing."""
    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(_YES)

    assert "'True'" in str(raised.value)


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

    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN_COUNT.resolve(None)

    assert "'-1'" in str(raised.value)

    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN_FLAG.resolve(None)

    assert "package default" in str(raised.value)


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
