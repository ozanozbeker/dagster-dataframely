"""The three sources of a setting, asserted at the one call every setting resolves through.

The three sources meet in `resolve`, so precedence and validation are both testable without going near an asset. The shipped settings are exercised through it. A fake one covers the default source, because a shipped default is valid by construction.

A flag and a count parse the environment variable rather than match it, because the environment arrives as a string whatever the setting holds. A count accepts a range, so it is the one setting with something left to reject after a type checker has narrowed the source. A directory ships a package default of `None`, which means no directory rather than the absence of a setting.
"""

from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

from dagster_dataframely._settings import (
    CHECK_GRANULARITY,
    MAX_FAILURE_SAMPLES,
    QUARANTINE_DIR,
    ROW_SAMPLE,
    SCHEMA_RULES,
    STATISTICS,
)
from dagster_dataframely.errors import InvalidSettingError

_GRANULARITY_ENV = "DAGSTER_DATAFRAMELY_CHECK_GRANULARITY"
_STATISTICS_ENV = "DAGSTER_DATAFRAMELY_STATISTICS"
_ROW_SAMPLE_ENV = "DAGSTER_DATAFRAMELY_ROW_SAMPLE"
_QUARANTINE_DIR_ENV = "DAGSTER_DATAFRAMELY_QUARANTINE_DIR"

# The literal is already a static error, so the runtime guard is asserted through a name a type checker cannot narrow. A user without a type checker gets the same.
_WRONG: Any = "per_column"

# The same, for a count, whose allowed values a type checker narrows to `int`.
_FRACTION: Any = 2.5
_YES: Any = True

# The same, for the two-valued setting. It is the environment variable's own spelling, so it is the word somebody writes into the argument by mistake.
_WORD: Any = "false"

# The same, for the setting that holds a path. A user reaches for a `Path`; the setting holds the string spelling instead.
_PATH_OBJECT: Any = Path("/scratch")

# A setting whose package default is already outside its own allowed values. The shipped settings cannot be wrong from that source, so this is the only way to assert the default is validated, not trusted. Each is a shipped setting with its default swapped for a wrong one.
_BROKEN = replace(CHECK_GRANULARITY, name="fake_setting", default=_WRONG)

# A count whose default is a number it does not accept.
_BROKEN_COUNT = replace(MAX_FAILURE_SAMPLES, name="fake_count", default=-1)

# A flag whose default is the word for a value rather than the value.
_BROKEN_FLAG = replace(STATISTICS, name="fake_flag", default=_WORD)

# A directory whose default is the empty path, the only wrong value it has.
_BROKEN_DIRECTORY = replace(QUARANTINE_DIR, name="fake_directory", default="")


def test_a_setting_nobody_touched_is_the_package_default():
    assert CHECK_GRANULARITY.resolve(None) == "rule"
    assert SCHEMA_RULES.resolve(None) == "collapsed"
    assert STATISTICS.resolve(None) is True
    assert MAX_FAILURE_SAMPLES.resolve(None) == 5
    assert ROW_SAMPLE.resolve(None) == 5
    assert QUARANTINE_DIR.resolve(None) is None


def test_the_environment_variable_beats_the_package_default(
    monkeypatch: pytest.MonkeyPatch,
):
    """The house-style source: set once by a platform engineer, for every asset in the code location."""
    monkeypatch.setenv(_GRANULARITY_ENV, "column")

    assert CHECK_GRANULARITY.resolve(None) == "column"


def test_the_argument_beats_the_environment_variable(monkeypatch: pytest.MonkeyPatch):
    """The override: the house style holds everywhere except where an asset says otherwise."""
    monkeypatch.setenv(_GRANULARITY_ENV, "column")

    assert CHECK_GRANULARITY.resolve("schema") == "schema"


def test_every_setting_names_its_environment_variable_after_itself():
    """`DAGSTER_DATAFRAMELY_*` is collision-proof and obviously meant for a machine to read. It is derived, not transcribed, so the name and the setting cannot drift."""
    assert CHECK_GRANULARITY.env_var == _GRANULARITY_ENV
    assert SCHEMA_RULES.env_var == "DAGSTER_DATAFRAMELY_SCHEMA_RULES"
    assert STATISTICS.env_var == _STATISTICS_ENV
    assert MAX_FAILURE_SAMPLES.env_var == "DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES"
    assert ROW_SAMPLE.env_var == _ROW_SAMPLE_ENV
    assert QUARANTINE_DIR.env_var == _QUARANTINE_DIR_ENV


def test_a_flag_parses_the_environment_source_rather_than_matching_it(
    monkeypatch: pytest.MonkeyPatch,
):
    """The one source where a flag differs from a choice, asserted in both directions and in the casing a deployment is as likely to write."""
    monkeypatch.setenv(_STATISTICS_ENV, "false")
    assert STATISTICS.resolve(None) is False

    monkeypatch.setenv(_STATISTICS_ENV, "TRUE")
    assert STATISTICS.resolve(None) is True


def test_a_flag_turned_off_by_an_argument_is_off_rather_than_unset(
    monkeypatch: pytest.MonkeyPatch,
):
    """The case a truthiness test would get wrong. With the source below saying on, the setting would be impossible to turn off."""
    monkeypatch.setenv(_STATISTICS_ENV, "true")

    assert STATISTICS.resolve(argument=False) is False


def test_a_flag_rejects_a_word_that_is_not_one_of_its_two(
    monkeypatch: pytest.MonkeyPatch,
):
    """`1` is the plausible wrong word. The error has to be the same one a choice raises: same setting, same allowed values, same source order."""
    monkeypatch.setenv(_STATISTICS_ENV, "1")

    with pytest.raises(InvalidSettingError) as raised:
        STATISTICS.resolve(None)
    message = str(raised.value)

    assert "statistics" in message
    assert "'true', 'false'" in message
    assert _STATISTICS_ENV in message


def test_a_flag_rejects_a_word_from_the_argument():
    """The one setting where an unvalidated argument is silently the opposite of what was written.

    `statistics="false"` is a non-empty string, so trusting the `bool | None` annotation resolves it to the word and turns the pass on. The environment variable spells the same instruction that way, so the mistake is reachable.
    """
    with pytest.raises(InvalidSettingError) as raised:
        STATISTICS.resolve(_WORD)
    message = str(raised.value)

    assert "statistics" in message
    assert "'false'" in message
    assert "argument" in message


def test_a_count_reads_the_environment_source_as_a_number(
    monkeypatch: pytest.MonkeyPatch,
):
    """The other setting that parses rather than matches. A wrong reading here would be silent: `'0'` is a string every truthiness test calls true."""
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "3")
    assert ROW_SAMPLE.resolve(None) == 3

    monkeypatch.setenv(_ROW_SAMPLE_ENV, "0")
    assert ROW_SAMPLE.resolve(None) == 0


def test_a_count_turned_off_by_an_argument_is_off_rather_than_unset(
    monkeypatch: pytest.MonkeyPatch,
):
    """Zero turns a sample off, so the test cannot be truthiness. With the source below saying 5, the setting would be impossible to turn off."""
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "5")

    assert ROW_SAMPLE.resolve(0) == 0


def test_a_count_rejects_a_negative_from_the_argument():
    with pytest.raises(InvalidSettingError) as raised:
        MAX_FAILURE_SAMPLES.resolve(-1)
    message = str(raised.value)

    assert "max_failure_samples" in message
    assert "'-1'" in message
    assert "non-negative integers" in message
    assert "package default" in message
    assert "DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES" in message
    assert "argument" in message


def test_a_count_rejects_a_negative_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "-1")

    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(None)

    assert _ROW_SAMPLE_ENV in str(raised.value)


def test_a_count_rejects_a_word_the_environment_cannot_read_as_a_number(
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
    """Half a row is not a row. The value arrives through an untyped name because the literal is already a static error, like a value outside a setting's allowed values."""
    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(_FRACTION)

    assert "'2.5'" in str(raised.value)


def test_a_count_rejects_a_bool():
    """`True` is an `int` in Python, so a setting confused with `statistics` would otherwise resolve to one row and say nothing."""
    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(_YES)

    assert "'True'" in str(raised.value)


def test_a_directory_reads_the_environment_source_as_the_path_it_spells(
    monkeypatch: pytest.MonkeyPatch,
):
    """The setting with nothing to parse and nothing to match: the environment variable already arrives as what the setting holds."""
    monkeypatch.setenv(_QUARANTINE_DIR_ENV, "/scratch/quarantine")

    assert QUARANTINE_DIR.resolve(None) == "/scratch/quarantine"
    assert QUARANTINE_DIR.resolve("/mnt/volume") == "/mnt/volume"


def test_a_directory_rejects_an_empty_value_rather_than_reading_it_as_unset(
    monkeypatch: pytest.MonkeyPatch,
):
    """`DAGSTER_DATAFRAMELY_QUARANTINE_DIR=${SCRATCH}` in a deployment whose `SCRATCH` never got set arrives empty.

    Reading that as unset would report a setting nobody wrote when somebody wrote one wrong. The refusal names the variable instead, so the fix lands where the mistake is.
    """
    monkeypatch.setenv(_QUARANTINE_DIR_ENV, "   ")

    with pytest.raises(InvalidSettingError) as raised:
        QUARANTINE_DIR.resolve(None)
    message = str(raised.value)

    assert "quarantine_dir" in message
    assert "filesystem paths" in message
    assert _QUARANTINE_DIR_ENV in message


def test_a_directory_rejects_something_that_is_not_a_path():
    """A `Path` is the plausible wrong value here, wrong for the same reason a word is wrong in a count. Every source for this setting spells a path as a string, because the environment variable can spell it no other way."""
    with pytest.raises(InvalidSettingError) as raised:
        QUARANTINE_DIR.resolve(_PATH_OBJECT)

    assert "/scratch" in str(raised.value)
    assert "argument" in str(raised.value)


def test_a_value_outside_the_allowed_values_raises_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
):
    """A wrong value in a deployment's environment is the source where a silent misconfiguration would spread furthest."""
    monkeypatch.setenv(_GRANULARITY_ENV, "per_column")

    with pytest.raises(InvalidSettingError) as raised:
        CHECK_GRANULARITY.resolve(None)

    assert _GRANULARITY_ENV in str(raised.value)


def test_a_value_outside_the_allowed_values_raises_from_the_default_source():
    """The chain validates on resolve, so no source is trusted, including the package's own."""
    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN.resolve(None)

    assert "per_column" in str(raised.value)

    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN_COUNT.resolve(None)

    assert "'-1'" in str(raised.value)

    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN_FLAG.resolve(None)

    assert "package default" in str(raised.value)

    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN_DIRECTORY.resolve(None)

    assert "filesystem paths" in str(raised.value)


def test_the_error_names_the_setting_the_value_and_the_source_order():
    """Everything needed to find the typo without opening the package source: which setting, what it got, and every place it could have come from."""
    with pytest.raises(InvalidSettingError) as raised:
        CHECK_GRANULARITY.resolve(_WRONG)
    message = str(raised.value)

    assert "check_granularity" in message
    assert "per_column" in message
    assert "'rule', 'column', 'schema'" in message
    assert "package default" in message
    assert _GRANULARITY_ENV in message
    assert "argument" in message


# There is no fourth source and no `set_default_*()`. Dagster loads code locations lazily,
# so "has the default been set yet" would depend on an import order the user does not
# control, and the same asset would derive different checks depending on which module
# imported first. That is a grep over the source, so it is a `no-default-setter` hook in
# `prek.toml`.
