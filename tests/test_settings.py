"""Tests for `_Setting.resolve`, which reads each setting from its three sources.

The last two tests call `check_specs`, because no other test file sets `DAGSTER_DATAFRAMELY_SCHEMA_RULES`.
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
from dagster_dataframely.wiring import check_specs
from tests.scenario import Orders

_GRANULARITY_ENV = "DAGSTER_DATAFRAMELY_CHECK_GRANULARITY"
_SCHEMA_RULES_ENV = "DAGSTER_DATAFRAMELY_SCHEMA_RULES"
_STATISTICS_ENV = "DAGSTER_DATAFRAMELY_STATISTICS"
_ROW_SAMPLE_ENV = "DAGSTER_DATAFRAMELY_ROW_SAMPLE"
_QUARANTINE_DIR_ENV = "DAGSTER_DATAFRAMELY_QUARANTINE_DIR"

# Typed `Any`, so the type checker does not reject the wrong values these tests need.
_WRONG: Any = "per_column"
_FRACTION: Any = 2.5
_YES: Any = True
_WORD: Any = "false"
_PATH_OBJECT: Any = Path("/scratch")

# Every shipped default is valid, so these copies with a wrong default test the default source.
_BROKEN = replace(CHECK_GRANULARITY, name="fake_setting", default=_WRONG)
_BROKEN_COUNT = replace(MAX_FAILURE_SAMPLES, name="fake_count", default=-1)
_BROKEN_FLAG = replace(STATISTICS, name="fake_flag", default=_WORD)
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
    monkeypatch.setenv(_GRANULARITY_ENV, "column")

    assert CHECK_GRANULARITY.resolve(None) == "column"


def test_the_argument_beats_the_environment_variable(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv(_GRANULARITY_ENV, "column")

    assert CHECK_GRANULARITY.resolve("schema") == "schema"


def test_every_setting_names_its_environment_variable_after_itself():
    assert CHECK_GRANULARITY.env_var == _GRANULARITY_ENV
    assert SCHEMA_RULES.env_var == _SCHEMA_RULES_ENV
    assert STATISTICS.env_var == _STATISTICS_ENV
    assert MAX_FAILURE_SAMPLES.env_var == "DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES"
    assert ROW_SAMPLE.env_var == _ROW_SAMPLE_ENV
    assert QUARANTINE_DIR.env_var == _QUARANTINE_DIR_ENV


def test_a_flag_parses_the_environment_source_rather_than_matching_it(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_STATISTICS_ENV, "false")
    assert STATISTICS.resolve(None) is False

    monkeypatch.setenv(_STATISTICS_ENV, "TRUE")
    assert STATISTICS.resolve(None) is True


def test_a_flag_turned_off_by_an_argument_is_off_rather_than_unset(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_STATISTICS_ENV, "true")

    assert STATISTICS.resolve(argument=False) is False


def test_a_flag_rejects_a_word_that_is_not_one_of_its_two(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_STATISTICS_ENV, "1")

    with pytest.raises(InvalidSettingError) as raised:
        STATISTICS.resolve(None)
    message = str(raised.value)

    assert "statistics" in message
    assert "'true', 'false'" in message
    assert _STATISTICS_ENV in message


def test_a_flag_rejects_a_word_from_the_argument():
    """The setting rejects `statistics="false"`, because a non-empty string is truthy and would turn statistics on."""
    with pytest.raises(InvalidSettingError) as raised:
        STATISTICS.resolve(_WORD)

    assert "Setting `statistics` got 'false' from the `statistics=` argument" in str(
        raised.value
    )


def test_a_count_reads_the_environment_source_as_a_number(
    monkeypatch: pytest.MonkeyPatch,
):
    """`'0'` resolves to the number `0`, not to the truthy string `'0'`."""
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "3")
    assert ROW_SAMPLE.resolve(None) == 3

    monkeypatch.setenv(_ROW_SAMPLE_ENV, "0")
    assert ROW_SAMPLE.resolve(None) == 0


def test_a_count_turned_off_by_an_argument_is_off_rather_than_unset(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "5")

    assert ROW_SAMPLE.resolve(0) == 0


def test_a_count_rejects_a_negative_from_the_argument():
    with pytest.raises(InvalidSettingError) as raised:
        MAX_FAILURE_SAMPLES.resolve(-1)
    message = str(raised.value)

    assert (
        "Setting `max_failure_samples` got '-1' from the `max_failure_samples=` argument"
        in message
    )
    assert "non-negative integers" in message


def test_a_count_rejects_a_negative_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "-1")

    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(None)

    assert f"got '-1' from the environment variable {_ROW_SAMPLE_ENV}" in str(
        raised.value
    )


def test_a_count_rejects_a_word_the_environment_cannot_read_as_a_number(
    monkeypatch: pytest.MonkeyPatch,
):
    """`resolve` raises the same error for a word as for a negative number."""
    monkeypatch.setenv(_ROW_SAMPLE_ENV, "five")

    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(None)
    message = str(raised.value)

    assert "row_sample" in message
    assert "'five'" in message
    assert "non-negative integers" in message


def test_a_count_rejects_a_fraction():
    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(_FRACTION)

    assert "'2.5'" in str(raised.value)


def test_a_count_rejects_a_bool():
    """The setting rejects `True` even though `bool` subclasses `int`."""
    with pytest.raises(InvalidSettingError) as raised:
        ROW_SAMPLE.resolve(_YES)

    assert "'True'" in str(raised.value)


def test_a_directory_reads_the_environment_source_as_the_path_it_spells(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_QUARANTINE_DIR_ENV, "/scratch/quarantine")

    assert QUARANTINE_DIR.resolve(None) == "/scratch/quarantine"
    assert QUARANTINE_DIR.resolve("/mnt/volume") == "/mnt/volume"


def test_a_directory_rejects_an_empty_value_rather_than_reading_it_as_unset(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_QUARANTINE_DIR_ENV, "   ")

    with pytest.raises(InvalidSettingError) as raised:
        QUARANTINE_DIR.resolve(None)
    message = str(raised.value)

    assert f"got '   ' from the environment variable {_QUARANTINE_DIR_ENV}" in message
    assert "filesystem paths" in message


def test_a_directory_rejects_something_that_is_not_a_path():
    """The setting rejects a `Path` object, because it holds a string, like its environment variable."""
    with pytest.raises(InvalidSettingError) as raised:
        QUARANTINE_DIR.resolve(_PATH_OBJECT)

    # This test does not assert the source: it names a `quarantine_dir=` argument, which `dd.asset` does not take.
    assert "Setting `quarantine_dir` got '/scratch'" in str(raised.value)


def test_a_value_outside_the_allowed_values_raises_from_the_environment(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_GRANULARITY_ENV, "per_column")

    with pytest.raises(InvalidSettingError) as raised:
        CHECK_GRANULARITY.resolve(None)

    assert f"got 'per_column' from the environment variable {_GRANULARITY_ENV}" in str(
        raised.value
    )


def test_a_value_outside_the_allowed_values_raises_from_the_default_source():
    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN.resolve(None)

    assert "got 'per_column' from the package default" in str(raised.value)

    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN_COUNT.resolve(None)

    assert "got '-1' from the package default" in str(raised.value)

    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN_FLAG.resolve(None)

    assert "got 'false' from the package default" in str(raised.value)

    with pytest.raises(InvalidSettingError) as raised:
        _BROKEN_DIRECTORY.resolve(None)

    assert "got '' from the package default" in str(raised.value)


def test_the_error_names_the_setting_the_value_and_the_source_order():
    with pytest.raises(InvalidSettingError) as raised:
        CHECK_GRANULARITY.resolve(_WRONG)
    message = str(raised.value)

    assert (
        "Setting `check_granularity` got 'per_column' from the `check_granularity=` argument"
        in message
    )
    assert "'rule', 'column', 'schema'" in message
    assert (
        "It is read from three sources, each overriding the one before: the package default,"
        in message
    )
    assert f"then the environment variable {_GRANULARITY_ENV}," in message
    assert "then the `check_granularity=` argument." in message


def test_the_one_setting_with_no_argument_names_two_sources_and_no_argument(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_QUARANTINE_DIR_ENV, "")

    with pytest.raises(InvalidSettingError) as raised:
        QUARANTINE_DIR.resolve(None)
    message = str(raised.value)

    assert (
        "It is read from two sources, each overriding the one before: the package default,"
        in message
    )
    assert f"then the environment variable {_QUARANTINE_DIR_ENV}." in message
    assert "There is no `quarantine_dir=` argument." in message


def test_the_schema_rules_environment_variable_changes_the_check_specs(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_SCHEMA_RULES_ENV, "per_rule")

    # `schema_rules` has no effect at the other granularities.
    names = [
        spec.name
        for spec in check_specs(Orders, asset="orders", check_granularity="column")
    ]

    assert "dy_rule__primary_key" in names
    assert "dy_schema__rules" not in names


def test_a_schema_rules_value_outside_the_allowed_values_raises(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv(_SCHEMA_RULES_ENV, "per_column")

    with pytest.raises(InvalidSettingError) as raised:
        check_specs(Orders, asset="orders", check_granularity="column")
    message = str(raised.value)

    assert (
        f"Setting `schema_rules` got 'per_column' from the environment variable {_SCHEMA_RULES_ENV}"
        in message
    )
    assert "'collapsed', 'per_rule'" in message
