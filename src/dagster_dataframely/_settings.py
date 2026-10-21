"""Each setting is a `_Setting`, whose `resolve` returns the argument, else `DAGSTER_DATAFRAMELY_*`, else the package default.

Each public function resolves the settings it uses when called, and the writer that `build_error_rows_writer` returns resolves `error_rows_dir` again when it writes (#115).
There is no `set_default_*()` function, because Dagster loads code locations lazily and the default would depend on import order (`docs/out-of-scope/dbt-test-configs.md#severity-store_failures-and-schema-in-dbt_projectyml`).
"""

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal, cast

from dagster_dataframely.errors import InvalidSettingError

type Granularity = Literal["rule", "column"]
"""How many asset checks report a schema's rules: one per rule at `rule`, and at `column` one per column with rules plus one per schema-level rule."""


@dataclass(frozen=True)
class _Setting[T]:
    """One setting and its sources."""

    name: str
    default: T
    allowed: Sequence[str] | str
    accepts: Callable[[object], bool]
    argument_allowed: Sequence[str] | str | None = None
    parse: Callable[[str], object] = str
    takes_argument: bool = True

    @property
    def env_var(self) -> str:
        """The setting's environment variable."""
        return f"DAGSTER_DATAFRAMELY_{self.name.upper()}"

    @property
    def sources(self) -> str:
        """The sentence that names every source the setting reads, for `InvalidSettingError`."""
        if self.takes_argument:
            return f"The package reads it from three sources, each overriding the one before: the package default, then the environment variable {self.env_var}, then the `{self.name}=` argument."
        return f"The package reads it from two sources, each overriding the one before: the package default, then the environment variable {self.env_var}. There is no `{self.name}=` argument."

    def resolve(self, argument: T | None) -> T:
        """Return the setting's value, validated."""
        if argument is not None:
            return self._check(
                argument, f"the `{self.name}=` argument", self.argument_allowed
            )
        word: str | None = os.environ.get(self.env_var)
        if word is not None:
            return self._check(
                self.parse(word), f"the environment variable {self.env_var}"
            )
        return self._check(self.default, "the package default")

    def _check(
        self,
        value: object,
        source: str,
        allowed: Sequence[str] | str | None = None,
    ) -> T:
        """Return `value` if `accepts` passes it, else raise."""
        if not self.accepts(value):
            raise InvalidSettingError(
                self.name,
                value,
                allowed or self.allowed,
                source=source,
                sources=self.sources,
            )
        return cast("T", value)


_GRANULARITIES: tuple[Granularity, ...] = ("rule", "column")

_FLAG_WORDS = {"true": True, "false": False}


def _parse_flag(word: str) -> object:
    """Return the word as a `bool`, or unchanged so the error shows it."""
    return _FLAG_WORDS.get(word.lower(), word)


def _parse_count(word: str) -> object:
    """Return the word as an `int`, or unchanged."""
    try:
        return int(word)
    except ValueError:
        return word


def _is_non_negative(value: object) -> bool:
    """Accept a non-negative `int`, excluding `bool`, which subclasses it."""
    return type(value) is int and value >= 0


def _is_path(value: object) -> bool:
    """Accept a non-blank path, or `None` for no directory.

    It rejects a blank value instead of reading it as unset, because `${SCRATCH}` expands to an empty string when nothing sets `SCRATCH`.
    """
    return value is None or (isinstance(value, str) and bool(value.strip()))


CHECK_GRANULARITY = _Setting[Granularity](
    name="check_granularity",
    default="rule",
    allowed=_GRANULARITIES,
    accepts=_GRANULARITIES.__contains__,
)

STATISTICS = _Setting[bool](
    name="statistics",
    default=True,
    allowed=tuple(_FLAG_WORDS),
    accepts=lambda value: type(value) is bool,
    argument_allowed="True or False",
    parse=_parse_flag,
)

MAX_FAILURE_EXAMPLES = _Setting[int](
    name="max_failure_examples",
    default=5,
    allowed="non-negative integers",
    accepts=_is_non_negative,
    parse=_parse_count,
)

ROW_SAMPLE = _Setting[int](
    name="row_sample",
    default=5,
    allowed="non-negative integers",
    accepts=_is_non_negative,
    parse=_parse_count,
)

ERROR_ROWS_DIR = _Setting[str | None](
    name="error_rows_dir",
    default=None,
    allowed="non-blank filesystem paths",
    accepts=_is_path,
    takes_argument=False,
)
"""`dd.asset` takes no argument for it, because the package writes under it only in a direct invocation (`docs/out-of-scope/error-rows-dir-argument.md`)."""
