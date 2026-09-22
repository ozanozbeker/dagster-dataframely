"""The settings, each resolved from three sources: the package default, then `DAGSTER_DATAFRAMELY_*`, then the argument.

`_asset.py` resolves every setting where you declare the asset, and the `wiring` functions resolve the ones they use.
There is no `set_default_*()` function, because Dagster loads code locations lazily and the default would depend on import order.
"""

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal, cast

from dagster_dataframely.errors import InvalidSettingError

type Granularity = Literal["rule", "column", "schema"]
"""How many asset checks report a schema's rules: one per rule at `rule`, one per column with rules at `column`, and one for the schema at `schema`."""

type SchemaRules = Literal["collapsed", "per_rule"]
"""Which checks report the schema-level rules at `column` granularity.

`collapsed` puts them all in one check, `dy_schema__rules`, and `per_rule` gives each its own check. A schema-level rule is a `@dy.rule()` or the primary key, including a single-column key. At `rule` and `schema` granularity this setting has no effect."""


@dataclass(frozen=True)
class _Setting[T]:
    """One setting and the three sources it reads, in order."""

    name: str
    default: T
    allowed: Sequence[str] | str
    accepts: Callable[[object], bool]
    parse: Callable[[str], object] = str
    takes_argument: bool = True

    @property
    def env_var(self) -> str:
        """The setting's environment variable."""
        return f"DAGSTER_DATAFRAMELY_{self.name.upper()}"

    def resolve(self, argument: T | None) -> T:
        """Return the setting's value, validated."""
        if argument is not None:
            return self._checked(argument, f"the `{self.name}=` argument")
        word: str | None = os.environ.get(self.env_var)
        if word is not None:
            return self._checked(
                self.parse(word), f"the environment variable {self.env_var}"
            )
        return self._checked(self.default, "the package default")

    def _checked(self, value: object, source: str) -> T:
        """Return `value` if `accepts` passes it, else raise."""
        if not self.accepts(value):
            raise InvalidSettingError(
                self.name,
                str(value),
                self.allowed,
                source=source,
                env_var=self.env_var,
                takes_argument=self.takes_argument,
            )
        return cast("T", value)


_GRANULARITIES: tuple[Granularity, ...] = ("rule", "column", "schema")
_SCHEMA_RULES: tuple[SchemaRules, ...] = ("collapsed", "per_rule")

_FLAG_WORDS = {"true": True, "false": False}


def _flag(word: str) -> object:
    """Return the word as a `bool`, or unchanged so the error shows it."""
    return _FLAG_WORDS.get(word.lower(), word)


def _count(word: str) -> object:
    """Return the word as an `int`, or unchanged."""
    try:
        return int(word)
    except ValueError:
        return word


def _non_negative(value: object) -> bool:
    """Accept a non-negative `int`, excluding `bool`, which subclasses it."""
    return type(value) is int and value >= 0


def _path(value: object) -> bool:
    """Accept a non-blank path, or `None` for no directory.

    It rejects a blank value instead of reading it as unset, because `${SCRATCH}` expands to one when nothing sets `SCRATCH`.
    """
    return value is None or (isinstance(value, str) and bool(value.strip()))


CHECK_GRANULARITY = _Setting[Granularity](
    name="check_granularity",
    default="rule",
    allowed=_GRANULARITIES,
    accepts=_GRANULARITIES.__contains__,
)

SCHEMA_RULES = _Setting[SchemaRules](
    name="schema_rules",
    default="collapsed",
    allowed=_SCHEMA_RULES,
    accepts=_SCHEMA_RULES.__contains__,
)

STATISTICS = _Setting[bool](
    name="statistics",
    default=True,
    allowed=tuple(_FLAG_WORDS),
    accepts=lambda value: type(value) is bool,
    parse=_flag,
)

MAX_FAILURE_SAMPLES = _Setting[int](
    name="max_failure_samples",
    default=5,
    allowed="non-negative integers",
    accepts=_non_negative,
    parse=_count,
)

ROW_SAMPLE = _Setting[int](
    name="row_sample",
    default=5,
    allowed="non-negative integers",
    accepts=_non_negative,
    parse=_count,
)

QUARANTINE_DIR = _Setting[str | None](
    name="quarantine_dir",
    default=None,
    allowed="filesystem paths",
    accepts=_path,
    takes_argument=False,
)
"""`dd.asset` takes no argument for it, because a warehouse stores tables, not files (ADR-0006)."""
