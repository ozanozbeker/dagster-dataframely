"""Every setting resolves in order: the package default, then a `DAGSTER_DATAFRAMELY_*` environment variable, then the argument on the asset.

Every source validates on resolve, the package default included. A typo raises at the source that wrote it instead of becoming something else three modules later.

One class, not one per kind of value. A setting differs from the next in two places: how the environment variable's word becomes a value, and which values it accepts. Both are callables on the instance, so the precedence is written once and a new setting is a few lines of data.

There is no fourth source and no `set_default_*()` function. Dagster loads code locations lazily, so "has the default been set yet" would depend on an import order the user does not control. The same asset would derive different checks depending on which module imported first.
"""

import os
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Literal, cast

from dagster_dataframely.errors import InvalidSettingError

type Granularity = Literal["rule", "column", "schema"]
"""How many asset checks a schema's rules collapse into."""

type SchemaRules = Literal["collapsed", "per_rule"]
"""Where a schema-level rule lands at `column` granularity.

Dataframely's own word: it warns about "Schema-level rules" and keeps them in `_schema_validation_rules()`, apart from the `column_rules` it builds as `f"{col_name}|{rule_name}"`. `primary_key` joins them here because Dataframely builds it at schema level too, and a single-column key is still not a column rule."""


@dataclass(frozen=True)
class _Setting[T]:
    """One setting and the three sources it resolves through.

    Attributes
    ----------
    name
        The setting's name. It is also the argument's name and the suffix of its environment variable, so the three cannot drift.
    default
        The value the package ships.
    allowed
        What the setting accepts, for the error. A closed set arrives as its members, in the order the docs list them. A setting over a range arrives as a phrase, because every value it accepts cannot be printed.
    accepts
        Whether a value from any source is one the setting holds. The argument and the package default arrive as values a type checker has already narrowed, and this still runs over them: `statistics="false"` is a non-empty string that would otherwise turn the pass on, and `True` is an `int` that would resolve a count to one row.
    parse
        How the environment variable's word becomes a value. The word itself unless the setting holds something other than a string. A word that spells nothing comes back as it is, so `accepts` refuses it and the error quotes what was written.
    takes_argument
        Whether `dd.asset` declares a parameter for this setting. Only `quarantine_dir` does not, and the error has to say so rather than name an argument that raises `TypeError`.

        `resolve` keeps its argument branch either way, and for such a setting nothing can reach it: the decorator calls `QUARANTINE_DIR.resolve(None)` for the refusal alone, and the writer calls it the same way. A branch that cannot run needs no second wording, so the phrase it would build is left as it is rather than made to agree with a chain nobody will see beside it.
    """

    name: str
    default: T
    allowed: Sequence[str] | str
    accepts: Callable[[object], bool]
    parse: Callable[[str], object] = str
    takes_argument: bool = True

    @property
    def env_var(self) -> str:
        """The environment variable this setting reads."""
        return f"DAGSTER_DATAFRAMELY_{self.name.upper()}"

    def resolve(self, argument: T | None) -> T:
        """Resolve the setting through the three sources, validating the one that supplied the value.

        Parameters
        ----------
        argument
            What the caller passed, or `None` for nothing. `None` alone means unset. So every setting on the decorator defaults to `None` rather than to the package value, and a flag a caller turned off reads as off, not as unset.

        Returns
        -------
        The resolved value.

        Raises
        ------
        InvalidSettingError
            The value is outside what the setting accepts, from whichever source supplied it.
        """
        if argument is not None:
            return self._checked(argument, f"the `{self.name}=` argument")
        word: str | None = os.environ.get(self.env_var)
        if word is not None:
            return self._checked(
                self.parse(word), f"the environment variable {self.env_var}"
            )
        return self._checked(self.default, "the package default")

    def _checked(self, value: object, source: str) -> T:
        """Return the value `accepts` passed, or refuse it naming its source."""
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
"""The two words the environment variable spells a flag as. Case does not matter: `TRUE` is the same instruction as `true`, and refusing it buys nothing. `1`, `yes` and `on` are plausible and wrong, so the allowed values stay closed and the error names them."""


def _flag(word: str) -> object:
    """Read a word as the flag it spells."""
    return _FLAG_WORDS.get(word.lower(), word)


def _count(word: str) -> object:
    """Read a word as the number it spells."""
    try:
        return int(word)
    except ValueError:
        return word


def _non_negative(value: object) -> bool:
    """Accept a non-negative `int`.

    `type`, not `isinstance`: `bool` subclasses `int`, so a `True` meant for `statistics` would otherwise resolve to one row and say nothing.
    """
    return type(value) is int and value >= 0


def _path(value: object) -> bool:
    """Accept a written path, or `None` for no path at all.

    An empty value is refused rather than read as unset. `DAGSTER_DATAFRAMELY_QUARANTINE_DIR=${SCRATCH}` in a deployment whose `SCRATCH` never got set arrives empty. Reading that as unset would report a setting nobody wrote when somebody wrote one wrong. The refusal names the variable, so the fix lands where the mistake is.
    """
    return value is None or (isinstance(value, str) and bool(value.strip()))


CHECK_GRANULARITY = _Setting[Granularity](
    name="check_granularity",
    default="rule",
    allowed=_GRANULARITIES,
    accepts=_GRANULARITIES.__contains__,
)
"""How many checks a schema's rules become. Definition-time: see `dd.asset` for what changing it costs a check's history."""

SCHEMA_RULES = _Setting[SchemaRules](
    name="schema_rules",
    default="collapsed",
    allowed=_SCHEMA_RULES,
    accepts=_SCHEMA_RULES.__contains__,
)
"""Where a schema-level rule lands at `column` granularity. The other two granularities have no second place to put them, so nothing else reads it."""

STATISTICS = _Setting[bool](
    name="statistics",
    default=True,
    allowed=tuple(_FLAG_WORDS),
    accepts=lambda value: type(value) is bool,
    parse=_flag,
)
"""Whether a materialization carries the four statistics tables. On by default: a data consumer opens an asset to read its distribution, and the pass is one aggregate per family over a frame already in memory. Whoever pays for that pass can turn it off."""

MAX_FAILURE_SAMPLES = _Setting[int](
    name="max_failure_samples",
    default=5,
    allowed="non-negative integers",
    accepts=_non_negative,
    parse=_count,
)
"""How many of the rows that failed a rule reach that rule's check metadata. On by default, because a failing check asks what the rows that failed look like and the counts cannot answer. Separate from `statistics`: consenting to summary statistics is not consenting to raw values."""

ROW_SAMPLE = _Setting[int](
    name="row_sample",
    default=5,
    allowed="non-negative integers",
    accepts=_non_negative,
    parse=_count,
)
"""How many of the valid rows a materialization carries. On by default on the same terms. Separate from the failure sample and from `statistics`: seeing what failed and seeing what was kept are different consents."""

QUARANTINE_DIR = _Setting[str | None](
    name="quarantine_dir",
    default=None,
    allowed="filesystem paths",
    accepts=_path,
    takes_argument=False,
)
"""Where a quarantine goes when no IO manager places it, which is direct invocation. The one setting with two sources: `dd.asset` takes no argument for it, because a directory is meaningless to a warehouse and ADR-0006 defers the override until somebody asks for one. Unset, a call with invalid rows to write raises rather than choosing a directory on the operator's behalf. It is also the one setting whose package default is `None`, which means no directory rather than the absence of a setting, and the one whose resolved value the decorator drops: the rows decide whether a directory is needed at all, so the one a call writes under is read where the rows are written. The decorator resolves it anyway, for the refusal alone, so a malformed variable still fails where the asset is declared (#115)."""
