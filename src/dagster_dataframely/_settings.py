"""Every setting resolves in order: the package default, then a `DAGSTER_DATAFRAMELY_*` environment variable, then the argument on the asset.

A platform engineer sets a house style once for a code location. An asset overrides it where that style is wrong. The environment variables carry the package name because a deployment sets them, and `DAGSTER_DATAFRAMELY_` is long enough that nothing else claims it.

Every source validates on resolve, the package default included. A typo raises at the source that wrote it instead of becoming something else three modules later.

A setting is one of four subclasses.

- A `_Choice` holds a closed vocabulary of strings. Resolving is validating; nothing parses.
- A `_Flag` holds a `bool`. The environment variable arrives as a string whatever the setting holds, so a flag parses that one source.
- A `_Count` holds a non-negative `int` and parses for the same reason. Its vocabulary is a range, so it is the one subclass with something left to refuse after a type checker has narrowed a source.
- A `_Directory` holds a filesystem path and has no vocabulary. Every string names a legal directory, so the only check is that one was written.

There is no fourth source and no `set_default_*()` function. Dagster loads code locations lazily, so "has the default been set yet" would depend on an import order the user does not control. The same asset would derive different checks depending on which module imported first.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, override

from dagster_dataframely.errors import InvalidSettingError

type Granularity = Literal["rule", "column", "schema"]
"""How many asset checks a schema's rules collapse into."""

type MultiColumnRules = Literal["schema", "per_rule"]
"""Where the rules that no single column owns land at `column` granularity."""


@dataclass(frozen=True)
class _Setting[T](ABC):
    """One setting and the three sources it resolves through.

    The precedence lives here and nowhere else, so a new subclass cannot read its sources in a different order. A subclass decides only how a source's value is checked, and which source has to be read out of a string.

    Attributes
    ----------
    name
        The setting's name. It is also the argument's name and the suffix of its environment variable, so the three cannot drift.
    default
        The value the package ships.
    """

    name: str
    default: T

    @property
    def env_var(self) -> str:
        """The environment variable this setting reads."""
        return f"DAGSTER_DATAFRAMELY_{self.name.upper()}"

    @property
    def _environment_source(self) -> str:
        """How an error names the environment variable. Written once because every subclass refuses something from it."""
        return f"the environment variable {self.env_var}"

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
        environment: str | None = os.environ.get(self.env_var)
        if environment is not None:
            return self._environment(environment)
        return self._checked(self.default, "the package default")

    def _checked(self, value: T, source: str) -> T:  # noqa: ARG002 - the source is for whichever subclass has something to refuse
        """Validate a value that arrived as the setting's own type.

        The argument and the package default are already inside the setting's type, so a subclass whose values are Python values has nothing to check here. A subclass with a vocabulary of its own overrides this.
        """
        return value

    @abstractmethod
    def _environment(self, value: str) -> T:
        """Read the one source that arrives as a string whatever the setting holds."""


@dataclass(frozen=True)
class _Choice[T: str](_Setting[T]):
    """One setting and the vocabulary it resolves against.

    Attributes
    ----------
    allowed
        The whole vocabulary, in the order the docs list it.
    """

    allowed: tuple[T, ...]

    @override
    def _environment(self, value: str) -> T:
        # Nothing parses: the vocabulary is strings, so matching is the only check.
        return self._checked(value, self._environment_source)

    @override
    def _checked(self, value: str, source: str) -> T:
        """Return the vocabulary member the value matched.

        Returning the member rather than the value carries the literal type out without a cast.

        Parameters
        ----------
        value
            The value to check.
        source
            Where it came from, worded as a phrase for the error message.

        Raises
        ------
        InvalidSettingError
            The value is outside the vocabulary.
        """
        for allowed in self.allowed:
            if value == allowed:
                return allowed
        raise InvalidSettingError(
            self.name, value, self.allowed, source=source, env_var=self.env_var
        )


_FLAG_WORDS = {"true": True, "false": False}
"""The two words the environment variable spells a flag as. Case does not matter: `TRUE` is the same instruction as `true`, and refusing it buys nothing."""


@dataclass(frozen=True)
class _Flag(_Setting[bool]):
    """One two-valued setting.

    Its own subclass rather than a `_Choice` over `('true', 'false')`, because the argument and the package default carry a real `bool`. Only the environment has to be read as a word.
    """

    @override
    def _environment(self, value: str) -> bool:
        """Read a word as the value it stands for.

        Raises
        ------
        InvalidSettingError
            The word is neither `true` nor `false`. `1`, `yes` and `on` are plausible and wrong, so the vocabulary stays closed and the error names it.
        """
        parsed: bool | None = _FLAG_WORDS.get(value.lower())
        if parsed is None:
            raise self._rejected(value, self._environment_source)
        return parsed

    @override
    def _checked(self, value: bool, source: str) -> bool:
        """Refuse everything a `bool` annotation does not.

        Trusting the annotation fails silently here. `statistics="false"` is a non-empty string, so an unchecked argument turns the pass on, the opposite of what was written. The environment variable spells the same instruction that way, so the mistake is reachable.

        Raises
        ------
        InvalidSettingError
            The value is not a `bool`.
        """
        if type(value) is not bool:
            raise self._rejected(value, source)
        return value

    def _rejected(self, value: object, source: str) -> InvalidSettingError:
        """Build the error every source raises, so no two sources word the same refusal differently."""
        return InvalidSettingError(
            self.name,
            str(value),
            tuple(_FLAG_WORDS),
            source=source,
            env_var=self.env_var,
        )


_COUNT_VOCABULARY = "non-negative integers"
"""What a count accepts, worded because a range has nothing to enumerate."""


@dataclass(frozen=True)
class _Count(_Setting[int]):
    """One setting over the non-negative integers.

    Its own subclass because no list of the numbers somebody might write exists. It is also the one subclass a type checker cannot finish: `int` narrows the type, and this subclass refuses the negatives.
    """

    @override
    def _environment(self, value: str) -> int:
        """Read a word as the number it spells.

        Raises
        ------
        InvalidSettingError
            The word is not a number, or is negative. Both are the same mistake to a deployment reading the failure, so they raise the same error.
        """
        try:
            count = int(value)
        except ValueError:
            raise self._rejected(value, self._environment_source) from None
        return self._checked(count, self._environment_source)

    @override
    def _checked(self, value: int, source: str) -> int:
        """Refuse everything an `int` annotation does not.

        Raises
        ------
        InvalidSettingError
            The value is negative, or is not an `int` at all.
        """
        # `type`, not `isinstance`: `bool` subclasses `int`, so a `True` meant for `statistics` would otherwise resolve to one row and say nothing.
        if type(value) is not int or value < 0:
            raise self._rejected(value, source)
        return value

    def _rejected(self, value: object, source: str) -> InvalidSettingError:
        """Build the error every source raises, so no two sources word the same refusal differently."""
        return InvalidSettingError(
            self.name,
            str(value),
            _COUNT_VOCABULARY,
            source=source,
            env_var=self.env_var,
        )


_DIRECTORY_VOCABULARY = "filesystem paths"
"""What a directory accepts, worded like a count's: every string names a legal directory, so there is nothing to enumerate."""


@dataclass(frozen=True)
class _Directory(_Setting[str | None]):
    """One setting over the filesystem paths, or the absence of one.

    Its own subclass because a path has no vocabulary: nothing here knows which directories a deployment has. It is also the subclass with the least to do, since the environment variable already arrives as what the setting holds.

    It is the one subclass whose package default is `None`. `None` reads as unset, not as a directory chosen on the operator's behalf: the code that needs one raises when nobody named it.
    """

    @override
    def _environment(self, value: str) -> str | None:
        # Nothing parses: a path is a string from every source.
        return self._checked(value, self._environment_source)

    @override
    def _checked(self, value: str | None, source: str) -> str | None:
        """Refuse everything that is not a written path.

        An empty value raises rather than reading as unset. `DAGSTER_DATAFRAMELY_QUARANTINE_DIR=${SCRATCH}` in a deployment whose `SCRATCH` never got set arrives empty. Reading that as unset would report a setting nobody wrote when somebody wrote one wrong. The refusal names the variable, so the fix lands where the mistake is.

        Raises
        ------
        InvalidSettingError
            The value is not a string, or holds nothing but whitespace.
        """
        if value is None:
            return None
        if type(value) is not str or not value.strip():
            raise InvalidSettingError(
                self.name,
                value,
                _DIRECTORY_VOCABULARY,
                source=source,
                env_var=self.env_var,
            )
        return value


CHECK_GRANULARITY = _Choice[Granularity](
    name="check_granularity", default="rule", allowed=("rule", "column", "schema")
)
"""How many checks a schema's rules become. Definition-time: see `dy_asset` for what changing it costs a check's history."""

MULTI_COLUMN_RULES = _Choice[MultiColumnRules](
    name="multi_column_rules", default="schema", allowed=("schema", "per_rule")
)
"""Where the rules no single column owns land at `column` granularity. The other two granularities have no second place to put them, so nothing else reads it."""

STATISTICS = _Flag(name="statistics", default=True)
"""Whether a materialization carries the four statistics tables. On by default: a data consumer opens an asset to read its distribution, and the pass is one aggregate per family over a frame already in memory. Whoever pays for that pass can turn it off."""

MAX_FAILURE_SAMPLES = _Count(name="max_failure_samples", default=5)
"""How many of the rows that failed a rule reach that rule's check metadata. On by default, because a failing check asks what the rows that failed look like and the counts cannot answer. Separate from `statistics`: consenting to summary statistics is not consenting to raw values."""

ROW_SAMPLE = _Count(name="row_sample", default=5)
"""How many of the valid rows a materialization carries. On by default on the same terms. Separate from the failure sample and from `statistics`: seeing what failed and seeing what was kept are different consents."""

QUARANTINE_DIR = _Directory(name="quarantine_dir", default=None)
"""Where a quarantine goes when no IO manager places it, which is direct invocation. The one setting with two sources: `dy_asset` takes no argument for it, because a directory is meaningless to a warehouse and ADR-0006 defers the override until somebody asks for one. Unset, a quarantined asset that reaches `file_writer` raises rather than choosing a directory on the operator's behalf."""
