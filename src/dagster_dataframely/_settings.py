"""Every setting resolves in order: the package default, then a `DAGSTER_DATAFRAMELY_*` environment variable, then the argument on the asset.

A platform engineer sets a house style once for a whole code location, and an asset overrides it where that style is wrong. The environment variables are named for the package because they are machine configuration a deployment sets, and `DAGSTER_DATAFRAMELY_` is long enough that nothing else will claim it.

The chain validates on resolve, whichever source supplied the value, the package's own included. Nothing here trusts a value because of where it came from, so a typo raises at the source that wrote it instead of quietly becoming something else three modules later.

A setting is one of four shapes.

- A `_Choice` holds a closed vocabulary of strings, so resolving is validating and nothing parses.
- A `_Flag` holds a `bool`, and it is the first shape that has to parse. The environment variable arrives as a string whatever the shape holds, and only a flag's other two sources hold something that is not one.
- A `_Count` holds a non-negative `int` and parses for the same reason. Its vocabulary is a range rather than a list, so it is the one shape with something left to refuse after a type checker has narrowed a source.
- A `_Directory` holds a filesystem path, the shape with no vocabulary at all. Every string names a legal directory, so all that is left to check is that one was written.

There is deliberately no fourth source and no `set_default_*()` function. Dagster loads code locations lazily, so "has the default been set yet" would depend on an import order the user does not control, and the same asset would derive different checks depending on which module happened to be imported first.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, override

from dagster_dataframely.errors import InvalidSettingError

#: How many asset checks a schema's rules collapse into.
type Granularity = Literal["rule", "column", "schema"]

#: Where the rules that no single column owns land at `column` granularity.
type MultiColumnRules = Literal["schema", "per_rule"]


@dataclass(frozen=True)
class _Setting[T](ABC):
    """One setting and the three sources it resolves through.

    The precedence lives here and nowhere else, so a setting of a new shape cannot come to read its sources in a different order. A shape decides only how a source's value is checked, and which source has to be read out of a string.

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
        """How an error names the environment variable. Every shape refuses something from it, so the phrase is written once."""
        return f"the environment variable {self.env_var}"

    def resolve(self, argument: T | None) -> T:
        """Resolve the setting through the three sources, validating the one that supplied the value.

        Parameters
        ----------
        argument
            What the caller passed, or `None` for a caller that passed nothing. `None` is the whole test for "unset". That is why every setting on the decorator defaults to `None` rather than to the value the package ships, and why a flag a caller turned off reads as off rather than as unset.

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

    def _checked(self, value: T, source: str) -> T:  # noqa: ARG002 - the source is for whichever shape has something to refuse
        """Validate a value that arrived as the setting's own type.

        Both sources that do are already inside the type a setting holds, so a shape whose values are Python values has nothing to check here. A shape with a vocabulary of its own overrides this.
        """
        return value

    @abstractmethod
    def _environment(self, value: str) -> T:
        """Read the one source that arrives as a string whatever the shape holds."""


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
        # Nothing parses: the vocabulary is strings, so matching it is the whole check.
        return self._checked(value, self._environment_source)

    @override
    def _checked(self, value: str, source: str) -> T:
        """Return the vocabulary's own member rather than the value that matched it, which carries the literal type out without a cast.

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


#: What the environment variable spells a flag's two values as. Case is not part of the vocabulary: `TRUE` in a deployment's environment is the same instruction as `true`, and refusing it would buy nothing.
_FLAG_WORDS = {"true": True, "false": False}


@dataclass(frozen=True)
class _Flag(_Setting[bool]):
    """One two-valued setting.

    Its own shape rather than a `_Choice` over `('true', 'false')`, because the two sources that are not the environment carry a real `bool`. The argument on the asset is typed `bool | None`, and the package default is a value rather than a word. Only the environment has to be read as one, and that is what this shape is.
    """

    @override
    def _environment(self, value: str) -> bool:
        """Read a word as the value it stands for.

        Raises
        ------
        InvalidSettingError
            The word is neither of the two. `1`, `yes` and `on` are all plausible and all wrong, so the vocabulary stays closed and the error names it.
        """
        parsed: bool | None = _FLAG_WORDS.get(value.lower())
        if parsed is None:
            raise self._rejected(value, self._environment_source)
        return parsed

    @override
    def _checked(self, value: bool, source: str) -> bool:
        """Refuse everything a `bool` annotation does not.

        The annotation alone is not enough, and this is the shape where trusting it fails silently rather than loudly. `statistics="false"` is a non-empty string, so an unchecked argument resolves to the word and turns the pass *on*, the opposite of what was written. The environment variable spells the same instruction exactly that way, which makes the mistake reachable rather than hypothetical.

        Raises
        ------
        InvalidSettingError
            The value is not a `bool`.
        """
        if type(value) is not bool:
            raise self._rejected(value, source)
        return value

    def _rejected(self, value: object, source: str) -> InvalidSettingError:
        """Build the error every source raises, so no two of them can word the same refusal differently."""
        return InvalidSettingError(
            self.name,
            str(value),
            tuple(_FLAG_WORDS),
            source=source,
            env_var=self.env_var,
        )


#: What a count accepts, worded rather than listed: the vocabulary is a range, so there is nothing to enumerate.
_COUNT_VOCABULARY = "non-negative integers"


@dataclass(frozen=True)
class _Count(_Setting[int]):
    """One setting over the non-negative integers.

    Its own shape rather than a `_Choice` over the numbers somebody might write, because there is no such list. It is also the one shape a type checker cannot finish. `_Choice` narrows an argument to its vocabulary and `_Flag` to two values, while `int` is only half of what this setting accepts and the other half is checked here.
    """

    @override
    def _environment(self, value: str) -> int:
        """Read a word as the number it spells.

        Raises
        ------
        InvalidSettingError
            The word is not a number, or is a negative one. Both are the same mistake to a deployment reading the failure, so they raise the same error.
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
        # `type` rather than `isinstance`, because `bool` subclasses `int`: a setting confused with `statistics` would otherwise resolve `True` to one row and say nothing.
        if type(value) is not int or value < 0:
            raise self._rejected(value, source)
        return value

    def _rejected(self, value: object, source: str) -> InvalidSettingError:
        """Build the error every source raises, so no two of them can word the same refusal differently."""
        return InvalidSettingError(
            self.name,
            str(value),
            _COUNT_VOCABULARY,
            source=source,
            env_var=self.env_var,
        )


#: What a directory accepts, worded rather than listed, like a count's: every string names a legal directory, so there is nothing to enumerate.
_DIRECTORY_VOCABULARY = "filesystem paths"


@dataclass(frozen=True)
class _Directory(_Setting[str | None]):
    """One setting over the filesystem paths, and over the absence of one.

    Its own shape rather than a `_Choice`, because a path has no vocabulary. The whole point is that nothing here knows which directories a deployment has. It is also the shape with the least to do, since the environment variable already arrives as what the setting holds.

    It is the one setting whose package default is `None`, and that reads as "wherever `tempfile` puts things" rather than as unset. The deferral is deliberate. The decorator resolves every setting where the asset is *declared*, so a default of `tempfile.gettempdir()` would bake the code location's temp directory into an asset whose frames are staged on a worker.
    """

    @override
    def _environment(self, value: str) -> str | None:
        # Nothing parses: a path is a string from every source, because the environment variable can spell it no other way.
        return self._checked(value, self._environment_source)

    @override
    def _checked(self, value: str | None, source: str) -> str | None:
        """Refuse everything that is not a written path.

        An empty value raises rather than reading as unset, which is the one decision in this shape worth arguing. `DAGSTER_DATAFRAMELY_TEMP_DIR=${SCRATCH}` in a deployment whose `SCRATCH` never got set arrives empty, and reading that as unset would stage the frame on the ephemeral disk the setting was set to move it off. That failure is silent, and the disk it fills is the one the pod dies on.

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


#: How many checks a schema's rules become. Definition-time: see `dy_asset` for what changing it costs a check's history.
CHECK_GRANULARITY = _Choice[Granularity](
    name="check_granularity", default="rule", allowed=("rule", "column", "schema")
)

#: Where the rules no single column owns land at `column` granularity. Read nowhere else, because the other two granularities have no second place to put them.
MULTI_COLUMN_RULES = _Choice[MultiColumnRules](
    name="multi_column_rules", default="schema", allowed=("schema", "per_rule")
)

#: Whether a materialization carries the four statistics tables. On by default: a distribution read is what a data consumer opens an asset for, and the pass is one aggregate per family over a frame that is already in memory. Whoever is paying for that pass is the one who can turn it off.
STATISTICS = _Flag(name="statistics", default=True)

#: How many of the rows that failed a rule reach that rule's check metadata. On by default, because what three of the failing rows look like is the question a red check raises and the counts cannot answer. It is deliberately not the `statistics` setting: consenting to summary statistics is not consenting to raw values.
MAX_FAILURE_SAMPLES = _Count(name="max_failure_samples", default=5)

#: How many of the valid rows a materialization carries. On by default on the same terms, and separate from the failure sample for the same reason the two are separate from `statistics`: seeing what failed and seeing what was kept are different consents.
ROW_SAMPLE = _Count(name="row_sample", default=5)

#: Which disk a lazy plan is staged on: `dy_asset` stages a lazy return before validating it. Unset is the system temp directory, which in a container is its ephemeral disk, and that is the whole reason the setting exists: a staged frame bigger than what the pod has spare fills it.
TEMP_DIR = _Directory(name="temp_dir", default=None)

#: Where a quarantine goes when no IO manager places it, which is direct invocation. The one setting with two sources rather than three: `dy_asset` takes no argument for it, because a directory is meaningless to a warehouse and ADR-0006 defers the override until somebody asks for one. Unset, a quarantined asset that reaches `file_writer` raises rather than choosing a directory on the operator's behalf.
QUARANTINE_DIR = _Directory(name="quarantine_dir", default=None)
