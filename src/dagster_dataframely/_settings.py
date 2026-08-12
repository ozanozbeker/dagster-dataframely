"""Three tiers for every setting: the package default, then a `DAGSTER_DATAFRAMELY_*` environment variable, then the argument on the asset.

A platform engineer sets a house style once for a whole code location, and an asset overrides it where that style is wrong. The environment variables are named for the package because they are machine surface a deployment sets, and `DAGSTER_DATAFRAMELY_` is long enough that nothing else will claim it.

The chain validates on resolve, at every tier including the package's own. Nothing here trusts a value because of where it came from, so a typo raises at the tier that wrote it instead of quietly becoming something else three modules later.

A setting is one of four shapes. A `_Choice` holds a closed vocabulary of strings, so resolving is validating and nothing parses. A `_Flag` holds a `bool`, and it is the first shape that has to parse: the environment tier arrives as a string whatever the shape holds, and only a flag's other two tiers hold something that is not one. A `_Count` holds a non-negative `int` and parses for the same reason, but its vocabulary is a range rather than a list, so it is the one shape with something left to reject after a type checker has narrowed a tier. A `_Directory` holds a filesystem path, which is the shape with no vocabulary at all: every string names a legal directory, so all that is left to check is that one was written.

There is deliberately no fourth tier and no `set_default_*()` function. Dagster loads code locations lazily, so "has the default been set yet" would depend on an import order the user does not control, and the same asset would derive different checks depending on which module happened to be imported first.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, override

from dagster_dataframely._errors import InvalidSettingError

#: How many asset checks a schema's rules collapse into.
Granularity = Literal["rule", "column", "schema"]

#: Where the rules that no single column owns land at `column` granularity.
MultiColumnRules = Literal["schema", "per_rule"]


@dataclass(frozen=True)
class _Setting[T](ABC):
    """One setting and the three tiers it resolves through.

    The precedence lives here and nowhere else, so a setting of a new shape cannot come to read its tiers in a different order. What a shape decides is only how a tier's value is checked, and which of them has to be read out of a string.

    Attributes:
        name: The setting's name. It is also the argument's name and the suffix of its environment variable, so the three cannot drift.
        default: The value the package ships.
    """

    name: str
    default: T

    @property
    def env_var(self) -> str:
        """The environment variable this setting reads."""
        return f"DAGSTER_DATAFRAMELY_{self.name.upper()}"

    @property
    def _environment_tier(self) -> str:
        """How an error names the environment tier. Every shape rejects something from it, so the phrase is written once."""
        return f"the environment variable {self.env_var}"

    def resolve(self, argument: T | None) -> T:
        """Resolves the setting through the three tiers, validating the one that supplied the value.

        Args:
            argument: What the caller passed, or `None` for a caller that passed nothing. `None` is the whole test for "unset": it is why every setting on the door defaults to `None` rather than to the value the package ships, and why a flag a caller turned off reads as off rather than as unset.

        Returns:
            The resolved value.

        Raises:
            InvalidSettingError: The value is outside what the setting accepts, from whichever tier supplied it.
        """
        if argument is not None:
            return self._checked(argument, f"the `{self.name}=` argument")
        environment: str | None = os.environ.get(self.env_var)
        if environment is not None:
            return self._environment(environment)
        return self._checked(self.default, "the package default")

    def _checked(self, value: T, tier: str) -> T:  # noqa: ARG002 - the tier is for whichever shape has something to reject
        """Validates a value that arrived as the setting's own type.

        Both tiers that do are already inside the type a setting holds, so a shape whose values are Python values has nothing to check here. A shape with a vocabulary of its own overrides this.
        """
        return value

    @abstractmethod
    def _environment(self, value: str) -> T:
        """Reads the one tier that arrives as a string whatever the shape holds."""


@dataclass(frozen=True)
class _Choice[T: str](_Setting[T]):
    """One setting and the vocabulary it resolves against.

    Attributes:
        allowed: The whole vocabulary, in the order the docs list it.
    """

    allowed: tuple[T, ...]

    @override
    def _environment(self, value: str) -> T:
        # Nothing parses: the vocabulary is strings, so matching it is the whole check.
        return self._checked(value, self._environment_tier)

    @override
    def _checked(self, value: str, tier: str) -> T:
        """Returns the vocabulary's own member rather than the value that matched it, which is what carries the literal type out without a cast.

        Args:
            value: The value to check.
            tier: Where it came from, worded as a phrase for the error message.

        Raises:
            InvalidSettingError: The value is outside the vocabulary.
        """
        for allowed in self.allowed:
            if value == allowed:
                return allowed
        raise InvalidSettingError(
            self.name, value, self.allowed, tier=tier, env_var=self.env_var
        )


#: What the environment tier spells a flag's two values as. Case is not part of the vocabulary: `TRUE` in a deployment's environment is the same instruction as `true`, and refusing it would buy nothing.
_FLAG_WORDS = {"true": True, "false": False}


@dataclass(frozen=True)
class _Flag(_Setting[bool]):
    """One two-valued setting.

    Its own shape rather than a `_Choice` over `('true', 'false')`, because the two tiers that are not the environment carry a real `bool`: the argument on the asset is typed `bool | None` and the package default is a value rather than a word. Only the environment has to be read as one, which is what this shape is.
    """

    @override
    def _environment(self, value: str) -> bool:
        """Reads a word as the value it stands for.

        Raises:
            InvalidSettingError: The word is neither of the two. `1`, `yes` and `on` are all plausible and all wrong, so the vocabulary stays closed and the error names it.
        """
        parsed: bool | None = _FLAG_WORDS.get(value.lower())
        if parsed is None:
            raise self._rejected(value, self._environment_tier)
        return parsed

    @override
    def _checked(self, value: bool, tier: str) -> bool:
        """Rejects everything a `bool` annotation does not.

        The annotation alone is not enough, and this is the shape where trusting it fails silently rather than loudly. `statistics="false"` is a non-empty string, so an unchecked argument tier resolves to the word and turns the pass *on*, which is the opposite of what was written. The environment tier spells the same instruction exactly that way, which is what makes the mistake reachable rather than hypothetical.

        Raises:
            InvalidSettingError: The value is not a `bool`.
        """
        if type(value) is not bool:
            raise self._rejected(value, tier)
        return value

    def _rejected(self, value: object, tier: str) -> InvalidSettingError:
        """Builds the error every tier raises, so no two of them can word the same rejection differently."""
        return InvalidSettingError(
            self.name,
            str(value),
            tuple(_FLAG_WORDS),
            tier=tier,
            env_var=self.env_var,
        )


#: What a count accepts, worded rather than listed: the vocabulary is a range, so there is nothing to enumerate.
_COUNT_VOCABULARY = "non-negative integers"


@dataclass(frozen=True)
class _Count(_Setting[int]):
    """One setting over the non-negative integers.

    Its own shape rather than a `_Choice` over the numbers somebody might write, because there is no such list. It is also the one shape a type checker cannot finish: `_Choice` narrows an argument to its vocabulary and `_Flag` to two values, while `int` is only half of what this setting accepts and the other half is checked here.
    """

    @override
    def _environment(self, value: str) -> int:
        """Reads a word as the number it spells.

        Raises:
            InvalidSettingError: The word is not a number, or is a negative one. Both are the same mistake to a deployment reading the failure, so they raise the same error.
        """
        try:
            count = int(value)
        except ValueError:
            raise self._rejected(value, self._environment_tier) from None
        return self._checked(count, self._environment_tier)

    @override
    def _checked(self, value: int, tier: str) -> int:
        """Rejects everything an `int` annotation does not.

        Raises:
            InvalidSettingError: The value is negative, or is not an `int` at all.
        """
        # `type` rather than `isinstance`, because `bool` subclasses `int`: a setting confused with `statistics` would otherwise resolve `True` to one row and say nothing.
        if type(value) is not int or value < 0:
            raise self._rejected(value, tier)
        return value

    def _rejected(self, value: object, tier: str) -> InvalidSettingError:
        """Builds the error every tier raises, so no two of them can word the same rejection differently."""
        return InvalidSettingError(
            self.name, str(value), _COUNT_VOCABULARY, tier=tier, env_var=self.env_var
        )


#: What a directory accepts, worded rather than listed, like a count's: every string names a legal directory, so there is nothing to enumerate.
_DIRECTORY_VOCABULARY = "filesystem paths"


@dataclass(frozen=True)
class _Directory(_Setting[str | None]):
    """One setting over the filesystem paths, and over the absence of one.

    Its own shape rather than a `_Choice`, because a path has no vocabulary: the whole point is that nothing here knows which directories a deployment has. It is also the shape with the least to do, since the environment tier already arrives as what the setting holds.

    It is the one setting whose package default is `None`, and that reads as "wherever `tempfile` puts things" rather than as unset. The deferral is deliberate: the door resolves every setting where the asset is *declared*, so a default of `tempfile.gettempdir()` would bake the code location's temp directory into an asset whose frames land on a worker.
    """

    @override
    def _environment(self, value: str) -> str | None:
        # Nothing parses: a path is a string at every tier, because the environment tier can spell it no other way.
        return self._checked(value, self._environment_tier)

    @override
    def _checked(self, value: str | None, tier: str) -> str | None:
        """Rejects everything that is not a written path.

        An empty value raises rather than reading as unset, which is the one decision in this shape worth arguing. `DAGSTER_DATAFRAMELY_TEMP_DIR=${SCRATCH}` in a deployment whose `SCRATCH` never got set arrives empty, and reading that as unset would land the frame on the ephemeral disk the setting was set to move it off. That failure is silent, and the disk it fills is the one the pod dies on.

        Raises:
            InvalidSettingError: The value is not a string, or holds nothing but whitespace.
        """
        if value is None:
            return None
        if type(value) is not str or not value.strip():
            raise InvalidSettingError(
                self.name,
                value,
                _DIRECTORY_VOCABULARY,
                tier=tier,
                env_var=self.env_var,
            )
        return value


#: How many checks a schema's rules become. Definition-time: see `dataframely_asset` for what changing it costs a check's history.
CHECK_GRANULARITY = _Choice[Granularity](
    name="check_granularity", default="rule", allowed=("rule", "column", "schema")
)

#: Where the rules no single column owns land at `column` granularity. Read nowhere else, because the other two granularities have no second place to put them.
MULTI_COLUMN_RULES = _Choice[MultiColumnRules](
    name="multi_column_rules", default="schema", allowed=("schema", "per_rule")
)

#: Whether a materialization carries the four statistics tables. On by default: a distribution read is what a data consumer opens an asset for, and the pass is one aggregate per family over a frame that is already in memory. Whoever is paying for that pass is the one who can turn it off.
STATISTICS = _Flag(name="statistics", default=True)

#: How many of the rows a rule rejected reach that rule's check metadata. On by default, because what three of the failing rows look like is the question a red check raises and the counts cannot answer. It is deliberately not the `statistics` setting: consenting to summary statistics is not consenting to raw values.
MAX_FAILURE_SAMPLES = _Count(name="max_failure_samples", default=5)

#: How many of the valid output's rows a materialization carries. On by default on the same terms, and separate from the failure sample for the same reason the two are separate from `statistics`: seeing what was rejected and seeing what was kept are different consents.
ROW_SAMPLE = _Count(name="row_sample", default=5)

#: Which disk a lazy plan lands on: `dataframely_asset` lands its transform before validating it, and an IO manager sinks a lazy output before promoting it to storage. Unset is the system temp directory, which in a container is its ephemeral disk, and that is the whole reason the setting exists: a landed frame bigger than what the pod has spare fills it.
TEMP_DIR = _Directory(name="temp_dir", default=None)
