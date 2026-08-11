"""Three tiers for every knob: the package default, then a `DAGSTER_DATAFRAMELY_*` environment variable, then the argument on the asset.

A platform engineer sets a house style once for a whole code location, and an asset overrides it where that style is wrong. The environment variables are named for the package because they are machine surface a deployment sets, and `DAGSTER_DATAFRAMELY_` is long enough that nothing else will claim it.

The chain validates on resolve, at every tier including the package's own. Nothing here trusts a value because of where it came from, so a typo raises at the tier that wrote it instead of quietly becoming something else three modules later.

A knob is one of two shapes. A `_Setting` holds a closed vocabulary of strings, so resolving is validating and nothing parses. A `_Flag` holds a `bool`, and it is the shape that has to parse: the environment tier arrives as a string whatever the setting holds, and only a flag's other two tiers hold something that is not one. A knob over an open range, an integer say, would be a third shape for the same reason.

There is deliberately no fourth tier and no `set_default_*()` function. Dagster loads code locations lazily, so "has the default been set yet" would depend on an import order the user does not control, and the same asset would derive different checks depending on which module happened to be imported first.
"""

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Literal, override

from dagster_dataframely.errors import InvalidSettingError

#: How many asset checks a schema's rules collapse into.
Granularity = Literal["rule", "column", "schema"]

#: Where the rules that no single column owns land at `column` granularity.
MultiColumnRules = Literal["schema", "per_rule"]


@dataclass(frozen=True)
class _Knob[T](ABC):
    """One knob and the three tiers it resolves through.

    The precedence lives here and nowhere else, so a knob of a new shape cannot come to read its tiers in a different order. What a shape decides is only how a tier's value is checked, and which of them has to be read out of a string.

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

    def resolve(self, argument: T | None) -> T:
        """Resolves the setting through the three tiers, validating the one that supplied the value.

        Args:
            argument: What the caller passed, or `None` for a caller that passed nothing. `None` is the whole test for "unset": it is why every knob on the door defaults to `None` rather than to the value the package ships, and why a flag a caller turned off reads as off rather than as unset.

        Returns:
            The resolved value.

        Raises:
            InvalidSettingError: The value is outside what the knob accepts, from whichever tier supplied it.
        """
        if argument is not None:
            return self._checked(argument, f"the `{self.name}=` argument")
        environment: str | None = os.environ.get(self.env_var)
        if environment is not None:
            return self._environment(environment)
        return self._checked(self.default, "the package default")

    def _checked(self, value: T, tier: str) -> T:  # noqa: ARG002 - the tier is for whichever shape has something to reject
        """Validates a value that arrived as the knob's own type.

        Both tiers that do are already inside the type a knob holds, so a shape whose values are Python values has nothing to check here. A shape with a vocabulary of its own overrides this.
        """
        return value

    @abstractmethod
    def _environment(self, value: str) -> T:
        """Reads the one tier that arrives as a string whatever the knob holds."""


@dataclass(frozen=True)
class _Setting[T: str](_Knob[T]):
    """One knob and the vocabulary it resolves against.

    Attributes:
        allowed: The whole vocabulary, in the order the docs list it.
    """

    allowed: tuple[T, ...]

    @override
    def _environment(self, value: str) -> T:
        # Nothing parses: the vocabulary is strings, so matching it is the whole check.
        return self._checked(value, f"the environment variable {self.env_var}")

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
class _Flag(_Knob[bool]):
    """One two-valued knob.

    Its own shape rather than a `_Setting` over `('true', 'false')`, because the two tiers that are not the environment carry a real `bool`: the argument on the asset is typed `bool | None` and the package default is a value rather than a word. Only the environment has to be read as one, which is what this shape is.
    """

    @override
    def _environment(self, value: str) -> bool:
        """Reads a word as the value it stands for.

        Raises:
            InvalidSettingError: The word is neither of the two. `1`, `yes` and `on` are all plausible and all wrong, so the vocabulary stays closed and the error names it.
        """
        parsed: bool | None = _FLAG_WORDS.get(value.lower())
        if parsed is None:
            raise InvalidSettingError(
                self.name,
                value,
                tuple(_FLAG_WORDS),
                tier=f"the environment variable {self.env_var}",
                env_var=self.env_var,
            )
        return parsed


#: How many checks a schema's rules become. Definition-time: see `dataframely_asset` for what changing it costs a check's history.
CHECK_GRANULARITY = _Setting[Granularity](
    name="check_granularity", default="rule", allowed=("rule", "column", "schema")
)

#: Where the rules no single column owns land at `column` granularity. Read nowhere else, because the other two granularities have no second place to put them.
MULTI_COLUMN_RULES = _Setting[MultiColumnRules](
    name="multi_column_rules", default="schema", allowed=("schema", "per_rule")
)

#: Whether a materialization carries the four statistics tables. On by default: a distribution read is what a data consumer opens an asset for, and the pass is one aggregate per family over a frame that is already in memory. Whoever is paying for that pass is the one who can turn it off.
STATISTICS = _Flag(name="statistics", default=True)
