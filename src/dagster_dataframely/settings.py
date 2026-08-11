"""Three tiers for every knob: the package default, then a `DAGSTER_DATAFRAMELY_*` environment variable, then the argument on the asset.

A platform engineer sets a house style once for a whole code location, and an asset overrides it where that style is wrong. The environment variables are named for the package because they are machine surface a deployment sets, and `DAGSTER_DATAFRAMELY_` is long enough that nothing else will claim it.

The chain validates on resolve, at every tier including the package's own. Nothing here trusts a value because of where it came from, so a typo raises at the tier that wrote it instead of quietly becoming something else three modules later.

Every setting here is a closed vocabulary of strings, so resolving is validating and nothing parses. A knob with an open range, an integer or a boolean, needs `_Setting` to parse the environment tier as well, because that tier arrives as a string whatever the setting holds.

There is deliberately no fourth tier and no `set_default_*()` function. Dagster loads code locations lazily, so "has the default been set yet" would depend on an import order the user does not control, and the same asset would derive different checks depending on which module happened to be imported first.
"""

import os
from dataclasses import dataclass
from typing import Literal

from dagster_dataframely.errors import InvalidSettingError

#: How many asset checks a schema's rules collapse into.
Granularity = Literal["rule", "column", "schema"]

#: Where the rules that no single column owns land at `column` granularity.
MultiColumnRules = Literal["schema", "per_rule"]


@dataclass(frozen=True)
class _Setting[T: str]:
    """One knob and the vocabulary it resolves against.

    Attributes:
        name: The setting's name. It is also the argument's name and the suffix of its environment variable, so the three cannot drift.
        default: The value the package ships.
        allowed: The whole vocabulary, in the order the docs list it.
    """

    name: str
    default: T
    allowed: tuple[T, ...]

    @property
    def env_var(self) -> str:
        """The environment variable this setting reads."""
        return f"DAGSTER_DATAFRAMELY_{self.name.upper()}"

    def resolve(self, argument: T | None) -> T:
        """Resolves the setting through the three tiers, validating the one that supplied the value.

        Args:
            argument: What the caller passed, or `None` for a caller that passed nothing. `None` is the whole test for "unset": it is why every knob on the door defaults to `None` rather than to the value the package ships.

        Returns:
            The resolved value.

        Raises:
            InvalidSettingError: The value is outside the setting's vocabulary.
        """
        if argument is not None:
            return self._checked(argument, f"the `{self.name}=` argument")
        environment: str | None = os.environ.get(self.env_var)
        if environment is not None:
            return self._checked(
                environment, f"the environment variable {self.env_var}"
            )
        return self._checked(self.default, "the package default")

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


#: How many checks a schema's rules become. Definition-time: see `dataframely_asset` for what changing it costs a check's history.
CHECK_GRANULARITY = _Setting[Granularity](
    name="check_granularity", default="rule", allowed=("rule", "column", "schema")
)

#: Where the rules no single column owns land at `column` granularity. Read nowhere else, because the other two granularities have no second place to put them.
MULTI_COLUMN_RULES = _Setting[MultiColumnRules](
    name="multi_column_rules", default="schema", allowed=("schema", "per_rule")
)
