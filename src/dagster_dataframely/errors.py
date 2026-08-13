"""The package's exception family, all subclassing `DagsterDataframelyError` so they can be caught together.

**The one module in this package with a public name.** Every other module is underscore-private so that the file tree stays free to change, and ten error names in the root would be ten of its twenty-four: a namespace where what a user reaches for most is outnumbered by what they reach for when something already went wrong. Polars settled the same question the same way and deprecated its root re-exports in 1.0.0 to finish the move; Dataframely keeps its four in `dataframely.exc`. What is given up is the freedom to rename or split this file, which is worth nothing here: a leaf that holds one class per failure has nothing to split along.

`errors` rather than `exceptions` or `exc`, because every member ends in `Error` and the base is `DagsterDataframelyError`, following Dagster's own `DagsterError`. The module is named for what it holds.

Every message names the schema, the culprit and the fix, because the message is the whole of what a user sees. It carries no colon: Python already prints `ModuleError: ` ahead of it, and a second colon in the first clause reads as a stutter. Each error takes its culprits as data and builds its own message; none of them knows how the culprits were found.
"""

from collections.abc import Mapping, Sequence

import polars as pl

__all__ = [
    "CheckNameCollisionError",
    "CollectionNotSupportedError",
    "DagsterDataframelyError",
    "InvalidSettingError",
    "MaterializeResultFieldError",
    "MaterializeResultValueError",
    "NothingSurvivedError",
    "QuarantineSettingError",
    "ReservedColumnError",
    "SchemaShapeError",
    "UnwritableDtypeError",
    "ValidationAbortError",
]


class DagsterDataframelyError(Exception):
    """Base for every error this package raises."""


class InvalidSettingError(DagsterDataframelyError):
    """A setting resolved to a value outside its vocabulary.

    Raised on resolve, from whichever tier supplied the value, so a typo is a failure at the place it was written rather than a silent misconfiguration everywhere downstream.
    """

    def __init__(
        self,
        setting: str,
        value: str,
        allowed: Sequence[str] | str,
        tier: str,
        env_var: str,
    ) -> None:
        """Names the setting, what it got, where that came from, and every tier it could have come from.

        Args:
            setting: The setting's name, which is also the argument's.
            value: The value that was rejected.
            allowed: The setting's whole vocabulary. A closed one arrives as its own members, in the order the docs list them, and is quoted here. A setting over a range arrives as the phrase that describes it, because printing every value it accepts is not a thing that can be done.
            tier: Where this value came from, worded as a phrase.
            env_var: The setting's environment variable.
        """
        vocabulary: str = (
            allowed
            if isinstance(allowed, str)
            else ", ".join(f"'{option}'" for option in allowed)
        )
        super().__init__(
            f"Setting `{setting}` got '{value}' from {tier}. Allowed values are {vocabulary}. It resolves in three tiers, each overriding the one before: the package default, then the environment variable {env_var}, then the `{setting}=` argument."
        )


class ReservedColumnError(DagsterDataframelyError):
    """A user column sits inside the reserved `dy_` namespace.

    Raised at definition time. Left to runtime, the collision would surface as a check name that quietly means two different things.
    """

    def __init__(self, schema_name: str, columns: list[str], prefix: str) -> None:
        """Names the offending columns only, never the whole schema.

        Args:
            schema_name: The schema the columns belong to.
            columns: The column names inside the reserved namespace.
            prefix: The reserved prefix itself.
        """
        culprits: str = ", ".join(f"'{column}'" for column in columns)
        plural, verb, pronoun = (
            ("", "uses", "it") if len(columns) == 1 else ("s", "use", "them")
        )
        super().__init__(
            f"Column{plural} {culprits} of {schema_name} {verb} the reserved '{prefix}' prefix. Rename {pronoun}. This package generates every check name and quarantine column under that namespace."
        )


class CheckNameCollisionError(DagsterDataframelyError):
    """Two rules rewrite to the same asset-check name.

    Raised at definition time, ahead of Dagster's own `Duplicate check specs`, which names the collision but not the rules that caused it.
    """

    def __init__(self, schema_name: str, first: str, second: str, name: str) -> None:
        """Names both culprits and the name they collide on.

        Args:
            schema_name: The schema both rules belong to.
            first: The rule seen first.
            second: The rule that collided with it.
            name: The asset-check name they both rewrite to.
        """
        super().__init__(
            f"Rules '{first}' and '{second}' of {schema_name} both become asset-check name '{name}' after the '|' -> '__' rewrite. Rename one of them."
        )


class CollectionNotSupportedError(DagsterDataframelyError):
    """`schema=` received a `dy.Collection`.

    Raised at decoration time. The guard exists because a Collection is real, adjacent, and the most plausible wrong thing a Dataframely user reaches for; it is deliberately not generalised into a type check on `schema=`.
    """

    def __init__(self, collection_name: str) -> None:
        """States the boundary and makes no promise about a future release.

        Args:
            collection_name: The collection class that was passed.
        """
        super().__init__(
            f"{collection_name} is a Dataframely Collection. This decorator takes a single `dy.Schema`. Declare one asset per member, each with the member's own schema."
        )


class MaterializeResultValueError(DagsterDataframelyError):
    """A returned `dg.MaterializeResult` carries no frame on `value`.

    Raised before the shape check, because there is nothing to check. The frame is what this package validates, filters and writes, so a result without one describes a materialization the asset never made.
    """

    def __init__(self, asset: str) -> None:
        """Names the asset and all three routes out.

        Two readers write this, and `value=` answers neither on its own. One wanted metadata on a table this package does write, and the `context` route is what they were reaching for: sending them to build a returned result around a frame they were not returning anyway would answer a question they did not ask. The other manages their own storage and has no frame at any point, which is a plain `@dg.asset`, and they keep the Columns tab through `wiring.schema_metadata`.

        The `context` route carries `asset_key=` here rather than being named bare. A reader meets this message having already got the returned result wrong, so a second call that raises on any asset with a quarantine would be the worse of the two failures.

        Args:
            asset: The asset key, rendered, whose decorated function returned the result.
        """
        super().__init__(
            f"The `dg.MaterializeResult` returned by '{asset}' carries no frame on `value`. Set it to the Polars DataFrame or LazyFrame this asset produces. To attach metadata to a table this package does write, return the frame and call `context.add_asset_metadata({{...}}, asset_key=context.asset_key_for_output(<this asset's name>))` from a `context` parameter, since the bare call raises as soon as the asset declares a quarantine. An asset that writes its own storage has no frame for this package to validate, so write it as a plain `@dg.asset`, where `dagster_dataframely.wiring.schema_metadata` still fills its Columns tab."
        )


class MaterializeResultFieldError(DagsterDataframelyError):
    """A returned `dg.MaterializeResult` sets a field the decorator owns.

    Raised before the shape check. Both fields are decided by the declaration rather than by the decorated function, so a returned one contends with what the step already yields instead of adding to it. Naming the culprit is worth more than dropping it silently, which would leave a user's check result nowhere and say nothing about why.
    """

    def __init__(self, asset: str, field: str) -> None:
        """Names the field, why the decorator owns it, and the four that fold in instead.

        Args:
            asset: The asset key, rendered, whose decorated function returned the result.
            field: The `dg.MaterializeResult` field that was set.
        """
        super().__init__(
            f"The `dg.MaterializeResult` returned by '{asset}' sets `{field}`. The decorator owns it: the asset keys come from the outs it declares, and the check results from the schema's rules. Drop it. `value`, `metadata`, `data_version` and `tags` are what this package folds into the materialization."
        )


class SchemaShapeError(DagsterDataframelyError):
    """A frame arrived with wrong dtypes or missing columns.

    A pipeline defect rather than a data defect, so the whole asset aborts: no rows are filtered and nothing is written.
    """

    def __init__(self, schema_name: str, problems: Sequence[Mapping[str, str]]) -> None:
        """Names each offending column with its expected and actual dtype.

        Args:
            schema_name: The schema the frame failed to match.
            problems: One mapping of `column`, `expected` and `actual` per offending column.
        """
        culprits: str = ", ".join(
            f"'{problem['column']}' (expected {problem['expected']}, got {problem['actual']})"
            for problem in problems
        )
        plural, verb = ("", "does") if len(problems) == 1 else ("s", "do")
        super().__init__(
            f"Column{plural} {culprits} {verb} not match {schema_name}. Fix the function that produced it, or cast deliberately with `{schema_name}.cast(frame)` in the asset body. This package never casts on your behalf."
        )


def _culprits(counts: Mapping[str, int]) -> str:
    """Renders a `FailureInfo.counts()` as prose, for the two errors that report damage."""
    return ", ".join(f"{count} by '{rule}'" for rule, count in counts.items())


class ValidationAbortError(DagsterDataframelyError):
    """Rows were rejected and no quarantine is declared, so the asset writes nothing.

    Without somewhere to route invalid rows, every row has to be valid. Landing the survivors and dropping the rest is the failure this package exists to make visible, so it is not reachable by configuration: a drop is a line the engineer writes in the asset body, the way a cast is.
    """

    def __init__(
        self, schema_name: str, rejected: int, counts: Mapping[str, int]
    ) -> None:
        """States the damage per rule, and the three fixes.

        Naming `quarantine=` is what makes this error the one place a user who has not read the README learns the option exists. It could only be named once the decorator accepted the keyword (#19); before that it would have sent the reader to a `TypeError`.

        Args:
            schema_name: The schema that rejected the rows.
            rejected: How many rows were rejected.
            counts: Failure count per rule, for the rules that rejected anything. The counts can sum past `rejected`, because one row can break several rules.
        """
        plural = "" if rejected == 1 else "s"
        super().__init__(
            f"{schema_name} rejected {rejected} row{plural}, {_culprits(counts)}. Nothing was written, so the last-known-good table survives. Fix the rows upstream, route them to a sibling asset with `quarantine=dg.AssetOut()`, or drop them deliberately in the asset body. This package never discards rows on your behalf."
        )


class NothingSurvivedError(DagsterDataframelyError):
    """Every row was rejected, so only the quarantine was written.

    The valid output is skipped rather than materialized empty. An empty table replacing a last-known-good snapshot is the one silent failure a declared quarantine could otherwise introduce, so consenting to partial data is never consent to no data.
    """

    def __init__(
        self, schema_name: str, rejected: int, counts: Mapping[str, int], key: str
    ) -> None:
        """States the damage per rule and where every row went.

        Args:
            schema_name: The schema that rejected the rows.
            rejected: How many rows were rejected, which is all of them.
            counts: Failure count per rule, for the rules that rejected anything.
            key: The quarantine's asset key, rendered, so the message says where to look.
        """
        plural = "" if rejected == 1 else "s"
        super().__init__(
            f"{schema_name} rejected all {rejected} row{plural}, {_culprits(counts)}. Every row is in {key} with its per-rule outcome, and the valid output was skipped rather than written empty, so the last-known-good table survives."
        )


class QuarantineSettingError(DagsterDataframelyError):
    """The quarantine's `dg.AssetOut` sets something that cannot differ between the two outs.

    Raised at definition time. `can_subset` is absent, so one step always produces both tables. Inheriting the decorator's value silently would discard something the engineer wrote, and honouring theirs would state a schedule or a version for one half of a step.
    """

    def __init__(self, setting: str) -> None:
        """Names the setting and the reason one step cannot hold two of it.

        Args:
            setting: The `dg.AssetOut` parameter that was set.
        """
        super().__init__(
            f"The quarantine's `dg.AssetOut` sets `{setting}`. One step always produces both tables, so a `{setting}` that differs between them cannot be true of either. Pass it to `dataframely_asset` instead, where it covers both."
        )


class UnwritableDtypeError(DagsterDataframelyError):
    """A column holds a dtype the bound IO manager cannot write.

    Raised from `handle_output` before the write. Left to Polars, the same frame fails with a `ComputeError` from inside the writer, or with a Rust panic.
    """

    def __init__(self, extension: str, columns: Mapping[str, pl.DataType]) -> None:
        """Names the culprits and the fix.

        Dagster's wrapping `DagsterExecutionHandleOutputError` already names the step, so naming the asset again here would only repeat it.

        Args:
            extension: The file extension being written, e.g. `.parquet`.
            columns: The offending column names, mapped to their dtypes.
        """
        culprits: str = ", ".join(
            f"'{name}' ({dtype})" for name, dtype in columns.items()
        )
        plural, pronoun = ("", "it") if len(columns) == 1 else ("s", "them")
        super().__init__(
            f"Column{plural} {culprits} cannot be written to {extension}. Convert or drop {pronoun} in the asset body. This IO manager never casts on your behalf."
        )
