"""The package's exception family, all subclassing `DagsterDataframelyError` so they can be caught together.

**The one module in this package with a public name.** Every other module is underscore-private so the file tree stays free to change. Eleven error names in the package's root namespace would be eleven of its twenty-six: a namespace where what a user reaches for most is outnumbered by what they reach for when something already went wrong. Polars settled the same question the same way and deprecated its root re-exports in 1.0.0 to finish the move. Dataframely keeps its four in `dataframely.exc`. What is given up is the freedom to rename or split this file, which is worth nothing here: a leaf that holds one class per failure has nothing to split along.

`errors` rather than `exceptions` or `exc`, because every member ends in `Error` and the base is `DagsterDataframelyError`, following Dagster's own `DagsterError`. The module is named for what it holds.

Every message names the schema, the culprit and the fix, because the message is the whole of what a user sees. It carries no colon: Python already prints `ModuleError: ` ahead of it, and a second colon in the first clause reads as a stutter. Each error takes its culprits as data and builds its own message. None of them knows how the culprits were found.
"""

from collections.abc import Mapping, Sequence

__all__ = [
    "CheckNameCollisionError",
    "CollectionNotSupportedError",
    "ColumnSchemaError",
    "DagsterDataframelyError",
    "InvalidSettingError",
    "MaterializeResultFieldError",
    "MaterializeResultValueError",
    "NothingSurvivedError",
    "QuarantineDirError",
    "ReservedColumnError",
    "ValidationAbortError",
]


class DagsterDataframelyError(Exception):
    """Base for every error this package raises."""


class InvalidSettingError(DagsterDataframelyError):
    """A setting resolved to a value outside its vocabulary.

    Raised on resolve, from whichever source supplied the value, so a typo is a failure at the place it was written rather than a silent misconfiguration everywhere downstream.
    """

    def __init__(
        self,
        setting: str,
        value: str,
        allowed: Sequence[str] | str,
        source: str,
        env_var: str,
    ) -> None:
        """Name the setting, what it got, where that came from, and every source it could have come from.

        Parameters
        ----------
        setting
            The setting's name, which is also the argument's.
        value
            The value that was refused.
        allowed
            The setting's whole vocabulary. A closed one arrives as its own members, in the order the docs list them, and is quoted here. A setting over a range arrives as the phrase that describes it, because printing every value it accepts cannot be done.
        source
            Where this value came from, worded as a phrase.
        env_var
            The setting's environment variable.
        """
        vocabulary: str = (
            allowed
            if isinstance(allowed, str)
            else ", ".join(f"'{allowed_value}'" for allowed_value in allowed)
        )
        super().__init__(
            f"Setting `{setting}` got '{value}' from {source}. Allowed values are {vocabulary}. It resolves in three, each overriding the one before: the package default, then the environment variable {env_var}, then the `{setting}=` argument."
        )


class ReservedColumnError(DagsterDataframelyError):
    """A user column sits inside the reserved `dy_` namespace.

    Raised at definition time. Left to runtime, the collision would surface as a check name that quietly means two different things.
    """

    def __init__(self, schema_name: str, columns: list[str], prefix: str) -> None:
        """Name the offending columns only, never the whole schema.

        Parameters
        ----------
        schema_name
            The schema the columns belong to.
        columns
            The column names inside the reserved namespace.
        prefix
            The reserved prefix itself.
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
        """Name both culprits and the name they collide on.

        Parameters
        ----------
        schema_name
            The schema both rules belong to.
        first
            The rule seen first.
        second
            The rule that collided with it.
        name
            The asset-check name they both rewrite to.
        """
        super().__init__(
            f"Rules '{first}' and '{second}' of {schema_name} both become asset-check name '{name}' after the '|' -> '__' rewrite. Rename one of them."
        )


class CollectionNotSupportedError(DagsterDataframelyError):
    """`schema=` received a `dy.Collection`.

    Raised at decoration time. The guard exists because a Collection is real, adjacent, and the most plausible wrong thing a Dataframely user reaches for. It is deliberately not generalised into a type check on `schema=`.
    """

    def __init__(self, collection_name: str) -> None:
        """State the boundary and make no promise about a future release.

        Parameters
        ----------
        collection_name
            The collection class that was passed.
        """
        super().__init__(
            f"{collection_name} is a Dataframely Collection. This decorator takes a single `dy.Schema`. Declare one asset per member, each with the member's own schema."
        )


class MaterializeResultValueError(DagsterDataframelyError):
    """A returned `dg.MaterializeResult` carries no frame on `value`.

    Raised before the column-schema check, because there is nothing to check. The frame is what this package validates, filters and writes, so a result without one describes a materialization the asset never made.
    """

    def __init__(self, asset: str) -> None:
        """Name the asset and all three routes out.

        Two readers write this, and `value=` answers neither on its own. One wanted metadata on a table this package does write, and the `context` route is what they were reaching for. Sending them to build a returned result around a frame they were not returning anyway would answer a question they did not ask. The other manages their own storage and has no frame at any point, which is a plain `@dg.asset`, and they keep the Columns tab through `wiring.schema_metadata`.

        The `context` route is named bare. A decorated function produces one asset, so `add_asset_metadata` has one materialization to land on and needs no `asset_key=` to say which.

        Parameters
        ----------
        asset
            The asset key, rendered, whose decorated function returned the result.
        """
        super().__init__(
            f"The `dg.MaterializeResult` returned by '{asset}' carries no frame on `value`. Set it to the Polars DataFrame or LazyFrame this asset produces. To attach metadata to a table this package does write, return the frame and call `context.add_asset_metadata({{...}})` from a `context` parameter. An asset that writes its own storage has no frame for this package to validate, so write it as a plain `@dg.asset`, where `dagster_dataframely.wiring.schema_metadata` still fills its Columns tab."
        )


class MaterializeResultFieldError(DagsterDataframelyError):
    """A returned `dg.MaterializeResult` sets a field the decorator owns.

    Raised before the column-schema check. Both fields are decided by the declaration rather than by the decorated function, so a returned one contends with what the step already yields instead of adding to it. Naming the culprit is worth more than dropping it silently, which would leave a user's check result nowhere and say nothing about why.
    """

    def __init__(self, asset: str, field: str) -> None:
        """Name the field, why the decorator owns it, and the four that fold in instead.

        Parameters
        ----------
        asset
            The asset key, rendered, whose decorated function returned the result.
        field
            The `dg.MaterializeResult` field that was set.
        """
        super().__init__(
            f"The `dg.MaterializeResult` returned by '{asset}' sets `{field}`. The decorator owns it: the asset keys come from the outs it declares, and the check results from the schema's rules. Drop it. `value`, `metadata`, `data_version` and `tags` are what this package folds into the materialization."
        )


class ColumnSchemaError(DagsterDataframelyError):
    """A frame arrived with wrong dtypes or missing columns.

    A pipeline defect rather than a data defect, so the whole asset aborts: no rows are filtered and nothing is written.
    """

    def __init__(self, schema_name: str, problems: Sequence[Mapping[str, str]]) -> None:
        """Name each offending column with its expected and actual dtype.

        Parameters
        ----------
        schema_name
            The schema the frame failed to match.
        problems
            One mapping of `column`, `expected` and `actual` per offending column.
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
    """Render a `FailureInfo.counts()` as prose, for the two errors that report damage."""
    return ", ".join(f"{count} by '{rule}'" for rule, count in counts.items())


class ValidationAbortError(DagsterDataframelyError):
    """Rows failed validation and no quarantine is declared, so the asset writes nothing.

    Without somewhere to route invalid rows, every row has to be valid. Writing the survivors and dropping the rest is the failure this package exists to make visible, so configuration cannot reach it. A drop is a line the engineer writes in the asset body, the way a cast is.
    """

    def __init__(
        self, schema_name: str, invalid_count: int, counts: Mapping[str, int]
    ) -> None:
        """State the damage per rule, and the three fixes.

        Naming `quarantine=` makes this error the one place a user who has not read the README learns the keyword exists. It could only be named once the decorator accepted the keyword (#19). Before that it would have sent the reader to a `TypeError`.

        Parameters
        ----------
        schema_name
            The schema the rows failed.
        invalid_count
            How many rows failed at least one rule.
        counts
            Failure count per rule, for the rules anything failed. The counts can sum past `invalid_count`, because one row can break several rules.
        """
        plural = "" if invalid_count == 1 else "s"
        super().__init__(
            f"{invalid_count} row{plural} failed {schema_name} validation, {_culprits(counts)}. Nothing was written, so the last-known-good table survives. Fix the rows upstream, keep them with `quarantine=True`, or drop them deliberately in the asset body. This package never discards rows on your behalf."
        )


class NothingSurvivedError(DagsterDataframelyError):
    """Every row failed validation, so only the quarantine was written.

    The valid rows are skipped rather than materialized empty. An empty table replacing a last-known-good snapshot is the one silent failure a declared quarantine could otherwise introduce, so consenting to partial data is never consent to no data.
    """

    def __init__(
        self,
        schema_name: str,
        invalid_count: int,
        counts: Mapping[str, int],
        address: str,
    ) -> None:
        """State the damage per rule and where every row went.

        Parameters
        ----------
        schema_name
            The schema the rows failed.
        invalid_count
            How many rows failed at least one rule, which is all of them.
        counts
            Failure count per rule, for the rules anything failed.
        address
            Where the writer put the rows, rendered, so the message says where to look. An asset key under delegation and a file path under the fallback, because the answer can be a database table.
        """
        plural = "" if invalid_count == 1 else "s"
        super().__init__(
            f"All {invalid_count} row{plural} failed {schema_name} validation, {_culprits(counts)}. Every row is in {address} with its per-rule outcome, and the valid rows were skipped rather than written empty, so the last-known-good table survives."
        )


class QuarantineDirError(DagsterDataframelyError):
    """A quarantined asset reached `file_writer` with no `quarantine_dir` set.

    Raised at run time, and only where there is no IO manager to delegate to, which is a decorated asset called directly rather than run. A run always has one, so this cannot reach a deployment.

    Choosing a directory instead was considered and declined. The rows are evidence, and writing them somewhere nobody named is how evidence gets lost.
    """

    def __init__(self, asset: str) -> None:
        """Name the asset and the one setting that answers.

        Parameters
        ----------
        asset
            The asset key, rendered, whose invalid rows had nowhere to go.
        """
        super().__init__(
            f"'{asset}' declares `quarantine=True` and was called with no IO manager to delegate to, so the invalid rows have nowhere to go. Set `DAGSTER_DATAFRAMELY_QUARANTINE_DIR` to the directory a called asset should write them under, or run the asset instead, where its own manager places them."
        )
