"""The package's exception family, all subclassing `DagsterDataframelyError` so they can be caught together.

Every message names the schema, the culprit and the fix, because the message is the whole of what a user sees. It carries no colon: Python already prints `ModuleError: ` ahead of it, and a second colon in the first clause reads as a stutter. Each error takes its culprits as data and builds its own message; none of them knows how the culprits were found.
"""

from collections.abc import Mapping, Sequence

import polars as pl


class DagsterDataframelyError(Exception):
    """Base for every error this package raises."""


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
        culprits = ", ".join(f"'{column}'" for column in columns)
        plural, verb, pronoun = (
            ("", "uses", "it") if len(columns) == 1 else ("s", "use", "them")
        )
        super().__init__(
            f"Column{plural} {culprits} of {schema_name} {verb} the reserved "
            f"'{prefix}' prefix. Rename {pronoun}. This package generates every "
            f"check name and quarantine column under that namespace."
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
            f"Rules '{first}' and '{second}' of {schema_name} both become "
            f"asset-check name '{name}' after the '|' -> '__' rewrite. "
            f"Rename one of them."
        )


class CollectionNotSupportedError(DagsterDataframelyError):
    """`schema=` received a `dy.Collection`.

    Raised at decoration time. The guard exists because a Collection is real, adjacent, and the most plausible wrong thing a dataframely user reaches for; it is deliberately not generalised into a type check on `schema=`.
    """

    def __init__(self, collection_name: str) -> None:
        """States the boundary and makes no promise about a future release.

        Args:
            collection_name: The collection class that was passed.
        """
        super().__init__(
            f"{collection_name} is a dataframely Collection. This decorator takes a single `dy.Schema`. Declare one asset per member, each with the member's own schema."
        )


class SchemaGateError(DagsterDataframelyError):
    """A frame arrived with wrong dtypes or missing columns.

    A pipeline defect rather than a data defect, so the whole asset aborts: no rows are filtered and nothing is written.
    """

    def __init__(self, schema_name: str, problems: Sequence[Mapping[str, str]]) -> None:
        """Names each offending column with its expected and actual dtype.

        Args:
            schema_name: The schema the frame failed to match.
            problems: One mapping of `column`, `expected` and `actual` per offending column.
        """
        culprits = ", ".join(
            f"'{problem['column']}' (expected {problem['expected']}, "
            f"got {problem['actual']})"
            for problem in problems
        )
        plural, verb = ("", "does") if len(problems) == 1 else ("s", "do")
        super().__init__(
            f"Column{plural} {culprits} {verb} not match {schema_name}. "
            f"Fix the transform, or cast deliberately with `{schema_name}.cast(frame)` "
            f"in the asset body. This package never casts on your behalf."
        )


class ValidationAbortError(DagsterDataframelyError):
    """Rows were rejected, so the asset writes nothing.

    Without somewhere to route rejected rows, every row has to be good. Landing the survivors and dropping the rest is the failure this package exists to make visible, so it is not reachable by configuration: a drop is a line the engineer writes in the asset body, the way a cast is.
    """

    def __init__(
        self, schema_name: str, rejected: int, counts: Mapping[str, int]
    ) -> None:
        """States the damage per rule, and the two fixes that exist today.

        The fix clause will name `quarantine=` once that out exists (#19). Until then it would send the reader to a keyword argument the door does not accept, and an error message is a promise.

        Args:
            schema_name: The schema that rejected the rows.
            rejected: How many rows were rejected.
            counts: Failure count per rule, for the rules that rejected anything. The counts can sum past `rejected`, because one row can break several rules.
        """
        culprits = ", ".join(f"{count} by '{rule}'" for rule, count in counts.items())
        plural = "" if rejected == 1 else "s"
        super().__init__(
            f"{schema_name} rejected {rejected} row{plural}, {culprits}. Nothing "
            f"was written, so the last-known-good table survives. Fix the rows "
            f"upstream, or drop them deliberately in the asset body. This package "
            f"never discards rows on your behalf."
        )


class UnwritableDtypeError(DagsterDataframelyError):
    """A column holds a dtype the bound IO manager cannot write.

    Raised from `handle_output` before the write. Left to polars, the same frame fails with a `ComputeError` from inside the writer, or with a Rust panic.
    """

    def __init__(self, extension: str, columns: Mapping[str, pl.DataType]) -> None:
        """Names the culprits and the fix.

        Dagster's wrapping `DagsterExecutionHandleOutputError` already names the step, so naming the asset again here would only repeat it.

        Args:
            extension: The file extension being written, e.g. `.parquet`.
            columns: The offending column names, mapped to their dtypes.
        """
        culprits = ", ".join(f"'{name}' ({dtype})" for name, dtype in columns.items())
        plural, pronoun = ("", "it") if len(columns) == 1 else ("s", "them")
        super().__init__(
            f"Column{plural} {culprits} cannot be written to {extension}. "
            f"Convert or drop {pronoun} in the asset body. "
            f"This IO manager never casts on your behalf."
        )
