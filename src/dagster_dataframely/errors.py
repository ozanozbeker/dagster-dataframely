"""The errors this package raises, all subclasses of `DagsterDataframelyError`.

They are in their own public module, so the root namespace holds only the names most users call.
Each message names what failed and how to fix it, with no colon in its first sentence, because Python already prints `ErrorName: ` before it.
"""

from collections.abc import Mapping, Sequence

__all__ = [
    "CheckNameCollisionError",
    "CollectionNotSupportedError",
    "ColumnSchemaError",
    "DagsterDataframelyError",
    "ErrorRowsDirError",
    "ErrorRowsKeyCollisionError",
    "FailurePropagationError",
    "InvalidColumnNameError",
    "InvalidMembersError",
    "InvalidSettingError",
    "InvalidSeverityError",
    "MaterializeResultFieldError",
    "MaterializeResultValueError",
    "NoValidRowsError",
    "ReservedColumnError",
    "ValidationAbortError",
]


class DagsterDataframelyError(Exception):
    """Base class for every error this package raises."""


class InvalidSettingError(DagsterDataframelyError):
    """A setting has a value it does not allow.

    Raised when the setting resolves, so the message names the source of the value.
    """

    def __init__(  # noqa: PLR0913 - the message names every argument
        self,
        setting: str,
        value: str,
        allowed: Sequence[str] | str,
        *,
        source: str,
        env_var: str,
        takes_argument: bool = True,
    ) -> None:
        """Name the setting, its value, the source of the value, and every source the setting reads.

        Parameters
        ----------
        allowed
            The allowed values, which the message quotes, or a phrase that describes a range.
        source
            Where the value came from, as a phrase.
        takes_argument
            Whether `dd.asset` has a parameter for this setting. `error_rows_dir` has none, so its message names no argument.
        """
        rendered: str = (
            allowed
            if isinstance(allowed, str)
            else ", ".join(f"'{allowed_value}'" for allowed_value in allowed)
        )
        sources: str = (
            f"It is read from three sources, each overriding the one before: the package default, then the environment variable {env_var}, then the `{setting}=` argument."
            if takes_argument
            else f"It is read from two sources, each overriding the one before: the package default, then the environment variable {env_var}. There is no `{setting}=` argument."
        )
        super().__init__(
            f"Setting `{setting}` got '{value}' from {source}. Allowed values are {rendered}. {sources}"
        )


class InvalidSeverityError(DagsterDataframelyError):
    """A schema declares a severity this package cannot read.

    Raised at definition time by every public function that takes a schema (ADR-0008), because no type checker reads inside `dy.Column(metadata=...)`.
    """

    def __init__(self, place: str, problem: str) -> None:
        """Name where the declaration is and what is wrong with it.

        Parameters
        ----------
        place
            The declaration, as the sentence's subject: a column's metadata, or the schema's `__dagster_dataframely__`.
        problem
            The rest of the sentence, and any fix.
        """
        super().__init__(f"{place} {problem}")


class ReservedColumnError(DagsterDataframelyError):
    """A column name is in the reserved `dy_` namespace.

    Raised at definition time, because the column would share a name with a check or a rule column this package generates.
    """

    def __init__(self, schema_name: str, columns: list[str]) -> None:
        """Name only the columns in the namespace."""
        names: str = ", ".join(f"'{column}'" for column in columns)
        plural, verb, pronoun = (
            ("", "uses", "it") if len(columns) == 1 else ("s", "use", "them")
        )
        super().__init__(
            f"Column{plural} {names} of {schema_name} {verb} the reserved 'dy_' namespace. Rename {pronoun}. This package generates every check name and rule column in it."
        )


class InvalidColumnNameError(DagsterDataframelyError):
    """A column name has a character Dagster does not allow in an asset check name.

    Raised at definition time (ADR-0008). Dagster allows only `A-Za-z0-9_`, and this package builds one check name per rule from the column name.
    """

    def __init__(self, schema_name: str, columns: list[str]) -> None:
        """Name only the columns whose names are invalid."""
        names: str = ", ".join(f"'{column}'" for column in columns)
        plural, verb, pronoun = (
            ("", "contains", "it") if len(columns) == 1 else ("s", "contain", "them")
        )
        super().__init__(
            f"Column{plural} {names} of {schema_name} {verb} characters outside 'A-Za-z0-9_', the only characters Dagster allows in an asset check name. Rename {pronoun}, or change the `alias=` that sets the name. This package builds one asset check name per rule from the column name."
        )


class CheckNameCollisionError(DagsterDataframelyError):
    """Two rules produce the same asset check name.

    Raised at definition time, before Dagster's own duplicate-check error, which does not name the rules.
    """

    def __init__(self, schema_name: str, first: str, second: str, name: str) -> None:
        """Name both rules and the check name they share."""
        super().__init__(
            f"Rules '{first}' and '{second}' of {schema_name} both become asset check name '{name}' after '|' is replaced with '__'. Rename one of them."
        )


class CollectionNotSupportedError(DagsterDataframelyError):
    """`schema=` received a `dy.Collection`.

    Raised at decoration time.
    """

    def __init__(self, collection_name: str) -> None:
        """Name the Collection and what to declare instead."""
        super().__init__(
            f"{collection_name} is a Dataframely Collection. This decorator takes a single `dy.Schema`. Declare one asset per member, each with the member's own schema, then pass them to `dd.collection_checks`."
        )


class InvalidMembersError(DagsterDataframelyError):
    """`members=` does not map a Collection's members to unpartitioned assets.

    Raised when `dd.collection_checks` is called.
    """

    def __init__(
        self, collection_name: str, problem: str, members: Sequence[str]
    ) -> None:
        """Name what is wrong with the mapping, then every member of the Collection.

        Parameters
        ----------
        problem
            The rest of a sentence whose subject is `members`, and any fix.
        """
        names: str = ", ".join(f"'{member}'" for member in members)
        super().__init__(
            f"`members` {problem} The members of {collection_name} are {names}."
        )


class FailurePropagationError(DagsterDataframelyError):
    """A Collection member sets `propagate_row_failures=True`.

    Raised when `dd.collection_checks` is called, because a check over the members' tables cannot evaluate failure propagation (`docs/out-of-scope/collection-assets.md`).
    """

    def __init__(self, collection_name: str, members: Sequence[str]) -> None:
        """Name the members that set it."""
        names: str = ", ".join(f"'{member}'" for member in members)
        plural, verb = ("", "sets") if len(members) == 1 else ("s", "set")
        super().__init__(
            f"Member{plural} {names} of {collection_name} {verb} `propagate_row_failures=True`, which `dd.collection_checks` cannot check. A row that fails its member's own rules is not in the member's table, so a check over the tables would always pass. Remove the setting, or write a collection filter that states the condition."
        )


class MaterializeResultValueError(DagsterDataframelyError):
    """A returned `dg.MaterializeResult` has no frame in `value`.

    Raised before the column-schema check, because there is no frame to check.
    """

    def __init__(self, asset: str) -> None:
        """Name the asset and the three ways to fix the return."""
        super().__init__(
            f"The `dg.MaterializeResult` returned by '{asset}' has no frame in `value`. Set `value` to the Polars DataFrame or LazyFrame this asset produces. To add metadata to the table, return the frame and call `context.add_asset_metadata({{...}})` from a `context` parameter. If the asset writes its own storage, there is no frame to validate, so write it as a plain `@dg.asset` and call `dagster_dataframely.wiring.schema_metadata` to fill its Columns tab."
        )


class MaterializeResultFieldError(DagsterDataframelyError):
    """A returned `dg.MaterializeResult` sets `asset_key` or `check_results`.

    Raised before the column-schema check. The decorator sets both fields itself.
    """

    def __init__(self, asset: str, field: str) -> None:
        """Name the field and the fields a returned result may set."""
        super().__init__(
            f"The `dg.MaterializeResult` returned by '{asset}' sets `{field}`, which the decorator sets itself: the asset key comes from the declaration, and the check results come from the schema's rules. Remove it. A returned result may set `value`, `metadata`, `data_version` and `tags`."
        )


class ColumnSchemaError(DagsterDataframelyError):
    """A frame's columns or dtypes do not match the schema.

    This is a bug in the pipeline, not bad data, so the run fails before `Schema.filter` runs, and the asset writes nothing.
    """

    def __init__(self, schema_name: str, problems: Sequence[Mapping[str, str]]) -> None:
        """Name each mismatched column with its expected and actual dtype.

        Parameters
        ----------
        problems
            One mapping of `column`, `expected` and `actual` per mismatched column, as `_column_schema_problems` returns.
        """
        mismatches: str = ", ".join(
            f"'{problem['column']}' (expected {problem['expected']}, got {problem['actual']})"
            for problem in problems
        )
        plural, verb = ("", "does") if len(problems) == 1 else ("s", "do")
        super().__init__(
            f"Column{plural} {mismatches} {verb} not match {schema_name}. Fix the function that produced it, or cast with `{schema_name}.cast(frame)` in the asset body. This package never casts for you."
        )


def _failure_counts(counts: Mapping[str, int]) -> str:
    """Render `FailureInfo.counts()` as text for the two errors that report failures."""
    return ", ".join(f"{count} by '{rule}'" for rule, count in counts.items())


class ValidationAbortError(DagsterDataframelyError):
    """A row failed an `"error"` rule and the asset has `filter=False`, so it writes nothing.

    With `filter=False`, every row has to pass every `"error"` rule. No setting drops those rows and writes the rest: to drop rows, filter them in the asset body.
    """

    def __init__(
        self, schema_name: str, error_count: int, counts: Mapping[str, int]
    ) -> None:
        """Give the failure count per `"error"` rule, and the four fixes.

        Parameters
        ----------
        counts
            The failure count for each `"error"` rule that any row failed. The counts can add up to more than `error_count`, because one row can fail several rules.
        """
        plural = "" if error_count == 1 else "s"
        super().__init__(
            f"{error_count} row{plural} failed an 'error' rule of {schema_name}, {_failure_counts(counts)}. Nothing was written, so the last-known-good table is unchanged. Fix the rows upstream, write them under `<name>_error_rows` with `filter=True`, declare the rule 'warn' in the schema, or drop them in the asset body. This package never drops rows for you."
        )


class NoValidRowsError(DagsterDataframelyError):
    """Every row failed an `"error"` rule, so the run wrote only the error rows.

    Nothing writes the asset's table, so an empty table never replaces the last-known-good one. A frame whose rows fail only `"warn"` rules has no valid rows in Dataframely's sense, and is written without this error.
    """

    def __init__(
        self,
        schema_name: str,
        error_count: int,
        counts: Mapping[str, int],
        address: str,
    ) -> None:
        """Give the failure count per `"error"` rule and the error-rows address.

        Parameters
        ----------
        address
            The error-rows address, so the message says where the rows are.
        """
        plural = "" if error_count == 1 else "s"
        super().__init__(
            f"All {error_count} row{plural} failed an 'error' rule of {schema_name}, {_failure_counts(counts)}. The rows are in {address}, with one column per rule showing which rules each row failed. The table was not written, so the last-known-good table is unchanged."
        )


class ErrorRowsKeyCollisionError(DagsterDataframelyError):
    """An asset already materializes the error-rows key.

    Raised before the decorated function runs, on every run of an asset with `filter=True` (ADR-0007). Otherwise both assets would write to the same key, and the second write would replace the first.
    """

    def __init__(self, asset: str, error_rows: str) -> None:
        """Name both assets and each fix."""
        super().__init__(
            f"'{asset}' declares `filter=True`, so its error rows are written to '{error_rows}', which an asset in this code location already materializes. Rename that asset, set `error_rows_key` on '{asset}' to another key, or remove `filter=True` from '{asset}'. If that asset is your own table for the error rows, delete it and use `error_rows_spec` instead, which adds the error rows themselves to the asset graph."
        )


class ErrorRowsDirError(DagsterDataframelyError):
    """Error rows need writing, and nothing sets `error_rows_dir`.

    Raised only when you call an asset with `filter=True` directly, because a run always has an IO manager to write the rows. There is no default directory, so nothing writes the rows somewhere nobody chose.
    """

    def __init__(self, asset: str) -> None:
        """Name the asset and the setting that fixes it."""
        super().__init__(
            f"'{asset}' declares `filter=True` and was called directly, so there is no IO manager to write its error rows. Set `DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR` to a directory for them, or run the asset, so its IO manager writes them."
        )
