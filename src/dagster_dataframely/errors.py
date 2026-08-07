"""The package's exception family, all subclassing `DagsterDataframelyError` so they can be caught together."""

from collections.abc import Mapping

import polars as pl


class DagsterDataframelyError(Exception):
    """Base for every error this package raises."""


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
