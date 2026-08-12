"""Lossless encodings for the dtypes a CSV cell cannot hold.

A cell holds text, so five polars dtypes have nowhere to land: `Duration`, `List`, `Array`, `Struct` and `Binary`. Four of the five refuse a plain cast outright. Each one here is an encoding paired with its declared inverse, so a column written and read back compares equal: nothing is rounded, widened or dropped, and the no-cast principle is untouched.

The inverse needs the dtype the schema declares, which is the whole reason the CSV manager reads a schema off the asset and the parquet manager does not. A JSON string is dtype-blind, and so is an integer.

Design decisions:
    - A `Duration` encodes to the integer count of its own time unit rather than to a fixed unit. `dy.Duration` is always microseconds, so a cell holds microseconds for every duration dataframely can declare, while a hand-built `Duration('ns')` stays exact instead of being truncated on the way out.
    - `List` and `Array` encode through a one-field struct named after the column, because polars exposes `json_encode` on the struct namespace alone. `Struct` needs no wrapper and does not get one, so its cell holds the object a reader expects.
    - A null is an empty cell in every column, whatever the codec, which costs one `when` per JSON encoder.
    - An `Array` decodes as a `List` and casts back, at every depth. polars panics deserializing a fixed-size list from JSON, and a Rust panic cannot be caught.
    - `Binary` and `Duration` are refused *inside* a nested dtype rather than encoded there. polars panics writing binary to JSON, and writes a nested duration as ISO-8601 that its own reader then rejects. Both are exactly the failure `UnwritableDtypeError` exists to get ahead of.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass

import polars as pl
from polars.datatypes import DataTypeClass

#: What polars annotates a nested dtype's member as. Both `Field.__init__` and the `List` and `Array` constructors parse their argument into an instance, so the class arm is unreachable at runtime, but a dtype tree cannot be walked without carrying it.
type _Member = pl.DataType | DataTypeClass


@dataclass(frozen=True)
class _Codec:
    """An encoding and its declared inverse.

    Attributes:
        label: What the encoded cell holds, for the log line that names the column.
        encode: Builds the expression that writes the column as text.
        decode: Builds the expression that reads it back, given the dtype the schema declares.
    """

    label: str
    encode: Callable[[str], pl.Expr]
    decode: Callable[[str, pl.DataType], pl.Expr]


def _decodable(dtype: _Member) -> _Member:
    """Rewrites every `Array` in a dtype tree to a `List`, so polars can deserialize it.

    The declared dtype is restored by a cast after the decode. Recursive because an array can sit at any depth, and the panic it causes fires wherever it sits.
    """
    if isinstance(dtype, (pl.Array, pl.List)):
        return pl.List(_decodable(dtype.inner))
    if isinstance(dtype, pl.Struct):
        return pl.Struct(
            {field.name: _decodable(field.dtype) for field in dtype.fields}
        )
    return dtype


def _duration_encode(name: str) -> pl.Expr:
    """Writes the duration's integer tick count, which is `to_physical` by definition."""
    return pl.col(name).to_physical().cast(pl.String)


def _duration_decode(name: str, dtype: pl.DataType) -> pl.Expr:
    """Reads the ticks back into the declared duration, which is what supplies the time unit."""
    return pl.col(name).cast(pl.Int64).cast(dtype)


def _cell(name: str, encoded: pl.Expr) -> pl.Expr:
    """Keeps a null row an empty cell.

    Left alone, a JSON encoder writes the text `{"column":null}` for a null list and `null` for a null struct, so a null would read differently in three columns of the same file. `str.json_decode` returns a null for an empty cell either way, which is what lets the encoder be the side that decides.
    """
    return (
        pl.when(pl.col(name).is_null())
        .then(pl.lit(None, pl.String))
        .otherwise(encoded)
        .alias(name)
    )


def _nested_encode(name: str) -> pl.Expr:
    """Writes a list or array as JSON, wrapped in a one-field struct named after the column."""
    return _cell(name, pl.struct(name).struct.json_encode())


def _nested_decode(name: str, dtype: pl.DataType) -> pl.Expr:
    """Reads the wrapper back and takes its one field."""
    return (
        pl.col(name)
        .str.json_decode(pl.Struct({name: _decodable(dtype)}))
        .struct.field(name)
        .cast(dtype)
    )


def _struct_encode(name: str) -> pl.Expr:
    """Writes a struct as JSON. No wrapper: `json_encode` takes a struct directly, so the cell holds the object a reader expects."""
    return _cell(name, pl.col(name).struct.json_encode())


def _struct_decode(name: str, dtype: pl.DataType) -> pl.Expr:
    """Reads the object back into the declared struct."""
    return pl.col(name).str.json_decode(_decodable(dtype)).cast(dtype)


def _binary_encode(name: str) -> pl.Expr:
    """Writes the bytes as base64, whose alphabet holds no delimiter and no quote, so the cell never needs escaping."""
    return pl.col(name).bin.encode("base64")


def _binary_decode(name: str, _dtype: pl.DataType) -> pl.Expr:
    """Reads the bytes back. The declared dtype is unused: base64 decodes to `Binary` and nothing else."""
    return pl.col(name).str.decode("base64")


# Keyed by base type rather than by dtype, because `List(String)` and `List(Int64)` take the same codec and only their decode differs.
_CODECS: Mapping[type[pl.DataType], _Codec] = {
    pl.Duration: _Codec("integer", _duration_encode, _duration_decode),
    pl.List: _Codec("JSON", _nested_encode, _nested_decode),
    pl.Array: _Codec("JSON", _nested_encode, _nested_decode),
    pl.Struct: _Codec("JSON", _struct_encode, _struct_decode),
    pl.Binary: _Codec("base64", _binary_encode, _binary_decode),
}

# No codec at any depth: nothing text can hold, and polars cannot nest it either.
_UNWRITABLE = (pl.Object,)

# Encodable at the top level, unreachable below one. Both fail inside JSON, one by panic and one by an inverse polars will not take back.
_TOP_LEVEL_ONLY = (pl.Binary, pl.Duration)


def _unsupported(dtype: _Member, *, nested: bool) -> bool:
    """Reports whether any codec can carry this dtype, at the depth it sits."""
    if dtype.base_type() in _UNWRITABLE:
        return True
    if nested and dtype.base_type() in _TOP_LEVEL_ONLY:
        return True
    if isinstance(dtype, (pl.List, pl.Array)):
        return _unsupported(dtype.inner, nested=True)
    if isinstance(dtype, pl.Struct):
        return any(_unsupported(field.dtype, nested=True) for field in dtype.fields)
    return False


def _phrase(dtype: pl.DataType, codec: _Codec) -> str:
    """Renders one column's encoding for the log line."""
    return f"{dtype} as {codec.label}"


def unwritable(dtypes: Mapping[str, pl.DataType]) -> dict[str, pl.DataType]:
    """Finds the columns no codec can carry.

    Args:
        dtypes: The frame's columns and their dtypes.

    Returns:
        The offending columns, in the frame's own order, ready to hand `UnwritableDtypeError`.
    """
    return {
        name: dtype
        for name, dtype in dtypes.items()
        if _unsupported(dtype, nested=False)
    }


def encode[F: (pl.DataFrame, pl.LazyFrame)](frame: F) -> tuple[F, dict[str, str]]:
    """Encodes every column CSV cannot hold, leaving the rest untouched.

    Takes a plan as readily as a frame, like `decode` below, and for the same reason: every codec is an expression over `with_columns`, so nothing here executes and nothing here needs the data.

    A constrained type parameter rather than the union `decode` returns, because this one's caller has to narrow what it gets back: an eager frame is written through `write_csv` and a plan is streamed through `sink_csv`, and only a same-type-out promise saves that from a cast.

    Args:
        frame: The frame or plan about to be written.

    Returns:
        What to write, of the type it arrived as, and the encoded columns mapped to what their cells now hold.
    """
    expressions: list[pl.Expr] = []
    encoded: dict[str, str] = {}
    for name, dtype in frame.collect_schema().items():
        codec = _CODECS.get(dtype.base_type())
        if codec is None:
            continue
        expressions.append(codec.encode(name))
        encoded[name] = _phrase(dtype, codec)
    return frame.with_columns(expressions), encoded


def read_schema(dtypes: Mapping[str, pl.DataType]) -> dict[str, pl.DataType]:
    """Builds what `read_csv` is told about the file.

    An encoded column is read as text and decoded afterwards; every other column is read as the schema declares it, which is what keeps a `Decimal` from arriving as a float and an `Enum` from arriving as a string.

    Args:
        dtypes: The columns the schema declares, and their dtypes.

    Returns:
        A mapping to hand `read_csv` as `schema_overrides`. It is by name, so a column the file does not carry is ignored rather than fatal.
    """
    return {
        name: pl.String() if dtype.base_type() in _CODECS else dtype
        for name, dtype in dtypes.items()
    }


def decode(
    frame: pl.DataFrame | pl.LazyFrame, dtypes: Mapping[str, pl.DataType]
) -> tuple[pl.DataFrame | pl.LazyFrame, dict[str, str]]:
    """Restores every encoded column to the dtype the schema declares.

    Takes a scan as readily as a frame, and hands back whichever it was given. Every codec is an expression over `with_columns`, so nothing here executes and nothing here needs the data. The column names come off `collect_schema` rather than `columns`, which asks a `LazyFrame` the same question without the performance warning polars attaches to the shorter spelling.

    Args:
        frame: The frame or scan `read_csv` and `scan_csv` return, with the encoded columns as text.
        dtypes: The columns the schema declares, and their dtypes. Empty when the asset carries no schema, which leaves the frame as it was read.

    Returns:
        The decoded frame, of the type it arrived as, and the decoded columns mapped to what their cells held.
    """
    expressions: list[pl.Expr] = []
    decoded: dict[str, str] = {}
    for name in frame.collect_schema().names():
        dtype = dtypes.get(name)
        if dtype is None:
            continue
        codec = _CODECS.get(dtype.base_type())
        if codec is None:
            continue
        expressions.append(codec.decode(name, dtype))
        decoded[name] = _phrase(dtype, codec)
    return frame.with_columns(expressions), decoded


def describe(columns: Mapping[str, str]) -> str:
    """Renders the columns a codec touched as the clause a log line ends on."""
    return ", ".join(f"'{name}' ({phrase})" for name, phrase in columns.items())
