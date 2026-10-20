"""`get_code_version` derives the code version `dd.asset` sets from a schema, as dagster-dbt's `default_code_version_fn` derives one from a model's SQL.

It hashes each rule's `meta.serialize` output, not its repr, because `str(pl.Expr)` truncates a long literal.
A column's repr also differs between processes, because it prints a function's memory address.
"""

import hashlib

import dataframely as dy
import polars as pl

from dagster_dataframely._rules import DescribedRule, described_rules


def _serialized_expression(rule: DescribedRule) -> bytes | str:
    """Return the rule's serialized expression, or its repr where Polars cannot serialize it, as for a Python UDF without `cloudpickle`."""
    expr: pl.Expr = rule.expr
    try:
        return expr.meta.serialize(format="binary")
    except pl.exceptions.ComputeError:
        return str(expr)


def get_code_version(schema: type[dy.Schema]) -> str:
    """Return the sha1 hexdigest of the schema's columns and rules, which `dd.asset` uses as the asset's code version.

    Returns
    -------
    :
        A 40-character hex string, to pass to `@dg.asset(code_version=...)`.
    """
    content = (
        [(name, str(column.dtype)) for name, column in schema.columns().items()],
        [
            (rule.name, rule.severity, _serialized_expression(rule))
            for rule in described_rules(schema).values()
        ],
    )
    return hashlib.sha1(repr(content).encode(), usedforsecurity=False).hexdigest()
