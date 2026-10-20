"""The functions in this module build a schema's asset check specs and the results that `_runtime` and `evaluate_checks` yield for them.

`build_asset_check_specs` and `evaluate_rule_checks` both call `_build_rule_sets`, so they name the same checks only when both receive the same `check_granularity`.
"""

from collections.abc import Generator, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Self

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._naming import COLUMN_SCHEMA_CHECK, get_check_name_for_column
from dagster_dataframely._rendering import (
    render_check_description,
    summarize_column_rules,
)
from dagster_dataframely._rules import (
    DescribedRule,
    describe_rules,
    validate_namespace,
)
from dagster_dataframely._samples import Row, build_sample_metadata, sample_rows
from dagster_dataframely._settings import (
    CHECK_GRANULARITY,
    MAX_FAILURE_EXAMPLES,
    Granularity,
)
from dagster_dataframely.errors import ColumnSchemaError


@dataclass(frozen=True)
class _RuleSet:
    """The rules one asset check reports for."""

    name: str
    description: str
    rules: list[DescribedRule]
    collapsed: bool = field(default=False, kw_only=True)

    @classmethod
    def from_rule(cls, schema: type[dy.Schema], rule: DescribedRule) -> Self:
        """Return the rule set of a check for one rule."""
        return cls(rule.check_name, render_check_description(schema, rule), [rule])


def _build_rule_sets(
    schema: type[dy.Schema], check_granularity: Granularity | None
) -> list[_RuleSet]:
    """Return a rule set for each check that reports the schema's rules.

    At `column`, a schema-level rule keeps its own check, because the user wrote it (`docs/out-of-scope/schema-rules.md`).
    """
    granularity: Granularity = CHECK_GRANULARITY.resolve(check_granularity)
    rules: list[DescribedRule] = list(describe_rules(schema).values())
    if not rules:
        return []
    if granularity == "rule":
        return [_RuleSet.from_rule(schema, rule) for rule in rules]

    columns: dict[str, list[DescribedRule]] = {}
    schema_level: list[DescribedRule] = []
    for rule in rules:
        if rule.column is None:
            schema_level.append(rule)
        else:
            columns.setdefault(rule.column, []).append(rule)

    rule_sets: list[_RuleSet] = [
        _RuleSet(
            get_check_name_for_column(column),
            f"Every rule on {column}: {summarize_column_rules(schema, members)}.",
            members,
            collapsed=True,
        )
        for column, members in columns.items()
    ]
    rule_sets.extend(_RuleSet.from_rule(schema, rule) for rule in schema_level)
    return rule_sets


def build_asset_check_specs(
    schema: type[dy.Schema],
    *,
    asset: str | dg.AssetKey,
    partitions_def: dg.PartitionsDefinition | None = None,
    check_granularity: Granularity | None = None,
    filter: bool = False,  # noqa: A002 - the name `dd.asset` uses
) -> list[dg.AssetCheckSpec]:
    """Return the asset check specs for the schema's rules, plus the column-schema check.

    Parameters
    ----------
    schema
        The schema whose rules become check specs.
    asset
        The key of the asset the checks report on.
    partitions_def
        The asset's partitions, so each check evaluation records its partition. Dagster does not check that it matches the asset's.
    check_granularity
        How many checks report the schema's rules. Changing it on an asset that has already run starts a new check history. `None` uses `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY` if set, else `rule`.
    filter
        Pass `True` when the asset writes its error rows instead of failing, as `dd.asset(filter=True)` does. With `False`, a check that reports for an `"error"` rule is blocking. With `True`, no rule's check is blocking, because a blocking check that fails at `ERROR` fails the step after the table is written.

    Returns
    -------
    :
        The column-schema check's spec first, then one spec per rule set, to pass to `@dg.asset(check_specs=...)`.

    Raises
    ------
    InvalidSettingError
        A setting's argument or environment variable has a value the setting does not allow.
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
    InvalidSeverityError
        The schema declares a severity this package cannot read.

    Examples
    --------
    ```python
    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema

    specs = dd.wiring.build_asset_check_specs(OrdersSchema, asset="orders")
    [(spec.name, spec.blocking) for spec in specs][:3]
    ```
    """
    validate_namespace(schema)
    rule_sets: list[_RuleSet] = _build_rule_sets(schema, check_granularity)
    # Dagster's guide documents partitioned checks, so this passes `partitions_def` despite its `PreviewWarning`.
    column_schema = dg.AssetCheckSpec(
        COLUMN_SCHEMA_CHECK,
        asset=asset,
        description=f"Columns and dtypes match {schema.__name__}.",
        blocking=True,
        partitions_def=partitions_def,
    )
    return [
        column_schema,
        *(
            dg.AssetCheckSpec(
                rule_set.name,
                asset=asset,
                description=rule_set.description,
                blocking=not filter
                and any(rule.severity == "error" for rule in rule_set.rules),
                partitions_def=partitions_def,
            )
            for rule_set in rule_sets
        ),
    ]


def _find_column_schema_problems(
    schema: type[dy.Schema], frame: pl.DataFrame | pl.LazyFrame
) -> list[dict[str, str]]:
    """Return one mapping per schema column the frame lacks or has with another dtype.

    `check_and_filter` calls it first, because `Schema.filter` raises only at `collect_all` and its error omits the columns with another dtype when a column is missing.
    """
    actual: pl.Schema = frame.collect_schema()
    return [
        {
            "column": name,
            "expected": str(column.dtype),
            "actual": str(actual[name]) if name in actual else "<missing>",
        }
        for name, column in schema.columns().items()
        if name not in actual or not column.validate_dtype(actual[name])
    ]


def _pluralize(count: int, noun: str) -> str:
    """Return `count` and `noun`, plural unless `count` is 1."""
    return f"{count} {noun}" if count == 1 else f"{count} {noun}s"


def build_column_schema_result(
    problems: Sequence[dict[str, str]] = (), *, asset_key: dg.AssetKey
) -> dg.AssetCheckResult:
    """Return the column-schema check's result.

    Its failure is always `ERROR`, the only severity at which a blocking check stops downstream assets.
    """
    if not problems:
        return dg.AssetCheckResult(
            check_name=COLUMN_SCHEMA_CHECK,
            asset_key=asset_key,
            passed=True,
            description="Every column matches.",
        )
    columns: str = ", ".join(problem["column"] for problem in problems)
    verb: str = "does" if len(problems) == 1 else "do"
    return dg.AssetCheckResult(
        check_name=COLUMN_SCHEMA_CHECK,
        asset_key=asset_key,
        passed=False,
        severity=dg.AssetCheckSeverity.ERROR,
        description=f"{_pluralize(len(problems), 'column')} {verb} not match: {columns}.",
        metadata={
            "dataframely/column_schema_errors": dg.MetadataValue.table([
                dg.TableRecord(problem) for problem in problems
            ])
        },
    )


def check_and_filter(
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    asset_key: dg.AssetKey,
) -> Generator[dg.AssetCheckResult, None, tuple[pl.DataFrame, dy.FailureInfo]]:
    """Run the column-schema check, then separate the valid rows from the invalid rows.

    On a mismatch it yields the failing result before it raises, or the check would have no result.
    """
    problems: list[dict[str, str]] = _find_column_schema_problems(schema, frame)
    if problems:
        yield build_column_schema_result(problems, asset_key=asset_key)
        raise ColumnSchemaError(schema.__name__, problems)
    # `docs/pre-1.0.md` has why; `tests/test_upstream_characterization.py` checks that Dataframely forwards `engine` to Polars.
    result, failure = schema.filter(frame.lazy(), cast=False).collect_all(
        engine="streaming"
    )
    valid: pl.DataFrame = result
    return valid, failure


_INVALID = "invalid"


def get_rule_columns(schema: type[dy.Schema], details: pl.DataFrame) -> list[str]:
    """Return the rule columns in `details`, in the schema's rule order."""
    present: pl.Schema = details.collect_schema()
    return [rule for rule in describe_rules(schema) if rule in present]


def _read_failure_examples(
    schema: type[dy.Schema],
    failure: dy.FailureInfo,
    counts: dict[str, int],
    limit: int,
) -> dict[str, list[Row]]:
    """Return up to `limit` rows that failed each rule in `counts`, under the same key."""
    if not limit or not counts:
        return {}
    # `details()` rebuilds the frame on every call.
    details: pl.DataFrame = failure.details()
    # The examples omit the rule columns, because the check already shows the rule.
    columns: list[str] = get_rule_columns(schema, details)
    return {
        rule: sample_rows(details.filter(pl.col(rule) == _INVALID).drop(columns), limit)
        for rule in counts
    }


def describe_failed_rows(failed: int) -> str:
    """Describe the result of a check for one rule or one collection filter.

    It omits the rule, because the spec's description already shows it.
    """
    return f"{_pluralize(failed, 'row')} failed." if failed else "No row failed."


def _describe_failed_rules(failed: dict[str, int]) -> str:
    """Describe the result of a check for several rules, naming each rule that failed."""
    counts: list[str] = [
        f"{rule} ({_pluralize(count, 'row')})"
        for rule, count in failed.items()
        if count
    ]
    if not counts:
        return "No rule failed."
    return f"{_pluralize(len(counts), 'rule')} failed: {', '.join(counts)}."


def _build_rule_metadata(
    rule: DescribedRule, failed: int, sampled: list[Row]
) -> dict[str, str | int | dg.TableMetadataValue]:
    """Return the metadata of a check for one rule."""
    metadata: dict[str, str | int | dg.TableMetadataValue] = {
        "dataframely/rule": rule.name,
        "dataframely/rule_expr": str(rule.expr),
    }
    if failed:
        # The key follows dagster-dbt's `dagster_dbt/failed_row_count`.
        metadata["dataframely/failed_row_count"] = failed
    return metadata | build_sample_metadata("dataframely/failed_examples", sampled)


def _build_collapsed_metadata(
    rules: Sequence[DescribedRule],
    failed: dict[str, int],
    sampled: dict[str, list[Row]],
) -> dict[str, dg.TableMetadataValue]:
    """Return the metadata of a check for several rules."""
    metadata: dict[str, dg.TableMetadataValue] = {
        "dataframely/rules": dg.MetadataValue.table([
            dg.TableRecord({
                "rule": rule.name,
                "failed": failed[rule.name],
                "expr": str(rule.expr),
            })
            for rule in rules
        ])
    }
    attributed: list[Row] = [
        {"dy_rule": rule.name, **row}
        for rule in rules
        for row in sampled.get(rule.name, [])
    ]
    return metadata | build_sample_metadata("dataframely/failed_examples", attributed)


def evaluate_rule_checks(
    schema: type[dy.Schema],
    failure: dy.FailureInfo,
    *,
    asset_key: dg.AssetKey,
    check_granularity: Granularity | None = None,
    max_failure_examples: int | None = None,
) -> list[dg.AssetCheckResult]:
    """Return one result per rule set from `failure`.

    A result is `ERROR` when an `"error"` rule in its set failed, so a collapsed check whose failing rules are all `"warn"` never fails the step.
    """
    counts: dict[str, int] = failure.counts()
    sampled: dict[str, list[Row]] = _read_failure_examples(
        schema, failure, counts, MAX_FAILURE_EXAMPLES.resolve(max_failure_examples)
    )
    results: list[dg.AssetCheckResult] = []
    for rule_set in _build_rule_sets(schema, check_granularity):
        failed: dict[str, int] = {
            rule.name: counts.get(rule.name, 0) for rule in rule_set.rules
        }
        first: DescribedRule = rule_set.rules[0]
        if rule_set.collapsed:
            description: str = _describe_failed_rules(failed)
            metadata: Mapping[str, str | int | dg.TableMetadataValue] = (
                _build_collapsed_metadata(rule_set.rules, failed, sampled)
            )
        else:
            description = describe_failed_rows(failed[first.name])
            metadata = _build_rule_metadata(
                first, failed[first.name], sampled.get(first.name, [])
            )
        results.append(
            dg.AssetCheckResult(
                check_name=rule_set.name,
                asset_key=asset_key,
                passed=not any(failed.values()),
                severity=dg.AssetCheckSeverity.ERROR
                if any(
                    failed[rule.name] and rule.severity == "error"
                    for rule in rule_set.rules
                )
                else dg.AssetCheckSeverity.WARN,
                description=description,
                metadata=metadata,
            )
        )
    return results


def evaluate_checks(
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    asset_key: dg.AssetKey,
    check_granularity: Granularity | None = None,
    max_failure_examples: int | None = None,
) -> Iterator[dg.AssetCheckResult]:
    """Yield a result for each check `build_asset_check_specs` declares, without writing any rows.

    Each failing rule's check has its rule's severity, as in `dd.asset`.

    Parameters
    ----------
    schema
        The schema whose rules the frame is checked against.
    frame
        The frame to check. A `pl.LazyFrame` is collected once.
    asset_key
        The key of the asset the results report on, which the specs name too.
    check_granularity
        Pass the value `build_asset_check_specs` received, or the results name checks the asset does not declare. `None` uses `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY` if set, else `rule`.
    max_failure_examples
        A rule's check metadata has at most this many rows that failed it. `None` uses `DAGSTER_DATAFRAMELY_MAX_FAILURE_EXAMPLES` if set, else `5`.

    Yields
    ------
    :
        The column-schema check's result, then one result per rule set, in the order of `build_asset_check_specs`.

    Raises
    ------
    InvalidSettingError
        A setting's argument or environment variable has a value the setting does not allow.
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
    InvalidSeverityError
        The schema declares a severity this package cannot read.
    ColumnSchemaError
        The frame's columns or dtypes do not match the schema. It yields the column-schema check's failing result first.

    Examples
    --------
    ```python
    import dagster as dg

    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema, marketplace_orders

    results = dd.wiring.evaluate_checks(
        OrdersSchema, marketplace_orders, asset_key=dg.AssetKey("orders")
    )
    [result.check_name for result in results if not result.passed]
    ```
    """
    # Before the settings resolve or anything reads the frame (ADR-0008).
    validate_namespace(schema)
    # Collects the valid rows too; `docs/pre-1.0.md` has why.
    _, failure = yield from check_and_filter(schema, frame, asset_key=asset_key)
    yield build_column_schema_result(asset_key=asset_key)
    yield from evaluate_rule_checks(
        schema,
        failure,
        asset_key=asset_key,
        check_granularity=check_granularity,
        max_failure_examples=max_failure_examples,
    )
