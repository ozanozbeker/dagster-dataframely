"""Asset check specs derived from a schema, and the results `_runtime` and `check_results` yield for them.

Specs and results both group the rules with `_rule_sets`, so they name the same checks only when given the same settings.
"""

from collections.abc import Generator, Iterator, Sequence
from dataclasses import dataclass, field

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._naming import (
    COLUMN_SCHEMA_CHECK,
    SCHEMA_RULES_CHECK,
    column_check_name,
)
from dagster_dataframely._rendering import check_description, column_rule_summary
from dagster_dataframely._rules import (
    DescribedRule,
    described_rules,
    validate_namespace,
)
from dagster_dataframely._samples import Row, sample_metadata, sample_rows
from dagster_dataframely._settings import (
    CHECK_GRANULARITY,
    MAX_FAILURE_SAMPLES,
    SCHEMA_RULES,
    Granularity,
    SchemaRules,
)
from dagster_dataframely.errors import ColumnSchemaError


@dataclass(frozen=True)
class _RuleSet:
    """The rules one asset check reports for."""

    name: str
    description: str
    rules: list[DescribedRule]
    collapsed: bool = field(default=False, kw_only=True)


def _single_rule_set(schema: type[dy.Schema], rule: DescribedRule) -> _RuleSet:
    """Return the rule set of a check for one rule."""
    return _RuleSet(rule.check_name, check_description(schema, rule), [rule])


def _rule_sets(
    schema: type[dy.Schema],
    check_granularity: Granularity | None,
    schema_rules: SchemaRules | None,
) -> list[_RuleSet]:
    """Group the schema's rules under the checks that report for them.

    A schema with no rules gets none, because a check for no rules would always pass.
    """
    granularity: Granularity = CHECK_GRANULARITY.resolve(check_granularity)
    schema_rule_checks: SchemaRules = SCHEMA_RULES.resolve(schema_rules)
    rules: list[DescribedRule] = list(described_rules(schema).values())
    if not rules:
        return []
    if granularity == "rule":
        return [_single_rule_set(schema, rule) for rule in rules]
    if granularity == "schema":
        return [
            _RuleSet(
                SCHEMA_RULES_CHECK,
                f"Every validation rule of {schema.__name__}.",
                rules,
                collapsed=True,
            )
        ]

    columns: dict[str, list[DescribedRule]] = {}
    unowned: list[DescribedRule] = []
    alone: list[DescribedRule] = []
    for rule in rules:
        if rule.column is None:
            (alone if schema_rule_checks == "per_rule" else unowned).append(rule)
        else:
            columns.setdefault(rule.column, []).append(rule)

    rule_sets: list[_RuleSet] = [
        _RuleSet(
            column_check_name(column),
            f"Every rule on {column}: {column_rule_summary(schema, members)}.",
            members,
            collapsed=True,
        )
        for column, members in columns.items()
    ]
    if unowned:
        names: str = ", ".join(rule.name for rule in unowned)
        rule_sets.append(
            _RuleSet(
                SCHEMA_RULES_CHECK,
                # Names: a `@dy.rule()` has no constraint to render, and a rendered `primary_key` has commas.
                f"Every rule of {schema.__name__} that no single column owns: {names}.",
                unowned,
                collapsed=True,
            )
        )
    rule_sets.extend(_single_rule_set(schema, rule) for rule in alone)
    return rule_sets


def check_specs(
    schema: type[dy.Schema],
    *,
    # `dagster` has no alias for this union.
    asset: str | dg.AssetKey,
    check_granularity: Granularity | None = None,
    schema_rules: SchemaRules | None = None,
) -> list[dg.AssetCheckSpec]:
    """Return the asset check specs for the schema's rules, plus the column-schema check.

    Parameters
    ----------
    check_granularity
        How many checks report the schema's rules. Changing it on an asset that has already run starts a new check history. `None` uses `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY` if set, else `rule`.
    schema_rules
        Which checks report the schema-level rules at `column` granularity. `None` uses `DAGSTER_DATAFRAMELY_SCHEMA_RULES` if set, else `collapsed`.

    Returns
    -------
    The column-schema check's spec first, then one spec per rule set.

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
    """
    validate_namespace(schema)
    rule_sets: list[_RuleSet] = _rule_sets(schema, check_granularity, schema_rules)
    column_schema = dg.AssetCheckSpec(
        COLUMN_SCHEMA_CHECK,
        asset=asset,
        description=f"Columns and dtypes match {schema.__name__}.",
        blocking=True,
    )
    return [
        column_schema,
        *(
            dg.AssetCheckSpec(
                rule_set.name, asset=asset, description=rule_set.description
            )
            for rule_set in rule_sets
        ),
    ]


def _column_schema_problems(
    schema: type[dy.Schema], frame: pl.DataFrame | pl.LazyFrame
) -> list[dict[str, str]]:
    """Return one mapping per schema column the frame lacks or has with another dtype.

    `filtered` calls it first, because `Schema.filter` fails only at `collect_all`, on the first mismatched column.
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


def column_schema_result(
    problems: Sequence[dict[str, str]] = (), *, asset_key: dg.AssetKey
) -> dg.AssetCheckResult:
    """Return the column-schema check's result.

    Its failure is always `ERROR`, the only severity at which a blocking check stops downstream assets (ADR-0004).
    """
    if not problems:
        return dg.AssetCheckResult(
            check_name=COLUMN_SCHEMA_CHECK, asset_key=asset_key, passed=True
        )
    return dg.AssetCheckResult(
        check_name=COLUMN_SCHEMA_CHECK,
        asset_key=asset_key,
        passed=False,
        severity=dg.AssetCheckSeverity.ERROR,
        metadata={
            "dy_schema__errors": dg.MetadataValue.table([
                dg.TableRecord(problem) for problem in problems
            ])
        },
    )


def filtered(
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    asset_key: dg.AssetKey,
) -> Generator[dg.AssetCheckResult, None, tuple[pl.DataFrame, dy.FailureInfo]]:
    """Run the column-schema check, then separate the valid rows from the invalid rows.

    On a mismatch it yields the failing result before it raises, or the check would have no result.
    """
    problems: list[dict[str, str]] = _column_schema_problems(schema, frame)
    if problems:
        yield column_schema_result(problems, asset_key=asset_key)
        raise ColumnSchemaError(schema.__name__, problems)
    # `docs/pre-1.0.md` has why; `tests/test_upstream_characterization.py` pins the engine forward.
    result, failure = schema.filter(frame.lazy(), cast=False).collect_all(
        engine="streaming"
    )
    # `dy.DataFrame[Schema]` exists only for type checkers.
    valid: pl.DataFrame = result
    return valid, failure


_INVALID = "invalid"


def rule_columns(schema: type[dy.Schema], details: pl.DataFrame) -> list[str]:
    """Return the rule columns in `details`, in the schema's rule order."""
    present: pl.Schema = details.collect_schema()
    return [rule for rule in described_rules(schema) if rule in present]


def _failed_rows(
    schema: type[dy.Schema],
    failure: dy.FailureInfo,
    counts: dict[str, int],
    limit: int,
) -> dict[str, list[Row]]:
    """Return up to `limit` rows that failed each rule, keyed by rule name."""
    if not limit or not counts:
        return {}
    # `details()` rebuilds the frame on every call.
    details: pl.DataFrame = failure.details()
    # Dropped: the check already shows the rule.
    columns: list[str] = rule_columns(schema, details)
    return {
        rule: sample_rows(details.filter(pl.col(rule) == _INVALID).drop(columns), limit)
        for rule in counts
    }


def _rule_metadata(
    rule: DescribedRule, failed: int, sampled: list[Row]
) -> dict[str, str | int | dg.TableMetadataValue]:
    """Return the metadata of a check for one rule."""
    metadata: dict[str, str | int | dg.TableMetadataValue] = {
        "dy_rule": rule.name,
        "dy_rule__expr": str(rule.expr),
    }
    if failed:
        metadata["dy_failed_count"] = failed
    return metadata | sample_metadata("dy_failed_sample", sampled)


def _collapsed_metadata(
    rules: Sequence[DescribedRule],
    failed: dict[str, int],
    sampled: dict[str, list[Row]],
) -> dict[str, dg.TableMetadataValue]:
    """Return the metadata of a check for several rules."""
    metadata: dict[str, dg.TableMetadataValue] = {
        "dy_rules": dg.MetadataValue.table([
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
    return metadata | sample_metadata("dy_failed_sample", attributed)


def rule_results(  # noqa: PLR0913 - takes the specs' settings
    schema: type[dy.Schema],
    failure: dy.FailureInfo,
    *,
    asset_key: dg.AssetKey,
    severity: dg.AssetCheckSeverity,
    check_granularity: Granularity | None = None,
    schema_rules: SchemaRules | None = None,
    max_failure_samples: int | None = None,
) -> list[dg.AssetCheckResult]:
    """Return one result per rule set from `failure`."""
    counts: dict[str, int] = failure.counts()
    sampled: dict[str, list[Row]] = _failed_rows(
        schema, failure, counts, MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    )
    results: list[dg.AssetCheckResult] = []
    for rule_set in _rule_sets(schema, check_granularity, schema_rules):
        failed: dict[str, int] = {
            rule.name: counts.get(rule.name, 0) for rule in rule_set.rules
        }
        first: DescribedRule = rule_set.rules[0]
        results.append(
            dg.AssetCheckResult(
                check_name=rule_set.name,
                asset_key=asset_key,
                passed=not any(failed.values()),
                severity=severity,
                metadata=_collapsed_metadata(rule_set.rules, failed, sampled)
                if rule_set.collapsed
                else _rule_metadata(
                    first, failed[first.name], sampled.get(first.name, [])
                ),
            )
        )
    return results


def check_results(  # noqa: PLR0913 - takes the specs' settings
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    asset_key: dg.AssetKey,
    severity: dg.AssetCheckSeverity,
    check_granularity: Granularity | None = None,
    schema_rules: SchemaRules | None = None,
    max_failure_samples: int | None = None,
) -> Iterator[dg.AssetCheckResult]:
    """Yield a result for each check `check_specs` declares, without writing any rows.

    Parameters
    ----------
    severity
        The severity of every failing check except the column-schema check, which is always `ERROR`.
    check_granularity
        Pass the value `check_specs` received, or the results name checks the asset does not declare. `None` uses `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY` if set, else `rule`.
    schema_rules
        Pass the value `check_specs` received, as for `check_granularity`. `None` uses `DAGSTER_DATAFRAMELY_SCHEMA_RULES` if set, else `collapsed`.
    max_failure_samples
        A rule's check metadata has at most this many rows that failed it. `None` uses `DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES` if set, else `5`.

    Yields
    ------
    The column-schema check's result, then one result per rule set, in the order of `check_specs`.

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
    ColumnSchemaError
        The frame's columns or dtypes do not match the schema. It yields the column-schema check's failing result first.
    """
    # Before the settings resolve or anything reads the frame (ADR-0008).
    validate_namespace(schema)
    # Collects the valid rows too; `docs/pre-1.0.md` has why.
    _, failure = yield from filtered(schema, frame, asset_key=asset_key)
    yield column_schema_result(asset_key=asset_key)
    yield from rule_results(
        schema,
        failure,
        asset_key=asset_key,
        severity=severity,
        check_granularity=check_granularity,
        schema_rules=schema_rules,
        max_failure_samples=max_failure_samples,
    )
