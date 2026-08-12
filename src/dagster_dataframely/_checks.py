"""Asset-check specs derived from a schema, and the results a run reports against them.

Specs come off the schema, never off a run's `FailureInfo`. A rule that rejected nothing still gets a spec and still reports `0 failed`, so a clean run is a row in every rule's history rather than a gap in it.

How many specs there are is `check_granularity`'s to decide, because a 40-column schema contributes around 120 rules and a check list that long is one nobody reads. One grouping answers that question on both sides: the specs and the results are built from the same call, so a check can never report for a set of rules its spec did not claim.
"""

from dataclasses import dataclass, field

import dagster as dg
import dataframely as dy
import polars as pl
from dataframely._rule import Rule

from dagster_dataframely._naming import (
    SCHEMA_RULES_CHECK,
    SHAPE_CHECK,
    check_name,
    column_check_name,
    split_rule,
    validate_namespace,
    validation_rules,
)
from dagster_dataframely._rendering import check_description, column_rule_summary
from dagster_dataframely._samples import Row, sample_metadata, sample_rows
from dagster_dataframely._settings import (
    CHECK_GRANULARITY,
    MAX_FAILURE_SAMPLES,
    MULTI_COLUMN_RULES,
    Granularity,
    MultiColumnRules,
)


@dataclass(frozen=True)
class _RuleSet:
    """The rules one check reports for, and how that check introduces itself.

    Attributes:
        name: The asset-check name.
        description: What the check says it covers.
        rules: The rules it reports for, in the schema's own order.
        collapsed: Whether the check stands for a set of rules rather than for one rule of its own. It is what the result's metadata shape follows, so a rule set holding a single rule still reports as a set when its siblings do.
    """

    name: str
    description: str
    rules: list[str]
    collapsed: bool = field(default=False, kw_only=True)


def _single_rule_set(schema: type[dy.Schema], rule_name: str) -> _RuleSet:
    """Builds the rule set for a check that is one rule's own, at any granularity."""
    return _RuleSet(
        check_name(rule_name), check_description(schema, rule_name), [rule_name]
    )


def _rule_sets(
    schema: type[dy.Schema],
    check_granularity: Granularity | None,
    multi_column_rules: MultiColumnRules | None,
) -> list[_RuleSet]:
    """Groups a schema's rules under the checks that report for them.

    The one place the settings are read, so the specs and the results cannot resolve them differently.

    At `column` granularity a column's rules land together whatever shape they are, which is what makes a wide `Struct` bearable: dataframely emits one `inner_<field>_nullability` rule per field, so a ten-field struct is ten checks here and one rule set.

    Args:
        schema: The schema to read rules from.
        check_granularity: How far the rules collapse, or `None` to resolve it through the settings chain.
        multi_column_rules: Where the rules no single column owns land at `column` granularity, or `None` to resolve it through the settings chain. Read nowhere else: the other two granularities have no second place to put them.

    Returns:
        One rule set per check, in the schema's own rule order, columns before the rules no column owns. A schema with no rules gets no rule sets at any granularity, because a check reporting for nothing would pass forever.
    """
    granularity: Granularity = CHECK_GRANULARITY.resolve(check_granularity)
    multi_column: MultiColumnRules = MULTI_COLUMN_RULES.resolve(multi_column_rules)
    rules: dict[str, Rule] = validation_rules(schema)
    if not rules:
        return []
    if granularity == "rule":
        return [_single_rule_set(schema, rule_name) for rule_name in rules]
    if granularity == "schema":
        return [
            _RuleSet(
                SCHEMA_RULES_CHECK,
                f"Every validation rule of {schema.__name__}.",
                list(rules),
                collapsed=True,
            )
        ]

    columns: dict[str, list[str]] = {}
    unowned: list[str] = []
    alone: list[str] = []
    for rule_name in rules:
        parts: tuple[str, str] | None = split_rule(rule_name)
        if parts is None:
            (alone if multi_column == "per_rule" else unowned).append(rule_name)
        else:
            columns.setdefault(parts[0], []).append(rule_name)

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
        rule_sets.append(
            _RuleSet(
                SCHEMA_RULES_CHECK,
                # By name, not rendered: a `@dy.rule()` has no constraint to render, and a rendered `primary_key` would put its own commas inside this comma-separated list.
                f"Every rule of {schema.__name__} that no single column owns: {', '.join(unowned)}.",
                unowned,
                collapsed=True,
            )
        )
    rule_sets.extend(_single_rule_set(schema, rule_name) for rule_name in alone)
    return rule_sets


def check_specs(
    schema: type[dy.Schema],
    *,
    # Not `dg.CoercibleToAssetKey`: typing-only, so absent at runtime.
    asset: str | dg.AssetKey,
    check_granularity: Granularity | None = None,
    multi_column_rules: MultiColumnRules | None = None,
) -> list[dg.AssetCheckSpec]:
    """Builds the schema's check specs, plus the shape check.

    Args:
        schema: The schema the checks are derived from.
        asset: The asset key the checks hang off. Build it once and pass the same key to the asset's out, so the two cannot drift.
        check_granularity: How far the rules collapse: one check per rule, per rule-bearing column, or one for the whole schema. Changing it on an asset that has already run orphans check history, because the old names stop being reported and the new ones start empty. Unset resolves through the settings chain.
        multi_column_rules: Where the rules no single column owns land at `column` granularity. Unset resolves through the settings chain.

    Returns:
        The shape check's spec first, then one spec per rule set.

    Raises:
        InvalidSettingError: A setting resolved to a value outside its vocabulary.
        ReservedColumnError: A user column sits inside the reserved namespace.
        CheckNameCollisionError: Two rules rewrite to the same check name.
    """
    validate_namespace(schema)
    rule_sets: list[_RuleSet] = _rule_sets(
        schema, check_granularity, multi_column_rules
    )
    shape = dg.AssetCheckSpec(
        SHAPE_CHECK,
        asset=asset,
        description=f"Columns and dtypes match {schema.__name__}.",
        blocking=True,
    )
    return [
        shape,
        *(
            dg.AssetCheckSpec(
                rule_set.name, asset=asset, description=rule_set.description
            )
            for rule_set in rule_sets
        ),
    ]


#: What `FailureInfo.details()` calls a row a rule rejected.
_INVALID = "invalid"


def _rejected_rows(
    schema: type[dy.Schema],
    failure: dy.FailureInfo,
    counts: dict[str, int],
    limit: int,
) -> dict[str, list[Row]]:
    """Samples the rows each rule rejected, bounded per rule.

    Per rule rather than per check, because a check can stand for a hundred of them: a bound shared across a rule set would let the rule that rejected a thousand rows crowd out the one that rejected one, and the second is the more interesting of the two.

    Args:
        schema: The schema the rules belong to.
        failure: What `Schema.filter` reported.
        counts: Failure count per rule, which the caller already asked for. Taken rather than read off `failure` again, because `counts()` is an aggregate over the invalid rows rather than a lookup.
        limit: How many rows to keep per rule. Zero samples nothing and never touches the frame.

    Returns:
        Up to `limit` rows per rule that rejected anything, keyed by the rule's own name. Rules that rejected nothing are absent, so a caller iterates what failed rather than what exists.
    """
    if not limit or not counts:
        return {}
    # Bound once: `details()` rebuilds the frame on every call.
    details: pl.DataFrame = failure.details()
    # Dropped from every sample: the rule columns are the quarantine's shape, and the check already says which rule this is.
    rule_columns: list[str] = [
        rule for rule in validation_rules(schema) if rule in details.collect_schema()
    ]
    return {
        rule: sample_rows(
            details.filter(pl.col(rule) == _INVALID).drop(rule_columns), limit
        )
        for rule in counts
    }


def _rule_metadata(
    rule_name: str, rule: Rule, failed: int, sampled: list[Row]
) -> dict[str, str | int | dg.TableMetadataValue]:
    """Builds the metadata of a check that reports for one rule."""
    metadata: dict[str, str | int | dg.TableMetadataValue] = {
        "dy_rule": rule_name,
        # The expression, not the bound: tightening `min` must not rename the check and orphan its history.
        "dy_rule__expr": str(rule.expr),
    }
    if failed:
        metadata["dy_failed_count"] = failed
    return metadata | sample_metadata("dy_failed_sample", sampled)


def _collapsed_metadata(
    rules: dict[str, Rule], failed: dict[str, int], sampled: dict[str, list[Row]]
) -> dict[str, dg.TableMetadataValue]:
    """Builds the metadata of a check that reports for several rules.

    One row per member rule, which is what collapsing would otherwise cost: the check says whether anything failed, and this says which rules and by how much.

    A single total is deliberately absent. Failure counts are per rule and one row can break several, so summing them would state a row count that is not one.

    The sample carries `dy_rule` for the same reason the counts do: a rule set stands for several rules, so a invalid row has to name the one that put it there. The column is safe to prepend because a user column cannot sit inside the reserved namespace.
    """
    metadata: dict[str, dg.TableMetadataValue] = {
        "dy_rules": dg.MetadataValue.table(
            [
                dg.TableRecord(
                    {
                        "rule": rule_name,
                        "failed": count,
                        "expr": str(rules[rule_name].expr),
                    }
                )
                for rule_name, count in failed.items()
            ]
        )
    }
    attributed: list[Row] = [
        {"dy_rule": rule_name, **row}
        for rule_name in failed
        for row in sampled.get(rule_name, [])
    ]
    return metadata | sample_metadata("dy_failed_sample", attributed)


def rule_results(  # noqa: PLR0913 - every setting the specs were derived with has to reach the results, or the two disagree
    schema: type[dy.Schema],
    failure: dy.FailureInfo,
    *,
    asset_key: dg.AssetKey,
    severity: dg.AssetCheckSeverity,
    check_granularity: Granularity | None = None,
    multi_column_rules: MultiColumnRules | None = None,
    max_failure_samples: int | None = None,
) -> list[dg.AssetCheckResult]:
    """Builds one result per check out of what the filter rejected.

    Severity is the run's outcome rather than the rule's: when nothing is written, no failure is a warning.

    The whole `FailureInfo` rather than its counts, because a check reports two things about a rule and they have to come from the same object: how many rows it rejected, and which rows those were.

    Args:
        schema: The schema the results report against.
        failure: What `Schema.filter` reported.
        asset_key: The asset the results hang off. Stated explicitly because the abort path yields results on their own, with no materialization to infer it from.
        severity: Severity for every failing result in this run.
        check_granularity: How far the rules collapse. Pass what the specs were derived with; the decorator does, so a run cannot report against a check list it did not declare.
        multi_column_rules: Where the rules no single column owns land at `column` granularity.
        max_failure_samples: How many invalid rows each rule shows. Unset resolves through the settings chain.

    Returns:
        One result per rule set, in the same order and under the same names `check_specs` claimed, because both read the rule sets from one call. A rule that rejected nothing still gets a result, so a clean run is a row in every rule's history rather than a gap in it.
    """
    counts: dict[str, int] = failure.counts()
    rules: dict[str, Rule] = validation_rules(schema)
    sampled: dict[str, list[Row]] = _rejected_rows(
        schema, failure, counts, MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    )
    results: list[dg.AssetCheckResult] = []
    for rule_set in _rule_sets(schema, check_granularity, multi_column_rules):
        failed: dict[str, int] = {
            rule_name: counts.get(rule_name, 0) for rule_name in rule_set.rules
        }
        first: str = rule_set.rules[0]
        results.append(
            dg.AssetCheckResult(
                check_name=rule_set.name,
                asset_key=asset_key,
                passed=not any(failed.values()),
                severity=severity,
                metadata=_collapsed_metadata(rules, failed, sampled)
                if rule_set.collapsed
                else _rule_metadata(
                    first, rules[first], failed[first], sampled.get(first, [])
                ),
            )
        )
    return results
