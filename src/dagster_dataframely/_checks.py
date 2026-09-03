"""Asset-check specs derived from a schema, and the results a run reports against them.

Specs come off the schema, never off a run's `FailureInfo`. A rule nothing failed still gets a spec and reports `0 failed`.

`check_granularity` decides how many specs there are. A 40-column schema contributes around 120 rules, and nobody reads a check list that long. Specs and results come from the same grouping call, so a check can never report for rules its spec did not claim.
"""

from dataclasses import dataclass, field

import dagster as dg
import dataframely as dy
import polars as pl
from dataframely._rule import Rule

from dagster_dataframely._naming import (
    COLUMN_SCHEMA_CHECK,
    SCHEMA_RULES_CHECK,
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

    Attributes
    ----------
    name
        The asset-check name.
    description
        What the check says it covers.
    rules
        The rules it reports for, in the schema's own order.
    collapsed
        Whether the check stands for a set of rules rather than one rule of its own. The result's metadata follows it, so a rule set holding a single rule still reports as a set when its siblings do.
    """

    name: str
    description: str
    rules: list[str]
    collapsed: bool = field(default=False, kw_only=True)


def _single_rule_set(schema: type[dy.Schema], rule_name: str) -> _RuleSet:
    """Build the rule set for a check that is one rule's own, at any granularity."""
    return _RuleSet(
        check_name(rule_name), check_description(schema, rule_name), [rule_name]
    )


def _rule_sets(
    schema: type[dy.Schema],
    check_granularity: Granularity | None,
    multi_column_rules: MultiColumnRules | None,
) -> list[_RuleSet]:
    """Group a schema's rules under the checks that report for them.

    The one place the settings are read, so the specs and the results cannot resolve them differently.

    At `column` granularity a column's rules land together whatever kind they are. This makes a wide `Struct` bearable: Dataframely emits one `inner_<field>_nullability` rule per field, so a ten-field struct is ten rules and one rule set.

    Parameters
    ----------
    schema
        The schema to read rules from.
    check_granularity
        How far the rules collapse, or `None` to resolve through the settings chain.
    multi_column_rules
        Where the rules no single column owns land, or `None` to resolve through the settings chain. Only `column` granularity reads it.

    Returns
    -------
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
    """Build the schema's check specs, plus the column-schema check.

    Parameters
    ----------
    schema
        The schema the checks are derived from.
    asset
        The asset key the checks hang off. Build it once and pass the same key to the asset's out, so the two cannot drift.
    check_granularity
        How far the rules collapse: one check per rule, per rule-bearing column, or one for the whole schema. Changing it on an asset that has already run orphans check history: the old names stop being reported and the new ones start empty. Unset resolves through the settings chain.
    multi_column_rules
        Where the rules no single column owns land at `column` granularity. Unset resolves through the settings chain.

    Returns
    -------
    The column-schema check's spec first, then one spec per rule set.

    Raises
    ------
    InvalidSettingError
        A setting resolved to a value outside its vocabulary.
    ReservedColumnError
        A user column sits inside the reserved namespace.
    CheckNameCollisionError
        Two rules rewrite to the same check name.
    """
    validate_namespace(schema)
    rule_sets: list[_RuleSet] = _rule_sets(
        schema, check_granularity, multi_column_rules
    )
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


_INVALID = "invalid"
"""What `FailureInfo.details()` calls a row that failed a rule."""


def _failed_rows(
    schema: type[dy.Schema],
    failure: dy.FailureInfo,
    counts: dict[str, int],
    limit: int,
) -> dict[str, list[Row]]:
    """Sample the rows that failed each rule, bounded per rule.

    Per rule, not per check, because a check can stand for a hundred rules. A bound shared across a rule set would let the rule a thousand rows failed crowd out the rule one row failed, and the second is the more interesting.

    Parameters
    ----------
    schema
        The schema the rules belong to.
    failure
        What `Schema.filter` reported.
    counts
        Failure count per rule, which the caller already asked for. `counts()` is an aggregate over the invalid rows, not a lookup, so it is not read off `failure` again.
    limit
        How many rows to keep per rule. Zero samples nothing and never touches the frame.

    Returns
    -------
    Up to `limit` rows per rule anything failed, keyed by rule name. Rules nothing failed are absent, so a caller iterates only what failed.
    """
    if not limit or not counts:
        return {}
    # Bound once: `details()` rebuilds the frame on every call.
    details: pl.DataFrame = failure.details()
    # The rule columns are the quarantine's own, and the check already says which rule this is.
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
    """Build the metadata of a check that reports for one rule."""
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
    """Build the metadata of a check that reports for several rules.

    One row per member rule, so collapsing loses nothing. The check says whether anything failed; this says which rules and by how much.

    There is no total. Failure counts are per rule and one row can break several, so a sum would state a row count that is not one.

    The sample carries `dy_rule` for the same reason: a rule set stands for several rules, so an invalid row has to name the one that put it there. Prepending the column is safe because a user column cannot sit inside the reserved namespace.
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
    """Build one result per check out of what the filter held back.

    Severity is the run's outcome, not the rule's. When nothing is written, no failure is a warning.

    The whole `FailureInfo`, not its counts, because a check reports two things about a rule that must come from the same object: how many rows failed it, and which rows.

    Parameters
    ----------
    schema
        The schema the results report against.
    failure
        What `Schema.filter` reported.
    asset_key
        The asset the results hang off. Stated because a run that writes nothing yields results with no materialization to infer it from.
    severity
        Severity for every failing result in this run.
    check_granularity
        How far the rules collapse. Pass what the specs were derived with; the decorator does, so a run cannot report against a check list it did not declare.
    multi_column_rules
        Where the rules no single column owns land at `column` granularity.
    max_failure_samples
        How many invalid rows each rule shows. Unset resolves through the settings chain.

    Returns
    -------
    One result per rule set, in the order and under the names `check_specs` claimed, because both read the rule sets from one call. A rule nothing failed still gets a result, so a clean run is a row in every rule's history, not a gap.
    """
    counts: dict[str, int] = failure.counts()
    rules: dict[str, Rule] = validation_rules(schema)
    sampled: dict[str, list[Row]] = _failed_rows(
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
