"""Asset-check specs derived from a schema, and the results a run reports against them.

Specs come off the schema, never off a run's `FailureInfo`. A rule nothing failed still gets a spec and reports `0 failed`.

`check_granularity` decides how many specs there are. A 40-column schema contributes around 120 rules, and nobody reads a check list that long. Specs and results come from the same grouping call, so a check can never report for rules its spec did not claim.
"""

from collections.abc import Iterator, Sequence
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
    MULTI_COLUMN_RULES,
    Granularity,
    MultiColumnRules,
)
from dagster_dataframely.errors import ColumnSchemaError


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
    rules: list[DescribedRule]
    collapsed: bool = field(default=False, kw_only=True)


def _single_rule_set(schema: type[dy.Schema], rule: DescribedRule) -> _RuleSet:
    """Build the rule set for a check that is one rule's own, at any granularity."""
    return _RuleSet(rule.check_name, check_description(schema, rule), [rule])


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
            (alone if multi_column == "per_rule" else unowned).append(rule)
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
                # By name, not rendered: a `@dy.rule()` has no constraint to render, and a rendered `primary_key` would put its own commas inside this comma-separated list.
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
    # Spelled out because `dagster` exports no name for the keys a check spec accepts.
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


def column_schema_problems(
    schema: type[dy.Schema], frame: pl.DataFrame | pl.LazyFrame
) -> list[dict[str, str]]:
    """Compare the frame's columns and dtypes against the schema, naming every mismatch.

    An explicit pre-check, not a `try`/`except` around `filter`. `Schema.filter` takes a plan, so a mismatch would appear only at `collect_all`, after the plan had run, as whatever Polars raises for the first bad column. This runs first, executes nothing, and names every offending column at once. `collect_schema()` resolves a `LazyFrame`'s column schema without running it.

    Parameters
    ----------
    schema
        The schema the frame claims to match.
    frame
        The frame to compare, eager or lazy.

    Returns
    -------
    One mapping of `column`, `expected` and `actual` per offending column, empty when the frame matches. The same list feeds the failing check's metadata and `ColumnSchemaError`, so the two cannot disagree.
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
    """Build the column-schema check's result, passing when there is nothing to report.

    A failure is `ERROR` whatever the run's outcome, and whatever severity a caller asked for its rules. This is the one blocking check, and a frame whose columns do not match the schema is a pipeline defect rather than a data one: grading it any lower would leave a drifted table feeding downstream.

    Parameters
    ----------
    problems
        What `column_schema_problems` found, empty when the frame matched.
    asset_key
        The asset the result hangs off.

    Returns
    -------
    The result, tabulating every offending column when there is one.
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
            "dy_schema__errors": dg.MetadataValue.table(
                [dg.TableRecord(problem) for problem in problems]
            )
        },
    )


_INVALID = "invalid"
"""What `FailureInfo.details()` calls a row that failed a rule."""


def rule_columns(schema: type[dy.Schema], details: pl.DataFrame) -> list[str]:
    """List the rule columns `FailureInfo.details()` carries, in the schema's own rule order.

    Parameters
    ----------
    schema
        The schema the rules belong to.
    details
        What `FailureInfo.details()` returned, bound once by the caller because it rebuilds the frame on every call.

    Returns
    -------
    The rule names present as columns, as Dataframely names them.
    """
    present: pl.Schema = details.collect_schema()
    return [rule for rule in described_rules(schema) if rule in present]


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
    columns: list[str] = rule_columns(schema, details)
    return {
        rule: sample_rows(details.filter(pl.col(rule) == _INVALID).drop(columns), limit)
        for rule in counts
    }


def _rule_metadata(
    rule: DescribedRule, failed: int, sampled: list[Row]
) -> dict[str, str | int | dg.TableMetadataValue]:
    """Build the metadata of a check that reports for one rule."""
    metadata: dict[str, str | int | dg.TableMetadataValue] = {
        "dy_rule": rule.name,
        # The expression, not the bound: tightening `min` must not rename the check and orphan its history.
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
    """Build the metadata of a check that reports for several rules.

    One row per member rule, so collapsing loses nothing. The check says whether anything failed; this says which rules and by how much.

    There is no total. Failure counts are per rule and one row can break several, so a sum would state a row count that is not one.

    The sample carries `dy_rule` for the same reason: a rule set stands for several rules, so an invalid row has to name the one that put it there. Prepending the column is safe because a user column cannot sit inside the reserved namespace, which every public function taking a schema now enforces rather than assumes (ADR-0008).
    """
    metadata: dict[str, dg.TableMetadataValue] = {
        "dy_rules": dg.MetadataValue.table(
            [
                dg.TableRecord(
                    {
                        "rule": rule.name,
                        "failed": failed[rule.name],
                        "expr": str(rule.expr),
                    }
                )
                for rule in rules
            ]
        )
    }
    attributed: list[Row] = [
        {"dy_rule": rule.name, **row}
        for rule in rules
        for row in sampled.get(rule.name, [])
    ]
    return metadata | sample_metadata("dy_failed_sample", attributed)


def rule_results(  # noqa: PLR0913 - the specs' settings reach the results, pinned by test_the_results_answer_exactly_the_specs_at_every_granularity
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
    sampled: dict[str, list[Row]] = _failed_rows(
        schema, failure, counts, MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    )
    results: list[dg.AssetCheckResult] = []
    for rule_set in _rule_sets(schema, check_granularity, multi_column_rules):
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


def check_results(  # noqa: PLR0913 - the specs' settings reach the results, pinned by test_the_results_answer_exactly_the_specs_at_every_granularity
    schema: type[dy.Schema],
    frame: pl.DataFrame | pl.LazyFrame,
    *,
    asset_key: dg.AssetKey,
    severity: dg.AssetCheckSeverity,
    check_granularity: Granularity | None = None,
    multi_column_rules: MultiColumnRules | None = None,
    max_failure_samples: int | None = None,
) -> Iterator[dg.AssetCheckResult]:
    """Answer every check `check_specs` declared, for an asset that reports and writes nothing.

    The counterpart to `check_specs`. One declares, the other evaluates, and neither knows anything about storage. Reach for it when the checks are all you want: an asset that manages its own storage, or a `@dg.multi_asset_check` reporting on a table that has already been written.

    `validation_results` minus the writing and the failure policy. This never raises `ValidationAbortError` or `NothingSurvivedError`, because both answer one question, what happens to rejected rows, and a caller that writes nothing has no rows to route and no table to withhold. It yields no materialization either.

    **Severity is stated, not derived.** `validation_results` grades it from whether the valid table was written, which is a property of the run's outcome. A caller here has no such outcome, and the precedent cuts both ways: the table was written, which `validation_results` calls `WARN`, but the rejected rows went into it rather than to a quarantine, which is worse than the case `validation_results` calls `ERROR`. So the caller says which, and every rule check in the run carries it.

    **A column-schema mismatch reports that check and raises, as `validation_results` does.** The rules never ran, so nothing reports for them. The raise is what stops Dagster looking for the outputs they would have answered: a generator that raises never reaches its missing-output check, so the step fails with this error rather than an opaque one about an output nobody wrote.

    The valid rows are collected with the invalid ones and discarded. It is the same `collect_all` call `validation_results` makes, so the two arrangements execute alike, and taking only the failure half measured worse: `FailureInfo` collects on `auto`, which keeps the plan's own peak.

    Parameters
    ----------
    schema
        The schema the results report against.
    frame
        The frame to evaluate, eager or lazy. A `LazyFrame` executes here, once. Nothing refuses a wrong type here; `docs/out-of-scope/wiring-argument-type-guards.md` says why (#124).
    asset_key
        The asset the results hang off. Stated because a standalone result has no materialization to infer it from.
    severity
        Severity for every failing rule check. The column-schema check keeps its own.
    check_granularity
        How far the rules collapse. Pass the value the check specs were derived with, or the results answer a check list the asset never declared. Unset resolves through the settings chain.
    multi_column_rules
        Where the rules no single column owns land at `column` granularity, on the same terms.
    max_failure_samples
        How many invalid rows each rule shows. Unset resolves through the settings chain.

    Yields
    ------
    The column-schema check's result first, then one result per rule set, in the order and under the names `check_specs` claimed. A rule nothing failed still gets a result.

    Raises
    ------
    InvalidSettingError
        A setting resolved to a value outside its vocabulary.
    ReservedColumnError
        A user column sits inside the reserved namespace.
    CheckNameCollisionError
        Two rules rewrite to the same check name.
    ColumnSchemaError
        The frame's columns or dtypes do not match the schema, reported through the column-schema check before this is raised.

    Examples
    --------
    ```python
    from collections.abc import Iterator

    import dagster as dg
    import dataframely as dy
    import polars as pl

    import dagster_dataframely as dd


    class Orders(dy.Schema):
        order_id = dy.String(primary_key=True)


    KEY = dg.AssetKey(["orders"])


    @dg.asset(name="orders", metadata=dd.wiring.schema_metadata(Orders))
    def orders() -> pl.DataFrame:
        return pl.DataFrame({"order_id": ["a"]})


    @dg.multi_asset_check(specs=dd.wiring.check_specs(Orders, asset=KEY))
    def orders_checks(orders: pl.DataFrame) -> Iterator[dg.AssetCheckResult]:
        yield from dd.wiring.check_results(
            Orders, orders, asset_key=KEY, severity=dg.AssetCheckSeverity.WARN
        )
    ```
    """
    # Before the settings resolve and before the frame is read, as `check_specs` does it. A schema this package cannot name is broken whatever the frame holds (ADR-0008).
    validate_namespace(schema)
    problems: list[dict[str, str]] = column_schema_problems(schema, frame)
    if problems:
        yield column_schema_result(problems, asset_key=asset_key)
        raise ColumnSchemaError(schema.__name__, problems)

    # One `collect_all` on the streaming engine, the call `validation_results` makes, for the reason it makes it.
    _, failure = schema.filter(frame.lazy(), cast=False).collect_all(engine="streaming")
    yield column_schema_result(asset_key=asset_key)
    yield from rule_results(
        schema,
        failure,
        asset_key=asset_key,
        severity=severity,
        check_granularity=check_granularity,
        multi_column_rules=multi_column_rules,
        max_failure_samples=max_failure_samples,
    )
