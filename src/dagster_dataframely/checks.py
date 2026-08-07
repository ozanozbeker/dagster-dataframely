"""Asset-check specs derived from a schema, and the results a run reports against them.

Specs come off the schema, never off a run's `FailureInfo`. A rule that rejected nothing still gets a spec and still reports `0 failed`, so a clean run is a row in every rule's history rather than a gap in it.
"""

import dagster as dg
import dataframely as dy

from dagster_dataframely.naming import (
    GATE_CHECK,
    check_name,
    rule_description,
    validate_namespace,
    validation_rules,
)


def check_specs(
    schema: type[dy.Schema],
    *,
    # Not `dg.CoercibleToAssetKey`: typing-only, so absent at runtime.
    asset: str | dg.AssetKey,
) -> list[dg.AssetCheckSpec]:
    """Builds one check spec per validation rule, plus the schema gate.

    Args:
        schema: The schema the checks are derived from.
        asset: The asset key the checks hang off. Build it once and pass the same key to the asset's out, so the two cannot drift.

    Returns:
        The gate spec first, then one spec per rule in the schema's own order.

    Raises:
        ReservedColumnError: A user column sits inside the reserved namespace.
        CheckNameCollisionError: Two rules rewrite to the same check name.
    """
    validate_namespace(schema)
    gate = dg.AssetCheckSpec(
        GATE_CHECK,
        asset=asset,
        description=f"Columns and dtypes match {schema.__name__}.",
        blocking=True,
    )
    return [
        gate,
        *(
            dg.AssetCheckSpec(
                check_name(rule),
                asset=asset,
                # The rendered-constraint rung of the ladder arrives with #20.
                description=rule_description(schema, rule) or rule,
            )
            for rule in validation_rules(schema)
        ),
    ]


def _rule_results(
    schema: type[dy.Schema],
    counts: dict[str, int],
    *,
    asset_key: dg.AssetKey,
    severity: dg.AssetCheckSeverity,
) -> list[dg.AssetCheckResult]:
    """Builds one result per rule from `FailureInfo.counts()`.

    Severity is the run's outcome rather than the rule's: when nothing lands, no failure is a warning.

    Args:
        schema: The schema the results report against.
        counts: Failure count per rule; rules that rejected nothing are absent.
        asset_key: The asset the results hang off. Stated explicitly because the abort path yields results on their own, with no materialization to infer it from.
        severity: Severity for every failing result in this run.
    """
    results: list[dg.AssetCheckResult] = []
    for rule, definition in validation_rules(schema).items():
        failed: int = counts.get(rule, 0)
        metadata: dict[str, str | int] = {
            "dy_rule": rule,
            # The expression, not the bound: tightening `min` must not rename the check and orphan its history.
            "dy_rule__expr": str(definition.expr),
        }
        if failed:
            metadata["dy_failed_count"] = failed
        results.append(
            dg.AssetCheckResult(
                check_name=check_name(rule),
                asset_key=asset_key,
                passed=not failed,
                severity=severity,
                metadata=metadata,
            )
        )
    return results
