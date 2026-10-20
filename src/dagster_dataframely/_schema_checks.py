"""`dd.schema_checks`, which reports a schema's rules as asset checks on a table that something other than `dd.asset` wrote.

It builds one `@dg.multi_asset_check` from `asset_check_specs` and `asset_check_results`, as `_collection.py` does for a Collection's collection filters.
It resolves each setting once and passes the value to both, so the results always name the checks the specs declare.
"""

from collections.abc import Iterator

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._checks import asset_check_results, asset_check_specs
from dagster_dataframely._settings import (
    CHECK_GRANULARITY,
    MAX_FAILURE_SAMPLES,
    SCHEMA_RULES,
    Granularity,
    SchemaRules,
)
from dagster_dataframely.errors import ColumnSchemaError


def schema_checks(  # noqa: PLR0913 - takes the specs' settings
    schema: type[dy.Schema],
    /,
    *,
    asset: str | dg.AssetKey,
    name: str | None = None,
    partitions_def: dg.PartitionsDefinition | None = None,
    check_granularity: Granularity | None = None,
    schema_rules: SchemaRules | None = None,
    max_failure_samples: int | None = None,
) -> dg.AssetChecksDefinition:
    """Return an asset check for each of the schema's rules on `asset`, plus the column-schema check.

    The checks read the asset's table through its own IO manager, as a `pl.DataFrame`, and write nothing.

    Parameters
    ----------
    name
        The name of the step that runs the checks. `None` uses the asset key's Python identifier followed by `__checks`, because the schema's name in snake case is often the asset's own op name.
    partitions_def
        The asset's partitions, so each run checks one partition. Dagster does not check that it matches the asset's.

    Returns
    -------
    :
        One definition with every check, which Dagster runs in one step.

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
        In a run, the table's columns or dtypes do not match the schema.
    """
    key: dg.AssetKey = dg.AssetKey.from_coercible(asset)
    granularity: Granularity = CHECK_GRANULARITY.resolve(check_granularity)
    schema_rule_checks: SchemaRules = SCHEMA_RULES.resolve(schema_rules)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    specs: list[dg.AssetCheckSpec] = asset_check_specs(
        schema,
        asset=key,
        partitions_def=partitions_def,
        check_granularity=granularity,
        schema_rules=schema_rule_checks,
    )

    def _results(
        context: dg.AssetCheckExecutionContext, table: pl.DataFrame
    ) -> Iterator[dg.AssetCheckResult]:
        selected: set[str] = {check.name for check in context.selected_asset_check_keys}
        results: Iterator[dg.AssetCheckResult] = asset_check_results(
            schema,
            table,
            asset_key=key,
            check_granularity=granularity,
            schema_rules=schema_rule_checks,
            max_failure_samples=failure_samples,
        )
        try:
            yield from (result for result in results if result.check_name in selected)
        except ColumnSchemaError as error:
            raise ColumnSchemaError(
                schema.__name__, error.problems, stored=True
            ) from None

    return dg.multi_asset_check(
        specs=specs,
        ins={"table": dg.AssetIn(key=key)},
        name=name or f"{key.to_python_identifier()}__checks",
        can_subset=True,
    )(_results)
