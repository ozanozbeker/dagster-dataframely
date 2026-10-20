"""`dd.build_rule_checks` returns a schema's asset checks on an asset that `dd.asset` did not build.

It builds one `@dg.multi_asset_check` from `build_asset_check_specs` and `evaluate_checks`, as `_collection.py` does for a Collection's collection filters.
It resolves `check_granularity` once and passes the value to both, so the results always name the checks the specs declare.
"""

from collections.abc import Iterator

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._checks import build_asset_check_specs, evaluate_checks
from dagster_dataframely._settings import (
    CHECK_GRANULARITY,
    MAX_FAILURE_EXAMPLES,
    Granularity,
)
from dagster_dataframely.errors import ColumnSchemaError


def build_rule_checks(  # noqa: PLR0913 - takes the specs' settings
    schema: type[dy.Schema],
    /,
    *,
    asset: str | dg.AssetKey,
    name: str | None = None,
    partitions_def: dg.PartitionsDefinition | None = None,
    check_granularity: Granularity | None = None,
    max_failure_examples: int | None = None,
) -> dg.AssetChecksDefinition:
    """Return an asset check for each of the schema's rules on `asset`, plus the column-schema check.

    The checks read the asset's table through its own IO manager, as a `pl.DataFrame`, and write nothing.

    Parameters
    ----------
    schema
        The schema whose rules become checks.
    asset
        The key of the asset whose table the checks read, which `dd.asset` did not build.
    name
        The name of the step that runs the checks. `None` uses the asset key's Python identifier followed by `__checks`, because a name built from the schema would repeat for two assets that share it, and Dagster raises on two steps with one name.
    partitions_def
        The asset's partitions, so each run checks one partition. Dagster does not check that it matches the asset's.
    check_granularity
        How many checks report the schema's rules: one per rule at `rule`, and at `column` one per column with rules plus one per schema-level rule. `None` uses `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY` if set, else `rule`.
    max_failure_examples
        A rule's check metadata has at most this many rows that failed it. `None` uses `DAGSTER_DATAFRAMELY_MAX_FAILURE_EXAMPLES` if set, else `5`.

    Returns
    -------
    :
        One definition with every check, which Dagster runs in one step. Pass it to `dg.Definitions(asset_checks=[...])`.

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

    Examples
    --------
    ```python
    import dagster_dataframely as dd
    from dagster_dataframely.data import OrdersSchema

    warehouse_checks = dd.build_rule_checks(OrdersSchema, asset="warehouse_orders")
    [spec.name for spec in warehouse_checks.check_specs][:4]
    ```
    """
    key: dg.AssetKey = dg.AssetKey.from_coercible(asset)
    granularity: Granularity = CHECK_GRANULARITY.resolve(check_granularity)
    failure_examples: int = MAX_FAILURE_EXAMPLES.resolve(max_failure_examples)
    specs: list[dg.AssetCheckSpec] = build_asset_check_specs(
        schema,
        asset=key,
        partitions_def=partitions_def,
        check_granularity=granularity,
    )

    # `pl.DataFrame`, because `DuckDBPolarsIOManager` raises on a `pl.LazyFrame` input.
    def _results(
        context: dg.AssetCheckExecutionContext, table: pl.DataFrame
    ) -> Iterator[dg.AssetCheckResult]:
        selected: set[str] = {check.name for check in context.selected_asset_check_keys}
        results: Iterator[dg.AssetCheckResult] = evaluate_checks(
            schema,
            table,
            asset_key=key,
            check_granularity=granularity,
            max_failure_examples=failure_examples,
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
