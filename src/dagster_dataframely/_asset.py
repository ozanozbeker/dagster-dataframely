"""The `dd.asset` decorator (ADR-0005).

It has no `from __future__ import annotations`, because Dagster reads the annotations at run time.
"""

import functools
import inspect
from collections.abc import Callable, Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from typing import Any

import dagster as dg
import dataframely as dy

# Dagster keeps both private; `tests/test_upstream_characterization.py` pins them.
from dagster._core.definitions.assets.definition.asset_dep import CoercibleToAssetDep
from dagster._core.definitions.decorators.op_decorator import is_context_provided

from dagster_dataframely._checks import check_specs
from dagster_dataframely._metadata import schema_metadata
from dagster_dataframely._quarantine import (
    QuarantineWriter,
    quarantine_writer,
    validate_quarantine_key,
)
from dagster_dataframely._returns import (
    DecoratedReturn,
    frame_and_result,
    with_returned_fields,
)
from dagster_dataframely._rules import validate_namespace
from dagster_dataframely._runtime import AssetYield, validation_results
from dagster_dataframely._settings import (
    CHECK_GRANULARITY,
    MAX_FAILURE_SAMPLES,
    QUARANTINE_DIR,
    ROW_SAMPLE,
    SCHEMA_RULES,
    STATISTICS,
    Granularity,
    SchemaRules,
)
from dagster_dataframely.errors import CollectionNotSupportedError

DecoratedFn = Callable[..., DecoratedReturn]

AutomationCondition = (
    dg.AutomationCondition[dg.AssetKey]
    | dg.AutomationCondition[dg.AssetKey | dg.AssetCheckKey]
)


def asset(  # noqa: PLR0913 - forwards `@dg.asset`'s parameters
    schema: type[dy.Schema],
    /,
    *,
    quarantine: bool = False,
    check_granularity: Granularity | None = None,
    schema_rules: SchemaRules | None = None,
    max_failure_samples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
    name: str | None = None,
    key_prefix: str | Sequence[str] | None = None,
    ins: Mapping[str, dg.AssetIn] | None = None,
    deps: Iterable[CoercibleToAssetDep] | None = None,
    metadata: Mapping[str, Any] | None = None,
    tags: Mapping[str, str] | None = None,
    description: str | None = None,
    config_schema: Mapping[str, Any] | None = None,
    required_resource_keys: AbstractSet[str] | None = None,
    resource_defs: Mapping[str, object] | None = None,
    hooks: AbstractSet[dg.HookDefinition] | None = None,
    io_manager_key: str | None = None,
    partitions_def: dg.PartitionsDefinition[str] | None = None,
    op_tags: Mapping[str, Any] | None = None,
    group_name: str | None = None,
    automation_condition: AutomationCondition | None = None,
    freshness_policy: dg.FreshnessPolicy | None = None,
    backfill_policy: dg.BackfillPolicy | None = None,
    retry_policy: dg.RetryPolicy | None = None,
    code_version: str | None = None,
    owners: Sequence[str] | None = None,
    kinds: AbstractSet[str] | None = None,
    pool: str | None = None,
) -> Callable[[DecoratedFn], dg.AssetsDefinition]:
    """Turn the decorated function into an asset that validates the frame it returns against `schema`.

    The decorated function returns a `pl.DataFrame` or `pl.LazyFrame`, a `dg.MaterializeResult` whose `value` is one of those, or `None` to skip.

    Every keyword parameter not listed below is `@dg.asset`'s, passed to it unchanged.

    Parameters
    ----------
    quarantine
        Whether a run writes the valid rows when some rows fail. With `True`, the run writes the invalid rows to the quarantine, under the asset key `<name>_quarantine`. A run fails with `QuarantineKeyCollisionError` if another asset already materializes that key. With `False`, any invalid row fails the run, and the asset writes nothing. `True` also adds a `context` parameter to the asset, whether or not the decorated function declares one.
    check_granularity
        How many asset checks report the schema's rules: one per rule at `rule`, one per column with rules at `column`, and one for the schema at `schema`. Changing it on an asset that has already run starts a new check history. `None` uses `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY` if set, else `rule`.
    schema_rules
        Which checks report the schema-level rules at `column` granularity: `collapsed` into one check, `dy_schema__rules`, or `per_rule`, one check each. `None` uses `DAGSTER_DATAFRAMELY_SCHEMA_RULES` if set, else `collapsed`.
    max_failure_samples
        A rule's check metadata has at most this many rows that failed it, under `dy_failed_sample`. **A run writes the rows to the Dagster event log unredacted.** `dy.Config.set_max_failure_examples` does not change this number. `None` uses `DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES` if set, else `5`.
    statistics
        Whether the materialization metadata has statistics of the written rows, one table per dtype group. `None` uses `DAGSTER_DATAFRAMELY_STATISTICS` if set, else `True`.
    row_sample
        The materialization metadata has at most this many valid rows, and this many invalid rows. **A run writes the rows to the Dagster event log unredacted.** `None` uses `DAGSTER_DATAFRAMELY_ROW_SAMPLE` if set, else `5`.
    key_prefix
        The quarantine's asset key has the same prefix.
    metadata
        Definition metadata. The schema's `dagster/column_schema` entry replaces a key of the same name.
    description
        `None` uses the schema's docstring, or the decorated function's docstring if the schema has none.
    io_manager_key
        In a run, `delegating_writer` passes the invalid rows to the same IO manager.
    partitions_def
        The writer writes the quarantine under the same partition key.

    Returns
    -------
    A decorator that returns a `dg.AssetsDefinition` with the schema's check specs and a Columns tab filled from the schema.

    Raises
    ------
    CollectionNotSupportedError
        `schema` is a `dy.Collection`.
    ReservedColumnError
        A column name is in the reserved `dy_` namespace.
    InvalidColumnNameError
        A column name has a character Dagster does not allow in an asset check name.
    CheckNameCollisionError
        Two rules produce the same asset check name.
    InvalidSettingError
        A setting's argument or environment variable has a value the setting does not allow.

    Examples
    --------
    ```python
    #| echo: false
    #| output: false
    import dataframely as dy
    import polars as pl

    import dagster_dataframely as dd
    ```

    At the default `rule` granularity, the asset has one check per rule, plus the column-schema check:

    ```python
    class Orders(dy.Schema):
        order_id = dy.String(primary_key=True)
        amount = dy.Float64(nullable=False, min=0.0)


    @dd.asset(Orders, quarantine=True)
    def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return raw_orders.select("order_id", "amount")


    [spec.name for spec in orders.check_specs]
    ```

    The check specs exist before the asset first runs, so the catalog lists every check before its first result.
    """
    if isinstance(schema, type) and issubclass(schema, dy.Collection):
        raise CollectionNotSupportedError(schema.__name__)

    validate_namespace(schema)

    # Resolved once, here, for both the check specs and the run.
    granularity: Granularity = CHECK_GRANULARITY.resolve(check_granularity)
    schema_rule_checks: SchemaRules = SCHEMA_RULES.resolve(schema_rules)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    emit_statistics: bool = STATISTICS.resolve(statistics)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)
    # Only to raise on a malformed value (#115).
    QUARANTINE_DIR.resolve(None)

    forwarded: dict[str, Any] = {
        "ins": ins,
        "deps": deps,
        "tags": tags,
        "description": description or inspect.cleandoc(schema.__doc__ or "") or None,
        "config_schema": config_schema,
        "required_resource_keys": required_resource_keys,
        "resource_defs": resource_defs,
        "hooks": hooks,
        "io_manager_key": io_manager_key,
        "partitions_def": partitions_def,
        "op_tags": op_tags,
        "group_name": group_name,
        "automation_condition": automation_condition,
        "freshness_policy": freshness_policy,
        "backfill_policy": backfill_policy,
        "retry_policy": retry_policy,
        "code_version": code_version,
        "owners": owners,
        "kinds": kinds,
        "pool": pool,
    }

    def decorate(fn: DecoratedFn) -> dg.AssetsDefinition:
        asset_name: str = name or fn.__name__
        prefix: list[str] = []
        if key_prefix is not None:
            prefix = [key_prefix] if isinstance(key_prefix, str) else list(key_prefix)
        key = dg.AssetKey([*prefix, asset_name])

        parameters = list(inspect.signature(fn).parameters.values())
        declares_context: bool = is_context_provided(parameters)

        def asset_yields(
            returned: DecoratedReturn, writer: QuarantineWriter | None
        ) -> AssetYield:
            frame, result = frame_and_result(returned, asset=key.to_user_string())
            yield from with_returned_fields(
                validation_results(
                    schema,
                    frame,
                    valid_key=key,
                    quarantine_writer=writer,
                    check_granularity=granularity,
                    schema_rules=schema_rule_checks,
                    max_failure_samples=failure_samples,
                    statistics=emit_statistics,
                    row_sample=sampled_rows,
                ),
                result,
                valid_key=key,
            )

        if quarantine:

            @functools.wraps(fn)
            def compute(
                context: dg.AssetExecutionContext, *args: object, **kwargs: object
            ) -> AssetYield:
                validate_quarantine_key(context)
                writer = quarantine_writer(context)
                returned: DecoratedReturn = (
                    fn(context, *args, **kwargs)
                    if declares_context
                    else fn(*args, **kwargs)
                )
                yield from asset_yields(returned, writer)

            if not declares_context:
                # Positional-only if the first parameter is, as `inspect.Signature` requires.
                leading = (
                    parameters[0].kind
                    if parameters
                    and parameters[0].kind is inspect.Parameter.POSITIONAL_ONLY
                    else inspect.Parameter.POSITIONAL_OR_KEYWORD
                )
                compute.__signature__ = inspect.Signature(  # pyrefly: ignore[missing-attribute]
                    [
                        inspect.Parameter(
                            "context", leading, annotation=dg.AssetExecutionContext
                        ),
                        *parameters,
                    ]
                )
        else:

            @functools.wraps(fn)
            def compute(*args: object, **kwargs: object) -> AssetYield:
                yield from asset_yields(fn(*args, **kwargs), None)

        return dg.asset(
            name=asset_name,
            key_prefix=prefix or None,
            output_required=False,
            metadata={**(metadata or {}), **schema_metadata(schema)},
            check_specs=check_specs(
                schema,
                asset=key,
                check_granularity=granularity,
                schema_rules=schema_rule_checks,
            ),
            **forwarded,
        )(compute)

    return decorate
