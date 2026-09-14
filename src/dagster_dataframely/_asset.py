"""The decorator: one argument attaches a Dataframely schema to a Dagster asset.

The decorator coordinates three artifacts no single `@dg.asset` parameter accepts as a bundle: the check specs, the definition metadata, and the wrapped runtime. `@dbt_assets` is the first-party precedent.

ADR-0005 has why the decorator wraps `dg.asset` rather than stacking under it: a check spec is an op output, and no public API adds one to a finished `AssetsDefinition` or swaps its compute function.

This module carries no `from __future__ import annotations`. At a 3.12 floor it would buy only unquoted forward references, and it would turn user-facing annotations into strings that Dagster's runtime introspection rejects. So every annotation is a runtime-real object, and `CoercibleToAssetDep` is imported from where Dagster defines it, because `dagster` does not export it.
"""

import functools
import inspect
from collections.abc import Callable, Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from typing import Any

import dagster as dg
import dataframely as dy

# The union `dg.asset` annotates `deps` with. Defined at runtime, not exported from `dagster`.
from dagster._core.definitions.assets.definition.asset_dep import CoercibleToAssetDep

# Upstream's own rule for whether a decorated function asked for a context.
# Characterization tests pin it.
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
"""What `dd.asset` accepts. The return union makes a function returning anything else a static error, which `tests/test_asset_runtime.py` pins with a `pyrefly: ignore` on the call that breaks it."""

AutomationCondition = (
    dg.AutomationCondition[dg.AssetKey]
    | dg.AutomationCondition[dg.AssetKey | dg.AssetCheckKey]
)
"""The union `@dg.asset` accepts, spelled out because `AutomationCondition` is generic and its two parameterizations are not interchangeable."""


def asset(  # noqa: PLR0913 - forwarding the whole parameter list is the point
    schema: type[dy.Schema],
    /,
    *,
    # --- decorator-owned ---
    quarantine: bool = False,
    check_granularity: Granularity | None = None,
    schema_rules: SchemaRules | None = None,
    max_failure_samples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
    # --- `@dg.asset`'s own, forwarded. ---
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

    The Columns tab fills from the schema before the asset has run, every rule reports through an asset check, and a frame whose column schema does not match aborts the run before a row is filtered.

    Five returns are accepted: a frame or a `dg.MaterializeResult` carrying one, eager or lazy, or `None` to skip. What the function returns decides what happens; the annotation is not enforced, because `Schema.filter` materializes the valid rows either way.

    The user guide has the failure policy, the quarantine, partitioning, settings and testing.

    Every parameter is declared with its runtime-real type, so editors autocomplete them and `group_nme="sales"` is a static error rather than an import-time crash. `check_specs` is absent because this decorator owns it.

    Parameters
    ----------
    schema
        The Dataframely schema the decorated function's output must satisfy. Positional-only, and the only such parameter: it is the reason this decorator exists, so it is required and never one keyword among thirty.
    quarantine
        Whether invalid rows are kept, which is the whole failure policy. **`True` adds a `context` parameter** to the asset whether or not the decorated function declared one, because the writer is built from the execution context. **`True` also reserves the asset key `<name>_quarantine`**, and a run fails with `QuarantineKeyCollisionError` when another asset already materializes it.
    check_granularity
        How far the rules collapse into checks: `rule`, `column` or `schema`. **Changing this on an existing asset orphans its check history.** Unset resolves through `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY`, then `rule`.
    schema_rules
        Where the schema-level rules land at `column` granularity: `collapsed` into `dy_schema__rules`, or `per_rule` for a check each. Read at no other granularity, because neither has a second place to put them. Unset resolves through `DAGSTER_DATAFRAMELY_SCHEMA_RULES`, then `collapsed`.
    max_failure_samples
        How many rows that failed a rule reach that rule's check metadata, under `dy_failed_sample`. **These are real rows in the Dagster event log**, which is shared, exported and not redacted. The bound is this package's own; `dy.Config.set_max_failure_examples` does not touch it. Unset resolves through `DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES`, then `5`.
    statistics
        Whether the materialization carries statistics for what it wrote, one table per dtype group present. Unset resolves through `DAGSTER_DATAFRAMELY_STATISTICS`, then `true`.
    row_sample
        How many rows reach the materialization metadata, of what was written and of what was held back. **These are real rows in the event log**, on the same terms as `max_failure_samples`. Unset resolves through `DAGSTER_DATAFRAMELY_ROW_SAMPLE`, then `5`.
    name
        Asset name. Defaults to the function name.
    key_prefix
        Prefix for the asset key. The checks and the quarantine follow it.
    ins
        Explicit input mapping, for the cases a parameter name cannot express.
    deps
        Upstream assets this one depends on without loading.
    metadata
        Definition metadata to carry alongside the schema's own. `dagster/column_schema` is the package's and wins a collision.
    tags
        Asset tags, for filtering and grouping in the catalog.
    description
        Asset description. Unset, the schema's own docstring fills it, and the decorated function's docstring stands only where the schema has none.
    config_schema
        Run configuration schema for the underlying op. Narrower than Dagster's six-member union: a mapping is the one form worth a static guarantee, and the rest are legacy.
    required_resource_keys
        Resources the decorated function reaches through the context. The quarantine needs none: its manager comes off the step, not off the context's resources.
    resource_defs
        Resources bound to this asset specifically.
    hooks
        Hooks to attach to the underlying op.
    io_manager_key
        Resource key the table is stored under. The quarantine goes to the same manager, so moving one moves both.
    partitions_def
        Partitioning for the asset. Validation runs per partition, and the quarantine is written under the same partition key.
    op_tags
        Tags on the underlying op, for run launcher and executor routing.
    backfill_policy
        How Dagster backfills this asset's partitions.
    retry_policy
        Retry policy for the underlying op.
    code_version
        Version string for change-based staleness.
    owners
        Asset owners, as emails or `team:<name>`.
    kinds
        Kind badges shown on the asset in the graph.
    pool
        Concurrency pool the underlying op runs in.

    Returns
    -------
    A decorator producing a `dg.asset` carrying the checks `check_granularity` asks for.

    Raises
    ------
    CollectionNotSupportedError
        `schema` is a `dy.Collection`.
    ReservedColumnError
        A user column sits inside the reserved namespace.
    UnnameableColumnError
        A user column is spelled in characters Dagster refuses in a name.
    CheckNameCollisionError
        Two rules rewrite to the same check name.
    InvalidSettingError
        A setting resolved to a value outside its allowed values, from any source.

    Examples
    --------
    ```{python}
    #| echo: false
    #| output: false
    import dataframely as dy
    import polars as pl

    import dagster_dataframely as dd
    ```

    One declaration is the schema, the checks, the filter and the quarantine:

    ```{python}
    class Orders(dy.Schema):
        order_id = dy.String(primary_key=True)
        amount = dy.Float64(nullable=False, min=0.0)


    @dd.asset(Orders, quarantine=True)
    def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
        return raw_orders.select("order_id", "amount")


    [spec.name for spec in orders.check_specs]
    ```

    Every one of those exists before the asset has run, so the catalog lists a failing
    check by name before it can fail.
    """
    # Only a `Collection` is refused here. Anything else keeps failing however it already fails.
    if isinstance(schema, type) and issubclass(schema, dy.Collection):
        raise CollectionNotSupportedError(schema.__name__)

    # Here rather than left to the `check_specs` below, so the factory refuses on the line that takes the schema and a `maker = dd.asset(Reserved)` cannot hand back a decorator that raises later (ADR-0008).
    validate_namespace(schema)

    # Resolved once, here, and handed to both the specs and the runtime. Resolving again inside the run would read the executing process's environment, so a worker with a different `DAGSTER_DATAFRAMELY_*` would report against checks the code location never declared. The last three affect nothing built at definition time, but they resolve here too: a mistyped environment variable then fails where the asset is declared rather than on whichever run reaches it first.
    granularity: Granularity = CHECK_GRANULARITY.resolve(check_granularity)
    schema_rule_checks: SchemaRules = SCHEMA_RULES.resolve(schema_rules)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    emit_statistics: bool = STATISTICS.resolve(statistics)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)
    # `quarantine_dir` is the one whose value is dropped. What a deployment writes after this module imports is what a call should use, and a call holding nothing back should never have to name a directory, so the writer reads the setting itself when the rows arrive (#115).
    # The resolve stays for the sentence above it: `${SCRATCH}` unexpanded arrives empty, and a variable written wrong is worth reporting where it was written rather than on whichever call first has a row to hold back. Unconditional, because a malformed variable is malformed whether or not this asset declares a quarantine.
    QUARANTINE_DIR.resolve(None)

    forwarded: dict[str, Any] = {
        "ins": ins,
        "deps": deps,
        "tags": tags,
        # The schema's docstring fills a description the decorator was not given, because the schema describes the table while the decorated function's docstring describes the code that fills it. Returning nothing leaves Dagster's own fallback to that docstring standing, so an empty string on either source reads as absent and neither can say "no description at all".
        # `__doc__` rather than `inspect.getdoc`, which walks the MRO: a schema with no docstring would inherit `dy.Schema`'s and describe itself as a base class for schema definitions. `cleandoc` because a raw docstring keeps its source indentation, which the catalog renders as a code block.
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

        # Read off the decorated function, so `compute` can hand the context to a function that asked for one and withhold it from one that did not.
        parameters = list(inspect.signature(fn).parameters.values())
        declares_context: bool = is_context_provided(parameters)

        def asset_yields(
            returned: DecoratedReturn, writer: QuarantineWriter | None
        ) -> AssetYield:
            """Validate what the decorated function handed back and report it.

            One stage either side of `validation_results`, and neither changes it (#77).
            """
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
            # The context is the one thing a wrapper cannot ask Dagster for after the fact, and building the writer is the only reason to want it (ADR-0001). So a quarantined asset declares it whether or not the decorated function did, and an asset without a quarantine keeps the signature it was written with.

            @functools.wraps(fn)
            def compute(
                context: dg.AssetExecutionContext, *args: object, **kwargs: object
            ) -> AssetYield:
                # Ahead of the body, and ahead of the writer: a body whose invalid rows would land on another asset's key has nowhere to put them, so nothing is gained by running it first (ADR-0007).
                validate_quarantine_key(context)
                # Bound to the context here, where it is in hand and nowhere else. The route is picked when the rows arrive, so a body that holds nothing back needs nowhere to put them.
                writer = quarantine_writer(context)
                returned: DecoratedReturn = (
                    fn(context, *args, **kwargs)
                    if declares_context
                    else fn(*args, **kwargs)
                )
                yield from asset_yields(returned, writer)

            if not declares_context:
                # `functools.wraps` forwards the decorated function's own signature, which Dagster resolves the asset's inputs from. Prepending the context is the whole edit; the rest is the decorated function's parameter list, untouched.
                # The context takes the first parameter's own kind when that kind is positional-only, because `inspect.Signature` refuses a positional-or-keyword parameter ahead of one. Dagster cannot resolve a positional-only input either way, so this buys its refusal rather than a bare `ValueError` naming no asset.
                # Unannotated: `inspect` keeps the kind enum private, and the name buys nothing a reader does not already see.
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
            # The column-schema check, both failures that write nothing, and the skip all end the step without yielding.
            output_required=False,
            # The package's own key is applied last, so a user cannot accidentally displace the Columns tab.
            # The opposite precedence from `description` above: this key belongs to the package, while a description is prose the author owns.
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
