"""The front door: one decorator argument attaches a dataframely schema to a Dagster asset.

The door coordinates four artifacts no single `@dg.asset` parameter accepts as a bundle: the out, the check specs, the definition metadata, and the wrapped runtime. First-party precedent for a decorator that does this is `@dbt_assets`.

This module carries no `from __future__ import annotations`. At a 3.12 floor it would buy only unquoted forward references, while turning user-facing annotations into strings that Dagster's runtime introspection rejects. The counter-trap is that typing-only names such as `dg.CoercibleToAssetDep` are absent at runtime, so they are spelled here with runtime-real types.
"""

import functools
from collections.abc import Callable, Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely.checks import check_specs
from dagster_dataframely.errors import CollectionNotSupportedError
from dagster_dataframely.metadata import schema_metadata
from dagster_dataframely.runtime import AssetYield, process

TransformFn = Callable[..., pl.DataFrame | pl.LazyFrame]

# The union `@dg.asset` accepts, spelled out because `AutomationCondition` is generic and its two parameterizations are not interchangeable.
AutomationCondition = (
    dg.AutomationCondition[dg.AssetKey]
    | dg.AutomationCondition[dg.AssetKey | dg.AssetCheckKey]
)

#: Runtime-real spelling of Dagster's `CoercibleToAssetDep`, which is typing-only.
AssetDep = (
    dg.AssetKey
    | str
    | Sequence[str]
    | dg.AssetSpec
    | dg.AssetsDefinition
    | dg.SourceAsset
    | dg.AssetDep
)


def dataframely_asset(  # noqa: PLR0913 - the forwarded surface is the point
    *,
    # --- door-owned ---
    schema: type[dy.Schema],
    key_prefix: str | Sequence[str] | None = None,
    # --- carried to the asset itself, matching @dg.asset's vocabulary ---
    io_manager_key: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    tags: Mapping[str, str] | None = None,
    owners: Sequence[str] | None = None,
    kinds: AbstractSet[str] | None = None,
    automation_condition: AutomationCondition | None = None,
    freshness_policy: dg.FreshnessPolicy | None = None,
    # --- forwarded to @dg.multi_asset, verbatim ---
    name: str | None = None,
    ins: Mapping[str, dg.AssetIn] | None = None,
    deps: Iterable[AssetDep] | None = None,
    description: str | None = None,
    # Narrower than Dagster's own six-member union, deliberately: a mapping is the
    # spelling worth a static guarantee, and the rest are legacy.
    config_schema: Mapping[str, Any] | None = None,
    required_resource_keys: AbstractSet[str] | None = None,
    partitions_def: dg.PartitionsDefinition[str] | None = None,
    hooks: AbstractSet[dg.HookDefinition] | None = None,
    backfill_policy: dg.BackfillPolicy | None = None,
    op_tags: Mapping[str, Any] | None = None,
    resource_defs: Mapping[str, object] | None = None,
    group_name: str | None = None,
    retry_policy: dg.RetryPolicy | None = None,
    code_version: str | None = None,
    pool: str | None = None,
) -> Callable[[TransformFn], dg.AssetsDefinition]:
    """Turns a polars transform into an asset that validates its output against `schema`.

    The contract then lives in exactly one place. From the single declaration, the Columns tab fills in before the asset has ever run, every dataframely rule becomes an asset check with its own pass/fail history, and a frame whose shape does not match the schema aborts the run before a single row is filtered.

    The transform keeps plain polars annotations: nothing rewrites the signature, upstream dependencies bind as ordinary parameters, and the return may be a `DataFrame` or a `LazyFrame`. It takes no `context` parameter; the wrapper reaches the context itself.

    **Every row has to be good.** A run that rejects even one row fails and writes nothing, leaving the last-known-good table in place. There is no lenient mode and no strict flag, deliberately: landing the survivors and dropping the rest is precisely the failure this package exists to make visible, so it is not reachable by configuration. To drop rows anyway, filter in the asset body, where the drop is a line you wrote:

        good, _ = Orders.filter(raw_orders)
        return good

    Routing rejected rows to a sibling asset instead of failing is planned work, tracked in issue #19.

    Every parameter is declared explicitly with its runtime-real type, so editors autocomplete them and `group_nme="sales"` is a static error rather than an import-time crash. `outs`, `check_specs` and `specs` are surfaces this decorator owns and are simply absent, so they cannot be contested. `can_subset` is absent too: a subset executes but saves nothing.

    `@dg.multi_asset` is the mechanism, but the vocabulary is `@dg.asset`'s, because this decorator is designed for a single table that happens to grow a quarantine sibling. Anything `@dg.asset` lets you say about one asset is sayable here under the same name, and a test asserts that in both directions.

    Args:
        schema: The dataframely schema the transform's output must satisfy.
        key_prefix: Prefix for the asset key. The checks follow it automatically.
        io_manager_key: Resource key the table is stored under. The quarantine inherits it unless its own `dg.AssetOut` names a different one (#19).
        metadata: Definition metadata to carry alongside the schema's own. `dagster/column_schema` and `dagster_dataframely/schema` are the package's and win a collision.
        tags: Asset tags, for filtering and grouping in the catalog.
        owners: Asset owners, as emails or `team:<name>`.
        kinds: Kind badges shown on the asset in the graph.
        automation_condition: Declarative automation condition for the asset.
        freshness_policy: Freshness policy for the asset.
        name: Asset name. Defaults to the function name.
        ins: Explicit input mapping, for the cases a parameter name cannot express.
        deps: Upstream assets this one depends on without loading.
        description: Asset description. Defaults to the function's docstring.
        config_schema: Run configuration schema for the underlying op.
        required_resource_keys: Resources the transform reaches through the context.
        partitions_def: Partitioning for the asset. The state machine then runs per partition, on that partition's frame.
        hooks: Hooks to attach to the underlying op.
        backfill_policy: How Dagster backfills this asset's partitions.
        op_tags: Tags on the underlying op, for run launcher and executor routing.
        resource_defs: Resources bound to this asset specifically.
        group_name: Asset group.
        retry_policy: Retry policy for the underlying op.
        code_version: Version string for change-based staleness.
        pool: Concurrency pool the underlying op runs in.

    Returns:
        A decorator producing a `multi_asset` with one out and one check per rule.

    Raises:
        CollectionNotSupportedError: `schema` is a `dy.Collection`.

    Example:
        >>> import dagster as dg
        >>> import dataframely as dy
        >>> import polars as pl
        >>> import dagster_dataframely as dd
        >>> class Orders(dy.Schema):
        ...     order_id = dy.String(primary_key=True)
        ...     amount = dy.Float64(nullable=False, min=0.0)
        >>> @dd.dataframely_asset(schema=Orders, group_name="sales")
        ... def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
        ...     return raw_orders.select("order_id", "amount")
    """
    # Deliberately narrow: anything else keeps failing however it already fails.
    if isinstance(schema, type) and issubclass(schema, dy.Collection):
        raise CollectionNotSupportedError(schema.__name__)

    forwarded: dict[str, Any] = {
        "ins": ins,
        "deps": deps,
        "description": description,
        "config_schema": config_schema,
        "required_resource_keys": required_resource_keys,
        "partitions_def": partitions_def,
        "hooks": hooks,
        "backfill_policy": backfill_policy,
        "op_tags": op_tags,
        "resource_defs": resource_defs,
        "group_name": group_name,
        "retry_policy": retry_policy,
        "code_version": code_version,
        "pool": pool,
    }

    def decorate(fn: TransformFn) -> dg.AssetsDefinition:
        asset_name = name or fn.__name__
        prefix: list[str] = []
        if key_prefix is not None:
            prefix = [key_prefix] if isinstance(key_prefix, str) else list(key_prefix)
        key = dg.AssetKey([*prefix, asset_name])

        @dg.multi_asset(
            name=asset_name,
            outs={
                asset_name: dg.AssetOut(
                    key=key,
                    # The gate and the abort path both end the step without yielding.
                    is_required=False,
                    # The package's two keys are applied last, so a user cannot accidentally displace the Columns tab or the schema carrier.
                    metadata={**(metadata or {}), **schema_metadata(schema)},
                    io_manager_key=io_manager_key,
                    tags=tags,
                    owners=owners,
                    kinds=set(kinds) if kinds else None,
                    automation_condition=automation_condition,
                    freshness_policy=freshness_policy,
                )
            },
            check_specs=check_specs(schema, asset=key),
            **forwarded,
        )
        @functools.wraps(fn)
        def compute(*args: object, **kwargs: object) -> AssetYield:
            # No `context` parameter, deliberately: a user-side postponed-annotations import makes Dagster reject a qualified annotation for one.
            yield from process(
                schema,
                fn(*args, **kwargs),
                context=dg.AssetExecutionContext.get(),
                good_out=asset_name,
            )

        return compute

    return decorate
