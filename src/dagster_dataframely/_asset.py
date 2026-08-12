"""The front door: one decorator argument attaches a dataframely schema to a Dagster asset.

The door coordinates four artifacts no single `@dg.asset` parameter accepts as a bundle: the out, the check specs, the definition metadata, and the wrapped runtime. First-party precedent for a decorator that does this is `@dbt_assets`.

This module carries no `from __future__ import annotations`. At a 3.12 floor it would buy only unquoted forward references, while turning user-facing annotations into strings that Dagster's runtime introspection rejects. The counter-trap is that typing-only names such as `dg.CoercibleToAssetDep` are absent at runtime, so they are spelled here with runtime-real types.
"""

import functools
import inspect
from collections.abc import Callable, Iterable, Mapping, Sequence
from collections.abc import Set as AbstractSet
from typing import Any

import dagster as dg
import dataframely as dy
import polars as pl

from dagster_dataframely._checks import check_specs
from dagster_dataframely._errors import (
    CollectionNotSupportedError,
    QuarantineSettingError,
)
from dagster_dataframely._metadata import _quarantine_metadata, schema_metadata
from dagster_dataframely._runtime import AssetYield, process
from dagster_dataframely._settings import (
    CHECK_GRANULARITY,
    MAX_FAILURE_SAMPLES,
    MULTI_COLUMN_RULES,
    ROW_SAMPLE,
    STATISTICS,
    TEMP_DIR,
    Granularity,
    MultiColumnRules,
)

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

# Every `dg.AssetOut` setting, read off its constructor rather than transcribed, so a new Dagster parameter is carried through the rebuild without this file being touched.
# `kwargs` is the constructor's own catch-all, not a setting.
_ASSET_OUT_SETTINGS = frozenset(inspect.signature(dg.AssetOut.__init__).parameters) - {
    "self",
    "kwargs",
}

# Door-owned: dropped from the rebuild so `dg.AssetOut`'s own default applies. The shape check and the nothing-survived path both skip an out, `dagster_type` was ruled out with evidence (#3), and a transform is not a virtual asset.
_DOOR_OWNED_SETTINGS = frozenset({"is_required", "dagster_type", "is_virtual"})

# Settings one step cannot hold two of. `partitions_def` and `backfill_policy` need no entry: they live on the `multi_asset`, so both outs carry them identically by construction. That is the property these three lack.
_CONTESTED_SETTINGS = ("automation_condition", "freshness_policy", "code_version")


def _rebuild(out: dg.AssetOut, overrides: Mapping[str, Any]) -> dg.AssetOut:
    """Reconstructs a `dg.AssetOut` with the door's overrides applied.

    It is immutable with no `_replace`, so the only route is to read its attributes back out and build a new one. That works because every constructor parameter currently has a readable attribute of the same name, which is an undocumented upstream property asserted by its own test: a parameter Dagster adds whose attribute is named differently would drop the user's value silently.

    `getattr` is deliberately unguarded for the same reason. A missing attribute is the failure the pin exists to catch, and swallowing it here would hide it.

    Args:
        out: The out to read settings off.
        overrides: Settings the door imposes, applied over what the out carries.

    Returns:
        A new out carrying the user's settings with the overrides applied.
    """
    settings = {
        name: getattr(out, name) for name in _ASSET_OUT_SETTINGS - _DOOR_OWNED_SETTINGS
    }
    return dg.AssetOut(**settings | dict(overrides))


def _quarantine_out(
    quarantine: dg.AssetOut,
    *,
    schema: type[dy.Schema],
    key: dg.AssetKey,
    io_manager_key: str | None,
    group_name: str | None,
) -> dg.AssetOut:
    """Composes the passed `dg.AssetOut` with the door's own parameters.

    `dg.AssetOut` is already the typed container for everything quarantine-specific, so the door takes one rather than growing a second two-valued container that could disagree with it. What it costs is a rule per setting that also exists on the door:

    - `key`, `key_prefix`, `owners`, `tags`, `description` and `kinds` are free and the passed value wins. The sensitive-data case is exactly this: rejected rows to a different key and ownership domain.
    - `metadata` is free too, but `dagster/column_schema` and `dagster_dataframely/schema` are applied over it, exactly as on the good out: the Columns tab is what the decorator is for and the carrier is how a CSV read finds its dtypes, so a colliding user key loses.
    - `io_manager_key` and `group_name` are inherited when unset, so one declaration stores both tables beside each other, and moving the rejected rows elsewhere stays a one-word change.
    - The three in `_CONTESTED_SETTINGS` raise.

    The door's own `automation_condition` and `freshness_policy` stay on the good out rather than reaching this one. A freshness policy here would fail forever on a healthy pipeline, because a clean run skips the quarantine by design; and a condition here would request a step the good asset's condition already requests, since neither out can execute alone.

    Args:
        quarantine: The out the user declared, verbatim.
        schema: The schema whose rejected rows the out holds.
        key: The default sibling key, used only when the user named neither a key nor a prefix.
        io_manager_key: The good out's manager, inherited when the quarantine names none.
        group_name: The good out's group, inherited when the quarantine names none.

    Returns:
        The out to hand `@dg.multi_asset`.

    Raises:
        QuarantineSettingError: The out sets something one step cannot hold two of.
    """
    for setting in _CONTESTED_SETTINGS:
        if getattr(quarantine, setting) is not None:
            raise QuarantineSettingError(setting)

    overrides: dict[str, Any] = {
        "is_required": False,
        # The package's key is applied last, as on the good out.
        "metadata": {**(quarantine.metadata or {}), **_quarantine_metadata(schema)},
        "io_manager_key": quarantine.io_manager_key or io_manager_key,
        "group_name": quarantine.group_name or group_name,
    }
    # Naming either one is a complete override, and Dagster rejects both together.
    if quarantine.key is None and not quarantine.key_prefix:
        overrides["key"] = key
    return _rebuild(quarantine, overrides)


def dataframely_asset(  # noqa: PLR0913 - the forwarded surface is the point
    *,
    # --- door-owned ---
    schema: type[dy.Schema],
    quarantine: dg.AssetOut | None = None,
    check_granularity: Granularity | None = None,
    multi_column_rules: MultiColumnRules | None = None,
    max_failure_samples: int | None = None,
    statistics: bool | None = None,
    row_sample: int | None = None,
    temp_dir: str | None = None,
    key_prefix: str | Sequence[str] | None = None,
    # --- carried to the outs themselves, matching @dg.asset's vocabulary ---
    io_manager_key: str | None = None,
    group_name: str | None = None,
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
    # --- Narrower than Dagster's own six-member union, deliberately: a mapping is the spelling worth a static guarantee, and the rest are legacy. ---
    config_schema: Mapping[str, Any] | None = None,
    required_resource_keys: AbstractSet[str] | None = None,
    partitions_def: dg.PartitionsDefinition[str] | None = None,
    hooks: AbstractSet[dg.HookDefinition] | None = None,
    backfill_policy: dg.BackfillPolicy | None = None,
    op_tags: Mapping[str, Any] | None = None,
    resource_defs: Mapping[str, object] | None = None,
    retry_policy: dg.RetryPolicy | None = None,
    code_version: str | None = None,
    pool: str | None = None,
) -> Callable[[TransformFn], dg.AssetsDefinition]:
    """Turns a polars transform into an asset that validates its output against `schema`.

    The contract then lives in exactly one place. From the single declaration, the Columns tab fills in before the asset has ever run, every dataframely rule reports through an asset check with pass/fail history, one check per rule until `check_granularity` collapses them, and a frame whose shape does not match the schema aborts the run before a single row is filtered.

    The transform keeps plain polars annotations: nothing rewrites the signature, upstream dependencies bind as ordinary parameters, and the return may be a `DataFrame` or a `LazyFrame`. It takes no `context` parameter; the wrapper reaches the context itself.

    **The asset's declared shape is the failure policy.** There is no lenient mode and no strict flag, deliberately: declaring `quarantine=dg.AssetOut()` *is* the consent to partial data, so what a rejected row costs is visible in the definition and cannot disagree with what the asset declares.

    With no quarantine, every row has to be good: a run that rejects even one row fails and writes nothing, leaving the last-known-good table in place. To drop rows anyway, filter in the asset body, where the drop is a line you wrote:

        good, _ = Orders.filter(raw_orders)
        return good

    With a quarantine, rejected rows land in a sibling asset carrying the original columns plus one outcome column per rule, the checks fail at `WARN`, and the run stays green so downstream proceeds on the data that is fine. A clean run skips the quarantine rather than writing an empty table, and a run where *nothing* survived skips the good output rather than emptying it.

    Every parameter is declared explicitly with its runtime-real type, so editors autocomplete them and `group_nme="sales"` is a static error rather than an import-time crash. `outs`, `check_specs` and `specs` are surfaces this decorator owns and are simply absent, so they cannot be contested. `can_subset` is absent too: a subset executes but saves nothing.

    `@dg.multi_asset` is the mechanism, but the vocabulary is `@dg.asset`'s, because this decorator is designed for a single table that happens to grow a quarantine sibling. Anything `@dg.asset` lets you say about one asset is sayable here under the same name, and a test asserts that in both directions.

    Args:
        schema: The dataframely schema the transform's output must satisfy.
        quarantine: Where rejected rows go. Passing one is the consent to partial data; leaving it `None` is the refusal. The out is free to name its own key, group, owners and IO manager, which is how rejected rows reach a separate storage and ownership domain. Its key defaults to a sibling of the good asset and its IO manager to the good asset's.
        check_granularity: How far the schema's rules collapse into checks. `rule` gives each rule its own check and its own history. `column` gives one check per rule-bearing column, `dy_col__<column>`, which is what makes a wide schema's check list readable. `schema` gives a single `dy_schema__rules` for all of them. **Changing this on an existing asset orphans check history**: the old check names stop being reported and their timelines end where the change landed, while the new ones start empty. Nothing migrates them, so choose it before the asset ships rather than after. Unset resolves through `DAGSTER_DATAFRAMELY_CHECK_GRANULARITY`, then the package default `rule`.
        multi_column_rules: Where the rules no single column owns land at `column` granularity: grouped into `dy_schema__rules`, or `per_rule` for a check each. Read at no other granularity, because neither has a second place to put them. Unset resolves through `DAGSTER_DATAFRAMELY_MULTI_COLUMN_RULES`, then the package default `schema`.
        max_failure_samples: How many of the rows a rule rejected reach that rule's check metadata, under `dy_failed_sample`. What a red check raises and the counts cannot answer, so it is opt-out and `0` is what turns it off. **These are real rows in the Dagster event log**, which is shared, exported and not redacted; the bound is this package's own and `dy.Config.set_max_failure_examples` does not touch it. Bounded per rule, so a collapsed check shows this many for each rule that rejected anything. Unset resolves through `DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES`, then the package default `5`.
        statistics: Whether each materialization carries a `skimr`-style profile of what it wrote: one table per dtype family present, on both the good out and the quarantine. Opt-out rather than opt-in, so `False` is what turns the pass off. The string family deliberately carries no value-bearing statistic at either value, only lengths and cardinality: consenting to summary statistics is not consenting to raw values. That is what the two sample settings are for, which is why they are separate from this one. Unset resolves through `DAGSTER_DATAFRAMELY_STATISTICS`, then the package default `true`.
        row_sample: How many of the good output's rows reach its materialization metadata, under the display key `sample`. Opt-out on the same terms as `max_failure_samples`, with the same consequence: **these are real rows in the event log**, and `0` is what turns them off. The quarantine carries none, because its rows already reach the log through the checks that rejected them. Unset resolves through `DAGSTER_DATAFRAMELY_ROW_SAMPLE`, then the package default `5`.
        temp_dir: Where a `LazyFrame` return lands before it is validated. Read on that path only, so an asset returning a `DataFrame` is unaffected by it. **Unset, the landing goes to the system temp directory, which in a container is its ephemeral disk**, and a landed frame bigger than what the pod has spare fills it; pointing this at a mounted volume is the fix. A directory that does not exist raises rather than being created, because a mistyped path silently created on that disk is the failure this setting was set to avoid. Unset resolves through `DAGSTER_DATAFRAMELY_TEMP_DIR`.
        key_prefix: Prefix for the asset key. The checks and the quarantine follow it automatically.
        io_manager_key: Resource key the table is stored under. The quarantine inherits it unless its own `dg.AssetOut` names a different one.
        group_name: Asset group. The quarantine inherits it unless its own `dg.AssetOut` names a different one.
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
        partitions_def: Partitioning for the asset. The state machine then runs per partition, on that partition's frame, and both outs carry it, so the quarantine cannot escape its asset's partitioning. The transform takes no `context` parameter, so a partitioned one reaches its own key with `dg.AssetExecutionContext.get().partition_key`.
        hooks: Hooks to attach to the underlying op.
        backfill_policy: How Dagster backfills this asset's partitions.
        op_tags: Tags on the underlying op, for run launcher and executor routing.
        resource_defs: Resources bound to this asset specifically.
        retry_policy: Retry policy for the underlying op.
        code_version: Version string for change-based staleness.
        pool: Concurrency pool the underlying op runs in.

    Returns:
        A decorator producing a `multi_asset` carrying the checks `check_granularity` asks for, and a second out when a quarantine is declared.

    Raises:
        CollectionNotSupportedError: `schema` is a `dy.Collection`.
        InvalidSettingError: A setting resolved to a value outside its vocabulary, from any tier.
        QuarantineSettingError: The quarantine's `dg.AssetOut` sets something one step cannot hold two of.

    Example:
        >>> import dagster as dg
        >>> import dataframely as dy
        >>> import polars as pl
        >>> import dagster_dataframely as dd
        >>> class Orders(dy.Schema):
        ...     order_id = dy.String(primary_key=True)
        ...     amount = dy.Float64(nullable=False, min=0.0)
        >>> @dd.dataframely_asset(
        ...     schema=Orders, quarantine=dg.AssetOut(), group_name="sales"
        ... )
        ... def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
        ...     return raw_orders.select("order_id", "amount")
    """
    # Deliberately narrow: anything else keeps failing however it already fails.
    if isinstance(schema, type) and issubclass(schema, dy.Collection):
        raise CollectionNotSupportedError(schema.__name__)

    # Resolved once, here, and handed to both the specs and the runtime. Resolving again inside the run would read the executing process's environment, so a worker with a different `DAGSTER_DATAFRAMELY_*` would report against checks the code location never declared. The last three reach no definition-time surface, but they resolve here too: a mistyped environment variable then fails where the asset is declared rather than on whichever run happens to reach it first.
    granularity: Granularity = CHECK_GRANULARITY.resolve(check_granularity)
    multi_column: MultiColumnRules = MULTI_COLUMN_RULES.resolve(multi_column_rules)
    failure_samples: int = MAX_FAILURE_SAMPLES.resolve(max_failure_samples)
    emit_statistics: bool = STATISTICS.resolve(statistics)
    sampled_rows: int = ROW_SAMPLE.resolve(row_sample)
    landing_dir: str | None = TEMP_DIR.resolve(temp_dir)

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
        "retry_policy": retry_policy,
        "code_version": code_version,
        "pool": pool,
    }

    def decorate(fn: TransformFn) -> dg.AssetsDefinition:
        asset_name: str = name or fn.__name__
        prefix: list[str] = []
        if key_prefix is not None:
            prefix = [key_prefix] if isinstance(key_prefix, str) else list(key_prefix)
        key = dg.AssetKey([*prefix, asset_name])

        outs = {
            asset_name: dg.AssetOut(
                key=key,
                # The shape check and both abort paths end the step without yielding.
                is_required=False,
                # The package's two keys are applied last, so a user cannot accidentally displace the Columns tab or the schema carrier.
                metadata={**(metadata or {}), **schema_metadata(schema)},
                io_manager_key=io_manager_key,
                tags=tags,
                owners=owners,
                kinds=set(kinds) if kinds else None,
                automation_condition=automation_condition,
                freshness_policy=freshness_policy,
                # Not forwarded to the `multi_asset`, which refuses it outright when any out names one. Naming it here is what leaves the quarantine free to claim a group of its own.
                group_name=group_name,
            )
        }
        quarantine_name: str | None = None
        if quarantine is not None:
            quarantine_name = f"{asset_name}_quarantine"
            outs[quarantine_name] = _quarantine_out(
                quarantine,
                schema=schema,
                # The default key is a sibling inheriting the good asset's prefix, so the two land next to each other with no configuration.
                key=dg.AssetKey([*prefix, quarantine_name]),
                io_manager_key=io_manager_key,
                group_name=group_name,
            )

        @dg.multi_asset(
            name=asset_name,
            outs=outs,
            check_specs=check_specs(
                schema,
                asset=key,
                check_granularity=granularity,
                multi_column_rules=multi_column,
            ),
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
                quarantine_out=quarantine_name,
                check_granularity=granularity,
                multi_column_rules=multi_column,
                max_failure_samples=failure_samples,
                statistics=emit_statistics,
                row_sample=sampled_rows,
                temp_dir=landing_dir,
            )

        return compute

    return decorate
