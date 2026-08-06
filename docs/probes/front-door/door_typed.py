"""PROTOTYPE - throwaway (wayfinder #11). The door as it would ship.

Shape A with both call-site concerns resolved:

- Named `dataframely_asset`, not `@dd.asset` - one keystroke from `@dg.asset`
  was a typo trap, and the first-party precedent (`@dbt_assets`) uses a
  distinct, greppable name anyway.
- No `**kwargs`. Every forwarded `@dg.multi_asset` parameter is declared
  explicitly with its runtime-real type, so editors autocomplete them and a
  typo'd or unsupported parameter is a *static* error. The curated list is
  also where policy lives: `outs`/`check_specs`/`specs` are door-owned and
  simply do not exist here, and `can_subset` is omitted deliberately
  (#4: subsetting executes but saves nothing).

Run: uv run python docs/probes/front-door/door_typed.py
"""

import functools
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from collections.abc import Set as AbstractSet
from typing import Any

import _core
import dagster as dg
import dataframely as dy
import polars as pl
from _scenario import Orders, mixed_orders

TransformFn = Callable[..., pl.DataFrame | pl.LazyFrame]
AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]
# Runtime-real spelling of dagster's CoercibleToAssetDep (typing-only upstream).
AssetDep = (
    dg.AssetKey
    | str
    | Sequence[str]
    | dg.AssetSpec
    | dg.AssetsDefinition
    | dg.SourceAsset
    | dg.AssetDep
)


def dataframely_asset(
    *,
    # -- door-owned -------------------------------------------------------------
    schema: type[dy.Schema],
    quarantine: dg.AssetOut | None = None,
    check_granularity: _core.Granularity | None = None,
    multi_column_rules: _core.MultiColumnRules | None = None,
    key_prefix: str | Sequence[str] | None = None,
    # -- forwarded to @dg.multi_asset, verbatim ---------------------------------
    name: str | None = None,
    ins: Mapping[str, dg.AssetIn] | None = None,
    deps: Iterable[AssetDep] | None = None,
    description: str | None = None,
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
    """The front door, final shape: one declaration, fully typed, no kwargs."""
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
    forwarded = {k: v for k, v in forwarded.items() if v is not None}

    def decorate(fn: TransformFn) -> dg.AssetsDefinition:
        asset_name = name or fn.__name__
        granularity = _core.resolve_granularity(check_granularity)
        mcr = _core.resolve_multi_column_rules(multi_column_rules)

        key = None
        prefix = None
        if key_prefix is not None:
            prefix = [key_prefix] if isinstance(key_prefix, str) else list(key_prefix)
            key = dg.AssetKey([*prefix, asset_name])
        quarantine_out = f"{asset_name}_quarantine" if quarantine is not None else None

        @dg.multi_asset(
            name=asset_name,
            outs=_core.build_outs(
                asset_name, schema, quarantine, key=key, key_prefix=prefix
            ),
            check_specs=_core.check_specs(
                schema,
                asset=key or asset_name,
                granularity=granularity,
                multi_column_rules=mcr,
            ),
            **forwarded,
        )
        @functools.wraps(fn)
        def compute(*args: Any, **kwargs: Any) -> AssetYield:
            yield from _core.process(
                schema,
                fn(*args, **kwargs),
                context=dg.AssetExecutionContext.get(),
                good_out=asset_name,
                quarantine_out=quarantine_out,
                granularity=granularity,
                multi_column_rules=mcr,
            )

        return compute

    return decorate


# --- call sites ----------------------------------------------------------------


@dg.asset
def raw_orders() -> pl.DataFrame:
    return mixed_orders()


@dataframely_asset(schema=Orders, quarantine=dg.AssetOut(), group_name="sales")
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return raw_orders


# The transform may hand back a LazyFrame; the door collects the good half
# before materializing (sub-decision 2).
@dataframely_asset(schema=Orders, quarantine=dg.AssetOut(), name="orders_lazy")
def orders_lazy_fn() -> pl.LazyFrame:
    return mixed_orders().lazy()


if __name__ == "__main__":
    import logging

    logging.getLogger("dagster").setLevel(logging.CRITICAL)

    print("=== typed door: derived surface ===")
    print("asset keys:", sorted(k.to_user_string() for k in orders.keys))
    print("groups:", dict(orders.group_names_by_key))
    print("check specs:", len(orders.check_specs_by_output_name))

    result = dg.materialize([orders, raw_orders], raise_on_error=False)
    materialized = [
        e.asset_key.to_user_string()
        for e in result.get_asset_materialization_events()
        if e.asset_key is not None
    ]
    failed = [e for e in result.get_asset_check_evaluations() if not e.passed]
    print(f"eager transform: success={result.success}, materialized={materialized}")
    print(f"  failed checks: {[(e.check_name, e.severity.value) for e in failed]}")

    result = dg.materialize([orders_lazy_fn], raise_on_error=False)
    materialized = [
        e.asset_key.to_user_string()
        for e in result.get_asset_materialization_events()
        if e.asset_key is not None
    ]
    print(f"lazy transform:  success={result.success}, materialized={materialized}")
