"""PROTOTYPE - throwaway (wayfinder #11). Shape A: the `@dd.asset` door.

One declaration: the decorator sees `schema=` and `quarantine=` once and derives
everything - outs, check specs, definition metadata, and the wrapped runtime.
Everything not owned by the package passes through to `@dg.multi_asset` verbatim.

Run: uv run python docs/probes/front-door/shape_a_dd_asset.py
"""

import functools
import os
from collections.abc import Callable, Iterator
from typing import Any

import _core
import dagster as dg
import dataframely as dy
import polars as pl
from _scenario import (
    Orders,
    clean_orders,
    hopeless_orders,
    mixed_orders,
    wrong_dtype_orders,
)

TransformFn = Callable[..., pl.DataFrame | pl.LazyFrame]
AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]


# --- the door (this is the entire package-side surface of shape A) -------------


def dd_asset(
    *,
    schema: type[dy.Schema],
    quarantine: dg.AssetOut | None = None,
    check_granularity: _core.Granularity | None = None,
    multi_column_rules: _core.MultiColumnRules | None = None,
    name: str | None = None,
    key_prefix: str | list[str] | None = None,
    **multi_asset_kwargs: Any,
) -> Callable[[TransformFn], dg.AssetsDefinition]:
    """`@dd.asset` - the front door. Everything else is `@dg.multi_asset`'s vocabulary."""

    def decorate(fn: TransformFn) -> dg.AssetsDefinition:
        asset_name = name or fn.__name__
        granularity = _core.resolve_granularity(check_granularity)
        mcr = _core.resolve_multi_column_rules(multi_column_rules)

        # The good key is constructed once and handed to both the out and the
        # specs - never re-derived from Dagster's resolution (the archive's sin).
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
            **multi_asset_kwargs,
        )
        @functools.wraps(fn)
        def compute(*args: Any, **kwargs: Any) -> AssetYield:
            frame = fn(*args, **kwargs)
            yield from _core.process(
                schema,
                frame,
                context=dg.AssetExecutionContext.get(),
                good_out=asset_name,
                quarantine_out=quarantine_out,
                granularity=granularity,
                multi_column_rules=mcr,
            )

        return compute

    return decorate


# --- call sites ----------------------------------------------------------------


# 1. Abort policy: one out. Any rejected row -> ERROR checks, raise, nothing kept.
@dd_asset(schema=Orders)
def orders_strict() -> pl.DataFrame:
    return clean_orders()


# 2. Quarantine policy: declaring the second out IS the consent (#6/#9).
@dd_asset(schema=Orders, quarantine=dg.AssetOut(), group_name="sales")
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return raw_orders


# Upstream feeding case 2 - proves ins bind through the wrapper.
@dg.asset
def raw_orders() -> pl.DataFrame:
    return mixed_orders()


# 3. Routed quarantine: AssetOut(key=) overrides completely (#9) - different
#    storage/ownership domain with no feature from this package.
@dd_asset(
    schema=Orders,
    quarantine=dg.AssetOut(
        key=dg.AssetKey(["quarantine", "orders"]), io_manager_key="io_manager"
    ),
)
def orders_routed() -> pl.DataFrame:
    return mixed_orders()


# 4. Nothing survives: quarantine materializes, good out skipped, run fails.
@dd_asset(schema=Orders, quarantine=dg.AssetOut(), name="orders_hopeless")
def orders_hopeless_fn() -> pl.DataFrame:
    return hopeless_orders()


# 5. Pipeline defect: wrong dtype -> gate check fails, raise before filter (#7).
@dd_asset(schema=Orders)
def orders_wrong_dtype() -> pl.DataFrame:
    return wrong_dtype_orders()


# 6. Prefixed good key: constructed once, shared by out + specs.
@dd_asset(schema=Orders, key_prefix="warehouse", quarantine=dg.AssetOut())
def orders_prefixed() -> pl.DataFrame:
    return clean_orders()


# --- demos ---------------------------------------------------------------------


def show_surface() -> None:
    print("=== derived definition surface (orders, quarantine declared) ===")
    print("asset keys:", sorted(k.to_user_string() for k in orders.keys))
    print("check specs:", len(orders.check_specs_by_output_name))
    for spec in list(orders.check_specs_by_output_name.values())[:6]:
        print(f"  {spec.name}  (blocking={spec.blocking})  - {spec.description}")
    print("  ...")
    print(
        "prefixed variant keys:",
        sorted(k.to_user_string() for k in orders_prefixed.keys),
    )
    print(
        "routed quarantine keys:",
        sorted(k.to_user_string() for k in orders_routed.keys),
    )

    print("\n=== three-tier config: env var flips granularity (#8) ===")
    os.environ["DAGSTER_DATAFRAMELY_CHECK_GRANULARITY"] = "column"

    @dd_asset(schema=Orders, name="orders_env")
    def orders_env() -> pl.DataFrame:
        return clean_orders()

    del os.environ["DAGSTER_DATAFRAMELY_CHECK_GRANULARITY"]
    for spec in orders_env.check_specs_by_output_name.values():
        print(f"  {spec.name}")

    print("\n=== loud definition-time errors (#8/#9) ===")
    try:

        class Collide(dy.Schema):
            amount = dy.Int64(min=0)

            @dy.rule()
            def amount__min(cls) -> pl.Expr:
                return cls.amount.col < 10

        _core.check_specs(Collide, asset="collide")
    except _core.CheckNameCollisionError as e:
        print("  CheckNameCollisionError:", e)
    try:

        class Reserved(dy.Schema):
            dy_flag = dy.Bool()

        _core.check_specs(Reserved, asset="reserved")
    except _core.ReservedColumnError as e:
        print("  ReservedColumnError:", e)


def run_case(
    label: str, assets: list[dg.AssetsDefinition], *, expect_success: bool
) -> None:
    result = dg.materialize(assets, raise_on_error=False)
    materialized = [
        e.asset_key.to_user_string()
        for e in result.get_asset_materialization_events()
        if e.asset_key is not None
    ]
    print(f"\n=== {label} ===")
    print(f"run success: {result.success} (expected {expect_success})")
    print(f"materialized: {materialized or '(nothing)'}")
    evals = result.get_asset_check_evaluations()
    failed = [e for e in evals if not e.passed]
    print(f"checks: {len(evals)} evaluated, {len(failed)} failed")
    for e in failed:
        print(f"  FAIL {e.check_name} severity={e.severity.value}")


if __name__ == "__main__":
    import logging

    logging.getLogger("dagster").setLevel(logging.CRITICAL)
    show_surface()
    run_case(
        "case 1: clean + abort policy -> green, quarantine absent",
        [orders_strict],
        expect_success=True,
    )
    run_case(
        "case 2: mixed + quarantine -> both outs, WARN, green",
        [orders, raw_orders],
        expect_success=True,
    )
    run_case(
        "case 3: mixed + abort policy -> ERROR checks, raise, nothing kept",
        [dd_asset(schema=Orders, name="orders_strict_mixed")(mixed_orders)],
        expect_success=False,
    )
    run_case(
        "case 4: hopeless + quarantine -> quarantine only, run fails",
        [orders_hopeless_fn],
        expect_success=False,
    )
    run_case(
        "case 5: wrong dtype -> gate fails before filter",
        [orders_wrong_dtype],
        expect_success=False,
    )
