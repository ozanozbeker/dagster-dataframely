"""PROTOTYPE - throwaway (wayfinder #11). Shape B: decorator layered under `@dg.asset`.

The inner decorator wraps the *function*; the outer `@dg.asset` builds the
definition. The defect is structural: the inner layer cannot reach the outer
call, so everything definition-time (check specs, metadata, outs) must be
restated by the user in the outer decorator - and the quarantine cannot exist
at all, because `@dg.asset` has exactly one out.

Run: uv run python docs/probes/front-door/shape_b_layered.py
"""

import functools
from collections.abc import Callable, Iterator
from typing import Any

import _core
import dagster as dg
import dataframely as dy
import polars as pl
from _scenario import Orders, clean_orders

TransformFn = Callable[..., pl.DataFrame | pl.LazyFrame]
ComputeFn = Callable[
    ..., Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]
]

# --- the package-side surface of shape B ---------------------------------------


def validated(
    schema: type[dy.Schema], **kwargs: Any
) -> Callable[[TransformFn], ComputeFn]:
    """Wrap the compute function so its return value runs the #6/#7 state machine."""

    def decorate(fn: TransformFn) -> ComputeFn:
        @functools.wraps(fn)
        def compute(
            *args: Any, **fn_kwargs: Any
        ) -> Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]:
            frame = fn(*args, **fn_kwargs)
            context = dg.AssetExecutionContext.get()
            yield from _core.process(
                schema,
                frame,
                context=context,
                good_out=context.op_def.output_defs[0].name,
                quarantine_out=None,  # structurally unreachable under @dg.asset
                **kwargs,
            )

        return compute

    return decorate


# --- call site: abort policy ---------------------------------------------------
# The schema is stated THREE times (check_specs, metadata, validated), the asset
# name once more inside check_specs, and nothing detects drift between them.


@dg.asset(
    check_specs=_core.check_specs(Orders, asset="orders_layered"),
    metadata=_core.definition_metadata(Orders),
)
@validated(Orders)
def orders_layered() -> pl.DataFrame:
    return clean_orders()


# --- call site: quarantine policy ----------------------------------------------
# Does not exist. `@dg.asset` has one out; declaring the quarantine means
# abandoning this shape for a hand-wired `@dg.multi_asset` - which is shape C.
# The structural consent of #6 ("declaring the second asset is the consent")
# cannot be expressed as a parameter of this decorator.


# --- decorator-order fragility -------------------------------------------------
# Flipping the layers silently produces a broken definition: @dd.validated would
# receive an AssetsDefinition, not a function. Nothing enforces the order.


def demo_flipped_order() -> None:
    try:
        # The checker statically rejects the flipped order (an AssetsDefinition
        # is not a transform fn) - which is the demo's point, so suppress it.
        @validated(Orders)  # pyrefly: ignore[bad-argument-type]
        @dg.asset(check_specs=_core.check_specs(Orders, asset="orders_flipped"))
        def orders_flipped() -> pl.DataFrame:
            return clean_orders()

        materialized = isinstance(orders_flipped, dg.AssetsDefinition)
        print(f"flipped order: still an AssetsDefinition? {materialized}")
        print(
            f"  type is now: {type(orders_flipped).__name__} - a plain function Dagster never loads"
        )
    except Exception as e:  # noqa: BLE001
        print(f"flipped order raised: {type(e).__name__}: {e}")


if __name__ == "__main__":
    import logging

    logging.getLogger("dagster").setLevel(logging.CRITICAL)
    result = dg.materialize([orders_layered], raise_on_error=False)
    evals = result.get_asset_check_evaluations()
    print(f"plain case: success={result.success}, checks evaluated={len(evals)}")
    demo_flipped_order()
