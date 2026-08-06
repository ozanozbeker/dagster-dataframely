"""PROTOTYPE - throwaway (wayfinder #11). Shape C: plain helpers, user-wired.

The same kit shape A wraps, used raw: the user writes the `@dg.multi_asset`
themselves and calls the helpers at each coordination point. Maximum
composability - any asset shape Dagster supports - at the cost of restating
the asset name and schema at every seam, with the seams free to drift.

Run: uv run python docs/probes/front-door/shape_c_helpers.py
"""

from collections.abc import Iterator

import _core
import dagster as dg
import polars as pl
from _scenario import Orders, mixed_orders

AssetYield = Iterator[dg.MaterializeResult[pl.DataFrame] | dg.AssetCheckResult]

# --- call site: quarantine policy ----------------------------------------------
# Coordination points the USER now owns:
#   1. outs dict - names, is_required=False, definition metadata weaving
#   2. check_specs - schema again, asset name again
#   3. process()  - schema again, both out names again
# The package cannot force is_required=False, cannot default the quarantine
# sibling name, and cannot see AssetOut(key=) to keep the specs aligned.


@dg.multi_asset(
    outs={
        "orders_wired": dg.AssetOut(
            is_required=False, metadata=_core.definition_metadata(Orders)
        ),
        "orders_wired_quarantine": dg.AssetOut(
            is_required=False,
            metadata={"dagster/column_schema": _core.quarantine_table_schema(Orders)},
        ),
    },
    check_specs=_core.check_specs(Orders, asset="orders_wired"),
)
# `context: dg.AssetExecutionContext` works here only because this module does
# NOT use `from __future__ import annotations` - with it, the annotation reaches
# Dagster as the *string* "dg.AssetExecutionContext", which its qualified-name
# check rejects outright. A user-side trap shape A never exposes: its user
# functions take no context parameter at all.
def orders_wired(context: dg.AssetExecutionContext) -> AssetYield:
    frame = mixed_orders()
    yield from _core.process(
        Orders,
        frame,
        context=context,
        good_out="orders_wired",
        quarantine_out="orders_wired_quarantine",
    )


# --- the desync demo -----------------------------------------------------------
# The user routes the good asset under a prefix but forgets the specs (or the
# reverse). In shape A the door constructs the key once; here two call sites
# must agree by hand, and the failure is Dagster's error, not the package's.


def demo_key_desync() -> None:
    try:

        @dg.multi_asset(
            outs={
                "orders_moved": dg.AssetOut(key_prefix="warehouse", is_required=False)
            },
            check_specs=_core.check_specs(Orders, asset="orders_moved"),  # stale key
        )
        def orders_moved(context: dg.AssetExecutionContext) -> AssetYield:
            yield from _core.process(
                Orders, mixed_orders(), context=context, good_out="orders_moved"
            )

        print(
            "desync: constructed without error (checks silently target a foreign key)"
        )
    except Exception as e:  # noqa: BLE001
        message = str(e).split("\n")[0][:200]
        print(f"desync: {type(e).__name__}: {message}")


if __name__ == "__main__":
    import logging

    logging.getLogger("dagster").setLevel(logging.CRITICAL)
    result = dg.materialize([orders_wired], raise_on_error=False)
    materialized = [
        e.asset_key.to_user_string()
        for e in result.get_asset_materialization_events()
        if e.asset_key is not None
    ]
    evals = result.get_asset_check_evaluations()
    failed = [e for e in evals if not e.passed]
    print(f"quarantine case: success={result.success}, materialized={materialized}")
    print(f"checks: {len(evals)} evaluated, {len(failed)} failed (WARN)")
    demo_key_desync()
