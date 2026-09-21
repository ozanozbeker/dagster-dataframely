"""Group `wiring`: the same surfaces, assembled by hand.

The decorator is one arrangement of parts the package also exports under `dd.wiring`. Reach for them when the decorator's shape is not the shape you need: a schema attached to an asset you did not declare, or a reporting arrangement the decorator does not offer.

Three arrangements here, ordered by how much of the decorator they keep.

`hand_wired_orders` keeps all of it and is the decorator written out: the same Columns tab, the same check list, the same failure policy, the same quarantine. `hand_wired_clean_orders` drops the quarantine, which drops the writer with it. `unfiltered_orders` splits the checks off entirely, which is the one arrangement that gives up the guarantee the decorator exists for.

This is not a route to `dy.Collection` support. `validation_results` is single-schema by signature, so hand-wiring a Collection means reimplementing the hardest part of the package rather than assembling it. Declare one asset per member instead.
"""

from collections.abc import Iterator

import dagster as dg
import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders

GROUP = "wiring"

#: Resolved at definition time so the check specs and the check results cannot name different keys.
UNFILTERED = dg.AssetKey(["unfiltered_orders"])


@dg.asset(
    group_name=GROUP,
    metadata=dd.wiring.schema_metadata(Orders),
    check_specs=dd.wiring.check_specs(Orders, asset="hand_wired_orders"),
    # The column-schema check, both aborts and the skip end the step without yielding. Leave
    # this off and every path that does not raise has to yield the output.
    output_required=False,
    description=(
        "The decorator written out: `schema_metadata`, `check_specs`, `validate_quarantine_key` "
        "and `validation_results`, on a `@dg.asset` you declared.\n\n"
        "Its Columns tab, its check list and its materialization metadata are the ones "
        "`quarantined_orders` has, on the same twenty rows. That is the point of the group: the "
        "decorator is assembly, not magic.\n\n"
        "`context.asset_key` is the whole of the key resolution, because a single-output asset "
        "has exactly one key to resolve. `delegating_writer` hands the invalid rows to the IO "
        "manager this asset is already bound to, so nothing here learns which manager that is."
    ),
)
def hand_wired_orders(
    context: dg.AssetExecutionContext, defective_raw_orders: pl.DataFrame
) -> dd.wiring.AssetYield:
    """Run what `dd.asset` runs, on an asset and check specs written by hand.

    `validate_quarantine_key` goes before the body, where the decorator calls it on every run: it fails the run when another asset in the code location already materializes `hand_wired_orders_quarantine`. A direct call passes straight through it, since there is no graph to check against.
    """
    dd.wiring.validate_quarantine_key(context)
    yield from dd.wiring.validation_results(
        Orders,
        defective_raw_orders,
        valid_key=context.asset_key,
        quarantine_writer=dd.wiring.delegating_writer(context),
    )


hand_wired_orders_quarantine = dd.quarantine_spec(
    Orders, hand_wired_orders
).replace_attributes(
    group_name=GROUP,
    description="The invalid rows, written by `delegating_writer` rather than by the decorator. Same key, same manager, same file.",
)


@dg.asset(
    group_name=GROUP,
    metadata=dd.wiring.schema_metadata(Orders),
    check_specs=dd.wiring.check_specs(Orders, asset="hand_wired_clean_orders"),
    output_required=False,
    description=(
        "The same arrangement with no `quarantine_writer`, which is the whole of the failure "
        "policy.\n\n"
        "Passing none makes invalid rows abort the run, exactly as `quarantine=False` does on "
        "the decorator. So this one runs on the clean frame, and what it gives up against "
        "`hand_wired_orders` is where invalid rows would land."
    ),
)
def hand_wired_clean_orders(
    context: dg.AssetExecutionContext, raw_orders: pl.DataFrame
) -> dd.wiring.AssetYield:
    """One out, no writer: the smallest useful hand-wiring there is."""
    yield from dd.wiring.validation_results(
        Orders, raw_orders, valid_key=context.asset_key
    )


@dg.asset(
    group_name=GROUP,
    metadata=dd.wiring.schema_metadata(Orders),
    description=(
        "An ordinary asset that writes whatever it returns, with the checks split off into a "
        "node of their own.\n\n"
        "`schema_metadata` still fills the Columns tab, so the catalog states what the table "
        "should be. Nothing validates it here: `validation_results` is what fuses the write and "
        "the verdict into one step, and this arrangement does not call it.\n\n"
        "Reach for it when the write must not depend on the verdict. What you give up is the "
        "guarantee the decorator exists for: the eight invalid rows are in this table, and a red "
        "check beside it is what tells you. The decorator would have refused to write them."
    ),
)
def unfiltered_orders(defective_raw_orders: pl.LazyFrame) -> pl.LazyFrame:
    """Return the frame unfiltered, so its checks have something to report."""
    return defective_raw_orders


@dg.multi_asset_check(
    specs=dd.wiring.check_specs(Orders, asset=UNFILTERED),
    description="Every check `unfiltered_orders` declares, answered after the table was written.",
)
def unfiltered_orders_checks(
    unfiltered_orders: pl.LazyFrame,
) -> Iterator[dg.AssetCheckResult]:
    """Read the table back through the IO manager and report on what is in it.

    `check_results` is `validation_results` without the writing: no materialization, no quarantine, and neither error that carries the failure policy. A caller that writes nothing has no rows to route and no table to withhold.

    Two things it asks for that `validation_results` works out for itself. `severity`, because there is no write outcome to grade from: `WARN` says the table was written anyway. And the same settings the specs were derived with, because a result answers a spec by name, so two calls that group the rules differently would leave the step with outputs nobody wrote. Both calls here take the defaults.
    """
    yield from dd.wiring.check_results(
        Orders,
        unfiltered_orders,
        asset_key=UNFILTERED,
        severity=dg.AssetCheckSeverity.WARN,
    )
