"""Group `catalog`: what one decorator argument puts in the catalog.

**Start here.** Two assets over `raw_orders`, which declares nothing about its own shape. Open either Columns tab beside the base table's and the difference is the whole of this project; everything after this group is a variation on it.

The prose these assets show in the UI is passed as `description=` rather than left in a docstring. Unset, the decorator fills the description from the schema's own docstring, which is the right default for a real project and the wrong one here: every asset in this demo would then read "A customer order line", and the point of each would be invisible. `orders_undescribed` is the one asset that leaves it unset, so the fallback is visible once.
"""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders

GROUP = "catalog"


@dd.asset(
    Orders,
    group_name=GROUP,
    owners=["team:data-platform"],
    kinds={"polars"},
    tags={"layer": "silver"},
    description=(
        "The whole integration, in one decorator argument.\n\n"
        "The Columns tab was filled in before this asset had ever run, straight from `Orders`: "
        "dtypes, descriptions, nullability, `tracking_id` marked unique, the primary key stated "
        "once at table level, and one constraint listed beside each column. `amount` carries the "
        "column tags its `metadata=` declared.\n\n"
        "Every run reports one asset check per Dataframely rule, each with its own history, "
        "behind the blocking `dy_schema__columns` check. The materialization carries "
        "`dagster/row_count`, `dataframely/valid_sample` and one "
        "`dataframely/valid_statistics/<group>` table per dtype group present."
    ),
)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """Return the rows every other group's asset varies."""
    return raw_orders


@dd.asset(Orders, group_name=GROUP)
def orders_undescribed(raw_orders: pl.DataFrame) -> pl.DataFrame:
    """Leave `description=` unset, so this docstring never reaches the UI.

    With no `description=` passed, the decorator fills it from `Orders.__doc__`, because the schema is what describes the table while a function's docstring describes the code that fills it. Dagster's own fallback to this docstring only stands where the schema has none.
    """
    return raw_orders
