"""Groups `failure/*`: the five exits a run can take, one subgroup each.

The library's failure-policy table has six rows and five of them are here. The sixth is `catalog/orders`, where every row is valid and the question never arises.

Declaring a quarantine **is** the consent to partial data, so what an invalid row costs is visible in the definition and cannot disagree with what the asset declares. `failure/quarantine` and `failure/no_quarantine` are the same twenty rows either side of that one argument, which is why they read from the same base table.

**Three of these fail on every run, deliberately.** A red column-schema check and an aborted run are surfaces too, and each raises a different error from the package with a different thing to say. Keep them out of a bulk materialize, or the aborted run buries the assets you wanted populated.

Each quarantine here gets a `dd.quarantine_spec` beside its asset. The rows are written either way; the spec is what gives them a node in the lineage view, which is what there is to look at.
"""

import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders

QUARANTINE = "failure/quarantine"
NO_QUARANTINE = "failure/no_quarantine"
COLUMN_SCHEMA = "failure/column_schema"
NOTHING_SURVIVES = "failure/nothing_survives"
SKIP = "failure/skip"


@dd.asset(
    Orders,
    group_name=QUARANTINE,
    quarantine=True,
    description=(
        "Twenty lines, eight of them invalid, with `quarantine=True` declared.\n\n"
        "The eight invalid lines are written beside this table under "
        "`quarantined_orders_quarantine`, the checks that rejected them fail at `WARN`, and the "
        "run stays green so downstream proceeds on the data that is fine. Each red check carries "
        "up to five of the rows it rejected.\n\n"
        "Compare its check list against `strict_orders`, which is the same data with no "
        "quarantine declared."
    ),
)
def quarantined_orders(defective_raw_orders: pl.DataFrame) -> pl.DataFrame:
    """Take the middle exit: survivors written, the rest inspectable next door."""
    return defective_raw_orders


#: The quarantine's node in the graph. `quarantine=True` writes the rows; this is what lets you click on them.
quarantined_orders_quarantine = dd.quarantine_spec(
    Orders, quarantined_orders
).replace_attributes(
    group_name=QUARANTINE,
    description=(
        "The rows `Orders` rejected: the original columns, then one `String` column per rule "
        "reading `valid`, `invalid` or `unknown` and named for the rule.\n\n"
        "Its Columns tab states no constraints, because every constraint the valid table states "
        "is one these rows break. The primary key is stated on `quarantined_orders` and nowhere "
        "here, since a duplicate key is exactly what ends up in a quarantine.\n\n"
        "It never receives a materialization event. Nothing materializes it: the rows were "
        "written inside `quarantined_orders`' own step."
    ),
)


@dd.asset(
    Orders,
    group_name=NO_QUARANTINE,
    description=(
        "The same twenty lines as `quarantined_orders` with no quarantine declared, so the run "
        "fails and writes nothing.\n\n"
        "Without a quarantine every row has to be valid. The checks fail at `ERROR`, the run "
        "raises `ValidationAbortError`, and the last-known-good table stays in place. Landing the "
        "survivors and dropping the rest is the failure this package exists to make visible, so "
        "it is not reachable by configuration."
    ),
)
def strict_orders(defective_raw_orders: pl.DataFrame) -> pl.DataFrame:
    """Take the abort exit: rows rejected with nowhere to route them."""
    return defective_raw_orders


@dd.asset(
    Orders,
    group_name=COLUMN_SCHEMA,
    description=(
        "Clean rows arriving from `mistyped_raw_orders` with `quantity` as `Int64` where the "
        "schema declares `Int32`.\n\n"
        "The blocking `dy_schema__columns` check fails and the run raises `ColumnSchemaError` "
        "before a single row is filtered, so no rule check reports at all. The package never "
        "casts: silently widening a dtype is how a thousandfold error reaches a table nobody "
        "re-reads.\n\n"
        "The failing check carries `dy_schema__errors`, a row per offending column."
    ),
)
def mistyped_orders(mistyped_raw_orders: pl.DataFrame) -> pl.DataFrame:
    """Take the column-schema exit: a pipeline defect rather than a data one."""
    return mistyped_raw_orders


@dd.asset(
    Orders,
    group_name=NOTHING_SURVIVES,
    quarantine=True,
    description=(
        "Three lines, all of them negative, with a quarantine declared.\n\n"
        "Nothing survives, so the valid output is skipped rather than emptied, the quarantine "
        "takes all three rows, the checks fail at `ERROR`, and the run raises "
        "`NothingSurvivedError`. An empty table where data used to be is the quietest possible "
        "pipeline failure, so the package refuses to write one: consenting to partial data was "
        "never consent to no data.\n\n"
        "The rows are written by the run that fails, which is the run you most want them from."
    ),
)
def doomed_orders(hopeless_raw_orders: pl.DataFrame) -> pl.DataFrame:
    """Take the nothing-survived exit."""
    return hopeless_raw_orders


doomed_orders_quarantine = dd.quarantine_spec(Orders, doomed_orders).replace_attributes(
    group_name=NOTHING_SURVIVES,
    description="Every row, because every row was rejected. This one is written; the valid table is not.",
)


@dd.asset(
    Orders,
    group_name=SKIP,
    deps=["raw_orders"],
    description=(
        "No source data this run, which is neither a failure nor an empty table.\n\n"
        "Returning `None` skips: nothing is validated, nothing materializes, the run succeeds, "
        "and the asset stays unmaterialized. Every check still reports and passes, evaluated "
        "over `Schema.create_empty()`, because a check spec is a non-optional output whatever "
        "the asset declares. Dagster records those evaluations with no materialization attached, "
        "so a green check here claims nothing about a run that had rows.\n\n"
        "A real asset tests for its source and returns `None` when it is legitimately absent. "
        "The existence test is yours to write: the decorator catches no `FileNotFoundError`, "
        "because it cannot tell an absent partition from a misconfigured path."
    ),
)
def skipped_orders() -> pl.DataFrame | None:
    """Take the skip exit: a partition that has no data and never will."""
    return None
