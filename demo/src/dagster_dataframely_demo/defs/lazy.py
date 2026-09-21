"""Group `lazy`: what a `pl.LazyFrame` does on the way in and on the way out.

Two places meet one, and they read it differently on purpose. A read has no object yet, so the annotation is the only signal it has. A return is executed, once, in `Schema.filter`.

The pair is the point. Both assets take a lazy input and return a lazy plan; one has a schema and the other does not, and that is the whole difference between streaming end to end and materializing at the filter.
"""

import dagster as dg
import polars as pl

import dagster_dataframely as dd
from dagster_dataframely_demo.schema import Orders

GROUP = "lazy"


@dg.asset(
    group_name=GROUP,
    description=(
        "A plain `@dg.asset` returning a `pl.LazyFrame`, so the plan streams straight to "
        "storage.\n\n"
        "No schema means no validation, no per-rule checks and no statistics pass, so nothing "
        "forces the result into memory: the IO manager sinks the plan and nothing is read back. "
        "Peak memory is the engine's buffers rather than the frame.\n\n"
        "The input is annotated `pl.LazyFrame` too, so the manager hands back an unexecuted scan "
        "and the filter below prunes rows before anything is decoded. That dispatch is the IO "
        "manager's and works the same on any asset."
    ),
)
def streamed_extract(raw_orders: pl.LazyFrame) -> pl.LazyFrame:
    """Sink a plan straight to storage, with no schema in the way."""
    return raw_orders.filter(pl.col("amount") > 0)


@dd.asset(
    Orders,
    group_name=GROUP,
    description=(
        "The same lazy return, this time validated.\n\n"
        "The column-schema check runs first off `collect_schema()`, executing nothing, so a plan "
        "whose columns disagree is refused before a single row is pulled through it. Then "
        "`Schema.filter` takes the plan and hands back two, and the package collects both in one "
        "`collect_all` on the streaming engine, so the source is never read twice.\n\n"
        "Your joins, filters and aggregations therefore run in the streaming engine. What comes "
        "into memory is what the plan produced, not the plan: a join that fans out before "
        "filtering back down never pays for the fan-out.\n\n"
        "What stays eager is storage, not the computation. Every outcome past `Schema.filter` "
        "counts, samples or profiles both halves, so validation cannot pick an outcome without "
        "executing the plan, and writing straight through would have written before it knew."
    ),
)
def validated_stream(streamed_extract: pl.LazyFrame) -> pl.LazyFrame:
    """Return a lazy plan under a schema: executed once, at the filter."""
    return streamed_extract.filter(pl.col("quantity") >= 1)
