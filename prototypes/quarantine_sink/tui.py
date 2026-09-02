"""PROTOTYPE SHELL. Drive `sink.py` by hand and watch where the quarantine lands.

Run: uv run --group duckdb python prototypes/quarantine_sink/tui.py
"""

import shutil
import sys
import tempfile
import warnings
from pathlib import Path

import dagster as dg
import dataframely as dy
import duckdb
import polars as pl
from dagster_duckdb_polars import DuckDBPolarsIOManager
from dagster_polars import PolarsParquetIOManager

sys.path.insert(0, str(Path(__file__).parent))
from sink import write_quarantine  # noqa: E402

warnings.filterwarnings("ignore")
for noisy in ("dagster",):
    import logging; logging.getLogger(noisy).setLevel(logging.CRITICAL)

BOLD, DIM, OFF = "\x1b[1m", "\x1b[2m", "\x1b[0m"
DAYS = dg.StaticPartitionsDefinition(["2026-01-02", "2026-01-03"])


class Orders(dy.Schema):
    order_id = dy.String(primary_key=True, regex=r"^ORD-\d+$")
    amount = dy.Float64(nullable=False, min=0.0)
    day = dy.String(nullable=False)


RAW = pl.DataFrame({"order_id": ["ORD-1", "ORD-2"], "amount": [10.0, -4.0], "day": ["2026-01-02"] * 2})
HOPELESS = pl.DataFrame({"order_id": ["ORD-2"], "amount": [-4.0], "day": ["2026-01-02"]})

state = {"manager": "parquet", "partitioned": False, "exit": "partial",
         "partition_expr": True, "last": "(nothing run yet)"}


def run() -> str:
    """Materialize one asset under the current settings, report what landed."""
    workspace = Path(tempfile.mkdtemp())
    database = workspace / "w.duckdb"
    with duckdb.connect(str(database)) as seed:
        seed.sql("CREATE SCHEMA IF NOT EXISTS analytics")

    partitions = DAYS if state["partitioned"] else None
    source = HOPELESS if state["exit"] == "nothing_survived" else RAW

    # What a DuckDB user must already declare for their own partitioned table. The sink
    # forwards it because it copies definition metadata, never because it reads it.
    metadata = {"partition_expr": "day"} if state["partition_expr"] else {}

    @dg.asset(name="orders", key_prefix="analytics", partitions_def=partitions,
              metadata=metadata, required_resource_keys={"io_manager"})
    def orders(context: dg.AssetExecutionContext) -> pl.DataFrame:
        valid, failure = Orders.filter(source)
        written = write_quarantine(context, failure.invalid())
        context.log.info(f"quarantine -> {written.to_user_string()}")
        if state["exit"] == "abort":
            raise RuntimeError("the run dies after the quarantine is written")
        if state["exit"] == "nothing_survived":
            raise RuntimeError("nothing survived the filter")
        return valid

    manager = (
        PolarsParquetIOManager(base_dir=str(workspace / "wh"))
        if state["manager"] == "parquet"
        else DuckDBPolarsIOManager(database=str(database), schema="analytics")
    )
    result = dg.materialize([orders], partition_key="2026-01-02" if partitions else None,
                            resources={"io_manager": manager}, raise_on_error=False)

    files = sorted(p.relative_to(workspace).as_posix() for p in workspace.rglob("*.parquet"))
    with duckdb.connect(str(database)) as check:
        tables = [t[0] for t in check.sql(
            "select table_schema||'.'||table_name from information_schema.tables "
            "where table_schema='analytics' order by 1").fetchall()]
    shutil.rmtree(workspace)
    landed = files if state["manager"] == "parquet" else tables
    return f"run success={result.success}\n  landed: {landed or '(nothing)'}"


def frame() -> None:
    print("\033[2J\033[H", end="")
    print(f"{BOLD}quarantine sink prototype{OFF}  {DIM}does the asset's own manager place the quarantine?{OFF}\n")
    print(f"  {BOLD}manager{OFF}      {state['manager']}")
    print(f"  {BOLD}partitioned{OFF}  {state['partitioned']}")
    print(f"  {BOLD}exit{OFF}         {state['exit']}")
    print(f"  {BOLD}partition_expr{OFF}  {state['partition_expr']}  {DIM}(DuckDB needs it for any partitioned table){OFF}")
    print(f"\n  {BOLD}last run{OFF}     {state['last']}\n")
    print(f"{DIM}[m]{OFF} manager  {DIM}[p]{OFF} partitioned  {DIM}[e]{OFF} exit  "
          f"{DIM}[x]{OFF} partition_expr  {DIM}[r]{OFF} run  {DIM}[a]{OFF} run all 12  {DIM}[q]{OFF} quit")


def run_all() -> str:
    lines = []
    for mgr in ("parquet", "duckdb"):
        for part in (False, True):
            for exit_ in ("partial", "abort", "nothing_survived"):
                state.update(manager=mgr, partitioned=part, exit=exit_)
                out = run().replace("\n  landed: ", " | ")
                lines.append(f"{mgr:8} part={str(part):5} {exit_:16} {out}")
    return "\n            ".join(lines)


EXITS = ["partial", "abort", "nothing_survived"]
frame()
while True:
    try:
        key = input("> ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        break
    if key == "q":
        break
    if key == "m":
        state["manager"] = "duckdb" if state["manager"] == "parquet" else "parquet"
    elif key == "p":
        state["partitioned"] = not state["partitioned"]
    elif key == "e":
        state["exit"] = EXITS[(EXITS.index(state["exit"]) + 1) % len(EXITS)]
    elif key == "x":
        state["partition_expr"] = not state["partition_expr"]
    elif key == "r":
        state["last"] = run()
    elif key == "a":
        state["last"] = run_all()
    frame()
