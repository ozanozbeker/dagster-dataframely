# ruff: noqa: D102, D103, D205, INP001, PLR0913, TC003
"""PROTOTYPE for issue #27. Throwaway code. Wipe freely.

The question: which execution strategy should a LazyFrame-aware write path
use, measured at a frame size where the differences matter? Four candidates
run the same validate-and-store pipeline end to end. The TUI races them and
tabulates wall time per phase, peak child RSS, per-rule counts, and where
`dagster/row_count` came from. Two demos cover the rest of the ticket: lazy
reads on their own, and the UPathIOManager missing-file wart.

This module is the liftable part: pure functions over paths and configs, no
terminal code. The TUI imports it; nothing flows the other way.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path

import dataframely as dy
import polars as pl

# Violation bands repeat every BUCKET rows, so the failure fraction holds at
# any frame size and two runs can never disagree. No RNG anywhere.
BUCKET = 100_000
PK_SENTINEL = -1

SIZES = [100_000, 1_000_000, 10_000_000]
FAIL_PCTS = [0.0, 0.1, 1.0, 10.0, 50.0]
UPSTREAMS = ["cheap", "heavy"]
# Tuned so the heavy tail costs whole seconds at ten million rows. Eight
# rounds measured near-free at 1M (0.04s), which would let sink costs drown
# the double-execution penalty the upstream knob exists to expose.
HEAVY_ROUNDS = 64


class Reading(dy.Schema):
    """The schema the race validates against.

    Three column rules and one cross-column rule, so per-rule counts have
    several entries. `calibrated_when_negative` fails on exactly the rows
    that fail `value|min`, on purpose: it gives co-occurrence something to
    show without changing the total failed-row count.
    """

    reading_id = dy.Int64(primary_key=True)
    sensor = dy.String(nullable=False)
    value = dy.Float64(nullable=False, min=0.0)
    payload = dy.String(nullable=True)

    @dy.rule()
    def calibrated_when_negative(cls) -> pl.Expr:
        return (cls.value.col >= 0.0) | cls.sensor.col.is_null()


class ReadingNoKey(dy.Schema):
    """`Reading` with the primary key demoted to a plain column.

    The knob that decides whether "lazy end to end" is even reachable.
    Uniqueness is a global question: the engine cannot answer it from one
    streaming batch, so a `primary_key` forces the whole frame into memory no
    matter which strategy sinks it. Every other rule here is row-local and
    streams. Toggling this against `Reading` separates what the storage
    layer costs from what the schema costs, and the ticket's question hangs
    on that difference.
    """

    reading_id = dy.Int64()
    sensor = dy.String(nullable=False)
    value = dy.Float64(nullable=False, min=0.0)
    payload = dy.String(nullable=True)

    @dy.rule()
    def calibrated_when_negative(cls) -> pl.Expr:
        return (cls.value.col >= 0.0) | cls.sensor.col.is_null()


SCHEMAS: dict[str, type[dy.Schema]] = {"pk": Reading, "nopk": ReadingNoKey}


# pyrefly: ignore [implicit-any-type-argument]
def schema_for(cfg: dict) -> type[dy.Schema]:
    """The schema a run validates against. Defaults to the keyed one."""
    return SCHEMAS[cfg.get("schema", "pk")]


def source_path(root: Path, rows: int, fail_pct: float) -> Path:
    return root / f"source-r{rows}-f{fail_pct}.parquet"


def ensure_source(root: Path, rows: int, fail_pct: float) -> Path:
    """Generates the source parquet once per (rows, fail_pct), then reuses it.

    Violations live in the data, not the plan, so every strategy reads the
    identical file. Three disjoint bands over `idx % BUCKET` violate one rule
    each: duplicated primary key, null sensor, negative value. The negative
    band also trips the cross-column rule.
    """
    path = source_path(root, rows, fail_pct)
    if path.exists():
        return path
    band = round(fail_pct / 100 * BUCKET)
    pk_n = band // 3
    null_n = band // 3
    idx = pl.col("idx")
    b = idx % BUCKET
    (
        # One eager column to seed the plan: int_range needs a context, and
        # 8 bytes a row only ever exists in the generating process.
        pl.select(pl.int_range(0, rows, dtype=pl.Int64).alias("idx"))
        .lazy()
        .select(
            pl.when(b < pk_n)
            .then(pl.lit(PK_SENTINEL, dtype=pl.Int64))
            .otherwise(idx)
            .alias("reading_id"),
            pl.when((b >= pk_n) & (b < pk_n + null_n))
            .then(pl.lit(None, dtype=pl.String))
            .otherwise(pl.format("sensor_{}", idx % 37))
            .alias("sensor"),
            pl.when((b >= pk_n + null_n) & (b < band))
            .then(-1.0 - (idx % 97).cast(pl.Float64))
            .otherwise((idx % 9973).cast(pl.Float64) / 7.0)
            .alias("value"),
            pl.format("{}-{}", idx, idx % 1000).alias("payload"),
        )
        .sink_parquet(path)
    )
    return path


def source_plan(source: Path, upstream: str) -> pl.LazyFrame:
    """The plan a transform would hand the write path: a scan, with an
    optionally expensive tail.

    The heavy tail rewrites `payload`, a column the schema keeps, because the
    optimizer deletes work that feeds nothing. Hash rounds cost real CPU per
    row and stay streaming-safe. A sort or join would also be realistic
    upstream, but either would dominate the memory column and drown the
    strategy differences this prototype exists to expose.
    """
    lf = pl.scan_parquet(source)
    if upstream == "heavy":
        expr = pl.col("payload")
        for _ in range(HEAVY_ROUNDS):
            expr = expr.hash().cast(pl.String)
        lf = lf.with_columns(expr.alias("payload"))
    return lf


def check_name(rule: str) -> str:
    # Mirrors dagster_dataframely._naming.check_name. Cloned rather than
    # imported so the child process never pays the dagster import, which
    # would inflate every RSS baseline.
    return f"dy_rule__{rule.replace('|', '__')}"


def lazy_quarantine(failure: dy.FailureInfo) -> pl.LazyFrame:
    """The lazy twin of `_runtime.quarantine_frame`: same columns, no collect.

    Reaches into `_lf` and `_rule_columns` because dataframely's public lazy
    surface is `FailureInfo.sink_parquet`, which writes boolean rule columns,
    not the details shape the quarantine promises. If #27 goes this way, the
    package either pins these two privates (as `_naming` already pins two
    others) or asks dataframely for a lazy `details()`.

    Outcome columns go straight to String rather than through the Enum,
    because `quarantine_frame` casts them to String anyway for the Delta
    writer.
    """
    rules = failure._rule_columns  # noqa: SLF001
    return failure._lf.select(  # noqa: SLF001
        pl.exclude(rules),
        pl.col(*rules).replace_strict(
            {True: "valid", False: "invalid", None: "unknown"},
            return_dtype=pl.String,
        ),
    ).rename({rule: check_name(rule) for rule in rules})


def eager_quarantine(failure: dy.FailureInfo) -> pl.DataFrame:
    """What `_runtime.quarantine_frame` builds today, for the eager strategies."""
    details = failure.details()
    renames = {
        rule: check_name(rule)
        for rule in failure._rule_columns  # noqa: SLF001
        if rule in details.collect_schema()
    }
    return details.rename(renames).with_columns(
        pl.col(name).cast(pl.String) for name in renames.values()
    )


def counts_from_parquet(path: Path, rules: list[str]) -> dict[str, int]:
    """Per-rule counts read off the written quarantine, not off the plan.

    This is what frees the failure half from ever being collected: the
    quarantine file already holds one outcome column per rule, and summing
    string equality over it streams.
    """
    if not rules:
        return {}
    row = (
        pl.scan_parquet(path)
        .select((pl.col(check_name(r)) == "invalid").sum().alias(r) for r in rules)
        .collect()
        .row(0, named=True)
    )
    return {name: count for name, count in row.items() if count}


def footer_row_count(path: Path) -> int:
    """Row count from the parquet footer. The scan reads no data pages."""
    return pl.scan_parquet(path).select(pl.len()).collect().item()


@contextmanager
def _phase(phases: dict[str, float], name: str) -> Iterator[None]:
    start = time.perf_counter()
    yield
    phases[name] = time.perf_counter() - start


def _result(
    # pyrefly: ignore [implicit-any-type-argument]
    cfg: dict,
    phases: dict[str, float],
    *,
    good_rows: int,
    quar_rows: int,
    counts: dict[str, int],
    row_count_via: str,
    good_path: Path,
    quar_path: Path | None,
    land_bytes: int = 0,
    # pyrefly: ignore [implicit-any-type-argument]
) -> dict:
    return {
        "strategy": cfg["strategy"],
        "rows": cfg["rows"],
        "fail_pct": cfg["fail_pct"],
        "upstream": cfg["upstream"],
        "schema": cfg.get("schema", "pk"),
        "phases": {name: round(seconds, 4) for name, seconds in phases.items()},
        "total_s": round(sum(phases.values()), 4),
        "good_rows": good_rows,
        "quar_rows": quar_rows,
        "consistent": good_rows + quar_rows == cfg["rows"],
        "counts": counts,
        "row_count_via": row_count_via,
        "good_bytes": good_path.stat().st_size,
        "quar_bytes": quar_path.stat().st_size if quar_path else 0,
        "land_bytes": land_bytes,
    }


# pyrefly: ignore [implicit-any-type-argument]
def run_eager(cfg: dict, work: Path) -> dict:
    """Today's v1 path: collect the whole plan, then filter eagerly.

    The anchor the lazy strategies race against. This is the silent
    materialization the ticket wants to remove.
    """
    phases: dict[str, float] = {}
    good_path = work / "good.parquet"
    quar_path = work / "quarantine.parquet"
    lf = source_plan(Path(cfg["source"]), cfg["upstream"])
    with _phase(phases, "collect"):
        df = lf.collect()
    with _phase(phases, "filter"):
        good, failure = schema_for(cfg).filter(df, cast=False)
    with _phase(phases, "write_good"):
        good.write_parquet(good_path)
    with _phase(phases, "counts"):
        counts = failure.counts()
        quar_rows = len(failure)
    with _phase(phases, "quarantine"):
        eager_quarantine(failure).write_parquet(quar_path)
    with _phase(phases, "row_count"):
        good_rows = len(good)
    return _result(
        cfg,
        phases,
        good_rows=good_rows,
        quar_rows=quar_rows,
        counts=counts,
        row_count_via="len()",
        good_path=good_path,
        quar_path=quar_path,
    )


# pyrefly: ignore [implicit-any-type-argument]
def run_naive_lazy(cfg: dict, work: Path) -> dict:
    """Banked finding 2's shape: sink the good half, then collect the failure
    half for its counts.

    Everything upstream runs twice: once for the sink, once when `counts()`
    collects `_df`. The failure half lands in memory; the good half never
    does. The quarantine write after that is free, because `_df` is a
    cached property.
    """
    phases: dict[str, float] = {}
    good_path = work / "good.parquet"
    quar_path = work / "quarantine.parquet"
    good_lf, failure = schema_for(cfg).filter(
        source_plan(Path(cfg["source"]), cfg["upstream"]), cast=False
    )
    with _phase(phases, "sink_good"):
        good_lf.sink_parquet(good_path)
    with _phase(phases, "counts"):
        counts = failure.counts()
        quar_rows = len(failure)
    with _phase(phases, "quarantine"):
        eager_quarantine(failure).write_parquet(quar_path)
    with _phase(phases, "row_count"):
        good_rows = footer_row_count(good_path)
    return _result(
        cfg,
        phases,
        good_rows=good_rows,
        quar_rows=quar_rows,
        counts=counts,
        row_count_via="footer",
        good_path=good_path,
        quar_path=quar_path,
    )


# pyrefly: ignore [implicit-any-type-argument]
def run_collect_all(cfg: dict, work: Path) -> dict:
    """Banked finding 3's shape: one `collect_all`, then everything is eager.

    One upstream execution, both halves in memory at once. Also the live
    check the ticket asks for on polars#24129: if this runs no faster than
    naive at heavy upstream, the dataframely docstring's warning still
    applies to the installed polars.
    """
    phases: dict[str, float] = {}
    good_path = work / "good.parquet"
    quar_path = work / "quarantine.parquet"
    lazy = schema_for(cfg).filter(
        source_plan(Path(cfg["source"]), cfg["upstream"]), cast=False
    )
    with _phase(phases, "collect_all"):
        good, failure = lazy.collect_all()
    with _phase(phases, "write_good"):
        good.write_parquet(good_path)
    with _phase(phases, "counts"):
        counts = failure.counts()
        quar_rows = len(failure)
    with _phase(phases, "quarantine"):
        eager_quarantine(failure).write_parquet(quar_path)
    with _phase(phases, "row_count"):
        good_rows = len(good)
    return _result(
        cfg,
        phases,
        good_rows=good_rows,
        quar_rows=quar_rows,
        counts=counts,
        row_count_via="len()",
        good_path=good_path,
        quar_path=quar_path,
    )


# pyrefly: ignore [implicit-any-type-argument]
def run_temp_landing(cfg: dict, work: Path) -> dict:
    """Banked finding 5's shape: land the evaluated plan locally once, then
    stream everything else off files.

    Upstream runs once, into a temp parquet. The filter, both sinks, the
    counts, and both row counts read parquet after that. Neither half is
    ever collected, so memory stays at streaming batch size. The price is
    one extra local write and read, and the landed bytes on whatever disk
    the temp dir lives on.
    """
    phases: dict[str, float] = {}
    good_path = work / "good.parquet"
    quar_path = work / "quarantine.parquet"
    land_path = work / "landed.parquet"
    with _phase(phases, "land"):
        source_plan(Path(cfg["source"]), cfg["upstream"]).sink_parquet(land_path)
    good_lf, failure = schema_for(cfg).filter(pl.scan_parquet(land_path), cast=False)
    with _phase(phases, "sink_good"):
        good_lf.sink_parquet(good_path)
    with _phase(phases, "quarantine"):
        lazy_quarantine(failure).sink_parquet(quar_path)
    with _phase(phases, "counts"):
        counts = counts_from_parquet(quar_path, failure._rule_columns)  # noqa: SLF001
    with _phase(phases, "row_count"):
        good_rows = footer_row_count(good_path)
        quar_rows = footer_row_count(quar_path)
    land_bytes = land_path.stat().st_size
    # What the real manager would do. Also keeps repeat runs from lying about
    # disk cost.
    land_path.unlink()
    return _result(
        cfg,
        phases,
        good_rows=good_rows,
        quar_rows=quar_rows,
        counts=counts,
        row_count_via="footer",
        good_path=good_path,
        quar_path=quar_path,
        land_bytes=land_bytes,
    )


# pyrefly: ignore [implicit-any-type-argument]
def run_sink_only(cfg: dict, work: Path) -> dict:
    """The control: the same plan sunk with no validation at all.

    A bare streaming sink measured a couple hundred MB of engine appetite on
    its own, so the other strategies' memory deltas mean nothing against
    zero. Read them against this row instead. Every input row lands, so the
    good count equals the row count and the consistency check still holds.
    """
    phases: dict[str, float] = {}
    good_path = work / "good.parquet"
    with _phase(phases, "sink_good"):
        source_plan(Path(cfg["source"]), cfg["upstream"]).sink_parquet(good_path)
    with _phase(phases, "row_count"):
        good_rows = footer_row_count(good_path)
    return _result(
        cfg,
        phases,
        good_rows=good_rows,
        quar_rows=0,
        counts={},
        row_count_via="footer",
        good_path=good_path,
        quar_path=None,
    )


# pyrefly: ignore [implicit-any-type-argument]
STRATEGIES: dict[str, Callable[[dict, Path], dict]] = {
    "eager": run_eager,
    "naive": run_naive_lazy,
    "collect_all": run_collect_all,
    "temp_land": run_temp_landing,
    "sink_only": run_sink_only,
}


# pyrefly: ignore [implicit-any-type-argument]
def run(cfg: dict) -> dict:
    work = Path(cfg["work_dir"])
    work.mkdir(parents=True, exist_ok=True)
    return STRATEGIES[cfg["strategy"]](cfg, work)


def _ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000


def read_demo(good_path: Path) -> list[str]:
    """Times the read half on its own: the ticket's cheapest question.

    A `load_from_path` that returns a scan needs nothing from whatever the
    write side decided, which is the case for offering lazy reads
    independently of lazy writes.
    """
    lines: list[str] = []
    start = time.perf_counter()
    lf = pl.scan_parquet(good_path)
    lines.append(f"scan_parquet: {_ms(start):.2f} ms, no data read")
    start = time.perf_counter()
    schema = lf.collect_schema()
    lines.append(f"collect_schema: {_ms(start):.2f} ms, {len(schema)} columns")
    start = time.perf_counter()
    n = lf.select(pl.len()).collect().item()
    lines.append(f"row count via footer: {_ms(start):.2f} ms, {n:,} rows")
    start = time.perf_counter()
    lf.head(3).collect()
    lines.append(f"head(3).collect(): {_ms(start):.2f} ms")
    lines.append("nothing above needed the write side: lazy reads stand alone")
    return lines


def wart_demo(work: Path) -> list[str]:
    """Reproduces the missing-file wart against the mechanism dagster uses.

    `UPathIOManager._load_partition_from_path` (upath_io_manager.py:355 in
    dagster 1.13.16) wraps `load_from_path` in try/except FileNotFoundError,
    and `allow_missing_partitions` is that except returning None. The demo
    plays a lazy and a guarded `load_from_path` through the same shape.
    """
    missing = work / "no-such-partition.parquet"
    missing.unlink(missing_ok=True)
    lines: list[str] = []

    try:
        poisoned = pl.scan_parquet(missing)
        lines.append(
            "scan_parquet(missing): returns a LazyFrame, nothing raised at plan time"
        )
    except FileNotFoundError:
        lines.append(
            "scan_parquet(missing): raised at plan time, so the wart is moot on this polars"
        )
        poisoned = None

    if poisoned is not None:
        # A lazy load_from_path returns that scan, so the manager's except
        # clause never fires and the missing partition looks present.
        lines.append(
            "manager's try/except (upath_io_manager.py:355): saw no error, missing partition looks present"
        )
        try:
            poisoned.collect()
            lines.append("collect(): succeeded, which contradicts the banked finding")
        except Exception as error:  # noqa: BLE001 - the demo's job is to report whatever fires
            kind = type(error).__name__
            where = (
                "a FileNotFoundError, but the manager is long gone"
                if isinstance(error, FileNotFoundError)
                else "not even a FileNotFoundError, so the except would miss it anyway"
            )
            lines.append(f"collect() outside the manager: {kind} fires here ({where})")

    def guarded_load_from_path(path: Path) -> pl.LazyFrame:
        # The workaround: surface the miss where the manager can catch it.
        if not path.exists():
            raise FileNotFoundError(path)
        return pl.scan_parquet(path)

    try:
        guarded_load_from_path(missing)
        lines.append(
            "workaround: exists() guard failed to raise, which should not happen"
        )
    except FileNotFoundError:
        lines.append(
            "workaround: exists() guard raises at load time, so allow_missing_partitions skips the partition again"
        )
    return lines
