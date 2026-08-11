# PROTOTYPE: issue #27, LazyFrame end to end

Throwaway code answering one question.
It never ships; when the question is answered it moves to a throwaway branch.
Its scratch space is a `PROTOTYPE-issue27-wipe-me` directory under the system temp dir; the TUI header shows the exact path.
Wipe it whenever.

## The question

[#27](https://github.com/ozanozbeker/dagster-dataframely/issues/27): can validation and storage stay lazy end to end, and which execution strategy should the write path use at a frame size where the differences matter?

The issue's banked findings already settle how many times each shape executes the upstream plan, on a toy frame.
What they leave open is wall time and peak memory at realistic size, what per-rule counts cost on a lazy path, whether quarantine can stay lazy, and where `dagster/row_count` comes from.
This workbench measures exactly those, on the pinned versions (polars 1.43.2, dataframely 3.0.0, dagster 1.13.16).

## Run it

```sh
uv run python prototypes/issue-27-lazyframe/tui.py
```

Keys: `s` cycles rows (100k, 1M, 10M), `f` cycles failure percent (0, 0.1, 1, 10, 50), `u` toggles upstream cost (cheap scan vs 64 hash rounds), `k` toggles the schema's primary key, `0`..
`4` run one strategy, `a` races all five, `r` runs the lazy-read demo, `w` runs the missing-file wart demo, `x` clears, `q` quits.

Every measurement runs in a fresh child process (`runner.py`), because `ru_maxrss` is a process-lifetime high-water mark and a long-lived process cannot compare memory between runs.

## The strategies

| key | strategy      | upstream runs      | what it models                                                                                                                   |
| --- | ------------- | ------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| `0` | `eager`       | 1                  | v1 today: collect the plan, filter eagerly. The silent materialization.                                                          |
| `1` | `naive`       | 2                  | Sink the good half, then collect the failure half for counts. Banked finding 2.                                                  |
| `2` | `collect_all` | 1                  | `LazyFilterResult.collect_all()`, then everything eager. Banked finding 3, and the live check on polars#24129.                   |
| `3` | `temp_land`   | 1 + local re-reads | Sink the plan to local temp once, stream both halves and the counts off files. Banked finding 5.                                 |
| `4` | `sink_only`   | 1                  | The control: the same plan sunk with no validation. The streaming engine's own appetite, and the floor any lazy write path pays. |

## The `k` knob, and why it may be the whole answer

`k` swaps `Reading` for `ReadingNoKey`, which is the same schema with `primary_key=True` dropped.
Uniqueness is a global question, so the engine cannot answer it from one streaming batch; every other rule in the schema is row-local and streams fine.

That makes `k` the knob that decides whether "lazy end to end" is reachable at all, independent of which strategy sinks the data.
Measured on the same 10M-row streaming sink, no strategy involved:

| schema | sink_good | RSS delta |
| ------ | --------- | --------- |
| `pk`   | 0.15s     | 1370 MB   |
| `nopk` | 0.06s     | 608 MB    |

If that gap holds across the strategies, the ticket's question has a different shape than it assumed: the ceiling on laziness is the schema's rules, not the storage layer, and #27's answer has to say which rules a user can keep and still get streaming.

## First measurements

Taken while validating the harness, not driven by hand. 10,000,000 rows, `fail 10%`, upstream `heavy`, one M-series laptop, one run each.
Treat them as a starting point to reproduce, not as the verdict.

| strategy      | pk total | pk RSS  | nopk total | nopk RSS |
| ------------- | -------- | ------- | ---------- | -------- |
| `sink_only`   | 0.87s    | 708 MB  | 0.87s      | 699 MB   |
| `eager`       | 1.06s    | 2086 MB | 0.89s      | 1551 MB  |
| `temp_land`   | 1.26s    | 1770 MB | 1.09s      | 1130 MB  |
| `collect_all` | 10.68s   | 1861 MB | 1.05s      | 1550 MB  |
| `naive`       | 10.93s   | 1777 MB | 1.61s      | 1310 MB  |

Three things fall out, each of which the ticket should confirm by hand:

1. **The primary key, not the strategy, is what blows up the lazy paths.** `collect_all` costs 10.68s with the key and 1.05s without.
   Banked finding 3 holds only in the `nopk` column; a `primary_key` rule appears to defeat the shared-subplan elimination it depends on.
   `pl.explain_all` does emit two `CACHE` nodes either way, so the cache is present and not helping.
2. **`temp_land` is the only strategy that is fast in both regimes** and has the lowest memory of the four that validate.
   That is banked finding 5's prediction, holding at scale.
3. **No strategy reaches the `sink_only` floor.**
   Even `nopk temp_land` sits 431 MB above it, so "lazy end to end" buys less than the phrase suggests.

## What to look for

- Race all five (`a`) at 10,000,000 rows with upstream `heavy`, then press `k` and race again.
  The pair is the headline measurement.
- Read memory deltas against the `sink_only` row, not against zero.
  A bare streaming sink measured a couple hundred MB of engine buffers on its own, so only the spread above the control belongs to a strategy.
- Push fail% to 50 and watch the `counts` phase and quarantine bytes: the
  cost of per-rule metadata when the failure half is worst-case.
- `temp_land`'s landed bytes is the ephemeral-disk cost a k8s pod would pay.
  A laptop cannot price a small ephemeral disk; the byte count and the `land` phase time are the proxy this prototype can give.
- `w` reproduces the wart: `UPathIOManager._load_partition_from_path` (upath_io_manager.py:355 in dagster 1.13.16) implements `allow_missing_partitions` by catching `FileNotFoundError` from `load_from_path`.
  A lazy load raises nothing there, so the miss detonates at the user's `collect()` instead.
  The demo also confirms the workaround: an `exists()` guard that raises where the manager can catch.
- `r` shows the read half stands alone: scan, schema, footer row count, and
  a head all work off the written file with no help from the write side.

## What the code surfaces without measuring

- A lazy quarantine in the details shape needs dataframely privates (`FailureInfo._lf` and `_rule_columns`); the public lazy surface is `FailureInfo.sink_parquet`, which writes boolean rule columns instead.
  See `logic.lazy_quarantine`.
  The package would pin those privates the way `_naming` already pins two others, or ask dataframely for a lazy `details()`.
- Per-rule counts can come off the written quarantine file rather than the plan (`logic.counts_from_parquet`).
  That is what frees the failure half from ever being collected.
- `dagster/row_count` on a lazy path comes from the parquet footer
  (`logic.footer_row_count`); the `rc` column shows what each strategy used.

## Limits

- One local disk.
  Cloud sinking rides the same fsspec handle the manager already uses (banked finding 4) and is not re-proven here.
- Engines as shipped: sinks use the streaming engine, collects the in-memory engine.
  If `collect_all` disappoints, a `collect_all(engine="streaming")` variant is a one-line edit in `logic.run_collect_all`.

## Capture

**The verdict is in.**
It lives on main, in [`docs/research/lazyframe-end-to-end.md`](https://github.com/ozanozbeker/dagster-dataframely/blob/main/docs/research/lazyframe-end-to-end.md), which this prototype is the primary source for.
The link is absolute rather than relative because this branch is never merged, so the file it points at does not exist here.

The short version: storage can stay lazy, validation cannot, and validation is what the package sells.
Ship lazy reads on their own, ship annotation-driven temp landing for writes, build no memory guard.

[#27](https://github.com/ozanozbeker/dagster-dataframely/issues/27) is closed with the verdict, and this branch is the parking spot.
Main keeps only the decision, which is the research doc.

The follow-on work is [#52](https://github.com/ozanozbeker/dagster-dataframely/issues/52) (lazy reads), [#53](https://github.com/ozanozbeker/dagster-dataframely/issues/53) (temp landing) and [#54](https://github.com/ozanozbeker/dagster-dataframely/issues/54) (sink and promote, blocked by #53).

Two things here are worth lifting if #53 or #54 get built: `logic.counts_from_parquet` and `logic.footer_row_count`, both small and already correct.
`logic.lazy_quarantine` is **not**, because it reaches into two dataframely privates.
Neither is the TUI.

Re-run this whenever a claim in the research doc needs rechecking.
Its uncertainty ledger flags two: the `collect_all` magnitude could move on a future polars, and every number came from one laptop.
