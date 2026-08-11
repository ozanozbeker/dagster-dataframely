# Partitioned assets: what actually happens

Observation for [#25](https://github.com/ozanozbeker/dagster-dataframely/issues/25).
Verified against the installed `dagster 1.13.16`, `dataframely 3.0.0`, `polars 1.43.2`.

Every claim is tagged **[RAN]** (executed and read off the result) or **[READ]** (traced through library source).
The durable half of this document is [`tests/test_partitions.py`](../../tests/test_partitions.py): every claim below is pinned there, including the two that are Dagster's behaviour rather than this package's, so a release that changes either one fails a test rather than leaving this page quietly wrong.
Scratch scripts lived in `/tmp/dd25/`.

---

## Answer

Partitioning works, and it needed no code.
Everything the spec predicted holds: the state machine runs per partition on that partition's frame, `dagster/row_count` is that partition's good count, a drifting partition aborts at the gate, and the quarantine cannot escape its asset's partitioning.

One finding is worth acting on later.
**Asset checks are not partitioned.**
A check accumulates one timeline for the whole asset, so the last partition to run is the catalog's latest word on every rule, and a partition that failed at `WARN` is invisible behind a later clean one.
On a time-window partitions definition it is worse: every run also leaves behind a planned check row that never resolves.

Two smaller findings: a single-run backfill policy cannot reach storage through the package's IO manager, and a fan-in over every partition arrives as a `dict`.

| what §10 of the spec claims | verdict |
| --- | --- |
| `partitions_def` forwards to the `multi_asset` verbatim | holds **[RAN]** |
| every out is partitioned identically; the quarantine cannot escape | holds, and it is structural **[RAN]** |
| the state machine runs per partition, on that partition's frame | holds **[RAN]** |
| `dagster/row_count` is the partition's good count | holds **[RAN]** |
| a drifting partition aborts at the gate before any row check reports | holds **[RAN]** |
| per-partition check evaluations are *observed, not designed around* | observed, and they are not per-partition at all **[RAN]** |

---

## 1. What works, and why it needed no code

**Per-partition execution and storage.** **[RAN]** A partitioned door asset materializes one file per partition under the asset's own directory, and the quarantine sits beside it under the same key:

```text
orders/clean.parquet
orders/mixed.parquet
orders_quarantine/mixed.parquet
```

None of that is this package's code.
`UPathIOManager.handle_output` resolves the partition path before it calls `dump_to_path`, so the manager's hooks are partition-blind by design.
It works by inheritance, which is why `tests/test_parquet_io_manager.py` now pins it.

**`dagster/row_count` is the partition's count.** **[RAN]** The `mixed` partition holds six rows, three of which survive.
Its materialization reports `3`, and the quarantine's reports `3`.
Nothing sums across partitions, so `dg.build_metadata_bounds_checks` trends a partition against itself with no knob from this package.

**The quarantine cannot escape.** **[RAN]** Both outs report the asset's `partitions_def`, and `dg.AssetOut` has no `partitions_def` parameter at all.
The guarantee is therefore structural rather than enforced: there is no spelling in which a partitioned asset produces an unpartitioned pile of bad rows.

**A drifting partition is contained.** **[RAN]** The `wrong` partition fails the gate, evaluates no rule check, writes neither out, and leaves the `clean` partition's file untouched.
One partition is one run, so a pipeline defect in today's data cannot reach yesterday's table.

**The transform reaches its own partition key.** **[RAN]** The door's transform takes no `context` parameter, so a partitioned one reaches the key the way the wrapper reaches the context:

```python
@dataframely_asset(schema=Orders, partitions_def=daily)
def orders() -> pl.DataFrame:
    day = dg.AssetExecutionContext.get().partition_key
    return read_day(day)
```

---

## 2. Finding: check evaluations name no partition

`dg.AssetCheckResult` has no partition field, and Dagster fills one in for the evaluation only when the check's own spec carries a `partitions_def` **[READ]**:

```python
# AssetCheckResult.to_asset_check_evaluation
evaluation_partition = (
    step_context.partition_key
    if step_context.has_partition_key and check_spec.partitions_def is not None
    else None
)
```

This package builds every spec without one, so every evaluation on a partitioned asset carries `partition=None` **[RAN]**.
The result is one timeline per `(asset, check)` pair rather than one per partition.

Backfilling two partitions, `mixed` first and `clean` second, leaves this history for `dy_rule__amount__min` **[RAN]**:

| row | status | partition | passed | severity |
| --- | --- | --- | --- | --- |
| newest | `SUCCEEDED` | `None` | `True` | `WARN` |
| oldest | `FAILED` | `None` | `False` | `WARN` |

What a reader gets from the catalog is the newest row: the rule passed.
The partition that failed is one row down, and nothing in that row says which partition it was.

Three consequences, in the order a user will hit them:

1. **Order decides the story.**
   The last partition to finish owns every rule's latest state.
   A backfill that ends on a clean partition erases the visible trace of a dirty one, and a backfill that ends on a dirty partition marks the rule failed for an asset that is mostly fine.
2. **`WARN` is per run, not per partition.**
   The quarantine still lands, per partition, with full per-row attribution, so the data is never lost.
   Only the check surface is coarse.
3. **Attribution is possible, by hand.** `target_materialization_data` *is* scoped to the step's partition, deliberately, so that a concurrent run of another partition cannot become the target **[READ]**.
   A history row therefore points at exactly one materialization, and that materialization knows its partition key **[RAN]**.
   Getting from a red check to a partition means resolving that storage id yourself.

---

## 3. Finding: a time-window partition orphans the planned check row

Static partitions produce one history row per run.
Daily partitions produce two, and one of them never resolves **[RAN]**:

| partitions definition | rows after one run |
| --- | --- |
| `StaticPartitionsDefinition` | `SUCCEEDED`, partition `None` |
| `DailyPartitionsDefinition` | `SUCCEEDED`, partition `None`, plus `PLANNED`, partition `2026-01-01` |

The mechanism is a mismatch between the two halves of one row **[READ]**:

- Dagster writes a planned row per check when the run starts, keyed by the partitions subset the run carries.
  A time-window run carries a subset; a static one carries `None` **[RAN]**, so only the time-window case stamps a partition onto the planned row.
- The evaluation arrives with `partition=None`, per finding 2.
- `_update_asset_check_evaluation` closes the planned row by matching `(asset_key, check_name, run_id, partition)`.
  The partition clause misses, no row updates, and the result is inserted as a second row.

So the planned row is stranded in `PLANNED` for good.
`get_asset_check_partition_info` then reports every partition as never-executed, with the real result on a partition-less row beside them **[RAN]**:

```text
[(None, 'SUCCEEDED'), ('2026-01-01', 'PLANNED'), ('2026-01-02', 'PLANNED')]
```

A planned row whose run has finished resolves to `SKIPPED`, described upstream as "the run finished, didn't fail, but the check didn't execute".
That is what a per-partition check view reads off.

None of this is the package's doing: a stock `@dg.asset` with a `check_spec` and a daily partitions definition reproduces it exactly **[RAN]**.
The insert branch also fires a `DeprecationWarning` from `datetime.utcfromtimestamp` inside Dagster's SQL event log, which is a second reason to watch this path.

---

## 4. Finding: a single-run backfill cannot reach storage

`backfill_policy` forwards like every other `multi_asset` parameter, but `dg.BackfillPolicy.single_run()` never gets as far as a write **[RAN]**:

```text
The current IO manager <class 'dagster_dataframely.io_managers._ParquetIOManager'> does not
support persisting an output associated with multiple partitions. This error is likely
occurring because a backfill was launched using the 'single run' option. Instead, launch the
backfill with a multi-run backfill policy.
```

`UPathIOManager` resolves one path per output and refuses a range.
The refusal is upstream's, it names the fix, and it arrives on the first run rather than after a wrong write, so the door leaves it alone: an asset cannot know which IO manager it will be bound to, which is the same line `_ParquetIOManager` already draws for unwritable dtypes.

---

## 5. If the behaviour turns out to be wrong: what the fix looks like

`dg.AssetCheckSpec` accepts a `partitions_def`, and setting it to the asset's own fixes findings 2 and 3 together **[RAN]**.
The same two-partition backfill then records:

| row | status | partition | passed |
| --- | --- | --- | --- |
| newest | `SUCCEEDED` | `clean` | `True` |
| oldest | `FAILED` | `mixed` | `False` |

`get_asset_check_partition_info` reports `[('clean', 'SUCCEEDED'), ('mixed', 'FAILED')]`, and no planned row is orphaned, because both halves of the row now agree on the partition.

It is not taken in v1 for one reason: constructing that spec emits

> Specifying a partitions_def on an AssetCheckSpec is currently in preview, and may have breaking changes in patch version

and this package pins `dagster>=1.13.16` with no upper bound.
Adopting it would key every check's history off a surface that can change in a patch release, and a change in how those rows are keyed is exactly the kind that orphans the history it was adopted to fix.
The spec's own instruction is to observe rather than design around, so this is written down and left alone.

---

## 6. The two decisions banked from #18

**The fan-in shape stays documented, not exported.** **[RAN]** An unpartitioned asset depending on every partition of a partitioned one receives one frame per partition, because the base manager calls `load_from_path` once per key and assembles the results:

```python
@dg.asset
def rollup(orders: dict[str, pl.DataFrame]) -> None: ...
```

The obvious annotation, `orders: pl.DataFrame`, fails Dagster's type check after the frames have already been read.
Ecosystem prior art exports `DataFramePartitions` for this, and this package does not, for now.
The alias would be public surface, and its lazy twin (`dict[str, pl.LazyFrame]`) depends on [#27](https://github.com/ozanozbeker/dagster-dataframely/issues/27), which is unresolved: naming one before the other is decided fixes half a pair.
[#26](https://github.com/ozanozbeker/dagster-dataframely/issues/26) owns the public surface and the README, and should carry the annotation as documentation there.
Both shapes are pinned in `tests/test_parquet_io_manager.py`, so the decision can be revisited against evidence rather than memory.

> **Reversed by [#35](https://github.com/ozanozbeker/dagster-dataframely/issues/35).**
> The alias ships as `dd.DataFramePartitions`, under the prior art's name.
> What was deferred was the *pair*, and taking the prior art's pair is what decides it without waiting: `LazyFramePartitions` is then the only name the lazy half could take, so [#27](https://github.com/ozanozbeker/dagster-dataframely/issues/27) inherits a naming decision rather than making one.
> Only the half a read can currently return is exported.
> Nothing upstream reserves the other.
> It is a plain assignment and not a `type` statement, because Dagster resolves annotations at runtime and rejects the `TypeAliasType` a PEP 695 alias produces.
> That refusal is pinned in `tests/test_upstream_pins.py`, so the modern spelling becomes available the moment upstream unwraps it.

**The IO manager carries a partitioned round-trip test.** `load_from_path` and `dump_to_path` mention no partitions and need to mention none, so the layout is inherited rather than written.
That is exactly what makes it worth a test: nothing in this repo would notice if the base class stopped resolving the partition path.

---

## 7. Candidate follow-ups

Recorded, not fixed here.

1. **Partition-scoped check history, once `AssetCheckSpec(partitions_def=)` leaves preview.**
   Findings 2 and 3 both close.
   The blocker is the preview warning, not the mechanics, so the trigger is an upstream release note rather than more research.
   *Tracked as [#31](https://github.com/ozanozbeker/dagster-dataframely/issues/31).*
2. **Decide whether the orphaned planned row is worth an upstream issue.**
   It is Dagster's, it reproduces on a stock `@dg.asset` carrying one check spec, and its effect is a per-partition check view that reads never-executed on an asset whose checks all ran.
   *Tracked as [#32](https://github.com/ozanozbeker/dagster-dataframely/issues/32).*
3. **Document the single-run backfill refusal in the README.**
   Belongs to [#26](https://github.com/ozanozbeker/dagster-dataframely/issues/26), beside the sentence about the IO managers being the supported path.
   *Tracked as [#33](https://github.com/ozanozbeker/dagster-dataframely/issues/33), which outlived #26; the README's Partitioning section is now its home.*
4. **Document how a partitioned transform reaches its partition key.** `dg.AssetExecutionContext.get()` is the answer, the door's docstring does not say so, and it is the first thing a partitioned user needs.
   Also [#26](https://github.com/ozanozbeker/dagster-dataframely/issues/26).
   *Done in [#34](https://github.com/ozanozbeker/dagster-dataframely/issues/34): the README's Partitioning section and the door's `partitions_def` docstring.*
5. **Export a partitions type alias for the fan-in shape.**
   Deliberately deferred above; revisit with [#27](https://github.com/ozanozbeker/dagster-dataframely/issues/27) so the eager and lazy names are decided together.
   *Done in [#35](https://github.com/ozanozbeker/dagster-dataframely/issues/35): both names decided, the eager one exported.*
   *See the note in §6.*
