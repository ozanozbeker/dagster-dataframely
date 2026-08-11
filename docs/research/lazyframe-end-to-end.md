# LazyFrame support: where laziness actually ends

Answers [#27](https://github.com/ozanozbeker/dagster-dataframely/issues/27).
Verified against the installed `dagster 1.13.16`, `dataframely 3.0.0`, `polars 1.43.2`.

Every claim is tagged **[RAN]** (executed and read off the result) or **[READ]** (traced through library source).
Measurements marked **[RAN]** come from a throwaway workbench that races five write strategies in a fresh child process each and reports wall time, peak RSS, per-rule counts and row-count provenance.
It is kept off main, as a primary source, on the [`prototype/issue-27-lazyframe`](https://github.com/ozanozbeker/dagster-dataframely/tree/prototype/issue-27-lazyframe) branch.
One M-series laptop, one run each, so treat a 10% difference as noise and a 10x difference as real.

---

## Answer

**Storage can stay lazy.
Validation cannot, and validation is what this package sells.**

The ticket asked which execution strategy the write path should use.
The measurements say the strategy barely matters, and two things upstream of it decide everything: whether the schema has a `primary_key`, and the fact that `dy.FailureInfo` is eager by construction.

The recommendation is to stop chasing streaming end to end and build the shape that actually banks the available win:

1. **Ship lazy reads now, on their own.** `load_from_path` returning a scan is independent of every finding below, costs about ten lines, and gives downstream assets predicate and projection pushdown into the file.
2. **Ship annotation-driven temp landing for writes.**
   A `pl.LazyFrame` return streams the transform's plan to a local parquet, then the manager reads it back eagerly for checks, statistics and the final write.
   A `pl.DataFrame` return keeps today's path, because a frame the user already materialized has nothing left to stream.
3. **Build no memory guard.**
   Considered and declined; see §7.

What this buys is narrower than the ticket assumed, and worth stating plainly.
Peak memory drops from *the whole plan's peak* to `max(streaming buffers, final frame)`.
For a transform with an exploding intermediate that is a real saving.
For a straight scan-and-project transform it is zero, which is why the annotation has to gate it.

| what the ticket assumed | what the measurements say |
| --- | --- |
| the choice is between execution strategies | the choice is dominated by the schema's rules **[RAN]** |
| `collect_all` eliminates the shared subplan (banked finding 3) | true only without a `primary_key`; with one it costs 10x **[RAN]** |
| a lazy path streams to the final destination | it cannot: four separate reporting duties collect **[READ]** |
| per-rule counts are the open question | counts are solvable; `FailureInfo` being eager is not **[READ]** |

---

## 1. The strategy race

Five strategies, same pipeline, 10,000,000 rows, 10% failures, expensive upstream. **[RAN]**

| strategy | with `primary_key` | | without `primary_key` | |
| --- | --- | --- | --- | --- |
| | total | peak RSS | total | peak RSS |
| `sink_only` (control, no validation) | 0.87s | 708 MB | 0.87s | 699 MB |
| `eager` (today's path) | 1.06s | 2086 MB | 0.89s | 1551 MB |
| `temp_land` | 1.26s | 1770 MB | 1.09s | 1130 MB |
| `collect_all` | 10.68s | 1861 MB | 1.05s | 1550 MB |
| `naive` (sink good, collect failures) | 10.93s | 1777 MB | 1.61s | 1310 MB |

Read the memory column against `sink_only`, never against zero.
A bare streaming sink wants ~700 MB of engine buffers at this size on its own, so only the spread above that control belongs to a strategy.

Three readings:

- **No strategy reaches the control.**
  The best lazy candidate still sits 431 MB above a sink that does no validation.
  Laziness buys less than the phrase suggests.
- **`temp_land` is the only strategy that is fast in both regimes**, and it is the lightest of the four that validate.
  That is banked finding 5 holding at scale.
- **The eager path is already fast.**
  At 1.06s it beats every lazy candidate.
  The case for changing it is memory, not time, and it is worth 316 MB against `temp_land`, not the order of magnitude the ticket implied.

---

## 2. The primary key is the dominant variable

`collect_all` costs 10.68s with a `primary_key` rule and 1.05s without, with nothing else in the run changed. **[RAN]**

Uniqueness is a global question.
The engine cannot answer it from one streaming morsel, so the rule forces the whole frame through a blocking operation.
Measured on a bare streaming sink with no strategy wrapped around it, the same schema with and without the key: **[RAN]**

| schema | sink | peak RSS |
| --- | --- | --- |
| with `primary_key` | 0.15s | 1370 MB |
| without | 0.06s | 608 MB |

The key more than doubles memory on a path that is otherwise pure streaming.

This revises banked finding 3 rather than contradicting it.
`collect_all` does eliminate the shared subplan, and `pl.explain_all` emits two `CACHE` nodes either way **[RAN]**, so the cache is present.
**[RAN]** It simply stops helping once a global rule sits on top of it: against a bare collect of the same plan, `collect_all` costs 6.4x at cheap upstream and 14.4x at expensive upstream.

The practical consequence is that the ticket's question cannot be answered per-package.
It is answered per-schema, by the user, and almost every useful schema in this package's audience has a primary key.

---

## 3. The reporting contract forces materialization

This package does not promise to write a file.
It promises to write a file and report on it, and four of those reporting duties collect.

**`dy.FailureInfo` is eager by construction, and this is the binding constraint. [READ]** `_df` is a `cached_property` that calls `self._lf.collect()` (`filter_result.py:108`).
`counts()`, `cooccurrence_counts()`, `invalid()`, `details()` and `__len__` all route through it.
`sink_parquet` is the only method that touches `_lf`.
So the moment `process()` asks for per-rule check metadata, which is the package's core value, the failure half is in memory.
No strategy on our side changes that.

**`statistics_metadata` runs two global aggregates. [READ]** `p50` is a median (`_statistics.py:87`) and the string family computes `n_unique` (`_statistics.py:128`).
**[RAN]** Measured at 10M rows, those two alone cost 771 MB, against 381 MB for every streamable aggregate in the same pass.

The `statistics` knob already exists, so a user can turn this off, but it is on by default and it is a headline feature.

Two duties are cheap and are not the problem.
`dagster/row_count` comes off the parquet footer for free, and `sample_rows` is a bounded `head`.

---

## 4. The state machine cannot choose an exit lazily

`_runtime.py:236` reads:

```python
rejected: int = len(failure)
aborting = bool(rejected) and (quarantine_out is None or not len(good))
```

Choosing among the five exits requires counting both halves. **[READ]**

On a lazy path, counting means executing.
So the two exits whose entire purpose is that *nothing is written* would have to execute the whole plan before they could learn to take that route.

Sinking first and counting after is the only way to avoid a second execution, and it writes to the good output's final path before discovering it should have aborted.
That breaks the invariant `NothingSurvivedError` is built on, stated in the same file: the good out is skipped so that an empty table cannot replace a last-known-good snapshot.

Recovering the invariant means sinking to a temp path and promoting on success.
That is `temp_land` plus a rename, which is how the recommendation in §8 arrives at temp landing from a second, independent direction.

---

## 5. Lazy reads stand alone

The read half needs nothing from the write half, measured against a written parquet: `scan_parquet` 0.02 ms, `collect_schema` 0.13 ms, footer row count 1.02 ms, `head(3).collect()` 1.51 ms. **[RAN]**

None of §2 through §4 applies, because a read performs no validation.
This is the cheapest genuine win in the ticket and it should not wait on the rest.

`_io_managers.py:35` already reserves the `LazyFramePartitions` name for the fan-in shape, decided alongside the eager name in [#35](https://github.com/ozanozbeker/dagster-dataframely/issues/35).

---

## 6. The missing-file wart, reproduced and worked around

`UPathIOManager._load_partition_from_path` implements `allow_missing_partitions` by catching `FileNotFoundError` raised by `load_from_path` (`upath_io_manager.py:355`). **[READ]**

A lazy `load_from_path` defeats it. **[RAN]**

`pl.scan_parquet` on a missing file returns a `LazyFrame` and raises nothing, so the manager's `except` never fires and a missing partition reads as present.
The `FileNotFoundError` does arrive, at the user's `collect()`, long after the manager that would have handled it has returned.

The workaround is one line and it is confirmed: guard `load_from_path` with an `exists()` check that raises where the manager can still catch it. **[RAN]**

The cost is one stat call per partition, which is the same call `UPathIOManager` already makes to build the path.

---

## 7. The memory guard, considered and declined

A pre-flight refusal was considered: estimate the frame's in-memory size before reading the landed parquet, compare against available memory, raise a custom error rather than dying.

**Declined.**
Out-of-memory is the platform's problem, solved by pod sizing, partitioning or a bigger machine.
A validation library that refuses to read data because it did arithmetic on the host's RAM is solving someone else's problem badly, and doing it with machinery whose only job is to fail more gracefully.

Recorded because the investigation produced facts worth not re-deriving:

- **A guard cannot rely on catching the failure.**
  Python `MemoryError` is catchable but rare here, because polars allocates in Rust.
  A Rust allocation failure aborts the process.
  In Kubernetes, OOMKilled is `SIGKILL`: no exception, no traceback, no custom error.
  Any guard has to be pre-flight, and pre-flight only.
- **Host memory is the wrong number in a container.** `psutil.virtual_memory()` and `/proc/meminfo` report the node, not the pod's cgroup limit.
  A pod capped at 4 GB on a 64 GB node reads 64 GB and gets OOMKilled anyway.
  The correct read is `/sys/fs/cgroup/memory.max` (cgroup v2) or `memory/memory.limit_in_bytes` (v1).
  `psutil` is not currently a dependency.
- **File size does not predict in-memory size.** **[RAN]** Measured in-memory to on-disk ratios: 15.9x for low-cardinality strings, 7.2x for high-cardinality strings, 3.5x for floats.
  A guard on file size needs a 16x safety factor to be safe and would then refuse frames that fit four times over.
- **Sampling does predict it.**
  Row count from the footer, a bounded 50,000-row head, scale `estimated_size() / len()` by the row count.
  **[RAN]** Errors of 0.0%, -15.8%, 0.0% and -6.8% across the same shapes plus the prototype's own 10M source.
  Every error was an under-prediction, because a head sample is unrepresentative when row size correlates with position; a random sample fixes it and costs more.

If this is ever revisited, the last two bullets are the design.

---

## 8. What to build

Two changes, independent, in this order.

**A.
Lazy reads.** `load_from_path` returns `pl.scan_parquet` when the input annotation is `pl.LazyFrame`, guarded by the `exists()` check from §6.
Export `LazyFramePartitions` alongside `DataFramePartitions`.
No validation implications, no interaction with anything below.

**B.
Annotation-driven temp landing for writes.**
A `pl.LazyFrame` return sinks the transform's plan to a local parquet with the streaming engine.
The manager then reads that file back eagerly and everything downstream, the gate, the filter, the checks, the statistics and the final write, is unchanged.
A `pl.DataFrame` return keeps today's path, because a materialized frame has nothing left to stream.

The annotation is the opt-in, and it has to be, because temp landing is not free.
It costs one extra local write and read of the whole frame, and ephemeral disk equal to the compressed frame (118 MB for the 10M-row case **[RAN]**).
On a cheap transform that is pure cost.
On a transform with a large intermediate it is the whole point.

This is banked finding 6's annotation-driven dispatch, which survives intact.
What does not survive is streaming to the final destination, and the IO manager docstrings currently imply #27 will deliver one.
Both need their `LazyFrame` paragraph rewritten to say what actually ships.

---

## 9. Candidate follow-ups

Recorded, not fixed here.

1. **Ask dataframely for a lazy `FailureInfo`.**
   A `counts()` that answers without collecting is the single change that would reopen streaming end to end.
   It is upstream of this package and belongs in their tracker, not ours.
2. **Decide whether `temp_land`'s location is configurable.**
   The prototype used the system temp dir.
   A k8s pod with a small ephemeral disk is the case that decides this, and a laptop cannot measure it.
3. **Reconsider `p50` and `n_unique` in the statistics pass.**
   They are the two global aggregates in an otherwise streamable profile.
   Whether they earn their cost is a separate question from #27 and worth asking on its own.

---

## Uncertainty ledger

Verified by running code or reading installed source:

- Everything tagged **[RAN]** or **[READ]** above.
- The prototype is the primary source for every measurement, and it is re-runnable: see the branch named at the top of this document.
  Its README carries the run command and the knobs.

Inferred, not verified:

- **That the measurements transfer off a laptop.**
  Every number came from one M-series machine with fast local NVMe and ample RAM.
  The relative ordering should hold; the absolute figures will not, and the `temp_land` landing cost in particular is disk-bound and will look different on network-backed ephemeral storage.
- **That `collect_all`'s behaviour is stable.** dataframely's own docstring warns the advantage is limited until [pola-rs/polars#24129](https://github.com/pola-rs/polars/pull/24129) ships.
  The §2 finding says a `primary_key` defeats it regardless, but a future polars could change the magnitude.

Not investigated:

- Cloud sinking.
  Banked finding 4 established `sink_parquet` accepts an `IO[bytes]` through a `UPath` handle, and the recommendation in §8 lands locally then writes through the existing path, so nothing new was needed.
- Whether `dy.Collection` changes any of this.
  Every measurement used a single `dy.Schema`.
- The CSV manager.
  Everything above assumes parquet; CSV cannot stream a landed intermediate the same way, and its `_check_declaration` gate is unaffected either way.
