# PROTOTYPE: quarantine sink

Throwaway.
Belongs on a scratch branch, never on `main`.

```sh
uv run --group duckdb python prototypes/quarantine_sink/tui.py
```

`[m]` manager, `[p]` partitioned, `[e]` exit, `[x]` partition_expr, `[r]` run one, `[a]` run all twelve.

## The question

Can a `dy_asset` hand its invalid rows to whatever IO manager the asset is already bound to, with no code that knows which manager that is, so the quarantine lands natively beside the valid table?
And does it still land when the run aborts?

## The answer: yes, both

Twelve of twelve combinations green: two managers, partitioned and not, across all three exits that reject rows.

| manager | partitioned | exit | where the quarantine landed |
| --- | --- | --- | --- |
| parquet | no | partial | `wh/analytics/orders_quarantine.parquet` |
| parquet | no | abort | `wh/analytics/orders_quarantine.parquet`, valid table absent |
| parquet | no | nothing survived | `wh/analytics/orders_quarantine.parquet`, valid table absent |
| parquet | yes | partial | `wh/analytics/orders_quarantine/2026-01-02.parquet` |
| duckdb | no | partial | `analytics.orders_quarantine` |
| duckdb | yes | partial | `analytics.orders_quarantine` |

The abort rows land.
ADR-0004 promises the file survives a run that dies, and delegation keeps that promise, because we call `handle_output` ourselves inside the asset body before raising.

## What made it work

The blocker was `resource_config`. `DbIOManager` backends read the database and connection settings off the output context at write time rather than off themselves, so a hand-built context needs per-manager knowledge, which is the thing this design exists to avoid.

The fix is to stop building a context and start **borrowing** one.
The step's real `OutputContext` already carries the correct `resource_config`, `dagster_type` and `definition_metadata`.
Clone it, re-point it at the quarantine's asset key, hand it to the manager.
Copying fields is not interpreting them, which is what keeps the sink manager-blind.

Two dead ends, recorded so nobody retries them:

- `context.resources.io_manager` gives the **built** manager (`DbIOManager`), not the pydantic factory, so its config is not reachable that way.
- `context.resources.original_resource_dict` gives the same built object, and `context.run.run_config` is empty when resources are passed as objects rather than config.

## What this costs

- **Private Dagster API**: `context.get_step_execution_context()`, `StepOutputHandle`, `step.get_output_context`.
  House convention says pin these with characterization tests.
- **Partitioned database assets need `partition_expr`**, or `DbIOManager` raises.
  Not our limitation: the user must already declare it for their own partitioned table, and the sink forwards it automatically because it copies definition metadata.
  Toggle `[x]` to watch it fail without.
- **Metadata the manager emits is dropped.**
  It calls `add_output_metadata` on a context that is not a real output, so `path` and `Query` go nowhere unless we capture them.
- **Breaks ADR-0001/0002/0004 deliberately.**
  This reads `context.resources.io_manager`, which ADR-0004 rejected because direct invocation supplies no resources.
  Direct invocation needs a manager passed in, or a fall back to the file root.

## What lifts into the real code

`sink.py` only. `quarantine_key`, `borrowed_output_context` and `write_quarantine` are pure and portable. `tui.py` is the shell and stays here.
