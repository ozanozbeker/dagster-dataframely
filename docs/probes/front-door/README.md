# Front-door probe (wayfinder [#11](https://github.com/ozanozbeker/dagster-dataframely/issues/11))

**Kept, not maintained.**
Like [`../ui-surfaces/`](../ui-surfaces/), this probe is a deliberate exception to the map's *prototypes are throwaway* rule: it is the executable evidence behind the front-door decision, so the claims can be re-run rather than re-argued.
It is not package code and does not track the spec — when `src/` exists, that is the source of truth.

It answers *"how does a dataframely schema attach to a Dagster asset?"* by rendering one scenario through each candidate shape and executing the [#6](https://github.com/ozanozbeker/dagster-dataframely/issues/6)/[#7](https://github.com/ozanozbeker/dagster-dataframely/issues/7) state machine against real Dagster (1.13.16) and dataframely (3.0.0).

The verdict, side-by-side call sites, and settled sub-decisions live in [COMPARISON.md](COMPARISON.md).

## Files

| file | what it is |
| --- | --- |
| `door_typed.py` | **The chosen door as it ships** — `dataframely_asset`, explicit typed signature, no `**kwargs` |
| `_scenario.py` | The `Orders` schema + sample frames (clean / mixed / hopeless / wrong-dtype) |
| `_core.py` | The kit every shape needs: spec derivation (#8), namespace + collisions (#9), definition metadata (#10/#14), runtime state machine (#6/#7) |
| `shape_a_dd_asset.py` | **Shape A** (chosen) — one decorator, one declaration; kept under its prototype name for the comparison |
| `shape_b_layered.py` | **Shape B** (rejected) — validation decorator layered under plain `@dg.asset` |
| `shape_c_helpers.py` | **Shape C** (kept as the public kit) — the helpers used raw, user-wired `@dg.multi_asset` |

## Run

```sh
uv run python docs/probes/front-door/door_typed.py
uv run python docs/probes/front-door/shape_a_dd_asset.py
uv run python docs/probes/front-door/shape_b_layered.py
uv run python docs/probes/front-door/shape_c_helpers.py
```

Each prints its derived definition surface and/or executes the failure-policy cases in-process with `dg.materialize` (ephemeral instance, nothing persisted).
