# `dagster-dataframely` demo

Every UI surface this package touches, in one Dagster code location.
One schema, twenty-nine asset keys, twelve groups.

Everything here derives from a single `dy.Schema` in [`schema.py`](src/dagster_dataframely_demo/schema.py).
That is the claim the project exists to demonstrate.

Four tables in `base` hold the rows; every other asset reads one of them and has exactly one parent.
So each group is a chain you can follow rather than a fan off a shared root, and the lineage view stays legible on a projector.

## Getting started

It is a member of this repo's uv workspace, so it runs the working tree rather than a published wheel.
Edit `src/dagster_dataframely/` and the next `dg dev` shows the change.

```bash
cd demo && uv run dg dev
```

Then open <http://localhost:3000>.

There is no `DAGSTER_HOME` to set.
`dg dev` builds its instance in a temp directory and deletes it on exit, so every session starts empty and leaves nothing behind.

That is also why the runs below happen in the UI rather than the CLI.
A `dg launch` in another shell gets a throwaway instance of its own, and nothing it records reaches the tab you have open.

Syncing the demo installs the webserver into the workspace's shared `.venv`.
`uv sync` back at the repo root puts it back to the library's own environment.

Materialize before you look.
Half these surfaces only exist once an asset has run.
The asset graph's search bar takes the same selection syntax, so paste each of these in and hit **Materialize**:

1. `* and not group:"failure/*" and not group:"partitions"` is everything that goes green, in one run.
2. `key:"daily_orders"` opens the backfill dialog: take all five days.
3. `key:"regional_orders"` opens it too: take all ten cells.
4. `key:"orders_rollup"` fans in over the five days, so run it after step 2.
5. `group:"failure/*"` is all five exits at once.
   Three of them fail on purpose, so this run ends red.

A quoted glob is how you select a whole subtree: `group:"failure/*"` takes all five subgroups, `group:"failure/quarantine"` takes one.

Quote anything with a `/` in it, and quote `partitions` too: it is a keyword in the selection grammar, so `group:partitions` is a syntax error where `group:catalog` is not.

Ctrl-C ends the session and the run history goes with it.
Tables under `storage/` outlive it, so `rm -rf storage` to start those over too.

## The files

| file | what it is |
| --- | --- |
| `schema.py` | **The one declaration.** Everything else derives from it. |
| `defs/base.py` | The four tables everything else reads. Plain `@dg.asset`s, no schema. |
| `defs/*.py` | One module per subject, autoloaded by `dg`. This is the code to read. |
| `defs/resources.py` | The IO manager. This package ships none, so the demo brings one. |
| `_data.py` | Plumbing. How the fake rows get built. Skip it. |

## What to click

Groups sort alphabetically, so they read in the order below.
Each one starts from a `base` table and runs in a straight line, so you can follow a group in the lineage view without tracing which upstream fed which asset.

| group | asset | what it shows |
| --- | --- | --- |
| `base` | `raw_orders` | Twelve clean lines, declaring nothing about their own shape. The Columns tab it does *not* have is the comparison for everything below. |
| | `defective_raw_orders` | The same twelve plus eight that break a rule. |
| | `hopeless_raw_orders` | Three lines, all invalid, so nothing can survive a filter. |
| | `mistyped_raw_orders` | The clean lines with `quantity` widened to `Int64`. The dtype drifted upstream, which is where dtype drift comes from. |
| `catalog` | **`orders`** | **Start here.** Columns tab filled in from the schema before the first run, 24 checks behind the blocking column-schema check, four statistics tables and a row sample on the materialization. |
| | `orders_undescribed` | The same asset with no `description=`, so the catalog shows the schema's docstring instead of the function's. |
| `failure/column_schema` | `mistyped_orders` | `quantity` arrives `Int64`. The blocking `dy_schema__columns` check fails, the run raises `ColumnSchemaError`, and no rule check reports at all. |
| `failure/no_quarantine` | `strict_orders` | Rows rejected with nowhere to route them. `ValidationAbortError`, checks red at `ERROR`, nothing written. |
| `failure/nothing_survives` | `doomed_orders` | Every row rejected. `NothingSurvivedError`, quarantine written, valid table skipped rather than emptied. |
| | `doomed_orders_quarantine` | Written by the run that failed, which is the run you most want the rows from. |
| `failure/quarantine` | `quarantined_orders` | The same rows as `strict_orders` with `quarantine=True`. Seven checks fail at `WARN` and the run stays green. |
| | `quarantined_orders_quarantine` | The invalid rows, one `dy_*` column per rule. Its only parent is `quarantined_orders`, not the base table both came from. |
| `failure/skip` | `skipped_orders` | A `None` return: nothing validated, nothing materialized, every check green, run succeeds. |
| `granularity` | `orders_by_rule` | `check_granularity="rule"`, the default: 24 checks. |
| | `orders_by_column` | `"column"`: 14, one `dy_col__<column>` per rule-bearing column. |
| | `orders_by_column_per_rule` | The same with `schema_rules="per_rule"`: 16, because the three schema-level rules stop sharing one check. |
| | `orders_by_schema` | `"schema"`: 2, and one of those is the column-schema check. |
| `lazy` | `streamed_extract` | A plain `@dg.asset` returning a `pl.LazyFrame`, so the plan sinks straight to storage with nothing read back. |
| | `validated_stream` | The same lazy return under a schema: executed once, in `Schema.filter`, on the streaming engine. |
| `metadata` | `annotated_orders` | A returned `dg.MaterializeResult`: your metadata, tags and data version on the event. |
| | `context_annotated_orders` | `context.add_asset_metadata`, and the collision it wins that a returned result loses. |
| `partitions` | `daily_orders` | Five daily partitions, each slicing its own day out of the whole of `raw_orders`. Checks report per partition. |
| | `regional_orders` | Day crossed with region: ten cells, nested one directory per dimension on disk. |
| | `regional_orders_quarantine` | Partitioned cell for cell, which the spec read off the definition rather than being told. |
| | `orders_rollup` | The fan-in, `dict[str, pl.LazyFrame]` keyed by partition. |
| `wiring` | `hand_wired_orders` | The decorator written out of `dd.wiring`, quarantine included. |
| | `hand_wired_clean_orders` | The same without a `quarantine_writer`, which is the whole of the failure policy. |
| | `unfiltered_orders` | The checks split off into a `@dg.multi_asset_check`, so the table is written first and the eight invalid rows are in it. |

`failure/*` is the library's failure-policy table, five rows of it.
The sixth is `catalog/orders`, where every row is valid and the question never comes up.

## Screenshots

`assets/images/` in the repo above holds the images the README and the user guide use, and `screenshots.sh` beside this file reshoots every one of them:

```bash
uv run dg dev          # in one shell, then materialize the five selections above
./screenshots.sh       # in another
```

It drives headless Chrome rather than a browser you are looking at, because a capture has to be 2x for the text to survive being scaled into a docs column.
Every view it shoots is reachable by URL, so nothing has to be clicked.
The one number to re-measure after a Dagster release is the left nav's width, which the script sets as `NAV`.

Shoot light or dark consistently, and note the Dagster version wherever the images land: they are assertions about someone else's UI.
The rows in `dataframely/valid_sample` and `dy_failed_sample` are the fake ones from `_data.py`, which is the only reason they are safe to publish.

## The red assets are the point

`strict_orders`, `mistyped_orders` and `doomed_orders` fail every run, by design, and `doomed_orders_quarantine` is written by the run that fails.
Each raises a different error from the package, and the message is a surface worth reading.

Keep them out of any bulk materialize, or the aborted run buries the assets you wanted populated.
That is what `not group:"failure/*"` in the first selection is for.

It takes `quarantined_orders` and `skipped_orders` out with them, which is why step 5 runs the whole subtree: five exits in one run, three red and two green, which is the comparison the group exists for.

## Notes

Partitioned assets cannot share a run with unpartitioned ones, which is why `partitions` gets steps of its own.

A quarantine is not an out, so selecting an asset that declares one takes only that asset's key.
Its rows are written inside that asset's own step, and the `quarantine_spec` beside it is a node with no compute.

Tables land in `storage/`, relative to wherever you started `dg dev`.

The partitioned assets hold disjoint orders rather than the same rows restamped, so `orders_rollup` can concatenate all five days and still satisfy the primary key.
