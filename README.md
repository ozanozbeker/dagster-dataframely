# dagster-dataframely

[dataframely](https://github.com/Quantco/dataframely) declares what a [polars](https://pola.rs) frame has to look like.
[Dagster](https://dagster.io) has first-class surfaces for exactly that: the Columns tab and asset checks.
This package connects them with one declaration.

```python
import dagster as dg
import dataframely as dy
import polars as pl

import dagster_dataframely as dd


class Orders(dy.Schema):
    order_id = dy.String(primary_key=True)
    amount = dy.Float64(nullable=False, min=0.0)


@dd.dataframely_asset(schema=Orders)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return raw_orders.select("order_id", "amount")
```

That is the whole integration.

Before the asset has ever run, `Orders` fills the catalog's Columns tab: dtypes, descriptions, nullability, uniqueness, the primary key stated once at table level, and one pill per remaining constraint.
Every run reports one asset check per dataframely rule, each with its own pass/fail history, behind a blocking gate check that compares the frame's columns and dtypes against the schema before a single row is filtered.
Add `quarantine=dg.AssetOut()` and the rows the schema rejects land in a sibling asset instead of failing the run, as long as something survives.

The transform keeps plain polars annotations.
Upstream assets bind as ordinary parameters, the return may be a `DataFrame` or a `LazyFrame`, and there is no `context` parameter to write: the wrapper reaches the context itself.

## Installation

```bash
uv add dagster-dataframely
```

Requires Python 3.12+.

> [!NOTE]
> **Pre-1.0.**
> The public surface is pinned by a test rather than held by convention, so it will not move quietly.
> It can still move: a `0.x` minor release is where a breaking change lands.
> Pin `dagster-dataframely>=0.1,<0.2` if that matters to you.

`dagster`, `dataframely` and `polars` are the dependencies.
The IO managers import `pydantic` and `universal-pathlib` directly, so both are declared too; both already arrive with `dagster`, so nothing new lands in your environment.
Writing to `s3://`, `gs://` or `az://` needs that scheme's fsspec filesystem installed alongside: `s3fs`, `gcsfs` or `adlfs`.

## The failure policy is the asset's declared shape

There is no lenient mode and no strict flag, deliberately.
Declaring a quarantine **is** the consent to partial data, so what a rejected row costs is visible in the definition and cannot disagree with what the asset declares.

| what the transform returned | good table | quarantine | checks | run |
| --- | --- | --- | --- | --- |
| columns or dtypes that are not the schema's | not written | not written | the gate fails, blocking | fails, `SchemaGateError` |
| every row valid | written | skipped, not written empty | all pass | green |
| some rows rejected, no quarantine declared | not written | n/a | fail at `ERROR` | fails, `ValidationAbortError` |
| some rows rejected, quarantine declared | the survivors | the rejected rows | fail at `WARN` | green |
| every row rejected, quarantine declared | skipped | every row | fail at `ERROR` | fails, `NothingSurvivedError` |

**Without a quarantine, every row has to be good.**
A run that rejects even one row fails and writes nothing, so the last-known-good table stays in place.
Landing the survivors and dropping the rest is the failure this package exists to make visible, so it is not reachable by configuration.
To drop rows anyway, drop them in your own asset body, where the drop is a line you wrote:

```python
@dd.dataframely_asset(schema=Orders)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    good, _ = Orders.filter(raw_orders)
    return good
```

The quarantine table carries the rejected rows with the original columns, then one `String` column per rule reading `valid`, `invalid` or `unknown`, named exactly as that rule's asset check.
Its materialization also carries a `cooccurrence` table, so one broken upstream field tripping three rules reads as one row rather than three unrelated counts.
It inherits the good asset's key prefix, group and IO manager, and its own `dg.AssetOut` can override any of them, which is how rejected rows reach a separate storage and ownership domain.
Three settings raise `QuarantineSettingError` instead of being honoured: `automation_condition`, `freshness_policy` and `code_version` cannot differ between two outs of one step, so they belong on the decorator, where they cover both.

## The package never casts

The gate compares the frame's dtypes against the schema's and aborts on a mismatch; the filter runs with `cast=False`.
A `Duration('ns')` arriving where the schema declares `Duration('us')` is a pipeline defect, and silently widening it is how a thousandfold error reaches a table nobody re-reads.

If you do want conformance, write the cast yourself, in your own asset body, as a line you can see:

```python
@dd.dataframely_asset(schema=Orders)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return Orders.cast(raw_orders)
```

The one cast the package makes is on columns it generated itself: the quarantine's outcome columns go from `Enum` to `String`, because a raw `Enum` panics the Delta writer.

## Storage

```python
defs = dg.Definitions(
    assets=[orders],
    resources={
        "io_manager": dd.DataframelyParquetIOManager(
            base_dir="s3://my-bucket/warehouse"
        )
    },
)
```

`DataframelyParquetIOManager` and `DataframelyCSVIOManager` write under a universal-pathlib `base_dir`, so a local directory and `s3://`, `gs://` or `az://` are the same call on credentials from the ambient environment.
A dtype the format cannot hold raises `UnwritableDtypeError` before the write rather than from inside polars.

**These are the supported path.**
They record `path`, `bytes_written` and `dagster/storage_kind` on each materialization and nothing else.
No column schema, in particular: the asset definition owns what the data is, and leaving the materialization bucket empty is what keeps the Columns tab showing the schema as declared.

What degrades on someone else's manager is one surface.
A stock polars IO manager writes its own `dagster/column_schema` onto the materialization, and with both metadata buckets populated Dagster merges them in the catalog Columns tab: column names come out lowercased and the constraint pills disappear, because the materialization's constraint-free schema becomes the base.
Dtypes, descriptions and column tags survive, and the Lineage Metadata accordion is definition-only and never merged, so full fidelity is still one click away.
This is documented, not designed around.
Parity is a preference, never a constraint.

### CSV without the losses

A CSV cell holds text, so five dtypes have nowhere to land.
Each is encoded on the way out and decoded on the way back by its declared inverse, so the frame read back compares equal to the frame written.

| dtype | what the cell holds | how it reads back |
| --- | --- | --- |
| `Duration` | the integer tick count, in the column's own time unit | the ticks cast into the declared `Duration` |
| `List` | JSON, wrapped in a one-field object named after the column | the wrapper decoded and its one field taken |
| `Array` | the same wrapper | decoded as a `List`, then cast to the declared `Array` |
| `Struct` | a JSON object | decoded into the declared `Struct` |
| `Binary` | base64 | decoded back to `Binary` |

A run log line names the encoded columns on both paths, because that is the only surface that can carry it.

Two cases are refused rather than encoded: `Binary` and `Duration` *inside* a `List`, `Array` or `Struct`.
Polars cannot write the first to JSON and cannot read the second back, so encoding either would land a file that no longer round-trips.
`Object` is refused by both managers.

**The read needs the schema.**
The decode reads each column's declared dtype off the carrier the decorator puts in the asset's definition metadata, which `dd.schema_metadata` also builds for a plain `@dg.asset`.
With no schema in reach the read is an ordinary inferred CSV read, and an encoded column arrives as text.
The schema never comes from a sidecar file and never from the data, so it costs no round trip and cannot drift.
The carrier holds the live class rather than a copy of it, and a live object does not cross a process boundary, so a manager that only reaches a deserialized definition falls back to the inferred read too.

Prefer parquet unless something downstream needs text.
Parquet is self-describing, keeps every dtype natively, and needs no schema to read.

## Partitioning

`partitions_def` forwards to the underlying `multi_asset` verbatim, so partitioning needed no code and has no setting here.
Both outs carry it, which is what makes the quarantine unable to escape its asset's partitioning.
The state machine runs per partition on that partition's frame, `dagster/row_count` is that partition's good count, and a partition whose frame drifts aborts at the gate without touching any other partition's file.

**The transform reaches its own partition key through the context.**
The transform takes no `context` parameter, so a partitioned one fetches the context the same way the wrapper does:

```python
daily = dg.DailyPartitionsDefinition(start_date="2026-01-01")


@dd.dataframely_asset(schema=Orders, partitions_def=daily)
def orders() -> pl.DataFrame:
    day = dg.AssetExecutionContext.get().partition_key
    landed = pl.read_parquet(f"landing/orders/{day}.parquet")
    return landed.select("order_id", "amount")
```

`dg.AssetExecutionContext.get()` is the door's own shape rather than a workaround: it is how the wrapper reaches the context, and a transform with no `context` parameter cannot hit the first of the two traps below, where Dagster rejects an annotated one under postponed annotations.

**A fan-in over every partition arrives as one frame per partition.**
An unpartitioned asset depending on the whole of a partitioned one gets a dict keyed by partition key, because the IO manager reads each key and assembles the results:

```python
@dg.asset
def rollup(orders: dd.DataFramePartitions) -> None: ...
```

`dd.DataFramePartitions` is `dict[str, pl.DataFrame]`, exported so the shape a fan-in has to annotate has a name.
The obvious annotation, `pl.DataFrame`, fails Dagster's type check after every partition has already been read.
`dd.LazyFramePartitions` is its lazy twin, `dict[str, pl.LazyFrame]`, and reads each partition the way the section below reads a single one.

## Reads dispatch on the annotation

Annotate an input `pl.LazyFrame` and the IO manager hands back an unexecuted scan, so a downstream `filter` or `select` prunes rows and columns before anything is decoded:

```python
@dg.asset
def recent(orders: pl.LazyFrame) -> pl.DataFrame:
    return orders.filter(pl.col("amount") > 100).select("order_id").collect()
```

Annotate `pl.DataFrame` and the file is read whole, as before.

The scan is built on the same fsspec handle the eager read uses, so a cloud scheme needs no second set of credentials, and polars reads the file's bytes when the scan is built.
What a scan saves is therefore decoding and materialization, not transfer.

## Writes dispatch on the runtime type

The asymmetry with the read above is deliberate: a write already holds the object, so `isinstance` is the honest test, while a read has no object yet and the annotation is the only signal for what to build.

**Return a `pl.LazyFrame` from a plain `@dg.asset` and the plan streams to storage.**
It is sunk through the streaming engine to a local temp file, then promoted to `base_dir` once the plan succeeded, so peak memory is the engine's buffers rather than the whole frame:

```python
@dg.asset
def orders(raw_orders: pl.LazyFrame) -> pl.LazyFrame:
    return raw_orders.filter(pl.col("amount") > 0).select("order_id", "amount")
```

A `pl.DataFrame` return is written where it lands, having nothing left to stream.

**The promote is what keeps a failing plan out of storage.**
Opening the destination truncates it, so a sink straight there would leave a zero-byte file where a good one was, or a plausible-looking partial one where the plan died late.
Nothing reaches the destination until the sink succeeded, and a file already there survives the failure untouched.
A local destination is renamed onto, so the promote itself is atomic there; where it has to copy, a failure partway through it corrupts the destination exactly as an eager write already does.

The temp file goes wherever [`temp_dir`](#temp_dir-decides-which-disk-a-lazy-transform-lands-on) says, and CSV encodes its columns on the way out as it does on the eager path.

## Validation materializes

`Schema.filter` collects, so a validated frame is a frame in memory.

**A `LazyFrame` return streams to a local parquet first, then is read back whole and validated exactly as a `DataFrame` return is.**
Peak memory is then the size of the frame the plan produced rather than the plan's own high-water mark, which is the saving for a transform with a large intermediate: a join that fans out before filtering back down otherwise pays for the fan-out in memory.
A `DataFrame` return skips the landing, because a frame you already materialized has nothing left to stream and landing it would be pure cost.
The gate runs before the landing, so a frame whose shape disagrees with the schema is refused before a single row is streamed, and the landed file is removed before the run picks an outcome.

> [!IMPORTANT]
> The landing goes to the system temp directory, which in a container is its **ephemeral disk**.
> A landed frame bigger than what the pod has spare fills it.
> `temp_dir` points it at a mounted volume instead.

Storage stays eager past that point, and that is the difference from the section above.
This package does not promise to write a file, it promises to write a file and report on it: `dy.FailureInfo` is eager by construction, the statistics profile runs two global aggregates, and the state machine cannot choose among its five exits without counting both halves of the split, so the exits whose whole purpose is that nothing gets written would have to execute the plan to learn that.
A plain `@dg.asset` streams end to end because it has none of those duties: no schema means no validation, no per-rule checks and no statistics pass, so nothing forces the plan into memory.
The measurements are in [`docs/research/lazyframe-end-to-end.md`](docs/research/lazyframe-end-to-end.md).

The habitat is post-landing transformation: bronze to silver to gold, where the data is already on your side and the question is whether it is fit to publish.
Ingestion-scale and larger-than-memory work belongs to other tools.

## Settings

Every setting resolves through three tiers, each overriding the one before: the package default, then an environment variable, then the argument on the asset.
A platform engineer sets a house style once for a whole code location, and an asset overrides it where that style is wrong.
Each variable is `DAGSTER_DATAFRAMELY_` plus the setting's name, upper-cased.

| setting | what it decides | default |
| --- | --- | --- |
| `check_granularity` | how far the schema's rules collapse into checks: `rule`, `column` or `schema` | `rule` |
| `multi_column_rules` | where the rules no single column owns land at `column` granularity: `schema` or `per_rule` | `schema` |
| `statistics` | whether each materialization carries a profile of what it wrote | `true` |
| `max_failure_samples` | how many of the rows a rule rejected reach that rule's check | `5` |
| `row_sample` | how many of the good table's rows reach its materialization | `5` |
| `temp_dir` | which disk a `LazyFrame` lands on, before it is validated or promoted to storage | the system temp directory |

The chain validates on resolve, at every tier including the package's own, so a typo raises `InvalidSettingError` naming the value and the tier that supplied it rather than quietly becoming something else three modules later.

### Changing `check_granularity` orphans check history

`rule` gives every rule its own check and its own timeline.
`column` gives one check per rule-bearing column, `dy_col__<column>`, which is what makes a 40-column schema's check list readable.
`schema` gives a single `dy_schema__rules` for all of them.

**Changing it on an asset that has already run orphans that asset's check history.**
The old check names stop being reported and their timelines end where the change landed, while the new ones start empty.
Nothing migrates them, so choose it before the asset ships rather than after.

### Statistics and both samples are on by default

Each materialization carries a `skimr`-style profile of what it wrote: one table per dtype family present, under `stats/numeric`, `stats/temporal`, `stats/string` and `stats/boolean`, on the quarantine as well as the good table.

> [!IMPORTANT]
> Two of the settings write **real rows of your data into the Dagster event log**, and both ship on.
> The event log is shared across a deployment, it is exportable, and nothing here is redacted.
> If a column holds an email address, a name or an account number, that value lands in the log and stays there.

| setting | what it writes | where |
| --- | --- | --- |
| `max_failure_samples` | up to this many of the rows each rule rejected | that rule's asset check, under `dy_failed_sample` |
| `row_sample` | up to this many of the rows the good table holds | its materialization, under `sample` |

The quarantine carries no row sample, deliberately.
Its rows already reach the event log once, through the check that rejected each of them and with the rule attached, so a second copy would carry less and cost the same.

dataframely's own comparable setting defaults to `0`, so this package is deliberately the more generous of the two.
The reason is that a red check raises exactly one question the counts cannot answer: not that `amount|min` rejected 43 rows, but what three of those rows held.
Paying for that in the event log should be a decision, which is what this section is for.

Setting either to `0` turns it off entirely, and the metadata key is then absent rather than empty.
Per asset:

```python
@dd.dataframely_asset(schema=Orders, max_failure_samples=0, row_sample=0)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return raw_orders.select("order_id", "amount")
```

Or once for a whole code location, in the deployment's environment:

```bash
DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES=0
DAGSTER_DATAFRAMELY_ROW_SAMPLE=0
```

Turning the samples off leaves `statistics` on.
The string family deliberately carries no value-bearing statistic at any setting, only lengths and cardinality: consenting to summary statistics is not consenting to raw values.

### `temp_dir` decides which disk a lazy transform lands on

Two landings read it, both on the lazy path only, so an asset that returns a `DataFrame` is unaffected by whatever it holds: a `dataframely_asset` lands its transform there before validating it, and an IO manager sinks a `LazyFrame` output there before promoting it to storage.

Unset, the landing goes wherever `tempfile` puts things, which in a container is the ephemeral disk its `/tmp` sits on.
That disk is usually small, it is shared with everything else in the pod, and filling it takes the pod down rather than failing the asset.
Point it at a volume for the whole code location:

```bash
DAGSTER_DATAFRAMELY_TEMP_DIR=/mnt/scratch
```

Or per asset, where one transform is the one with the large intermediate:

```python
@dd.dataframely_asset(schema=Orders, temp_dir="/mnt/scratch")
def orders(raw_orders: pl.LazyFrame) -> pl.LazyFrame:
    return raw_orders.filter(pl.col("amount") > 0).select("order_id", "amount")
```

The argument tier is the decorator's alone.
An IO manager reads the variable and the package default, because the decorator resolves its argument where the asset is declared and hands it to the state machine, which has landed and validated the frame long before a manager sees one.

A directory that does not exist raises rather than being created, and an empty value raises rather than reading as unset.
Both are the same decision: this setting is set to move the landing off the ephemeral disk, so a typo that quietly lands there anyway is the failure it exists to prevent.

## The kit

The decorator is one arrangement of parts the package also exports on their own: `check_specs`, `schema_metadata`, `table_schema`, `quarantine_table_schema`, `quarantine_frame`, `process` and `check_name`.
Reach for them when the decorator's shape is not the shape you need: a schema attached to an asset you did not declare, or an out arrangement the door does not offer.
Then the `@dg.multi_asset` is yours to wire, out of the same parts.

```python
@dg.multi_asset(
    outs={
        "orders": dg.AssetOut(metadata=dd.schema_metadata(Orders), is_required=False)
    },
    check_specs=dd.check_specs(Orders, asset="orders"),
)
def orders():
    yield from dd.process(
        Orders,
        transform(),
        context=dg.AssetExecutionContext.get(),
        good_out="orders",
    )
```

`is_required=False` matters: the gate and both abort paths end the step without yielding an out.

**This is not a route to `dy.Collection` support.**
Hand-wiring a Collection means reimplementing the hardest part of the package rather than assembling it, because `process` is single-schema by signature.
Declare one asset per member instead, each with the member's own schema.
Passing a Collection to the decorator raises `CollectionNotSupportedError` at decoration time.

## Two user-side traps

**A `from __future__ import annotations` in your own module breaks a `context` parameter.**
Under PEP 563 every annotation reaches Dagster as a string, and its check on the `context` parameter compares against the real classes, so it rejects `context: dg.AssetExecutionContext` and `context: AssetExecutionContext` alike:

```text
DagsterInvalidDefinitionError: Cannot annotate `context` parameter with type dg.AssetExecutionContext.
`context` must be annotated with AssetExecutionContext, AssetCheckExecutionContext, OpExecutionContext, or left blank.
```

Only the last option in that message survives PEP 563: leave the parameter blank, or take no `context` at all and reach it with `dg.AssetExecutionContext.get()`.
Decorator call sites avoid this structurally, because the wrapper fetches the context and your transform never takes one.
Kit call sites are yours to write, so this one is yours to hit.

**A `@dy.rule()` body needs its class parameter.**
Written without one, the class still builds and the asset still defines; the run then fails when this package reads the rule's expression for the check metadata:

```text
TypeError: Orders.amount_is_positive() takes 0 positional arguments but 1 was given
```

`@dy.rule()` is a classmethod-style decorator, so the body takes `cls`:

```python
class Orders(dy.Schema):
    status = dy.String(nullable=False)
    amount = dy.Float64(nullable=False)

    @dy.rule()
    def paid_orders_have_amount(cls) -> pl.Expr:
        """Paid orders must carry a positive amount."""
        return (cls.status.col != "paid") | (cls.amount.col > 0)
```

The docstring is not decoration: it becomes that check's description in the catalog.

## The reserved namespace

Every check name sits under `dy_`, and so does every quarantine outcome column and every key in check metadata: the gate check `dy_schema__dtypes`, the rule checks `dy_rule__<rule>`, the collapsed checks `dy_col__<column>` and `dy_schema__rules`.
The materialization keys are deliberately outside it, because `sample`, `cooccurrence` and `stats/*` are for a data consumer rather than for this package's bookkeeping.
A schema with a column of its own inside the namespace raises `ReservedColumnError` at definition time, and two rules that rewrite to one check name raise `CheckNameCollisionError`.
The prefix is hardcoded rather than configurable: its whole value is being the same string in every project.

## License

[Apache-2.0](LICENSE)
