# `dagster-dataframely`

Attaches a Dataframely schema to a Dagster asset, so one declaration fills the Columns tab, reports every rule as an asset check, and decides what a failing row costs.

Where a word exists in Dagster or Dataframely already, that word wins.
The terms below are the ones this package had to add, plus the few it kept getting wrong.

## Language

### The asset

**Decorator**: `@dy_asset`, which turns the function it decorates into an asset validated against a schema.
`dy` because it is Dataframely's own import alias and already the package's reserved prefix.
_Avoid_: dataframely_asset, door, front door

**Decorated function**: The function the decorator wraps.
Upstream assets bind into it as parameters, it may declare `context`, and it returns the frame to validate or a returned result carrying one.
Dagster's own phrase, and explicit for one reason: it names the function by its relation to the decorator rather than by what it happens to do inside.
_Avoid_: transform, compute function (Dagster's, but there it names the wrapper this decorator builds)

**Valid rows**: The rows `Schema.filter` kept.
They are what the asset materializes.
_Avoid_: good rows, the good table, the good out

**Invalid rows**: The rows `Schema.filter` removed, each having failed at least one rule.
_Avoid_: rejected rows, bad rows, failed rows

**Quarantine**: Where invalid rows are written, addressed by the asset key `<name>_quarantine`.
Not an asset: it is evidence of a run, and it holds no place in the graph unless a quarantine spec gives it one.
Declaring one is the consent to partial data; leaving it undeclared is the refusal.
_Avoid_: reject table, dead-letter asset, quarantine asset, sibling

**Writer**: What puts the invalid rows somewhere and hands back an address.
`process` takes one and learns nothing else about where the rows went.
_Avoid_: sink, emitter, exporter

**Delegating writer**: The writer that hands the rows to the IO manager the asset is already bound to.
The default, and the reason a quarantine lands beside its table on any backend with no configuration.
_Avoid_: borrowing writer, manager writer, passthrough

**File writer**: The writer that puts the rows in a parquet file under the root.
The fallback, for a decorated asset that is called rather than run.
_Avoid_: local writer, fallback writer

**Address**: Where a writer put the rows, rendered for a reader.
An asset key under delegation and a path under the fallback, because the answer can be a database table.
Dagster's own word, from `TableMetadataSet.extract_storage_address`.
_Avoid_: location, destination, path

**Root**: The directory the file writer writes under, with the asset key and partition spelling the rest of the path.
One per deployment through the setting, and nowhere else: a root is meaningless to a warehouse, so there is no per-asset override (ADR-0006).
_Avoid_: quarantine dir, base dir, output dir

**Quarantine spec**: A user-declared `dg.AssetSpec` that stands for a quarantine in the graph.
It has no compute, because the decorator already wrote the rows.
_Avoid_: quarantine asset, sibling, external asset

**Rule column**: A column of the quarantine carrying one rule's outcome per row, reading `valid`, `invalid` or `unknown`.
Dataframely's own term, from `FailureInfo.details()`.
_Avoid_: outcome column

**Returned result**: A `dg.MaterializeResult` a decorated function returns in place of a bare frame.
Its `value` is the frame to validate; the rest folds into the asset's materialization.
_Avoid_: wrapped frame, enriched result

**Unwrap**: Taking the frame off a returned result, before anything is validated.

**Fold**: What a returned result's remaining fields do to the materialization the package built.
_Avoid_: merge, enrich

**Hand-wiring**: Building a `@dg.asset` from the package's exported parts instead of using the decorator.
_Avoid_: the kit

### Validation

**Shape**: A frame's columns and their dtypes, against what the schema declares.
A mismatch is a pipeline defect.
It stops the run before any row is filtered, and reports through a blocking check.
_Avoid_: gate, schema gate

**Rule**: One Dataframely validation rule, under the name Dataframely gives it.

**Rule set**: The rules one asset check reports for.
One rule at `rule` granularity, one column's rules at `column`, every rule at `schema`.
_Avoid_: bucket

**Collapse**: Reducing several rules into a single asset check, which is what `check_granularity` decides.

**Constraint**: One condition a rule states, rendered for Dagster's Columns tab.
_Avoid_: pill, chip, badge

### Storage

**Staging**: The local temporary file a lazy frame streams to before it is validated.
"Land" stays available for where a rule or a dtype ends up, but never for a row or a file: a row is written, a file is staged.
_Avoid_: landing, spill, scratch

### Reporting

**Sample**: A bounded set of real rows copied into the Dagster event log, either of what a rule rejected or of what the asset wrote.

**Statistics**: The per-dtype-family summary the valid materialization carries, one table per family present in the frame.
_Avoid_: profile, skim

### Configuration

**Setting**: One configurable value, resolved through three tiers.
_Avoid_: knob, option

**Tier**: One level of a setting's resolution order: the argument on the asset, then `DAGSTER_DATAFRAMELY_*`, then the package default.
`quarantine_dir` has two: it takes no argument, because that would be the location override ADR-0006 defers.

### Naming

**Reserved namespace**: The `dy_` prefix every check name, rule column and check-metadata key sits under.
Hardcoded, never configurable.
