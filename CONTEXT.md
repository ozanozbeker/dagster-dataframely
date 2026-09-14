# `dagster-dataframely`

Attaches a Dataframely schema to a Dagster asset, so one declaration fills the Columns tab, reports every rule as an asset check, and decides what a failing row costs.

Where a word exists in Dagster, Dataframely or Polars already, that word wins, in prose and in identifiers.
The terms below are the ones this package had to add, plus the few it kept getting wrong.
_Avoid_ lists what a writer could reach for **to mean the term above it**.
The ban is on the meaning, not the string: `sink_parquet` is Polars' own and no writer, so it is not a collision.

This is a glossary.
Why a thing works the way it does belongs in `docs/adr/` or in the module that does it.

## Language

### The asset

**`dd.asset`**: The decorator, which turns the function it decorates into an asset validated against a schema.
Dagster's own word, because the thing is a `dg.asset` with a schema attached and `@dg.asset` is the mechanism underneath.
Dataframely settles the same question the same way, shadowing 28 Polars names under its alias: `dy.DataFrame` is a `pl.DataFrame` with a schema, as `dd.asset` is a `dg.asset` with one.
**Always written `dd.asset`, never bare**, in prose and in an example, because bare `asset` reads as Dagster's.
ADR-0002, ADR-0005 and ADR-0008 call it `dataframely_asset` or `dy_asset`, its names at the time.
_Avoid_: dataframely_asset, dy_asset, door, front door

**Schema-backed asset**: An asset `dd.asset` built, or one hand-wired out of the same parts.
_Avoid_: validated asset, dy asset

**Decorated function**: The function `dd.asset` wraps.
Upstream assets bind into it as parameters, it may declare `context`, and it returns the frame to validate, a `dg.MaterializeResult` carrying one, or `None` to skip.
Dagster's own phrase, and explicit for one reason: it names the function by its relation to the decorator rather than by what it happens to do inside.
_Avoid_: transform, compute function (Dagster's, but there it names the wrapper `dd.asset` builds)

**Valid rows**: The rows that passed every rule, which `Schema.filter` returns first.
They are what the asset materializes.
_Avoid_: good rows, kept rows

**Invalid rows**: The rows that failed at least one rule, which `FailureInfo.invalid()` returns.
Dataframely's own word, and the one its rule columns are written in.
_Avoid_: bad rows, rejected rows

**Failure**: One row failing one rule, which is what `FailureInfo.counts()` counts.
The per-check counts sum past `dataframely/invalid_count`, because a row that breaks three rules is three failures and one invalid row.
_Avoid_: rejection, violation

**Outcome**: How a run ended.
The word has one meaning in this package, the run's, so a rule column's `valid` / `invalid` / `unknown` is never an outcome.
Say what happened: no rows failed; rows failed and no quarantine is declared; no rows survived; the decorated function returned `None`.
_Avoid_: exit, the rule-column meaning

**Skip**: What a decorated function asks for by returning `None`.
Nothing is validated, nothing materializes, the partition stays unmaterialized, and the run succeeds.
Dagster has no noun for it, only `output_required=False` and "the function can conditionally not `yield` a result".
_Avoid_: empty run, no-op, conditional materialization

**Quarantine**: Where invalid rows are written, addressed by the asset key `<name>_quarantine`.
Not an asset: it is evidence of a run, and it holds no place in the graph unless `quarantine_spec` gives it one.
Declaring one is the consent to partial data; leaving it undeclared is the refusal.
_Avoid_: quarantine asset, dead-letter asset

**Writer**: What puts the invalid rows somewhere and hands back a quarantine address.
`validation_results` takes one and learns nothing else about where the rows went.
`delegating_writer` delegates to the IO manager the asset is already bound to; `file_writer` writes a parquet file under `quarantine_dir`.
Name the one you mean.
_Avoid_: sink, emitter, exporter, router, dispatcher, resolver, and **the fallback** standing in for `file_writer`

**Quarantine address**: Where a writer put the invalid rows, rendered for a reader.
An asset key under `delegating_writer` and a file path under `file_writer`, because the answer can be a database table.
Dagster's word, from `TableMetadataSet.extract_storage_address`.
Only the quarantine has one.
Where the valid rows went is the IO manager's business, and this package never renders it.
_Avoid_: location, destination, and `path` for the answer in general, which presumes a filesystem the warehouse case does not have

**`quarantine_dir`**: The directory `file_writer` writes under, with the asset key and partition naming the rest of the file path.
Set by `DAGSTER_DATAFRAMELY_QUARANTINE_DIR` and nothing else.
_Avoid_: root, base dir, output dir

**Rule column**: A column of the quarantine carrying one rule's result per row, reading `valid`, `invalid` or `unknown`.
Dataframely's own term, from `FailureInfo.details()`.
_Avoid_: outcome column

**Step**: Dagster's unit of execution, one node of the run's execution plan.
Only a run has one, which is how the package tells a run from a direct invocation.
Never a stage of this package's own work: the column-schema check and `Schema.filter` are two calls, not two steps.
_Avoid_: execution context (Dagster's, but there it names `context` itself), and "step" for a phase of validation

**Direct invocation**: Calling a decorated asset instead of running it, which is Dagster's documented unit-testing path and its own phrase.
The one case with no step, so it is the whole reason `file_writer` and `quarantine_dir` exist.
_Avoid_: calling it a test, which is what a reader does with it rather than what it is

**Hand-wiring**: Building a `@dg.asset` out of `dd.wiring` instead of using `dd.asset`.
_Avoid_: the kit

**`validation_results`**: What a schema-backed asset runs after its decorated function, and what hand-wiring calls.
ADR-0001 through ADR-0004 call it `process`, its name at the time.

### Validation

**Column schema**: A frame's columns and their dtypes, which is what `frame.collect_schema()` returns.
Dagster's name, from the `dagster/column_schema` key this package already writes.
A frame whose column schema does not match the schema's is a pipeline defect: the run stops before any row is filtered, and reports through a blocking check.
_Avoid_: shape (Polars' `.shape` is a row and column count), gate

**Rule**: One Dataframely validation rule, under the name Dataframely gives it.

**Column rule**: A rule one column owns, keyed `<column>|<rule name>`.
Dataframely's word, from the `column_rules` it builds as `f"{col_name}|{rule_name}"`.

**Rule name**: The part after the `|`, which is also the column argument the rule was generated from.
`min` for `amount|min`.
Dataframely's word for it, from that same line.
_Avoid_: kind (Dagster's `kinds=` is the asset's badges, and `dd.asset` forwards it), type, flavour

**Schema-level rule**: A rule no single column owns: a `@dy.rule()`, and `primary_key`.
Dataframely's phrase, from its own `_schema_validation_rules()` and the warning it raises about them.
A single-column primary key is one of these, which is why the setting is `schema_rules`.
_Avoid_: multi-column rule, which a single-column primary key makes false; table rule

**`DescribedRule`**: One rule with everything this package derives about it: its check name, its column and rule name, its docstring, and its expression.
_Avoid_: parsed rule, rule info, rule spec, enriched rule

**Rule set**: The rules one asset check reports for.
One rule at `rule` granularity, one column's rules at `column`, every rule at `schema`.
_Avoid_: bucket

**Granularity**: How far the rules collapse into checks, which `check_granularity` sets to `rule`, `column` or `schema`.
Coined: Dagster's only `granularity` is an internal concurrency setting, and neither Dataframely nor Polars has one.
_Avoid_: level, resolution, detail

**Collapse**: Reducing several rules into a single asset check.
Always name what collapses: the rules collapse into checks, and a check reporting for several of them is a collapsed check.
Bare "collapsing" leaves the reader to work out the object.
_Avoid_: collapsing with no object, merge, group (which is Polars' word for a set of dtypes)

**Column constraint**: One condition a rule states, rendered for Dagster's Columns tab.
Dagster's name, from `dg.TableColumnConstraints`.
_Avoid_: pill, chip

### Configuration

**Setting**: One configurable value, resolved through three sources, each overriding the one before: the package default, then `DAGSTER_DATAFRAMELY_*`, then the `dd.asset` argument.
`quarantine_dir` has only the first two, because it takes no argument.
Say "the setting's sources", not "the settings chain".
_Avoid_: knob, option, settings chain

**Allowed values**: What a setting accepts, which is what `InvalidSettingError` prints.
_Avoid_: vocabulary

**Statistics**: The `skimr`-style summary a materialization carries for what it wrote, under `dataframely/valid_statistics/<group>`.
Spelled out everywhere, including the metadata key.
_Avoid_: stats, profile, describe (Polars' `describe()` is a different thing this package does not use)

**Dtype group**: The set of dtypes sharing one statistics table: `numeric`, `temporal`, `string`, `boolean`.
Polars' word, from `polars.datatypes.DataTypeGroup` and the `*_DTYPES` sets beside it.
_Avoid_: family, class, category

**Sample**: Real rows a run puts in the event log, bounded by `max_failure_samples` for a check and `row_sample` for a materialization.
A sample is absent, never empty.
_Avoid_: preview, example, head

**Reserved namespace**: Three, and the third is unlike the other two.
`dy_` for every check name, rule column and check-result metadata key, and for nothing else.
It names what this package generates into Dagster, never anything a user types, which is why the decorator is `dd.asset` and not `dy_asset`.
`dataframely/` for every materialization metadata key this package names itself, which parallels `dagster/`.
The valid row count is the exception, because Dagster already has a key for it: it goes under `dagster/row_count`.
`<name>_quarantine` is the third, the asset key a quarantine is addressed by.
The first two name what this package generates, so nothing else can claim them.
The third sits in Dagster's own key space, which the user shares, so it is the one reservation somebody else can take first.
Namespace, never prefix, even where the string is one.
_Avoid_: prefix, reserved word

## Coinages to avoid

These name nothing in Dagster, Dataframely or Polars.
Each hides what the code does behind a word the reader has to learn first, so say what happens instead.

**split**, **halves**: `Schema.filter` separates valid rows from invalid rows, and those are the words for its two results.
The ban is on the coined noun, not the ordinary verb: "Dagster forces the split" above is fine.

**exit**: name the outcome.

**phase**: name the call.
The column-schema check runs, then `Schema.filter`.
Not "step", which is Dagster's.

**surface**: name the place.
The Columns tab, the check name, the check description.
"Public surface" for an export list is ordinary English and stays.

**shape**: banned above for column schema, and equally for a kind of setting, rule or constraint.

**spelling** for how a partition key reaches a path: say formatted.
That is `UPathIOManager`'s own word, in `_formatted_multipartitioned_path` and in the `formatted_partition_keys` it builds.
"Spelled out", and "spelling" for one written form of a value, are ordinary English and stay.

**green**, **red**: say what happened.
The run succeeds, the run fails, the check passes, the check fails.
The colours are the Dagster UI's rendering of those facts, not the facts.
