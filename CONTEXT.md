# `dagster-dataframely`

Attaches a Dataframely schema to a Dagster asset, so one declaration fills the Columns tab, reports every rule as an asset check, and decides what a failing row costs.

Where a word exists in Dagster, Dataframely or Polars already, that word wins, in prose and in identifiers.
The terms below are the ones this package had to add, plus the few it kept getting wrong.
_Avoid_ lists what a writer could reach for **to mean the term above it**.
The ban is on the meaning, not the string: `sink_parquet` is Polars' own and no writer, so it is not a collision.

## Language

### The asset

**`dy_asset`**: The decorator, which turns the function it decorates into an asset validated against a schema.
`dy` because it is Dataframely's own import alias and already the package's reserved prefix.
_Avoid_: dataframely_asset, door, front door

**Decorated function**: The function `dy_asset` wraps.
Upstream assets bind into it as parameters, it may declare `context`, and it returns the frame to validate, a `dg.MaterializeResult` carrying one, or `None` to skip.
Dagster's own phrase, and explicit for one reason: it names the function by its relation to the decorator rather than by what it happens to do inside.
_Avoid_: transform, compute function (Dagster's, but there it names the wrapper `dy_asset` builds)

**Valid rows**: The rows that passed every rule, which `Schema.filter` returns first.
They are what the asset materializes.
_Avoid_: good rows, kept rows

**Invalid rows**: The rows that failed at least one rule, which `FailureInfo.invalid()` returns.
Dataframely's own word, and the one its rule columns are written in.
_Avoid_: bad rows, rejected rows

**Failure**: One row failing one rule, which is what `FailureInfo.counts()` counts.
The per-check counts sum past `dataframely/invalid_count`, because a row that breaks three rules is three failures and one invalid row.
_Avoid_: rejection, violation

**Quarantine**: Where invalid rows are written, addressed by the asset key `<name>_quarantine`.
Not an asset: it is evidence of a run, and it holds no place in the graph unless `quarantine_spec` gives it one.
Declaring one is the consent to partial data; leaving it undeclared is the refusal.
Where an asset Dagster can materialize already owns that key, the run fails rather than one of the two overwriting the other (ADR-0007).
_Avoid_: quarantine asset, dead-letter asset

**`QuarantineWriter`**: What puts the invalid rows somewhere and hands back a quarantine address.
`validation_results` takes one and learns nothing else about where the rows went.
_Avoid_: sink, emitter, exporter

**`delegating_writer`**: The writer that hands the rows to the IO manager the asset is already bound to.
Tried first and unconditionally, which is why a quarantine is written beside its table on any backend with no configuration (ADR-0006).
_Avoid_: borrowing writer, manager writer, passthrough

**`file_writer`**: The writer that puts the rows in a parquet file under `quarantine_dir`.
Reached only when there is no step to delegate through, which is direct invocation.
_Avoid_: local writer, fallback writer

**Quarantine address**: Where a writer put the invalid rows, rendered for a reader.
An asset key under `delegating_writer` and a file path under `file_writer`, because the answer can be a database table.
Only the quarantine has one.
Where the valid rows went is the IO manager's business, and this package never renders it.
_Avoid_: location, destination, and `path` for the answer in general, which presumes a filesystem the warehouse case does not have

**`quarantine_dir`**: The directory `file_writer` writes under, with the asset key and partition spelling the rest of the file path.
Set by `DAGSTER_DATAFRAMELY_QUARANTINE_DIR` and nothing else: `dy_asset` takes no argument for it, because that would be the override ADR-0006 defers.
Unset, a call that reaches `file_writer` raises rather than choosing a directory on the operator's behalf.
_Avoid_: root, base dir, output dir

**Rule column**: A column of the quarantine carrying one rule's outcome per row, reading `valid`, `invalid` or `unknown`.
Dataframely's own term, from `FailureInfo.details()`.
_Avoid_: outcome column

**Step**: Dagster's unit of execution, one node of the run's execution plan.
Only a run has one, which is how the package tells a run from a direct invocation, and it carries the output context and IO manager `delegating_writer` borrows.
_Avoid_: execution context (Dagster's, but there it names `context` itself)

**Hand-wiring**: Building a `@dg.asset` out of `dd.wiring` instead of using `dy_asset`.
_Avoid_: the kit

### Validation

**Column schema**: A frame's columns and their dtypes, which is what `frame.collect_schema()` returns.
Dagster's name, from the `dagster/column_schema` key this package already writes.
A frame whose column schema does not match the schema's is a pipeline defect: the run stops before any row is filtered, and reports through a blocking check.
_Avoid_: shape (Polars' `.shape` is a row and column count), gate

**Rule**: One Dataframely validation rule, under the name Dataframely gives it.

**Rule set**: The rules one asset check reports for.
One rule at `rule` granularity, one column's rules at `column`, every rule at `schema`.
_Avoid_: bucket

**Collapse**: Reducing several rules into a single asset check, which is what `check_granularity` decides.
Always name what collapses: the rules collapse into checks, and a check reporting for several of them is a collapsed check.
Bare "collapsing" leaves the reader to work out the object.
_Avoid_: collapsing with no object, merge, group

**Column constraint**: One condition a rule states, rendered for Dagster's Columns tab.
Dagster's name, from `dg.TableColumnConstraints`.
_Avoid_: pill, chip

### Configuration

**Setting**: One configurable value, resolved in order: the `dy_asset` argument, then `DAGSTER_DATAFRAMELY_*`, then the package default.
`quarantine_dir` skips the first, because it takes no argument.
_Avoid_: knob, option

### Naming

**Reserved namespace**: Three, and the third is unlike the other two.
`dy_` for every check name, rule column and check-result metadata key: a check name becomes an op output, which Dagster validates against `^[A-Za-z0-9_]+$`, and the metadata beside it follows the name.
`dataframely/` for every materialization metadata key, which parallels `dagster/` and has no such limit.
Those two split because Dagster forces them apart, and both name what this package generates, so nothing else can claim them.
`<name>_quarantine` is the third, the asset key a quarantine is addressed by.
It sits in Dagster's own key space, which the user shares, so it is the one reservation somebody else can take first.
All three hardcoded, never configurable.

**Named for its product**: A function that returns a value is named after the value, not after what it does.
`check_specs` returns check specs, `quarantine_frame` returns the quarantine frame, `delegating_writer` returns a writer.
Where the product has no name, name it rather than reaching for a verb: `owned_rule` returns an `OwnedRule` and `separated_return` a `SeparatedReturn`, so the function is its record's own name in snake case.
A verb name says the function returns nothing, so `validate_quarantine_key` either raises or passes.
No prefix: the annotation carries the type, and the prefix is a word the reader skips.
The identifier only.
D401 keeps every summary imperative, so `owned_rule` still opens "Return the column a rule belongs to".
_Avoid_: get_*, build_*, make_*, compute_*

**Participle for a transformer**: A function handed a thing that hands back the same thing changed is named by the participle of what changed.
`_addressed` returns check results carrying the address, `_suffixed` returns key parts with the last suffixed, `_checked` returns a value that passed the setting's vocabulary.
Where only part of what it was handed changes, the participle overclaims.
Borrow Polars' `with_*` instead: `with_returned_fields` hands back the same results with three fields on one of them.
_Avoid_: apply_*, add_*, enrich_*

## Coinages to avoid

These name nothing in Dagster, Dataframely or Polars.
Each hides what the code does behind a word the reader has to learn first, so say what happens instead.

**split**, **halves**: `Schema.filter` separates valid rows from invalid rows, and those are the words for its two results.
The ban is on the coined noun, not the ordinary verb: "Dagster forces the split" above is fine.

**exit**: name the outcome.
No rows failed; rows failed and no quarantine is declared; no rows survived; the decorated function returned `None`.

**phase**: name the step.
The column-schema check runs, then `Schema.filter`.

**surface**: name the place.
The Columns tab, the check name, the check description.
"Public surface" for an export list is ordinary English and stays.

**shape**: banned above for column schema, and equally for a `_Setting` subclass or a kind of rule or constraint.

**green**, **red**: say what happened.
The run succeeds, the run fails, the check passes, the check fails.
The colours are the Dagster UI's rendering of those facts, not the facts.
