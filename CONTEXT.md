# `dagster-dataframely`

`dagster-dataframely` attaches a Dataframely schema to a Dagster asset.
One declaration fills the Columns tab, reports every rule as an asset check, and sets what happens to rows that fail.

Where Dagster, Dataframely or Polars already has a word for something, use that word, in prose and in identifiers.
The terms below are the ones this package had to add, plus a few that earlier prose used wrongly.
_Avoid_ lists words not to use **for the term above it**.
The rule covers the meaning, not the string: `sink_parquet` is Polars' own name and not a writer, so the rule allows it.

This is a glossary.
The reason a thing works the way it does belongs in `docs/pre-1.0.md` or in the module that does it.

## Language

### The asset

**`dd.asset`**: The decorator, which turns the function it decorates into an asset validated against a schema.
Dagster owns this word, and `dd.asset` builds a `dg.asset` with a schema attached, by calling `@dg.asset`.
Dataframely names its types the same way: `dy.DataFrame` is a `pl.DataFrame` with a schema, as `dd.asset` is a `dg.asset` with one.
**Always written `dd.asset`, never bare**, in prose and in an example, because bare `asset` reads as Dagster's.
ADR-0002, ADR-0005 and ADR-0008 call it `dataframely_asset` or `dy_asset`, its names at the time.
_Avoid_: dataframely_asset, dy_asset, door, front door

**Decorated function**: The function `dd.asset` wraps.
Dagster passes upstream assets to it as parameters, it may declare `context`, and it returns the frame to validate, a `dg.MaterializeResult` whose `value` is that frame, or `None` to skip.
Dagster uses the same phrase.
It names the function by its relation to the decorator, not by what it does.
_Avoid_: transform, compute function (Dagster's, but there it names the wrapper `dd.asset` builds)

**Valid rows**: The rows that passed every rule, which `Schema.filter` returns first.
The asset materializes them, plus the rows that failed only `"warn"` rules.
_Avoid_: good rows, kept rows, survivors

**Invalid rows**: The rows that failed at least one rule, which `FailureInfo.invalid()` returns.
Dataframely uses this word, and its rule columns report these rows.
_Avoid_: bad rows, rejected rows, held-back rows

**Failure**: One row failing one rule, which is what `FailureInfo.counts()` counts.
The per-check counts can add up to more than the number of invalid rows, because a row that fails three rules is three failures and one invalid row.
_Avoid_: rejection, violation

**Outcome**: How a run ended.
The word has only this meaning: a rule column's `valid`, `invalid` or `unknown` is not an outcome, and neither is a check's result.
Say what happened: no rows failed; an `"error"` rule failed and the asset has `filter=False`; every row failed an `"error"` rule; the decorated function returned `None`.
_Avoid_: exit, the rule-column meaning, "check outcome"

**Skip**: What happens when the decorated function returns `None`.
The package validates nothing, nothing materializes, the partition stays unmaterialized, and the run succeeds.
Dagster has no noun for it, only `output_required=False` and "the function can conditionally not `yield` a result".
_Avoid_: empty run, no-op, conditional materialization

**Error rows**: The rows that failed at least one `"error"` rule, which a writer writes under the error-rows key when the asset has `filter=True`.
They are not an executable asset: they have no step of their own.
`error_rows_spec` adds them to the asset graph, and then `delegating_writer` logs a materialization for the error-rows key after each write.
With `filter=True`, a run writes the other rows to the asset's table, so the two tables never share a row.
With `filter=False`, a row that fails an `"error"` rule fails the run, and the package writes nothing.
ADR-0004, ADR-0006 and ADR-0007 call it the quarantine, and the argument `quarantine`, their names at the time.
_Avoid_: quarantine, invalid rows (Dataframely's, and it includes rows that failed only `"warn"` rules), bad rows, dead-letter asset

**Writer**: A function that writes the error rows and returns the error-rows address.
`validation_results` receives only that address.
`delegating_writer` passes the rows to the asset's own IO manager, or to the one `error_rows_io_manager_key` names.
`file_writer` writes a parquet file under `error_rows_dir`.
Name the one you mean.
_Avoid_: sink, emitter, exporter, router, dispatcher, resolver, and **the fallback** for `file_writer`

**Error-rows address**: Where a writer wrote the error rows, as a string: an asset key from `delegating_writer`, or a file path from `file_writer`.
It is a string because the rows can be in a database table.
Dagster uses the word address, in `TableMetadataSet.extract_storage_address`.
This package reports the error-rows address and never the location of the asset's table.
_Avoid_: location, destination, and `path`, which assumes a filesystem

**Error-rows key**: The asset key a writer writes the error rows under.
It is `<name>__error_rows` with the asset's key prefix, unless `error_rows_key` sets another.
`delegating_writer` passes it to the IO manager, and `file_writer` turns it into a path under `error_rows_dir`.
_Avoid_: error-rows asset, because nothing executes the error rows; say _the error rows' spec_ for the `dg.AssetSpec`

**`error_rows_dir`**: The directory `file_writer` writes under.
The error-rows key and the partition key form the rest of the file path.
Set by `DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR` and nothing else.
_Avoid_: root, base dir, output dir

**Rule column**: A column of the error rows with one rule's result per row: `valid`, `invalid` or `unknown`.
Dataframely uses this term, in `FailureInfo.details()`.
_Avoid_: outcome column

**Step**: Dagster's unit of execution, one node of the run's execution plan.
Only a run has one, which is how the package tells a run from a direct invocation.
Never a part of this package's own work: the column-schema check and `Schema.filter` are two calls, not two steps.
_Avoid_: execution context (Dagster's, but there it names `context` itself), and "step" or "stage" for this package's calls

**Direct invocation**: Calling a decorated asset instead of running it, which is Dagster's documented unit-testing path and its own phrase.
It is the only case with no step, which is why `file_writer` and `error_rows_dir` exist.
_Avoid_: "test", which is one use of it, not what it is

**`DirectInvocationResult`**: The asset's materialization, or `None` on a skip, and every check result keyed by check name, which `dd.testing.direct_invocation_result` returns from what a direct invocation yields.
Dagster names its records this way: `dg.materialize` returns an `ExecuteInProcessResult`.
_Avoid_: outcome (how a run ended), events, invoke result, test result

**Hand-wiring**: Building a `@dg.asset` from `dd.wiring` instead of using `dd.asset`.
_Avoid_: the kit, hand-wirer (write "you", or name the function called)

**`validation_results`**: What an asset built with `dd.asset` runs after its decorated function.
A hand-wired asset calls it directly.
ADR-0001 through ADR-0004 call it `process`, its name at the time.

**Code version**: Dagster's `code_version`, which `dd.asset` derives from the schema when the decorator gets none.
Dagster uses this word, and dagster-dbt derives one from a model in the same way.
_Avoid_: schema hash, fingerprint, schema version

### Validation

**Column schema**: A frame's columns and their dtypes, which is what `frame.collect_schema()` returns.
Dagster uses this name, in the `dagster/column_schema` key this package already writes.
A frame whose column schema differs from the schema's is a bug in the pipeline, not bad data.
_Avoid_: shape (Polars' `.shape` is a row and column count), gate

**Column-schema check**: The blocking asset check `dy_schema__columns`, which compares the frame's column schema with the schema's.
When it fails, the run stops before `Schema.filter` runs.
It is not a rule, and it reports on its own at every granularity.

**Rule**: One Dataframely validation rule, under the name Dataframely gives it.

**Column rule**: A rule on one column, keyed `<column>|<rule name>`.
Dataframely uses this word, in the `column_rules` it builds as `f"{col_name}|{rule_name}"`.

**Rule name**: The part after the `|`.
For example, `amount|min` has the rule name `min`.
Dataframely uses this word, on that same line.
It differs from the column argument Dataframely generated the rule from for four arguments: `nullable=False` generates `nullability`, `allow_inf=False` generates `inf`, `allow_nan=False` generates `nan`, and `check={"lowercase": ...}` generates `check__lowercase`.
A `dy.List` or `dy.Array` adds `inner_` to each rule name of its inner column.
_Avoid_: kind (Dagster's `kinds=` is the asset's badges, and `dd.asset` forwards it), type, flavour

**Schema-level rule**: A rule that belongs to no single column: a `@dy.rule()`, and `primary_key`.
Dataframely uses this phrase, in `_schema_validation_rules()` and in the warning it raises about them.
A single-column primary key is one of these, which is why the setting is `schema_rules`.
_Avoid_: multi-column rule, which a single-column primary key makes false; table rule

**Severity**: A rule's `"error"` or `"warn"`, which the schema declares and which is `"error"` when it does not.
It is the severity of the rule's failing check, as Dagster's `AssetCheckSeverity` and dbt's test `severity` use the word.
A row that fails an `"error"` rule is never written to the asset's table, and a row that fails only `"warn"` rules is.
_Avoid_: blocking rule, aborting rule, strict rule, level

**Collection filter**: A `@dy.filter` method of a Dataframely `Collection`.
It returns the common primary key of every row that passes it.
Dataframely uses this phrase, in the docstring of `Collection.filter`.
A collection filter has one asset check, `dy_filter__<name>`, on each member without `ignored_in_filters=True`.
Write "collection filter", never "filter" alone, because `filter=True` and `Schema.filter` use that word.
_Avoid_: filter, cross-member rule, foreign key

**`dd.collection_checks`**: The function that returns a Dataframely `Collection`'s asset checks, as one `dg.AssetChecksDefinition`.
Each member of the Collection is an asset the user declares, and the checks report each collection filter on those assets.
It reads every member's whole table and writes nothing.
_Avoid_: collection asset, collection door

**`DescribedRule`**: One rule with everything this package derives from it: its check name, its column and rule name, its docstring, and its expression.
_Avoid_: parsed rule, rule info, rule spec, enriched rule

**Rule set**: The rules one asset check reports for.
A check reports one rule at `rule` granularity, one column's rules at `column`, and every rule at `schema`.
_Avoid_: bucket, "member rule" (member is Dataframely's word for a Collection member)

**Granularity**: How many checks the package collapses the rules into, which `check_granularity` sets: one per rule at `rule`, one per column at `column`, one for the schema at `schema`.
Coined: Dagster's only `granularity` is an internal concurrency setting, and neither Dataframely nor Polars has one.
_Avoid_: level, resolution, detail

**Collapse**: Reducing several rules into a single asset check.
Always say what the package collapses: it collapses rules into checks, and a check that reports for several of them is a collapsed check.
_Avoid_: collapsing with no object, merge, group (which is Polars' word for a set of dtypes)

**Column constraint**: One condition a rule states, rendered for Dagster's Columns tab.
Dagster uses this name, in `dg.TableColumnConstraints`.
_Avoid_: pill, chip

### Configuration

**Setting**: One configurable value, resolved from three sources, each overriding the one before: the package default, then `DAGSTER_DATAFRAMELY_*`, then the `dd.asset` argument.
`error_rows_dir` has only the first two, because it takes no argument.
Say "the setting's sources", not "the settings chain".
_Avoid_: knob, dial, option, settings chain, house style

**Allowed values**: What a setting accepts, which is what `InvalidSettingError` prints.
_Avoid_: vocabulary

**Statistics**: The `skimr`-style summary of the written rows, in the materialization's metadata under `dataframely/table_statistics/<group>`.
Spelled out everywhere, including the metadata key.
_Avoid_: stats, profile, describe (Polars' `describe()` is a different thing this package does not use)

**Dtype group**: The set of dtypes that share one statistics table: `numeric`, `temporal`, `string`, `boolean`.
Polars uses this word, in `polars.datatypes.DataTypeGroup` and the `*_DTYPES` sets beside it.
_Avoid_: family, class, category

**Sample**: Real rows a run writes to the event log: at most `max_failure_samples` for a check, and at most `row_sample` for a materialization.
A sample is absent, never empty.
_Avoid_: preview, example, head

**Reserved namespace**: Three namespaces.
`dy_` starts every check name, every rule column and the `dy_rule` column of a collapsed check's sample, and nothing a user types.
So the decorator is `dd.asset`, not `dy_asset`.
`dataframely/` starts every metadata key this package names, on a materialization, a definition or a check result, as `dagster/` does for Dagster.
The valid row count and the column schema are the exceptions, because Dagster already has keys for them.
The row count is under `dagster/partition_row_count` on a partitioned asset and `dagster/row_count` otherwise, and the column schema is under `dagster/column_schema`.
`<name>__error_rows` is the default error-rows key.
Only this package generates names in the first two.
The third is in Dagster's asset key space, which the user also uses, so another asset can already have that key.
Namespace, never prefix, even where the string is one.
_Avoid_: prefix, reserved word

## Coinages to avoid

These words name nothing in Dagster, Dataframely or Polars.
Each one makes the reader learn a new word for something that already has a plain description, so describe what happens instead.

**split**, **halves**: `Schema.filter` separates valid rows from invalid rows, and those are the words for its two results.
The verb "split" in its ordinary sense is fine.

**exit**: name the outcome.

**phase**: name the call.
The column-schema check runs, then `Schema.filter` runs.
Not "step", which is Dagster's.

**surface**: name the place.
Write the Columns tab, the check name, the check description.
"Public surface" for an export list is a standard term and stays.

**shape**: banned above for column schema, and equally for a kind of setting, rule or constraint.

**spelling**: say "formatted" for how a partition key becomes part of a path.
That is `UPathIOManager`'s own word, in `_formatted_multipartitioned_path` and in the `formatted_partition_keys` it builds.
Say "form" or "syntax" for how you write a value or a name.
"Spelled out" is ordinary English and stays.

**green**, **red**: say what happened.
The run succeeds, the run fails, the check passes, the check fails.
The colours are how the Dagster UI shows those results, not the results.

### Figures of speech

Say what the code does instead.

| Avoid | Write |
| --- | --- |
| refuse, refusal | raise, reject; the error |
| decide | set, determine |
| know, learn, see | has, receives, reads |
| ask, ask for | call, check; declare a parameter |
| answer a spec or a check | yield a result for |
| say, tell, promise, of code or data | report, show, contain, guarantee |
| agree, disagree | match, differ |
| land, arrive, reach | is written to, is passed to, is returned, appears in |
| carry | has, contains; is copied onto |
| hand, hand back, hand over | pass, return |
| ride | is copied onto |
| route, for the choice of writer | which writer is used |
| held back, hold back, withhold | invalid rows; has invalid rows; does not write |
| survive, survivor | valid rows; is kept |
| orphan a check's history | start a new check history |
| sibling | another check on the same asset |
| die | fail |
| consent to partial data | `filter=True`: the run writes the other rows when some rows fail an `"error"` rule |
| evidence | record |
| verdict | result |
| culprit, offending | the failing column, rule or value |
| cost, buy, pay, free, cheap, earn | means, requires, lets, needs no extra work, justifies |
| borrow | use, reuse |
| win, lose, outrank, contend | take precedence, be overridden, conflict |
| dial, knob, lever | setting, parameter |
| gate | check, blocking check |
| seam | extension point |
| voice, register | wording |

### Retired coinages

| Avoid | Write |
| --- | --- |
| schema-backed asset | an asset built with `dd.asset`, or name the asset |
| quarantined asset | an asset with `filter=True` |
| valid table, valid asset, valid out | the asset's table; the valid rows |
| rule check | the rule's check; the asset check |
| arrangement | example |
| runtime-real | a real object, not a string |
| manager-blind | without knowing which IO manager |
| asset-key-addressed IO manager | an IO manager that stores by asset key |
| rule-bearing column | a column with rules |
| value-bearing statistic | a statistic that shows values from the data |

### Upstream words, in their upstream meaning only

| Word | Its only meaning here | For anything else, write |
| --- | --- | --- |
| member | a member of a Dataframely `Collection` | a rule in a collapsed check; a primary-key column |
| step | a Dagster step | the call's name |
| frame | a Polars frame | stack frame |
| materialize | Dagster's materialization | collect, for running a Polars plan |
| job | a Dagster job | what has to happen |
| rule | a Dataframely rule | convention, condition |
| group | a Dagster asset group, which `group_name` sets | dtype group, for Polars' set of dtypes |
