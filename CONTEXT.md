# dagster-dataframely

Attaches a dataframely schema to a Dagster asset, so one declaration fills the Columns tab, reports every rule as an asset check, and decides what a failing row costs.

Where a word exists in Dagster or dataframely already, that word wins.
The terms below are the ones this package had to add, plus the few it kept getting wrong.

## Language

### The asset

**Decorator**: `@dataframely_asset`, which turns a polars transform into an asset validated against a schema.
_Avoid_: door, front door

**Valid rows**: The rows `Schema.filter` kept.
They materialize as the asset's main output.
_Avoid_: good rows, the good table, the good out

**Invalid rows**: The rows `Schema.filter` removed, each having failed at least one rule.
_Avoid_: rejected rows, bad rows, failed rows

**Quarantine**: The sibling asset invalid rows materialize into.
Declaring one is the consent to partial data; leaving it undeclared is the refusal.
_Avoid_: reject table, dead-letter asset

**Rule column**: A column of the quarantine carrying one rule's outcome per row, reading `valid`, `invalid` or `unknown`. dataframely's own term, from `FailureInfo.details()`.
_Avoid_: outcome column

**Hand-wiring**: Building a `@dg.multi_asset` from the package's exported parts instead of using the decorator.
_Avoid_: the kit

### Validation

**Shape**: A frame's columns and their dtypes, against what the schema declares.
A mismatch is a pipeline defect, so it stops the run before any row is filtered and reports through a blocking check.
_Avoid_: gate, schema gate

**Rule**: One dataframely validation rule, under the name dataframely gives it.

**Rule set**: The rules one asset check reports for.
One rule at `rule` granularity, one column's rules at `column`, every rule at `schema`.
_Avoid_: bucket

**Collapse**: Reducing several rules into a single asset check, which is what `check_granularity` decides.

**Constraint**: One condition a rule states, rendered for Dagster's Columns tab.
_Avoid_: pill, chip, badge

### Storage

**Staging**: The local temporary file a lazy frame streams to before it is validated or promoted.
_Avoid_: landing, spill, scratch

**Promote**: Moving a staged file to its destination, once the plan that wrote it has succeeded.

**Carrier**: The definition-metadata entry that takes the live schema class from the asset to the IO manager.
_Avoid_: sidecar, schema handle

### Reporting

**Sample**: A bounded set of real rows copied into the Dagster event log, either of what a rule rejected or of what the asset wrote.

**Statistics**: The per-dtype-family summary a materialization carries, one table per family present in the frame.
_Avoid_: profile, skim

### Configuration

**Setting**: One configurable value, resolved through three tiers.
_Avoid_: knob, option

**Tier**: One level of a setting's resolution order: the argument on the asset, then `DAGSTER_DATAFRAMELY_*`, then the package default.

### Naming

**Reserved namespace**: The `dy_` prefix every check name, rule column and check-metadata key sits under.
Hardcoded, never configurable.
