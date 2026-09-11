# 7. The quarantine's key is checked at run time, not at load

Accepted, 2026-09-10. Follows [ADR-0006](0006-the-quarantine-is-written-by-the-assets-own-io-manager.md), which made the asset key the quarantine's whole address.

## Context

Declaring `quarantine=True` on an asset whose code location already held an asset keyed `<name>_quarantine` was silent data loss (#114). The invalid rows were written first and the colliding asset overwrote them. The run succeeded and nothing said a thing.

The sibling-out shape ADR-0004 replaced caught this by accident. Two outs with one key was a definition-time duplicate, so Dagster refused the definition. Moving the quarantine out of the graph removed that protection without replacing it.

The package reserves `dy_` for check names and `dataframely/` for metadata keys. Since ADR-0006 it also reserves the `<name>_quarantine` leaf in Dagster's asset key space, and that reservation differs in kind. The first two name what the package generates, so nothing else can claim them. The key space belongs to the user, who can claim the name first.

Dagster's own duplicate-key check already covers the declared half. `Definitions.get_repository_def` raises `DagsterInvalidDefinitionError: Duplicate asset key` when a `build_quarantine_spec` spec sits beside a user asset of that key. What it cannot see is the quarantine nobody declared, because that quarantine is a file and not an asset (ADR-0004).

## Decision

**A run of a quarantined asset fails before its body when an executable asset owns the quarantine's key.**

`validate_quarantine_key` lives in `_quarantine.py`, beside the suffix that decides the name, and is exported through `wiring` so a hand-wired asset can call what the decorator calls. The decorator calls it in `compute`, next to the `_quarantine_writer(context)` build, which is where the context is in hand.

**Before the body, on every run, not when the rows arrive.** #115 deferred the route pick until there are invalid rows, because the route is a property of the rows. The key is not. It is a property of the declaration, so waiting would hold a naming mistake until the first bad row and then spend it as a run failure where a `WARN` was expected. Checking first also spends no compute on a body whose output has nowhere to go.

**The keys come from the run's own graph.** `repository_def.asset_graph` answers when the run has one, which is every run launched from a code location. Otherwise `job_def.asset_layer.asset_graph` answers, which holds the whole job in process. A user who reproduces this in their own `dg.materialize` test gets the error there rather than after deploying.

**Only executable keys contend.** An asset Dagster can materialize is the only thing that can overwrite the quarantine. A spec from `build_quarantine_spec` is unexecutable, so it is exempt by Dagster's own distinction, with no marker and no third member of the reserved namespace. It stands for the quarantine rather than competing with it.

**The error names both assets and every way out.** `QuarantineKeyCollisionError` takes the two rendered keys and builds its own message, as the rest of the family does. It names the quarantined asset, the asset holding the key, the key itself, and three fixes: rename that asset, drop `quarantine=True`, or declare `build_quarantine_spec` if the asset was a hand-rolled quarantine table all along.

## Evidence

Probed on dagster 1.13.20.

`dg.Definitions(...)` construction is silent. `get_repository_def()` raises `Duplicate asset key` for a `build_quarantine_spec` spec beside a user asset of that key.

`repository_def` raises rather than answering `None` when it is unset. Under `dg.materialize` and `execute_in_process` it raises `CheckError: No repository definition was set on the step context`. Under direct invocation it raises `DagsterInvalidPropertyError`, which `_chosen_writer` already routes on. A run reconstructed from a code location gets a real one through `ReconstructableJob.get_repository_definition`, which is read in upstream's source here rather than executed.

The job-scoped graph narrows with the selection:

```text
materialize([probe, pq])            -> ['probe', 'probe_quarantine']
materialize([probe, pq], [probe])   -> ['probe']
```

A spec from `build_quarantine_spec` lands in `unexecutable_asset_keys` and `external_asset_keys`, and its asset lands in `executable_asset_keys`.

## Consequences

**An eighth private Dagster import.** `CheckError` comes from `dagster._check`, which re-exports `dagster_shared`'s, so the guard stays inside a declared dependency and `pyproject.toml` does not move. It joins the characterization tests with the other seven. `repository_def`, `RepositoryDefinition.asset_graph` and `JobDefinition.asset_layer` are undecorated properties on public classes, so they are pinned there too.

**Coverage is complete where the data is real.** A run launched from a code location sees every key in it. In process, the check sees only what the job holds, so a subsetted `dg.materialize` is blind. Where it is blind the data at risk is a fixture.

**An external asset keyed `<name>_quarantine` is still overwritten.** A spec standing for a table something outside Dagster writes is unexecutable, so it is exempt along with the quarantine's own spec. The package cannot tell those two apart without a marker, and the marker was rejected below. Whoever hits this should open a ticket.

**A hand-wired asset is not covered unless it asks.** `validate_quarantine_key` is exported, and calling it is the hand-wirer's to do, as building the writer already is.

**Direct invocation checks nothing.** There is no graph to read, and `file_writer` puts the rows under `quarantine_dir` where no asset key resolves.

## Alternatives rejected

**A validation over the assembled `dg.Definitions`.** It fails at load, which is where a naming collision belongs, sees every key, needs no private API and is testable with no run at all. Rejected because Dagster registers no third-party load-time validators, so it can only be a function the user remembers to call, and a guard nobody calls protects nobody. `Definitions.validate_loadable` is upstream's own surface of this shape and it is user-called too.

**Emit the quarantine spec always.** The decorator would declare the quarantine, and Dagster's existing duplicate-key check would refuse the code location at load with no package check at all. Rejected because it reverses ADR-0004: graph presence is the user's declaration, and every quarantined asset would grow a permanent node that never receives a materialization event. It also breaks the decorator's return, which is one `AssetsDefinition`.

**Both surfaces over one predicate.** Rejected: two surfaces, two test suites and two docs entries for one bug.

**Check only when invalid rows arrive.** It follows #115 exactly and costs a clean run nothing. Rejected on the timing argued above.

**Warn on every run and raise before the write.** Rejected because a log line in a run that succeeded is the easiest thing in Dagster to miss, and the package has no other log-only failure.

**A marker on the spec, so every key contends.** `build_quarantine_spec` would stamp a reserved metadata key that the check reads as consent, which closes the external-asset gap. Rejected because Dagster's executable split already separates the two, and a marker would add a third member to the reserved namespace and fail a spec somebody assembled by hand.

**The check inside `delegating_writer`.** It would cover hand-wirers for free. Rejected because the writer is built when the rows arrive, which is the timing already rejected, and because it gives `delegating_writer` a second reason to raise beside the one `_chosen_writer` routes on.
