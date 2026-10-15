# A Dagster Component

This package ships no Dagster Component.
`dd.asset` is the only way it builds an asset, and a `dg` project loads those assets as it loads any other asset defined in Python.

## Why this is out of scope

[#11](https://github.com/ozanozbeker/dagster-dataframely/issues/11) rejected a Component for v1 because of who would use it.
A YAML file can reference the decorated function and the schema only by import path.
So the asset's definition would be in two files, for people who already write Python.

[#48](https://github.com/ozanozbeker/dagster-dataframely/issues/48) asked whether any user gets something from the YAML that `dd.asset` does not give them.
It named three candidates.
The maintainer reviewed them on 2026-10-15, found that each already has what it needs, and closed #48.

### A `dg` project already loads `dd.asset` assets

The demo is a `dg` project.
Each folder under `defs/` has a `defs.yaml` that names `dagster.DefsFolderComponent`, which loads the Python modules beside it.
Its `post_processing` sets `group_name` on every asset in the folder:

```yaml
type: dagster.DefsFolderComponent
post_processing:
  assets:
    - target: "*"
      attributes:
        group_name: bronze
```

`dg list defs` in `demo/` lists 20 assets in the groups those files set: 5 in `bronze`, 12 in `silver` and 3 in `gold`.
So a project that starts from `dg` needs nothing from this package to load its assets or to set their attributes.

### The environment variables already apply per code location

Issue #48 proposed that a Component could set a code location's settings, which is the middle of the three sources.
That source is `DAGSTER_DATAFRAMELY_*`.
Each code location runs in its own process, so the environment of its deployment already sets a value for that code location alone.
A platform team that wants the same settings across many code locations sets the same variables in each deployment.

A Component cannot set them any better.
`dd.asset` resolves the settings when Python imports the module that calls it.
So the Component would have to write `os.environ` before that import, and the value would still apply to the whole process.
Reading the Component's YAML instead would add a fourth source, and `user_guide/settings.qmd` states that there is none.

Storage is an IO manager, which is a Dagster resource.
The demo binds it in `defs/resources.py`, and a shared module binds the same one in many code locations.

### Many assets over one schema need a loop, not YAML

A loop declares many similar assets over one schema, calling `dd.asset` once per name.
A YAML list of those names still needs the decorated function by import path, which is the objection #11 made.

A project that wants the YAML anyway can write its own Component.
Its `build_defs` calls `dd.asset` like any other code.
On Dagster 1.13.24, a Component whose `build_defs` calls `dd.asset` for each name in a list of two returns definitions that pass `dg.Definitions.validate_loadable`.

## If someone proposes this again

A user would reopen it by showing YAML they want to write and cannot load today, through `DefsFolderComponent` or a Component of their own.
That YAML has to hold something other than an import path to Python the user already wrote.

The request also has to justify a second public API.
The Component's YAML schema would have to change with every argument `dd.asset` adds or changes.

## Prior requests

- [#48](https://github.com/ozanozbeker/dagster-dataframely/issues/48), "A Dagster Component, if the audience objection clears", deferred from #15.
