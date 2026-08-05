# dagster-dataframely

Dataframely plugin for Dagster.

> [!WARNING]
> **Pre-release placeholder.**
> This package is under active design and ships no functionality yet — the name is reserved on PyPI while the design spec is finalized.
> Do not depend on it.
> Follow [issue #1](https://github.com/ozanozbeker/dagster-dataframely/issues/1) for progress.

## What it will do

[dataframely](https://github.com/Quantco/dataframely) declares schemas and validation rules for [polars](https://pola.rs) data frames.
[Dagster](https://dagster.io) has first-class surfaces for data contracts — column schema metadata and asset checks.
This package connects the two:

- **Asset checks derived from your schema.**
  Each dataframely rule becomes a Dagster asset check, so per-rule pass/fail history shows up in the catalog without hand-writing checks.
- **Column schema metadata.**
  Your schema populates Dagster's Columns tab automatically.
- **Quarantine, opt-in.**
  Declare a second output and failing rows are routed there with per-rule attribution instead of failing the run.
- **IO-manager agnostic.**
  The package ships no IO manager, so `dagster-polars`, Delta, DuckDB, or a custom one all work unchanged.

Dependencies are `dagster` and `dataframely` (plus `polars`) only.

## Installation

```bash
uv add dagster-dataframely
```

Requires Python 3.12+.

## License

[Apache-2.0](LICENSE)
