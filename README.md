# dagster-dataframely

Dataframely plugin for Dagster.

> [!WARNING]
> **Pre-release placeholder.**
> This package is under active design and ships no functionality yet.
> The name is reserved on PyPI while the design spec is finalized.
> Do not depend on it.
> Follow [issue #1](https://github.com/ozanozbeker/dagster-dataframely/issues/1) for progress.

## What it will do

[dataframely](https://github.com/Quantco/dataframely) declares schemas and validation rules for [polars](https://pola.rs) data frames.
[Dagster](https://dagster.io) has first-class surfaces for data contracts: column schema metadata and asset checks.
This package connects the two:

- **Asset checks derived from your schema.**
  Each dataframely rule becomes a Dagster asset check, so per-rule pass/fail history shows up in the catalog without hand-writing checks.
- **Column schema metadata.**
  Your schema populates Dagster's Columns tab automatically.
- **Quarantine, opt-in.**
  Declare a second output and failing rows are routed there with per-rule attribution instead of failing the run.
- **Storage in the box.**
  `DataframelyParquetIOManager` writes `.parquet` to a local directory or to `s3://`, `gs://` and `az://`, and it is the supported path.
- **CSV without the losses.**
  `DataframelyCSVIOManager` writes to the same places, and encodes the five dtypes a CSV cell cannot hold, so the frame you read back equals the frame you wrote.
- **Row samples, on by default.**
  A failing check shows five of the rows it rejected, and a materialization shows five of the rows it wrote.
  Read [Row samples are on by default](#row-samples-are-on-by-default) before you run this against data you care about.

Dependencies are `dagster` and `dataframely` (plus `polars`) only.
The IO manager imports `universal-pathlib` and `pydantic` directly, so both are declared too.
Both already ship with `dagster`, so nothing new lands in your environment.

## Row samples are on by default

> [!IMPORTANT]
> Two settings write **real rows of your data into the Dagster event log**, and both ship on.
> The event log is shared across a deployment, it is exportable, and nothing here is redacted.
> If a column holds an email address, a name or an account number, that value lands in the log and stays there.

| setting | what it writes | where | default |
| --- | --- | --- | --- |
| `max_failure_samples` | up to this many of the rows each rule rejected | the asset check reporting for that rule, under `dy_failed_sample` | `5` |
| `row_sample` | up to this many of the rows the asset wrote | the materialization, under `sample` | `5` |

dataframely's own comparable setting defaults to `0`, so this package is deliberately the more generous of the two.
The reason is that a red check raises exactly one question the counts cannot answer: not that `amount|min` rejected 43 rows, but what three of those rows held.
Paying for that in the event log should be a decision, which is what this section is for.

Setting either to `0` turns it off entirely, and the metadata key is then absent rather than empty.
Per asset:

```python
@dd.dataframely_asset(schema=Orders, max_failure_samples=0, row_sample=0)
def orders(raw_orders: pl.DataFrame) -> pl.DataFrame:
    return transform(raw_orders)
```

Or once for a whole code location, in the deployment's environment:

```bash
DAGSTER_DATAFRAMELY_MAX_FAILURE_SAMPLES=0
DAGSTER_DATAFRAMELY_ROW_SAMPLE=0
```

Every setting in this package resolves the same way: the package default, then the environment variable, then the argument on the asset.

The summary statistics on a materialization are a separate setting, `statistics`.
Turning the samples off leaves it on, and it carries no value-bearing statistic on string columns at any setting.
Consenting to summary statistics is not consenting to raw values.

## Installation

```bash
uv add dagster-dataframely
```

Requires Python 3.12+.

## License

[Apache-2.0](LICENSE)
