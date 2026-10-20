# The Error Rows in `FailureInfo`'s Format

The error rows are not in the format `FailureInfo.write_parquet` writes, so `FailureInfo.read_parquet` and `FailureInfo.scan_parquet` cannot read them.

## Why this is out of scope

[#145](https://github.com/ozanozbeker/dagster-dataframely/issues/145) reported on 2026-10-18 that the guide did not give the reason.
The maintainer kept the format and closed the issue on 2026-10-19, when the guide added the reason below.

### A database table has no parquet metadata

Both functions read the names of the rule columns from the parquet metadata key `rule_columns`.
The package passes the error rows to the asset's IO manager, which can write them to a database table.
A database table has no parquet metadata to hold that key.

### The rule columns have check names

Each rule column has its rule's check name, such as `dy_rule__amount__min`, not the name Dataframely gives the rule, such as `amount|min`.
So a rule column and the rule's asset check share one name.

## Prior requests

- [#145](https://github.com/ozanozbeker/dagster-dataframely/issues/145), "The `filter=True` page does not say why the error rows are not in `FailureInfo.write_parquet`'s format".
