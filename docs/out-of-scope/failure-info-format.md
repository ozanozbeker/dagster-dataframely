# The Error Rows in `FailureInfo`'s Format

The error rows are not in the format `FailureInfo.write_parquet` writes, so `FailureInfo.read_parquet` and `FailureInfo.scan_parquet` cannot read them.

## Why this is out of scope

Both read the names of the rule columns from the parquet metadata key `rule_columns`.
The package passes the error rows to the asset's IO manager, which can write them to a database table.
A database table has no parquet metadata to hold that key.
