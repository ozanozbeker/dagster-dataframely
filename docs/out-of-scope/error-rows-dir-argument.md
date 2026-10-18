# An `error_rows_dir` Argument

`dd.asset` has no argument that sets `error_rows_dir`.
Only `DAGSTER_DATAFRAMELY_ERROR_ROWS_DIR` sets it, so in a direct invocation every asset in the process writes its error rows under the same directory.

## Why this is out of scope

Proposed as case 3 of [#111](https://github.com/ozanozbeker/dagster-dataframely/issues/111) on 2026-09-02.
The maintainer rejected it on 2026-10-18 and chose cases 1 and 2, `error_rows_key` and `error_rows_io_manager_key`, for 1.0.

### Only a direct invocation reads `error_rows_dir`

`file_writer` writes under `error_rows_dir` only when there is no step.
A run passes the error rows to an IO manager, so a per-asset directory would change only where a test writes its error rows.
A test that needs its own directory sets the variable, for example with pytest's `monkeypatch.setenv`.

### `error_rows_io_manager_key` sets the directory in a run

To write the error rows under another directory in a run, bind a `UPathIOManager` with that `base_dir` to a resource key, and pass the key as `error_rows_io_manager_key`.
The same keyword sends a warehouse asset's error rows to a parquet file, which a directory argument cannot do.

### A directory means nothing to a warehouse

ADR-0006 rejected a configured directory for a run, because a directory cannot place the error rows beside a warehouse table.
A directory argument on `dd.asset` would be its only filesystem-only argument.
