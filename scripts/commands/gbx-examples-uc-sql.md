# gbx:examples:uc-sql

Runs the Unity Catalog SQL examples in `examples/uc-functions/` on a SQL warehouse, and can import them into your workspace. A statement must succeed, and a query must return a row.

**Usage:** `bash scripts/commands/gbx-examples-uc-sql.sh --profile <name> [options]`

**Options:**
- `--profile <name>` - Databricks config profile (required; never chosen for you, see `databricks auth profiles`)
- `--warehouse-id ID` - SQL warehouse; default is `GBX_UC_WAREHOUSE_ID` from `examples/tests/databricks_cluster_config.env`. The run looks the warehouse up first and stops with a Fix if it is deleted.
- `--catalog C`, `--schema S` - where the functions are installed (default `geospatial_docs`, `geobrix_uc`)
- `--dir <folder>` - folder of example files (default `examples/uc-functions`)
- `--files <prefixes>` - comma list of file name prefixes to run, such as `00,05` (default all, in name order)
- `--out-dir <folder>` - folder for the per-file result JSON (default `test-logs/uc-examples`)
- `--timeout-s N` - cancel a statement still running after N seconds (default 600)
- `--parallel N` - run N example files side by side (default 1). File `00` (the setup) runs first, alone; the statements of a file stay in order.
- `--upload` - after a clean run, import the files into your workspace folder
- `--upload-only` - import the files and run nothing
- `--ws-dir <path>` - workspace folder to import into (default below)
- `--dry-run` - parse the files and print the plan; no workspace call, no auth
- `--log <path>` - tee output to `test-logs/<path>`
- `--help, -h` - show help

**Run order:** `00_setup.sql` builds the sample tables the other files read, so run it first. A full run takes about 25 minutes, because each statement that calls a Python function costs about 10 s on a warehouse.

**Upload:** the folder is `/Workspace/Users/<you>/geobrix/uc-function-examples/`. The user comes from `databricks current-user me` for `--profile`. Each file is a workspace file that opens in the SQL editor. The upload rewrites the catalog and schema names to `--catalog` and `--schema`. The command lists the folder afterwards and fails if a file is missing.

**Exit code:** 0 when every statement succeeded and every query returned a row. 1 for a failed statement, an empty query or a failed upload. 2 for a bad option.

**Install first:** the functions must be in the schema. See `docs/docs/api/uc-functions.mdx`.

**Interpreter:** the launcher needs `databricks-sdk` and Python 3.11+, chosen by `gbx_python_with_sdk` in `common.sh` (see `gbx:data:stage-jar`).

**Example:**
- `bash scripts/commands/gbx-examples-uc-sql.sh --profile oauth-fe --upload`
