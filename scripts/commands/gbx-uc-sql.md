# gbx:uc:sql

Run a file of `-- cid: <id>` statements on a SQL warehouse through the Statement API, time each one, and write what came back. It is the warehouse runner for the Unity Catalog function oracle: `bench.oracles.uc_sql` renders one `SELECT` per UC case (so a refusal in one case cannot hide another), and `bench.oracles.check_uc_results` judges the file `--out` writes.

Each statement runs `--repeat` times and every wall time is recorded; the value (or the error message) comes from the first run. BINARY cells arrive base64 in the API's JSON and are decoded to bytes, and a tile struct's `raster` field too. Any `Looking in indexes: ...` line is scrubbed from error text before it is printed or written.

**Usage:** `bash scripts/commands/gbx-uc-sql.sh --file <sql> --profile <name> [--warehouse-id ID] [--catalog C --schema S] [--out <json>] [--repeat N]`

**Options:**
- `--file <sql>` — statements, each after a `-- cid: <id>` line (required)
- `--profile <name>` — Databricks config profile (required; it is never chosen for you, see `databricks auth profiles`)
- `--warehouse-id ID` — SQL warehouse; default is `GBX_UC_WAREHOUSE_ID` from `examples/tests/databricks_cluster_config.env` (that one key is read, the file is never sourced)
- `--catalog C`, `--schema S` — default catalog and schema for the statements
- `--out <json>` — write `{cid: {"value": ..., "wall_s": [...]} | {"error": ...}}` here (rewritten after every statement, so a failure part-way keeps the results so far; bytes are stored as `{"$bytes": "<base64>"}`; `bench.oracles.load_uc_results` reads them back)
- `--repeat N` — runs per statement (default 1)
- `--timeout-s N` — cancel a statement still running after N seconds and record a timeout error for it (default 600)
- `--no-decode-binary` — keep BINARY cells as base64 text
- `--dry-run` — parse the file and print the plan: no workspace call, no auth
- `--log <path>` — tee output to `test-logs/<path>`
- `--help, -h` — show help

**Interpreter:** the workspace launcher needs `databricks-sdk` and Python 3.11+, chosen by `gbx_python_with_sdk` in `common.sh` (see `gbx:data:stage-jar`). `GBX_PYRX_VENV=<dir>` moves the venv.

**Example:**
- `bash scripts/commands/gbx-uc-sql.sh --profile oauth-fe --file out/mvp_s.sql --out out/mvp_s.json --repeat 3`
