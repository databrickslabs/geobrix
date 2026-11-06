# gbx:uc:tests

Run the Unity Catalog tests on Databricks: the UC oracle slice and the example files, on a SQL warehouse. Without `--against` it is the gated run that writes `evidence.json`. With `--against SCHEMA` it runs on an install that already exists.

**Usage:** `bash scripts/commands/gbx-uc-tests.sh --profile <name> [OPTIONS]`

**Options:**
- `--profile NAME` - Databricks profile. Required, never defaulted.
- `--scope smoke|core|full` - what to run. Default `core`.
  - `smoke` runs 12 slice statements and example files 00 and 01. It needs `--against`.
  - `core` runs the grid and vector oracle tables and all example files.
  - `full` adds the raster tables to `core`.
- `--against SCHEMA` - run the scope against the existing install in `SCHEMA`. No build, stage, install or cleanup. It writes `evidence-adhoc.json`, never `evidence.json`.
- `--warehouse-id ID` - SQL warehouse. Default `GBX_UC_WAREHOUSE_ID` in `examples/tests/databricks_cluster_config.env`.
- `--catalog C` - catalog of the schema. Default `geospatial_docs`.
- `--volroot DIR` - Volume folder that holds `uc-it/<sha12>/`. Default `/Volumes/geospatial_docs/geobrix/sample-data`.
- `--batch N` - value cases per warehouse statement. Default `25`; `1` runs every case alone. The warehouse pays about 10 s per statement that calls a UC Python function, whatever it computes, so the cases of a batch share one statement (one row, one column per case). A case that must be refused (an expected error aborts a statement) and a recipe always run alone. A batch that fails as a whole is rerun case by case, so a failure still names its case. Every case is judged on its own, as without batching.
- `--concurrency N` - warehouse statements in flight at once, per slice session. Default `16`.
- `--shards N` - separate slice sessions (processes). Default `1`; each session batches only its own share of the cases, so more sessions mean smaller batches.
- `--example-files-parallel N` - example files run side by side. Default `6`. File `00` (the setup that creates the tables the others read) runs first, alone; the statements of one file stay in order and the files' text is not batched.
- `--rebuild` - build the wheel even when `dist/env6` holds one that fits HEAD.
- `--keep` - keep the schema and the Volume folder (gated run).
- `--install-timeout-min N` - time limit of the install job. Default `90`.
- `--dry-run` - print the plan and the statement counts. No build, no workspace call.
- `--log <path>` - tee output to `test-logs/<path>`.
- `--help, -h` - show help.

**Examples:**
- `bash scripts/commands/gbx-uc-tests.sh --profile oauth-fe`
- `bash scripts/commands/gbx-uc-tests.sh --profile oauth-fe --scope full`
- `bash scripts/commands/gbx-uc-tests.sh --profile oauth-fe --scope smoke --against geobrix_uc`

## Gated run steps

- Refuse when a tracked file under the UC sources differs from HEAD.
- Reuse the wheel in `dist/env6` when no wheel input changed since its commit. Otherwise build the JAR and the env 6 wheel (needs Docker).
- Stage the wheel and its JAR alone into `<volroot>/uc-it/<sha12>/` and verify both sizes by listing.
- Install with `gbx.uc.install(catalog, "geobrix_uc_it_<sha12>", folder, procedures=True)` from a Serverless environment 6 job.
- Run the scope (`bench.oracles.uc_sql`, judged by `check_uc_results`) in parallel shards and `gbx:examples:uc-sql` against the schema.
- Drop the schema and delete the Volume folder and the workspace folder, unless `--keep`.

## Evidence

- File: `test-logs/uc-integration/evidence.json` (the logs of the run are in `test-logs/uc-integration/<sha12>/`).
- Fields: `head_sha`, `sources_hash`, `scope`, `started`, `finished`, `profile`, `warehouse_id`, `schema`, `job_run_ids`, `slice`, `examples`, `result`.
- `result` is `pass` when the slice has no failure outside the known-failures file and every example statement succeeded.
- Known failures: `scripts/ci/uc_integration_known_failures.txt`, one `<case id>: <reason>` per line. It is empty.
- An `--against` run writes `evidence-adhoc.json` with `"adhoc": true`. The QC check never reads it.
- The QC check `uc-integration-evidence` reads `evidence.json` on `gh pr create` and on a push to main. It compares `sources_hash` with `scripts/ci/uc_sources_hash.py`.
- Release rule: when the range changes the release version, the check needs `scope` `full`. Otherwise `core` or `full` passes. The version is `<version>` in `pom.xml` and `__version__` in `python/geobrix/src/databricks/labs/gbx/__init__.py`.

## Limits

- Dev-branch pushes (`streamline/*`, `beta/*`) are not gated.
- Warehouse time is bounded by the warehouse, not by the client: a shared X-Small, one-cluster warehouse (the default) ran `core` in about 35 minutes (366 cases as 122 statements plus 156 example statements; the same scope took about 70 minutes before batching). `full` is 1,510 cases as 509 statements; expect well over an hour on that warehouse, since 433 of them are refusals that cannot share a statement. A larger or multi-cluster warehouse (`--warehouse-id`) is the lever for a faster run; the example setup file (00) runs first and alone, so it is the floor of the examples leg.
- Auth is the caller's job: run `bash ~/.claude/hooks/databricks-auth-status.sh PreDispatch` first.
