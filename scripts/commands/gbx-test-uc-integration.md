# gbx:test:uc-integration

Install the Unity Catalog functions into a scratch schema on Databricks. Run the UC oracle slice and the example files against it, then write an evidence file. Run it before a pull request or a push to main.

**Usage:** `bash scripts/commands/gbx-test-uc-integration.sh --profile <name> [OPTIONS]`

**Options:**
- `--profile NAME` - Databricks profile. Required, never defaulted.
- `--warehouse-id ID` - SQL warehouse. Default `GBX_UC_WAREHOUSE_ID` in `examples/tests/databricks_cluster_config.env`.
- `--catalog C` - catalog of the scratch schema. Default `geospatial_docs`.
- `--volroot DIR` - Volume folder that holds `uc-it/<sha12>/`. Default `/Volumes/geospatial_docs/geobrix/sample-data`.
- `--slice core|full` - oracle tables. `core` is the grid and vector tables (default). `full` adds the raster tables.
- `--shards N` - parallel warehouse sessions for the slice. Default `8`.
- `--rebuild` - build the wheel even when `dist/env6` holds one that fits HEAD.
- `--keep` - keep the schema and the Volume folder.
- `--install-timeout-min N` - time limit of the install job. Default `90`.
- `--smoke SCHEMA` - wiring check on an existing install. No build, stage, install or cleanup. It runs 12 slice statements and example files 00 and 01, and writes `evidence-smoke.json`, never `evidence.json`.
- `--dry-run` - print the plan and the statement counts. No build, no workspace call.
- `--log <path>` - tee output to `test-logs/<path>`.
- `--help, -h` - show help.

**Example:** `bash scripts/commands/gbx-test-uc-integration.sh --profile oauth-fe`

## Steps

- Refuse when a tracked file under the UC sources differs from HEAD.
- Reuse the wheel in `dist/env6` when no wheel input changed since its commit. Otherwise build the JAR and the env 6 wheel (needs Docker).
- Stage the wheel and its JAR alone into `<volroot>/uc-it/<sha12>/` and verify both sizes by listing.
- Install with `gbx.uc.install(catalog, "geobrix_uc_it_<sha12>", folder, procedures=True)` from a Serverless environment 6 job.
- Run the slice (`bench.oracles.uc_sql`, judged by `check_uc_results`) in parallel shards and `gbx:examples:uc-sql` against the schema.
- Drop the schema and delete the Volume folder and the workspace folder, unless `--keep`.

## Evidence

- File: `test-logs/uc-integration/evidence.json` (the logs of the run are in `test-logs/uc-integration/<sha12>/`).
- Fields: `head_sha`, `sources_hash`, `started`, `finished`, `profile`, `warehouse_id`, `schema`, `job_run_ids`, `slice`, `examples`, `result`.
- `result` is `pass` when the slice has no failure outside the known-failures file and every example statement succeeded.
- Known failures: `scripts/ci/uc_integration_known_failures.txt`, one `<case id>: <reason>` per line. It is empty.
- The QC check `uc-integration-evidence` reads the file on `gh pr create` and on a push to main. It compares `sources_hash` with `scripts/ci/uc_sources_hash.py`.

## Limits

- Dev-branch pushes (`streamline/*`, `beta/*`) are not gated.
- Expect about 30 minutes for the `core` slice. The `full` slice runs about 1,290 statements and takes much longer.
- Auth is the caller's job: run `bash ~/.claude/hooks/databricks-auth-status.sh PreDispatch` first.
