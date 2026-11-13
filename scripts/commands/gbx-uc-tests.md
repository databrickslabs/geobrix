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
- `--warehouse-id ID` - SQL warehouse. Default `GBX_UC_GATE_WAREHOUSE_ID__<profile>` in `examples/tests/databricks_cluster_config.env`.
  - If that key is absent, the default is `GBX_UC_GATE_WAREHOUSE_ID`, then `GBX_UC_WAREHOUSE_ID`.
  - The ID is local config: it lives in that untracked file, never in a committed file.
- `--catalog C` - catalog of the schema. Default `GBX_GATE_CATALOG__<profile>`, else `geospatial_docs`.
- `--volroot DIR` - Volume folder that holds `uc-it/<sha12>/`. Default `GBX_GATE_VOLROOT__<profile>`, else `/Volumes/geospatial_docs/geobrix/sample-data`.
  - Before any build or stage, the run looks up the warehouse and the catalog in the workspace of `--profile`.
  - If either is missing there, the run stops with exit 1 and a Fix.
  - The run prints each value with its source, as `target: profile ...`.
- `--batch N` - value cases per warehouse statement. Default `25`; `1` runs every case alone. The warehouse pays about 10 s per statement that calls a UC Python function, whatever it computes. So the cases of a batch share one statement (one row, one column per case). A case that must be refused (an expected error aborts a statement) and a recipe always run alone. A batch that fails as a whole is rerun case by case, so a failure still names its case. Every case is judged on its own, as without batching.
- `--concurrency N` - warehouse statements in flight at once, per slice session. Default `16`.
- `--shards N` - separate slice sessions (processes). Default `1`; each session batches only its own share of the cases, so more sessions mean smaller batches.
- `--example-files-parallel N` - example files run side by side. Default `6`. File `00` (the setup that creates the tables the others read) runs first, alone. The statements of one file stay in order, and the files' text is not batched.
- `--rebuild` - build the wheel even when `dist/env6` holds one that fits HEAD.
- `--run-part I/N` - run part I of a gated run cut into N parts (N at least 2). See "Gated run in parts".
- `--drop-parts SHA12` - drop the schema and the folders that an unfinished parted run of commit SHA12 kept. Then stop.
- `--keep` - keep the schema and the Volume folder (gated run).
- `--install-into SCHEMA` - install into `SCHEMA` and keep it, for a demo. See "Install that stays".
- `--install-timeout-min N` - time limit of the install job. Default `90`.
- `--dry-run` - print the plan and the statement counts. No build, no workspace call.
- `--log <path>` - tee output to `test-logs/<path>`.
- `--help, -h` - show help.

**Examples:**
- `bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial`
- `bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial --scope full`
- `bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial --scope smoke --against geobrix_uc`
- `bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial --scope full --run-part 1/6`
- `bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial --install-into geobrix_uc`

## Gate workspace

- The gate workspace is the Geospatial Hub: profile `fevm-geospatial`, catalog `geospatial_catalog`.
- Set these keys once in your config, one set per profile (`-` becomes `_` in the profile name):
  - `GBX_GATE_CATALOG__fevm_geospatial=geospatial_catalog`
  - `GBX_UC_GATE_WAREHOUSE_ID__fevm_geospatial=<warehouse id in that workspace>`
  - `GBX_GATE_VOLROOT__fevm_geospatial=/Volumes/geospatial_catalog/<schema>/<volume>`
- Another workspace needs its own set of keys. An ID from one workspace is refused in another.

## Gated run steps

- Refuse when a tracked file under the UC sources differs from HEAD.
- Reuse the wheel in `dist/env6` when no wheel input changed since its commit. Otherwise build the JAR and the env 6 wheel (needs Docker).
- Stage the wheel and its JAR alone into `<volroot>/uc-it/<sha12>/` and verify both sizes by listing.
- Install with `gbx.uc.install(catalog, "geobrix_uc_it_<sha12>", folder, procedures=True)` from a Serverless environment 6 job.
- In the same job, call `gbx_probe_build()` once. It reports the build commit and a source digest that the function imports. The job fails when they differ from the staged wheel's.
- Run the scope (`bench.oracles.uc_sql`, judged by `check_uc_results`) in parallel shards and `gbx:examples:uc-sql` against the schema.
- Drop the schema and delete the Volume folder, unless `--keep`.
- Keep the workspace folder `jobs/geobrix/uc-it-<sha>`, so the install run's page opens its notebook. `gbx:workspace:sweep` removes it after 60 min (24 h after a failure).

## Install that stays

- `--install-into SCHEMA` stages the `dist/env6` wheel and its JAR into `<volroot>/uc-install/SCHEMA/`.
- Any clean build serves: an install is not evidence, so the wheel need not fit HEAD. `--rebuild` builds one.
- It installs with `procedures=True` from a Serverless environment 6 job, then calls `gbx_probe_build()`.
- The schema and the folder stay: each function reads the wheel from that folder.
- It runs no warehouse step, cleans up nothing and writes no evidence file.
- Run the example files on it next: `gbx:examples:uc-sql --catalog C --schema SCHEMA`.

## Gated run in parts

- Run the parts one after another, in any order. Parts 1 to N-1 deal the slice cases round-robin. Part N runs the example files.
- The first part to run builds, stages and installs. The other parts reuse that install. A part installs again when the schema is gone.
- The record of the run is `test-logs/uc-integration/<sha12>/parts-<scope>-of-<N>.json`.
- Commits may land between parts. A part resumes the record of an earlier commit when no UC source and no wheel input changed since.
- Then `evidence.json` names HEAD in `head_sha` and the install's commit in `install_head_sha`. Each entry of `part_runs` names its commit.
- Never run two parts at once: each part rewrites the record when it ends.
- A part that stops or fails can run again. Its new result replaces the old one.
- Between parts the schema and the Volume folder stay. No `evidence.json` exists until the last part.
- The last part to finish judges all parts as one run. Each case is judged on its own, so the verdict equals an unsplit run's. A case that no part ran fails.
- It writes `evidence.json` with `parts` and `part_runs`, then drops the schema and the folders, unless `--keep`.
- To abandon an unfinished parted run, run `--drop-parts <sha12>`. It drops only `geobrix_uc_it_<sha12>`.

## Transient API errors

- The runner retries one HTTP call to the Statement API when it fails in transit. It never retries a statement.
- Transient means an SDK `DeadlineExceeded`, a reset or timed-out connection, or HTTP 429, 502, 503 or 504.
- A failed status fetch asks again about the same statement id. A statement is never submitted twice.
- A submit that returned no id is submitted again. Nothing ran, or no id came back.
- Each call gets 2 more tries, after waits of 1 s and 3 s. Each retry logs one line: statement id, error class, attempt n.
- A statement that ran and returned a SQL error or a wrong value is never retried. It stays a case failure.
- When the retries run out, the statement is a harness error. The session goes on with the next statements.
- A batch lost this way runs its cases one by one, as a batch that fails does.
- The part counts harness errors apart from case failures. Its log shows `slice harness error (transient API)`, and `evidence.json` has `harness_errors`.
- A harness error never passes: the part and the run fail. Run the part again; its new result replaces the old one.
- This is not a job retry. It covers one HTTP call, not a job run.

## Evidence

- File: `test-logs/uc-integration/evidence.json` (the logs of the run are in `test-logs/uc-integration/<sha12>/`).
- Fields: `head_sha`, `sources_hash`, `scope`, `started`, `finished`, `profile`, `warehouse_id`, `schema`, `job_run_ids`, `slice`, `examples`, `result`.
- `result` is `pass` when the slice has no failure outside the known-failures file and every example statement succeeded.
- Known failures: `scripts/ci/uc_integration_known_failures.txt`, one `<case id>: <reason>` per line. It is empty.
- An `--against` run writes `evidence-adhoc.json` with `"adhoc": true`. The QC check never reads it.
- The QC check `uc-integration-evidence` reads `evidence.json` on `gh pr create` and on a push to main. It compares `sources_hash` with `scripts/ci/uc_sources_hash.py`.
- Release rule: when the range changes the release version, the check needs `scope` `full`. Otherwise `core` or `full` passes. The version is `<version>` in `pom.xml` and `__version__` in `python/geobrix/src/databricks/labs/gbx/__init__.py`.

## Raster statements

- The `full` scope builds raster statements that need `osgeo` (GDAL's Python bindings).
- When the host has no `osgeo`, the run builds the statements in the dev container through `gbx:docker:exec`. It writes `test-logs/uc-integration/slice-<scope>.json` and reads it on the host.
- If the container is not running, the run stops with a Fix: start it with `gbx:docker:start`.

## Limits

- Dev-branch pushes (`streamline/*`, `beta/*`) are not gated.
- Warehouse time is bounded by the warehouse, not by the client. A shared X-Small, one-cluster warehouse (the default) ran `core` in about 35 minutes (366 cases as 122 statements plus 156 example statements). The same scope took about 70 minutes before batching. `full` is 1,510 cases as 509 statements; expect well over an hour on that warehouse, since 433 of them are refusals that cannot share a statement. A larger or multi-cluster warehouse (`--warehouse-id`) is the lever for a faster run. The example setup file (00) runs first and alone, so it is the floor of the examples leg.
- Auth is the caller's job: run `bash ~/.claude/hooks/databricks-auth-status.sh PreDispatch` first.
