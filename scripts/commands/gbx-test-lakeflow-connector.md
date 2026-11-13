# gbx:test:lakeflow-connector

Run the GeoBrix Lakeflow connector end to end on Databricks, as the docs set it up. It writes `evidence.json` for the QC check.

**Usage:** `bash scripts/commands/gbx-test-lakeflow-connector.sh --profile <name> [OPTIONS]`

**Options:**
- `--profile NAME` - Databricks profile. Required, never defaulted.
- `--catalog C` - catalog of the scratch schema. Default `geospatial_docs`.
- `--warehouse-id ID` - SQL warehouse for the row counts. Default `GBX_UC_GATE_WAREHOUSE_ID`, else `GBX_UC_WAREHOUSE_ID`, in `examples/tests/databricks_cluster_config.env`.
- `--geobrix-wheel PATH` - a local GeoBrix env 6 wheel. Default: `dist/env6` when it fits HEAD, else a build.
- `--rebuild` - build the env 6 wheel even when `dist/env6` fits HEAD. Needs Docker.
- `--data-dir DIR` - local sample data. Default the essential bundle's `geobrix-examples`.
- `--timeout-min N` - limit per pipeline update. Default `30`.
- `--poll-secs N` - poll interval. Default `20`.
- `--skip-unreadable` - skip update 3, the truncated file.
- `--keep` - keep the pipeline, the connection and the schema.
- `--drop SHA12` - drop what a kept or broken run of commit SHA12 left, then stop.
- `--dry-run` - print the plan. No build, no workspace call.
- `--log <path>` - tee output to `test-logs/<path>`.
- `--help, -h` - show help.

**Examples:**
- `bash scripts/commands/gbx-test-lakeflow-connector.sh --profile oauth-fe`
- `bash scripts/commands/gbx-test-lakeflow-connector.sh --profile oauth-fe --dry-run`

## Steps

- Refuse when a tracked file under the connector sources differs from HEAD.
- Run the connector unit tests (`gbx:test:connectors`). They also make the connector venv.
- Get three wheels: the GeoBrix env 6 wheel, and the framework and connector wheels built as `BUILD_WHEELS` says. The connector wheel is built from `git archive HEAD`.
- Read a local copy of the landing folder with the connector's code. This gives each table's columns and each file's rows.
- Create the schema `geobrix_lfc_it_<sha12>` with a managed Volume `files`. Upload the landing folder and the wheels, and verify each size by listing.
- Create the connection `geobrix_lfc_it_<sha12>`: type `COMMUNITY`, `sourceName` geobrix, the allowlist of `connector_spec.yaml`, `root_path` the landing folder.
- Create a Serverless pipeline on the `PREVIEW` channel, with the documented dependency lines. Its source is `pipeline/ingest.py` with `pipeline_spec` filled in, in `/Users/<you>/jobs/geobrix/lfc-it-<sha12>/`.
- Update 1: each table exists, has the connector's columns (with `_source_path`, `_source_mtime`) and holds each file's rows.
- Update 2, after one new file lands: only that file's rows are appended.
- Update 3, after a truncated GeoJSON file lands: the update fails, its error names the file, and no row is appended.
- Drop the pipeline, the connection and the schema with its Volume, unless `--keep`.

## Tables

- `boroughs`: a GeoJSON folder, discovered by name.
- `taxi_zones`: the `taxi_zones` layer of a GeoPackage (`path`, `layer`).
- `subway`: a zipped shapefile folder.
- `elevation`: GeoTIFFs, the reader inferred from the extension. Update 2 adds a file here.
- `elevation_cog`: a `.tif` read with `format: cog`.

## Evidence

- File: `test-logs/lakeflow-connector/evidence.json` in the main checkout, also from a worktree run. Logs: `test-logs/lakeflow-connector/<sha12>/`.
- Fields: `head_sha`, `sources_hash`, `checks` (name, ok, detail), `tables` (rows per file after updates 1 and 2), `pipeline_id`, `update_ids`, `wheels` (sha256), `geobrix_wheel` (its commit, and whether it fits HEAD), `cleanup`, `result`.
- `result` is `pass` when every check passed.
- The check `scripts/ci/check_lakeflow_connector_evidence.py` compares `sources_hash` with HEAD over `LFC_PATHS`. Only main-bound ranges are gated.

## Limits

- The workspace must allow `COMMUNITY` connections. Otherwise the run prints one `PRECONDITION MISSING:` line and fails.
- Each update takes about 3 to 8 min on Serverless. A run takes about 20 to 30 min.
- The pipeline source folder stays for the run page. `gbx:workspace:sweep` (scope `lfc`) removes it after 60 min, or 24 h after a failure.
- A run that stopped part-way may leave its objects. The next run of that commit refuses; clean up with `--drop <sha12>`.
- Auth is the caller's job: run `bash ~/.claude/hooks/databricks-auth-status.sh PreDispatch` first.
