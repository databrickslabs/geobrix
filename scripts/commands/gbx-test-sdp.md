# Run a Lakeflow SDP example bundle

Validates, plans, deploys and runs a Lakeflow SDP example bundle with the staged GeoBrix wheel. Then it checks that every table its pipeline owns exists and holds rows. It deletes and drops nothing.

## Usage

```bash
bash scripts/commands/gbx-test-sdp.sh --bundle <name|dir> --profile <name> [OPTIONS]
```

## Options

- `--bundle <name|dir>` – `vapor_eyes_lf`, `wireless_coverage_lf`, or a folder with a `databricks.yml` (required).
- `--profile <name>` – Databricks config profile (required; never chosen for you).
- `--target <t>` – Bundle target (default: the bundle's default target).
- `--wheel <volpath>` – The env wheel, passed as `--var gbx_wheel`. Default: the versioned staged env5 wheel, `sample-data/geobrix/<version>/env5/`.
- `--var NAME=VALUE` – A bundle variable, for example `full_aoi=false` (repeatable).
- `--job <key>` – The job to run (default: the bundle's only job).
- `--only <tasks>` – Comma list of the job's task keys to run, for example `land,pipeline`.
- `--pipeline-only` – Start one update of the bundle's pipeline instead of the job.
- `--full-refresh` – Run the pipeline as a full refresh.
- `--validate-only` – Validate and plan; deploy and run nothing.
- `--no-run` – Validate, plan and deploy; run nothing.
- `--warehouse-id ID` – SQL warehouse for the row counts (default: `GBX_UC_WAREHOUSE_ID` in `examples/tests/databricks_cluster_config.env`).
- `--no-table-check` – Skip the table check.
- `--allow-empty TABLE` – A table that may be empty (repeatable).
- `--timeout-min N` – The run's limit in minutes (default 30). The run is cancelled after it.
- `--poll-secs N` – Poll interval in seconds (default 30).
- `--log <path>` – Write output to a log file (filename → `test-logs/<name>`).
- `--help`, `-h` – Show the help.

## Examples

```bash
# The wireless-coverage demo window, the whole job (land, pipeline, siting, validate)
bash scripts/commands/gbx-test-sdp.sh --bundle wireless_coverage_lf --profile oauth-fe \
  --var full_aoi=false --timeout-min 20 --log wc-sdp.log

# Only check that the vapor-eyes bundle validates and plans without a delete
bash scripts/commands/gbx-test-sdp.sh --bundle vapor_eyes_lf --profile oauth-fe --validate-only
```

## Steps

1. `databricks bundle validate` with `--var gbx_wheel=<wheel>` and every `--var`. The wheel's environment must match the bundle's job `environment_version` (both example bundles: 5).
2. `databricks bundle plan`. A plan that deletes or recreates a resource stops here, with one `DESTRUCTIVE PLAN:` line. Deleting a pipeline drops its tables.
3. The SQL warehouse is looked up. A deleted warehouse stops the command before the deploy.
4. `databricks bundle deploy` (no `--auto-approve`), then `bundle summary` for the job and pipeline ids.
5. The run: `run_now` of the job (one idempotency token), or one pipeline update with `--pipeline-only`.
6. The poll prints a line on each change of the job tasks and the pipeline update.
   - An ERROR event of this update (a failed flow) cancels the run at once.
   - At `--timeout-min` the run is cancelled.
7. The table check: each Unity Catalog table whose `pipeline_id` is the bundle's pipeline is counted with `SELECT count(*)`. An empty table fails, unless `--allow-empty` names it.

## Limits

- The command drops nothing. A failure that only a DROP fixes prints one `DROP NEEDED: DROP <kind> <table>` line for the orchestrator. Example: `wc_lidar_pts`, a materialized view in an older deployment, is a streaming table now.
- A bundle's own validation runs inside its job: wireless coverage has a `validate` task (`validate/validate_wc_lf.ipynb`). The vapor-eyes `tests/validate/*.sql` queries are not run; the table check covers existence and rows.
- The runner is `examples/tests/run_sdp_bundle.py`; its tests are `scripts/ci/test_gbx_test_sdp.py`.
