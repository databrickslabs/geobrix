#!/bin/bash
# gbx:uc:tests - Run the Unity Catalog tests on Databricks (smoke, core or full), as a gated run or against an existing install
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

CONFIG="examples/tests/databricks_cluster_config.env"

show_help() {
    show_banner "GeoBrix: Unity Catalog tests"
    cat <<'HELP'
Run the Unity Catalog tests on Databricks: the UC oracle slice and the example files, on a SQL warehouse.
Without --against this is the gated run. It builds the env 6 wheel, stages it with its JAR into a fresh
Volume folder, installs the UC functions into the scratch schema geobrix_uc_it_<sha12> from a Serverless
environment 6 job, runs the scope, drops the schema, and writes test-logs/uc-integration/evidence.json.
With --against SCHEMA it runs the scope on an install that exists and writes evidence-adhoc.json only.

Usage: bash scripts/commands/gbx-uc-tests.sh --profile <name> [options]

Options:
  --profile <name>        Databricks config profile (required; never chosen for you: see
                          `databricks auth profiles`)
  --scope smoke|core|full What to run (default core):
                            smoke  12 slice statements and example files 00 and 01 (needs --against)
                            core   grid and vector oracle tables, and all example files
                            full   core plus the raster tables
  --against SCHEMA        Run the scope against the existing install in SCHEMA. No build, stage, install or
                          cleanup. Writes evidence-adhoc.json, never evidence.json: the installed code may
                          not match HEAD, so the run is not evidence for a pull request.
  --warehouse-id ID       SQL warehouse. Default: GBX_UC_GATE_WAREHOUSE_ID__<profile> in
                          examples/tests/databricks_cluster_config.env (untracked: keep the ID there), then
                          GBX_UC_GATE_WAREHOUSE_ID, then GBX_UC_WAREHOUSE_ID. <profile> is the profile name
                          with each character outside letters and digits as _ (fevm-geospatial -> fevm_geospatial)
  --catalog C             Catalog of the schema. Default: GBX_GATE_CATALOG__<profile>, else geospatial_docs
  --volroot DIR           Volume folder that holds uc-it/<sha12>/. Default: GBX_GATE_VOLROOT__<profile>, else
                          /Volumes/geospatial_docs/geobrix/sample-data
                          Before any build, the run looks up the warehouse and the catalog in the workspace of
                          --profile. It stops with a Fix when either is missing there. It prints each value
                          with its source
  --deps pypi|volume      Where the UC functions' dependencies come from. Default: GBX_GATE_DEPS__<profile> in the
                          cluster config, else pypi. volume installs without PyPI access (the Hub sandboxes have none):
                          the gate installs the env wheel in a python3.12 venv, runs `python -m databricks.labs.gbx.stage
                          deps --arch both --uc` (needs PyPI on this host), stages the result with gbx.stage_deps into the
                          run's own uc-it/<sha12>/ folder, and installs with deps="volume"
  --batch N               Value cases per warehouse statement (default 25; 1 runs each alone). The warehouse
                          pays ~10 s per statement that calls a UC Python function, whatever it computes, so
                          the cases of a batch share one. A case that must be refused always runs alone, and a
                          batch that fails is rerun case by case: each case is judged on its own either way
  --concurrency N         Warehouse statements in flight at once, per slice session (default 16)
  --shards N              Separate slice sessions (default 1; each batches its own share, so more sessions
                          mean smaller batches)
  --example-files-parallel N  Example files run side by side (default 6; file 00, the setup, runs first)
  --rebuild               Build the wheel even when dist/env6 holds one that fits HEAD
  --run-part I/N          Run part I of a gated run cut into N parts (N >= 2), one part after another. Parts 1
                          to N-1 deal the slice; part N runs the example files. The first part to run installs,
                          the others reuse it. A failed part can be run again. The last part to finish writes
                          evidence.json (scope, parts N) and drops the schema. Record: <run dir>/parts-*.json
  --republish SHA12       Write evidence.json again from the record of a finished parted run (SHA12 = its install
                          commit; give its --scope). It judges the recorded parts again, with no workspace call and
                          no run. It refuses when a part is missing or failed, when the record covers other
                          sources than HEAD, or when it ran in another catalog or warehouse than the profile's.
                          The evidence has republished_from and the original finish time
  --drop-parts SHA12      Drop the schema and the folders kept by an unfinished parted run of commit SHA12
  --keep                  Keep the schema and the Volume folder (gated run)
  --install-into SCHEMA   Install into SCHEMA and keep it, for demos: stage the dist/env6 wheel (any clean build)
                          and its JAR into <volroot>/uc-install/SCHEMA/, install with procedures, probe the build.
                          No warehouse step, no cleanup, no evidence. Then run gbx:examples:uc-sql on SCHEMA
  --install-timeout-min N Time limit of the install job (default 90)
  --dry-run               Print the plan and counts; no build, no workspace call
  --log <path>            Write output to a log file (filename -> test-logs/<name>)
  --help, -h              Show this help

Behaviour:
  A gated run refuses when a tracked file under python/geobrix/src, examples/uc-functions, src/main/scala
  or pom.xml differs from HEAD. Docker is needed to build the wheel; a wheel in dist/env6 is reused when
  no wheel input changed since its commit. evidence.json records the scope. A change of the release
  version needs a full run: the QC check refuses core evidence for it.
  Time: warehouse capacity bounds it. On a shared X-Small, one-cluster warehouse `core` took about 35 min
  (about 70 min before batching); `full` takes well over an hour there. On a Serverless Pro Medium
  warehouse of 2 to 4 clusters `core` took about 22 min end to end (the warehouse step 15 min of it).
  Use a bigger warehouse with --warehouse-id, or set GBX_UC_GATE_WAREHOUSE_ID__<profile>, to go faster.
  The gate workspace is the Geospatial Hub: use --profile fevm-geospatial.
  Raster statements (scope full) need osgeo. A host without it builds them in the dev container (gbx:docker:exec),
  which must be running (gbx:docker:start).
  Exit code: 0 when the result is pass, 1 on fail or refusal, 2 for bad options.
  Known failures: scripts/ci/uc_integration_known_failures.txt (empty).

Examples:
  bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial
  bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial --scope full
  bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial --scope smoke --against geobrix_uc
  bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial --scope full --run-part 1/6
  bash scripts/commands/gbx-uc-tests.sh --profile fevm-geospatial --install-into geobrix_uc
HELP
}

LOG_PATH=""
ARGS=()
while [[ $# -gt 0 ]]; do case $1 in
    --help|-h) show_help; exit 0 ;;
    --log) [[ $# -ge 2 ]] || { echo "Option --log needs a value" >&2; exit 2; }
        LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --warehouse-id) [[ $# -ge 2 ]] || { echo "Option --warehouse-id needs a value" >&2; exit 2; }
        ARGS+=("$1" "$2"); shift 2 ;;
    --profile|--catalog|--volroot|--deps|--scope|--against|--install-into|--shards|--batch|--concurrency|--example-files-parallel|--install-timeout-min|--run-part|--drop-parts|--republish)
        [[ $# -ge 2 ]] || { echo "Option $1 needs a value" >&2; exit 2; }
        ARGS+=("$1" "$2"); shift 2 ;;
    --rebuild|--keep|--dry-run) ARGS+=("$1"); shift ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 2 ;;
esac; done

cd "$PROJECT_ROOT" || exit 1
show_banner "GeoBrix: Unity Catalog tests"
setup_log_file "$LOG_PATH"
# A detached worktree has no untracked config: the main checkout's is used.
gbx_export_cluster_config && CONFIG="$GBX_CLUSTER_CONFIG_ENV"

# The driver reads the catalog, the warehouse and the Volume folder of --profile from the (untracked) cluster
# config itself (examples/tests/_uc_warehouse.py), when an option does not give them.

# The driver needs databricks-sdk and Python >= 3.11: see gbx_python_with_sdk in common.sh.
gbx_python_with_sdk || exit 1
"${GBX_SDK_PYTHON[@]}" examples/tests/uc_integration.py "${ARGS[@]}"
exit $?
