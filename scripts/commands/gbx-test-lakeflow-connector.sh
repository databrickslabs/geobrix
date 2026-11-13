#!/bin/bash
# gbx:test:lakeflow-connector - Run the GeoBrix Lakeflow connector end to end on Databricks and write its evidence file
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Lakeflow connector integration test"
    cat <<'HELP'
Run the GeoBrix Lakeflow connector end to end on Databricks, in scratch objects named after the commit:
a schema geobrix_lfc_it_<sha12> with a Volume (landing folder and the three wheels), a COMMUNITY connection
and a Serverless pipeline (PREVIEW channel) from the connector's pipeline/ingest.py. Update 1 checks every
table's columns and rows per file, update 2 checks that only a new file's rows are appended, and update 3
checks that a truncated file fails the update. Then it drops what it made and writes
test-logs/lakeflow-connector/evidence.json.

Usage: bash scripts/commands/gbx-test-lakeflow-connector.sh --profile <name> [options]

Options:
  --profile <name>      Databricks config profile (required; never chosen for you)
  --catalog C           Catalog of the scratch schema. Default: GBX_GATE_CATALOG__<profile> in
                        examples/tests/databricks_cluster_config.env, else geospatial_docs
  --warehouse-id ID     SQL warehouse for the row counts. Default: GBX_UC_GATE_WAREHOUSE_ID__<profile> in the
                        same file, then GBX_UC_GATE_WAREHOUSE_ID, then GBX_UC_WAREHOUSE_ID. <profile> is the
                        profile name with each character outside letters and digits as _
                        (fevm-geospatial -> fevm_geospatial). The run looks up both in the workspace of
                        --profile and stops with a Fix when either is missing there
  --geobrix-wheel PATH  A local GeoBrix env 6 wheel to use (default: dist/env6 when it fits HEAD, else a build)
  --rebuild             Build the env 6 wheel even when dist/env6 fits HEAD (needs Docker)
  --data-dir DIR        Local sample data (default sample-data/.../geobrix-examples, the essential bundle)
  --timeout-min N       Limit per pipeline update (default 30); the pipeline is stopped after it
  --poll-secs N         Poll interval (default 20)
  --skip-unreadable     Skip update 3 (the truncated file)
  --keep                Keep the pipeline, the connection and the schema
  --drop SHA12          Drop what a kept or broken run of commit SHA12 left, then stop
  --dry-run             Print the plan; no build, no workspace call
  --log <path>          Write output to a log file (filename -> test-logs/<name>)
  --help, -h            Show this help

Exit code: 0 when every check passed, 1 on a failure or a refusal, 2 for bad options.

Examples:
  bash scripts/commands/gbx-test-lakeflow-connector.sh --profile fevm-geospatial
  bash scripts/commands/gbx-test-lakeflow-connector.sh --profile fevm-geospatial --dry-run
HELP
}

LOG_PATH=""
HAVE_WAREHOUSE=false
ARGS=()
while [[ $# -gt 0 ]]; do case $1 in
    --help|-h) show_help; exit 0 ;;
    --log) [[ $# -ge 2 ]] || { echo "Option --log needs a value" >&2; exit 2; }
        LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --warehouse-id) [[ $# -ge 2 ]] || { echo "Option --warehouse-id needs a value" >&2; exit 2; }
        HAVE_WAREHOUSE=true; ARGS+=("$1" "$2"); shift 2 ;;
    --profile|--catalog|--geobrix-wheel|--data-dir|--timeout-min|--poll-secs|--drop)
        [[ $# -ge 2 ]] || { echo "Option $1 needs a value" >&2; exit 2; }
        ARGS+=("$1" "$2"); shift 2 ;;
    --rebuild|--skip-unreadable|--keep|--dry-run) ARGS+=("$1"); shift ;;
    *) echo "Unknown option: $1" >&2; echo "Use --help for usage" >&2; exit 2 ;;
esac; done

cd "$PROJECT_ROOT" || exit 1
show_banner "GeoBrix: Lakeflow connector integration test"
setup_log_file "$LOG_PATH"
# A detached worktree has no untracked config: the main checkout's is used.
gbx_export_cluster_config && CONFIG="$GBX_CLUSTER_CONFIG_ENV"

# The driver reads the warehouse id from the (untracked) cluster config itself when --warehouse-id is absent.
# It needs databricks-sdk and Python >= 3.11: see gbx_python_with_sdk in common.sh.
gbx_python_with_sdk || exit 1
"${GBX_SDK_PYTHON[@]}" examples/tests/lakeflow_connector_it.py "${ARGS[@]}"
exit $?
