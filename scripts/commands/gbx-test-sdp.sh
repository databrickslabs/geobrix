#!/bin/bash
# gbx:test:sdp - Validate, deploy and run a Lakeflow SDP example bundle, then check the tables its pipeline published
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

CONFIG="examples/tests/databricks_cluster_config.env"

show_help() {
    show_banner "GeoBrix: Lakeflow SDP bundle run"
    cat <<'HELP'
Validate, plan, deploy and run a Lakeflow SDP example bundle with the staged GeoBrix wheel, then check
that every table its pipeline owns exists and holds rows. It deletes and drops nothing: a plan that
deletes or recreates a resource stops the command, and a failure that needs a DROP prints one
"DROP NEEDED:" line.

Usage: bash scripts/commands/gbx-test-sdp.sh --bundle <name|dir> --profile <name> [options]

Options:
  --bundle <name|dir>    vapor_eyes_lf, wireless_coverage_lf, or a folder with a databricks.yml (required)
  --profile <name>       Databricks config profile (required; never chosen for you)
  --target <t>           Bundle target (default: the bundle's default target)
  --wheel <volpath>      The env wheel passed as --var gbx_wheel (default: the versioned staged env6
                         wheel, sample-data/geobrix/<version>/env6/); it must match the job environments
  --var NAME=VALUE       A bundle variable, e.g. full_aoi=false (repeatable)
  --job <key>            The job to run (default: the bundle's only job)
  --only <tasks>         Comma list of the job's task keys to run (e.g. land,pipeline)
  --pipeline-only        Start one update of the bundle's pipeline instead of the job
  --full-refresh         Run the pipeline as a full refresh
  --validate-only        Validate and plan; deploy and run nothing
  --no-run               Validate, plan and deploy; run nothing
  --warehouse-id ID      SQL warehouse for the row counts (default: GBX_UC_WAREHOUSE_ID in
                         examples/tests/databricks_cluster_config.env)
  --no-table-check       Skip the table check
  --allow-empty TABLE    A table that may be empty (repeatable)
  --timeout-min N        The run's limit in minutes (default 30); the run is cancelled after it
  --poll-secs N          Poll interval (default 30)
  --log <path>           Write output to a log file (filename -> test-logs/<name>)
  --help, -h             Show this help

Exit code: 0 when the run succeeded and every table holds rows, 1 on a failure, 2 for bad options.

Example:
  bash scripts/commands/gbx-test-sdp.sh --bundle wireless_coverage_lf --profile oauth-fe \
    --var full_aoi=false --timeout-min 20 --log wc-sdp.log
HELP
}

LOG_PATH=""
HAVE_WAREHOUSE=false
NEED_WAREHOUSE=true
ARGS=()
while [[ $# -gt 0 ]]; do case $1 in
    --help|-h) show_help; exit 0 ;;
    --log) [[ $# -ge 2 ]] || { echo "Option --log needs a value" >&2; exit 2; }
        LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --warehouse-id) [[ $# -ge 2 ]] || { echo "Option --warehouse-id needs a value" >&2; exit 2; }
        HAVE_WAREHOUSE=true; ARGS+=("$1" "$2"); shift 2 ;;
    --bundle|--profile|--target|--wheel|--var|--job|--only|--allow-empty|--timeout-min|--poll-secs)
        [[ $# -ge 2 ]] || { echo "Option $1 needs a value" >&2; exit 2; }
        ARGS+=("$1" "$2"); shift 2 ;;
    --validate-only|--no-run|--no-table-check) NEED_WAREHOUSE=false; ARGS+=("$1"); shift ;;
    --pipeline-only|--full-refresh) ARGS+=("$1"); shift ;;
    *) echo "Unknown option: $1" >&2; echo "Use --help for usage" >&2; exit 2 ;;
esac; done

cd "$PROJECT_ROOT" || exit 1
show_banner "GeoBrix: Lakeflow SDP bundle run"
setup_log_file "$LOG_PATH"
# A detached worktree has no untracked config: the main checkout's is used.
gbx_export_cluster_config && CONFIG="$GBX_CLUSTER_CONFIG_ENV"

# The warehouse id may live in the (untracked) cluster config; read that one key, never source the file.
if [[ "$HAVE_WAREHOUSE" == false && "$NEED_WAREHOUSE" == true && -f "$CONFIG" ]]; then
    WAREHOUSE_ID="$(grep -E '^GBX_UC_WAREHOUSE_ID=' "$CONFIG" | tail -1 | cut -d= -f2- | tr -d '[:space:]"'"'")"
    [[ -n "$WAREHOUSE_ID" ]] && ARGS+=(--warehouse-id "$WAREHOUSE_ID")
fi

# The runner needs databricks-sdk and Python >= 3.11: see gbx_python_with_sdk in common.sh.
gbx_python_with_sdk || exit 1
"${GBX_SDK_PYTHON[@]}" examples/tests/run_sdp_bundle.py "${ARGS[@]}"
exit $?
