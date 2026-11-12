#!/bin/bash
# gbx:app:verify - Check that the Genie Map app (deployed, or deployed by this command) runs and answers
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Verify Genie Map"
    cat <<'HELP'
Check that the Genie Map app runs and answers: wait until it is RUNNING on ACTIVE compute, then GET its
root (or --path) with the profile's OAuth token; a 2xx answer passes. Optionally lint and test the app
first, and deploy it with gbx:app:deploy.

Usage: bash scripts/commands/gbx-app-verify.sh --profile <name> [options]

Options:
  --profile <name>   Databricks config profile of the app's workspace (required; never chosen for you)
  --app <name>       The app name (default: the bundle's, genie-map)
  --deploy           Run gbx:app:deploy --profile <name> first (build, bundle deploy, run, Genie re-seed)
  --local-checks     Run pnpm lint and pnpm test --run in examples/apps/genie_map first (host)
  --path <path>      The URL path to GET (default /)
  --wait-min N       How long to wait for RUNNING (default 10)
  --poll-secs N      Poll interval (default 15)
  --log <path>       Write output to a log file (filename -> test-logs/<name>)
  --help, -h         Show this help

Exit code: 0 when the app runs and answers 2xx, 1 on a failure, 2 for bad options.

Example:
  bash scripts/commands/gbx-app-verify.sh --profile genie-map-env --local-checks --deploy --log app.log
HELP
}

LOG_PATH=""
ARGS=()
while [[ $# -gt 0 ]]; do case $1 in
    --help|-h) show_help; exit 0 ;;
    --log) [[ $# -ge 2 ]] || { echo "Option --log needs a value" >&2; exit 2; }
        LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --profile|--app|--path|--wait-min|--poll-secs)
        [[ $# -ge 2 ]] || { echo "Option $1 needs a value" >&2; exit 2; }
        ARGS+=("$1" "$2"); shift 2 ;;
    --deploy|--local-checks) ARGS+=("$1"); shift ;;
    *) echo "Unknown option: $1" >&2; echo "Use --help for usage" >&2; exit 2 ;;
esac; done

cd "$PROJECT_ROOT" || exit 1
show_banner "GeoBrix: Verify Genie Map"
setup_log_file "$LOG_PATH"

# The runner needs databricks-sdk and Python >= 3.11: see gbx_python_with_sdk in common.sh.
gbx_python_with_sdk || exit 1
"${GBX_SDK_PYTHON[@]}" examples/tests/verify_app.py "${ARGS[@]}"
exit $?
