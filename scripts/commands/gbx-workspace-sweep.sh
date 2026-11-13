#!/bin/bash
# gbx:workspace:sweep - remove old GeoBrix runner notebooks and folders from the workspace (deferred cleanup).
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "gbx:workspace:sweep"
    cat <<'EOT'
Remove the runner notebooks and folders the launchers kept at run end, once they pass their window.
Only GeoBrix runner items under /Users/<you>/GeoBrix/ are removed; an item an active run uses is kept.

Usage: bash scripts/commands/gbx-workspace-sweep.sh --profile <name> [options]
  --profile <name>      Databricks profile (required)
  --older-than <age>    Remove items older than this, by modified time (default 60m; also 90s, 2h, 1d)
  --scope <s[,s]>       bench, runners, uc, notebooks; repeat or comma-separate (default all)
  --dry-run             Print what would be removed; remove nothing
  --log <path>          Write output to a log file
  --help                Show this help

A run that did not succeed is held 24 h from its end; one that succeeded, 60 min.
EOT
}

PASS_ARGS=()
LOG_PATH=""
HAS_PROFILE=0
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h) show_help; exit 0 ;;
        --log)
            [[ $# -ge 2 ]] || { echo -e "${RED}Option --log needs a value${NC}" >&2; exit 2; }
            LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        --profile|--older-than|--scope)
            [[ $# -ge 2 && -n "$2" ]] || { echo -e "${RED}Option $1 needs a value${NC}" >&2; exit 2; }
            [[ "$1" == "--profile" ]] && HAS_PROFILE=1
            PASS_ARGS+=("$1" "$2"); shift 2 ;;
        --dry-run) PASS_ARGS+=("$1"); shift ;;
        *) echo -e "${RED}Unknown option: $1${NC}" >&2; exit 2 ;;
    esac
done
if [[ $HAS_PROFILE -eq 0 ]]; then
    echo -e "${RED}--profile <name> is required: the sweep never picks a workspace for you.${NC}" >&2
    exit 2
fi

cd "$PROJECT_ROOT"
show_banner "gbx:workspace:sweep"
setup_log_file "$LOG_PATH"
CONFIG="examples/tests/databricks_cluster_config.env"
# A detached worktree has no untracked config: the main checkout's is used (it names GBX_RUNNER_DIR).
if GBX_CLUSTER_CONFIG_ENV="$(gbx_resolve_untracked "$CONFIG")" && [[ -f "$GBX_CLUSTER_CONFIG_ENV" ]]; then
    export GBX_CLUSTER_CONFIG_ENV
fi
gbx_python_with_sdk || exit 1
"${GBX_SDK_PYTHON[@]}" "$PROJECT_ROOT/examples/tests/_workspace_sweep.py" "${PASS_ARGS[@]}"
exit $?
