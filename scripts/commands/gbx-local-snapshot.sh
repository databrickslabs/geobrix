#!/bin/bash
# gbx:local:snapshot - Rolling local snapshot of what git does not hold (see gbx-local-snapshot.md)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "Local: project snapshot"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:local:snapshot${NC} ${YELLOW}[run | --dry-run | list | prune [--keep N] | restore <name> <dir> | status | install-hook | uninstall-hook]${NC} ${YELLOW}[--log <path>]${NC}"
    echo ""
    echo -e "${CYAN}Subcommands:${NC}"
    echo -e "  ${GREEN}run${NC}              Write a snapshot zip to GBX_SNAPSHOT_DIR, then keep the newest GBX_SNAPSHOT_KEEP (default)."
    echo -e "  ${GREEN}--dry-run${NC}        Report what would go in and what is left out; write nothing."
    echo -e "  ${GREEN}list${NC}             List the snapshots."
    echo -e "  ${GREEN}prune [--keep N]${NC} Delete snapshots beyond the newest N."
    echo -e "  ${GREEN}restore <name> <dir>${NC} Unpack a snapshot into an empty directory and print the bundle steps."
    echo -e "  ${GREEN}status${NC}           Show the last logged run and whether the hook is installed."
    echo -e "  ${GREEN}install-hook${NC}     Snapshot in the background after each successful push."
    echo -e "  ${GREEN}uninstall-hook${NC}   Remove that hook block and nothing else."
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}     Write output to a log file (filename -> test-logs/<name>)."
    echo -e "  ${GREEN}--help${NC}           Show this help."
    echo ""
    echo -e "${CYAN}Settings${NC} (untracked .gbx-local.env, or the environment):"
    echo -e "  GBX_SNAPSHOT_DIR          Destination folder. Default ~/GeoBrix-Snapshots."
    echo -e "  GBX_SNAPSHOT_KEEP         Snapshots to keep. Default 3."
    echo -e "  GBX_SNAPSHOT_MAX_FILE_MB  Skip files larger than this. Default 500."
    echo -e "  GBX_SNAPSHOT_GITLEAKS     Path to a gitleaks binary. Default: gitleaks on PATH."
    echo -e "  GBX_SNAPSHOT_SCAN_ALLOW   Comma-separated path globs whose secret-scan findings you accept. Default: none."
    echo ""
}

LOG_PATH=""
ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --log)
            [[ $# -ge 2 ]] || { echo "ERROR: --log needs a path" >&2; exit 2; }
            LOG_PATH="$(resolve_log_path "$2")"; shift 2 ;;
        --help|-h) show_help; exit 0 ;;
        --dry-run) ARGS+=("run" "--dry-run"); shift ;;
        *) ARGS+=("$1"); shift ;;
    esac
done

cd "$PROJECT_ROOT" || exit 1
setup_log_file "$LOG_PATH"
# Not exec: the log teardown trap installed by setup_log_file must run after the snapshot.
python3 "$PROJECT_ROOT/scripts/snapshot/snapshot.py" --root "$PROJECT_ROOT" "${ARGS[@]}"
exit $?
