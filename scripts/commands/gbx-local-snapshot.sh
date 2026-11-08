#!/bin/bash
# gbx:local:snapshot - Rolling local snapshot of what git does not hold (see gbx-local-snapshot.md)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "Local: project snapshot"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:local:snapshot${NC} ${YELLOW}[run [--upload [--respect-throttle]] [--dry-run] | list [--drive] | prune [--keep N] [--drive [--dry-run]] | restore [--from-drive] <name> <dir> | status | install-hook | uninstall-hook]${NC} ${YELLOW}[--log <path>]${NC}"
    echo ""
    echo -e "${CYAN}Subcommands:${NC}"
    echo -e "  ${GREEN}run${NC}              Write a snapshot zip to GBX_SNAPSHOT_DIR, then keep the newest GBX_SNAPSHOT_KEEP (default)."
    echo -e "  ${GREEN}--dry-run${NC}        Report what would go in and what is left out; write nothing."
    echo -e "  ${GREEN}--upload${NC}         With run: also copy the new zip to Google Drive, only if the secret scan was clean."
    echo -e "                    An explicit --upload ignores the upload throttle; add --respect-throttle to keep it."
    echo -e "                    With --dry-run: check the Drive folder, its sharing, and what a prune would trash (reads only)."
    echo -e "  ${GREEN}list [--drive]${NC}   List the snapshots (--drive: the Drive folder)."
    echo -e "  ${GREEN}prune [--keep N] [--drive [--dry-run]]${NC} Delete snapshots beyond the newest N (--drive: trash them in the Drive folder)."
    echo -e "  ${GREEN}restore [--from-drive] <name> <dir>${NC} Unpack a snapshot into an empty directory and print the bundle steps."
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
    echo -e "  GBX_SNAPSHOT_UPLOAD       1 to upload each new snapshot to Google Drive. Default off."
    echo -e "  GBX_SNAPSHOT_DRIVE_FOLDER_ID   The Drive folder to upload to (from its URL). Required for any Drive step."
    echo -e "  GBX_SNAPSHOT_DRIVE_KEEP   Snapshots to keep in that folder. Default 3."
    echo -e "  GBX_SNAPSHOT_GCP_QUOTA_PROJECT  Google Cloud project that bills the Drive API calls. Most accounts need it."
    echo -e "  GBX_SNAPSHOT_DRIVE_ALLOW_SHARED  1 to allow a Drive folder that others can open. Default off: it must be private."
    echo -e "  GBX_SNAPSHOT_UPLOAD_MIN_INTERVAL_MIN  Minutes between uploads started by GBX_SNAPSHOT_UPLOAD (the hook). Default 60; 0 = no limit."
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
        *) ARGS+=("$1"); shift ;;
    esac
done
# No subcommand (bare, or only run's flags such as --dry-run / --upload) means run.
case "${ARGS[0]:-}" in
    run|list|prune|restore|status|install-hook|uninstall-hook) ;;
    *) ARGS=("run" ${ARGS[@]+"${ARGS[@]}"}) ;;
esac

cd "$PROJECT_ROOT" || exit 1
setup_log_file "$LOG_PATH"
# Not exec: the log teardown trap installed by setup_log_file must run after the snapshot.
python3 "$PROJECT_ROOT/scripts/snapshot/snapshot.py" --root "$PROJECT_ROOT" "${ARGS[@]}"
exit $?
