#!/bin/bash
# gbx:manifest:gen - write or check every surface generated from the function manifest (host, curated venv)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: generated surfaces"
    echo "Usage: bash scripts/commands/gbx-manifest-gen.sh [--write | --check] [--only <surface>] [--log <path>]"
    echo ""
    echo "  --write          Write the Python API entries, docs blocks, function-info.json, the availability"
    echo "                   matrix, the doc-test examples and the count badges from manifest/functions.json"
    echo "  --check          The gate: exit 1 when a generated surface differs from the manifest, or a"
    echo "                   replacement gate (completeness, UC overlap, parameter names, docs tabs) fails"
    echo "  --only <surface> manifest | python | docs | function-info | availability | examples | counts"
    echo "  --log <path>     Write output to a log file (filename -> test-logs/<name>)"
    echo "  --help           Show this help"
    echo ""
    echo "Runs on the host in the curated venv (gbx:venv:sync); no Docker."
}

MODE="--write"; ONLY=""; LOG_PATH=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --write) MODE="--write"; shift ;;
        --check) MODE="--check"; shift ;;
        --only) [[ $# -ge 2 ]] || { echo "Option --only needs a value" >&2; exit 2; }; ONLY="--only $2"; shift 2 ;;
        --log) [[ $# -ge 2 ]] || { echo "Option --log needs a value" >&2; exit 2; }; LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        --help|-h) show_help; exit 0 ;;
        *) echo "Unknown option: $1" >&2; show_help; exit 2 ;;
    esac
done
setup_log_file "$LOG_PATH"
run_in_pyrx_venv "cd '$PROJECT_ROOT' && python scripts/manifest/gen_surfaces.py $MODE $ONLY"
