#!/bin/bash
# gbx:venv:check - Compare the host venv (.venv-pyrx) with the running geobrix-dev container.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

VENV_DIR="${GBX_PYRX_VENV:-${PROJECT_ROOT}/.venv-pyrx}"
REPORT="$PROJECT_ROOT/scripts/ci/venv_env_report.py"
LOG_PATH=""

show_help() {
    show_banner "gbx:venv:check — host venv vs container"
    cat <<'HELP'
Compare the host venv with the running geobrix-dev container and print what differs:
the Python patch version, the version of every package both have, and the natives
(GDAL, PROJ and GEOS as rasterio, pyproj and shapely report them). Exits 1 on any drift.

A host that drifts from the container proves nothing about it: a test that passes here may
fail there. Run this at the start of a host (no-Docker) task; on a drift fix the venv, not the
test: gbx:venv:sync --match-container --recreate.

A package only one side has is not drift (it cannot be compared); the count is printed. The
Spark Connect client packages that gbx:venv:sync --connect installs are listed as declared
connect-tier extras. GBX_PYRX_VENV=<dir> checks another venv than .venv-pyrx.

Usage: bash scripts/commands/gbx-venv-check.sh [--log <path>]
Options:
  --log <path>   Tee output to a log file (resolved under test-logs/)
  --help, -h     Show this help
HELP
}

while [[ $# -gt 0 ]]; do case $1 in
    --log) [[ $# -ge 2 ]] || { echo "Option --log needs a value" >&2; exit 2; }
        LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:venv:check — host venv vs container"
setup_log_file "$LOG_PATH"

if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    echo "ERROR: no host venv at $VENV_DIR. Run: bash scripts/commands/gbx-venv-sync.sh" >&2
    exit 1
fi
check_docker

WORK="$(mktemp -d "${TMPDIR:-/tmp}/gbx-venv-check.XXXXXX")"   # removed explicitly: an EXIT trap would replace the log's
# The same script reports both environments: read from stdin by each interpreter.
"$VENV_DIR/bin/python" - collect < "$REPORT" > "$WORK/host.json" \
    || { rm -rf "$WORK"; echo "ERROR: could not read the host venv" >&2; exit 1; }
docker exec -i geobrix-dev python3 - collect < "$REPORT" > "$WORK/container.json" \
    || { rm -rf "$WORK"; echo "ERROR: could not read the container's environment" >&2; exit 1; }

EXTRAS=()
[[ -f "$VENV_DIR/.gbx-connect-extras" ]] && EXTRAS=(--extras "$VENV_DIR/.gbx-connect-extras")
echo "host venv : $VENV_DIR"
echo "container : geobrix-dev"
echo ""
"$VENV_DIR/bin/python" "$REPORT" compare "$WORK/host.json" "$WORK/container.json" ${EXTRAS[@]+"${EXTRAS[@]}"}
RC=$?
rm -rf "$WORK"
echo ""
if [[ $RC -eq 0 ]]; then
    echo "✅ host venv matches the container"
else
    echo "❌ host venv drifts from the container: fix the venv (gbx:venv:sync --match-container --recreate), not the test"
fi
exit $RC
