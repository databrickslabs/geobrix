#!/bin/bash
# gbx:venv:sync — create/refresh the isolated uv venv for the lightweight (pyrx) API.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

VENV_DIR="${PROJECT_ROOT}/.venv-pyrx"
PYTHON_VERSION="3.12"
LOG_PATH=""

show_help() {
    show_banner "gbx:venv:sync — isolated pyrx venv"
    cat <<'EOF'
Create or refresh an isolated uv venv (no system site-packages) for the
lightweight pyrx API, installing the [pyrx,test] extras from python/geobrix.

Usage: bash scripts/commands/gbx-venv-sync.sh [--python 3.12] [--recreate] [--log <path>]
Options:
  --python <ver>   Python version for the venv (default 3.12)
  --recreate       Delete and recreate the venv from scratch
  --log <path>     Tee output to a log file (resolved under test-logs/)
  --help, -h       Show this help
EOF
}

RECREATE=0
while [[ $# -gt 0 ]]; do case $1 in
    --python) PYTHON_VERSION="$2"; shift 2 ;;
    --recreate) RECREATE=1; shift ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:venv:sync — isolated pyrx venv"
setup_log_file "$LOG_PATH"

if ! command -v uv >/dev/null 2>&1; then
    echo "ERROR: uv not found on PATH. Install uv: https://docs.astral.sh/uv/" >&2
    exit 1
fi

if [[ $RECREATE -eq 1 && -d "$VENV_DIR" ]]; then
    echo "Recreating venv (removing $VENV_DIR)"
    rm -rf "$VENV_DIR"
fi

# uv venvs are no-system-site-packages by default.
uv venv "$VENV_DIR" --python "$PYTHON_VERSION" || { echo "❌ uv venv failed" >&2; exit 1; }
uv pip install --python "$VENV_DIR/bin/python" -e "./python/geobrix[light_env6,test]" \
    || { echo "❌ uv pip install failed" >&2; exit 1; }

# Assert isolation: the venv must not see host site-packages.
"$VENV_DIR/bin/python" - <<'PY'
import sys
# Confirm rasterio resolves from the venv, not host.
import rasterio, pathlib
p = pathlib.Path(rasterio.__file__).resolve()
assert ".venv-pyrx" in str(p), f"rasterio leaked from host: {p}"
print("venv isolation OK:", sys.prefix)
print("rasterio", rasterio.__version__, "GDAL", rasterio.__gdal_version__)
PY
EXIT_CODE=$?
[[ $EXIT_CODE -eq 0 ]] && echo "✅ venv ready at $VENV_DIR" || echo "❌ venv sync failed"
exit $EXIT_CODE
