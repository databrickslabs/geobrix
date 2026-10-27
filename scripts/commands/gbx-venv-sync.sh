#!/bin/bash
# gbx:venv:sync — create/refresh the isolated uv venv for the light-tier (pyrx, zarr, ...) tests.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

VENV_DIR="${PROJECT_ROOT}/.venv-pyrx"
PYTHON_VERSION="3.12"
LOG_PATH=""
# Extras: the Serverless env 6 / DBR 19 light set plus the Zarr set (DBR 19 / env 6 only, so the
# venv matches the runtime test/zarr targets). NOT the `test` extra: it pins
# mapbox-vector-tile<2.2, which no light_env6 install can satisfy (light_env6 needs >=2.2).
EXTRAS="light_env6,zarr_env6"
# Test tools, installed directly instead. pyspark is capped because the Delta test jars that
# test/zarr loads (conftest _DELTA_VERSION 4.0.x) run on Spark 4.0.x only; the CI lock pins 4.0.0.
TEST_SPECS=("pytest>=7.0" "pyspark>=4.0,<4.1")

show_help() {
    show_banner "gbx:venv:sync — isolated pyrx venv"
    cat <<'EOF'
Create or refresh an isolated uv venv (no system site-packages) for the light-tier
tests (gbx:test:light / gbx:test:pyrx, zarr included), installing the
[light_env6,zarr_env6] extras from python/geobrix plus pytest and pyspark>=4.0,<4.1.
Re-running refreshes an existing venv in place; --recreate starts from scratch.

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
# `uv venv` refuses an existing venv, so only create a missing one; a re-run refreshes in place.
if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    uv venv "$VENV_DIR" --python "$PYTHON_VERSION" || { echo "❌ uv venv failed" >&2; exit 1; }
fi
uv pip install --python "$VENV_DIR/bin/python" -e "./python/geobrix[${EXTRAS}]" "${TEST_SPECS[@]}" \
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
# The Zarr extra and the Spark line the Delta test jars need.
import pyspark, zarr
assert pyspark.__version__.startswith("4.0."), f"pyspark {pyspark.__version__}: Delta 4.0.x jars need Spark 4.0.x"
print("pyspark", pyspark.__version__, "zarr", zarr.__version__)
PY
EXIT_CODE=$?
[[ $EXIT_CODE -eq 0 ]] && echo "✅ venv ready at $VENV_DIR" || echo "❌ venv sync failed"
exit $EXIT_CODE
