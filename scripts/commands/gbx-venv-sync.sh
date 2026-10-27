#!/bin/bash
# gbx:venv:sync — create/refresh the isolated uv venv for the light-tier (pyrx, zarr, ...) tests.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

# GBX_PYRX_VENV moves the venv (e.g. to try a new lock without touching a venv a test run is using).
VENV_DIR="${GBX_PYRX_VENV:-${PROJECT_ROOT}/.venv-pyrx}"
PYTHON_VERSION="3.12"
LOG_PATH=""
# The venv is the CI light phase's install, not an extras resolve: the hash-pinned lock (every
# test dependency at CI's exact version, incl. the Zarr set, pyspark 4.0.x for the Delta test
# jars, tenacity/anywidget for test/stac and test/vizx), then the package itself with --no-deps.
# An extras install floats: an older venv keeps stale versions (h3, pyogrio, numpy) that change
# test results, and the `test` extra pins mapbox-vector-tile<2.2, which light_env6 (>=2.2) rejects.
LIGHT_LOCK="${PROJECT_ROOT}/python/geobrix/requirements-light-env6-ci.txt"

show_help() {
    show_banner "gbx:venv:sync — isolated pyrx venv"
    cat <<'EOF'
Create or refresh an isolated uv venv (no system site-packages) for the light-tier
tests (gbx:test:light / gbx:test:pyrx, zarr included). It installs what the light CI phase
installs: the hash-pinned lock python/geobrix/requirements-light-env6-ci.txt, then
python/geobrix itself (editable, --no-deps). Re-running refreshes an existing venv in place;
--recreate starts from scratch. GBX_PYRX_VENV=<dir> moves the venv from .venv-pyrx.

Usage: bash scripts/commands/gbx-venv-sync.sh [--python 3.12] [--recreate] [--connect] [--log <path>]
Options:
  --python <ver>   Python version for the venv (default 3.12)
  --recreate       Delete and recreate the venv from scratch
  --connect        Also install the Spark Connect client deps for the venv's own pyspark
                   (grpcio, grpcio-status, googleapis-common-protos, zstandard: pyspark's
                   `connect` extra), so the local Connect tier runs (`spark_connect_local`
                   in python/geobrix/test/conftest.py). Opt-in: the CI lock does not carry
                   them, and nothing already installed changes version.
  --log <path>     Tee output to a log file (resolved under test-logs/)
  --help, -h       Show this help
EOF
}

RECREATE=0
CONNECT=0
while [[ $# -gt 0 ]]; do case $1 in
    --python|--log) [[ $# -ge 2 ]] || { echo "Option $1 needs a value" >&2; exit 2; }
        if [[ $1 == --python ]]; then PYTHON_VERSION="$2"; else LOG_PATH=$(resolve_log_path "$2"); fi
        shift 2 ;;
    --recreate) RECREATE=1; shift ;;
    --connect) CONNECT=1; shift ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:venv:sync — isolated pyrx venv"
setup_log_file "$LOG_PATH"

if [[ ! -f "$LIGHT_LOCK" ]]; then
    echo "ERROR: CI light lock not found: $LIGHT_LOCK" >&2
    exit 1
fi

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
uv pip install --python "$VENV_DIR/bin/python" --require-hashes -r "$LIGHT_LOCK" \
    || { echo "❌ uv pip install of the lock failed" >&2; exit 1; }
uv pip install --python "$VENV_DIR/bin/python" --no-deps -e ./python/geobrix \
    || { echo "❌ uv pip install of the package failed" >&2; exit 1; }

# Opt-in: the Spark Connect client deps. `pyspark[connect]==<installed>` asks pyspark's own
# metadata for them (grpcio, grpcio-status, googleapis-common-protos; zstandard is named too
# for the plan compression), so they match the venv's pyspark whatever its version. The
# constraints file is the venv as it is now: nothing the lock installed may move, and a
# conflict fails here, loudly, instead of silently changing the CI versions.
if [[ $CONNECT -eq 1 ]]; then
    PYSPARK_VERSION="$("$VENV_DIR/bin/python" -c 'import pyspark; print(pyspark.__version__)')" \
        || { echo "❌ pyspark is not importable in $VENV_DIR" >&2; exit 1; }
    CONSTRAINTS="$(mktemp "${TMPDIR:-/tmp}/gbx-connect-constraints.XXXXXX")"
    uv pip freeze --python "$VENV_DIR/bin/python" | grep -vE '^(-e |#)' > "$CONSTRAINTS"
    uv pip install --python "$VENV_DIR/bin/python" -c "$CONSTRAINTS" \
        "pyspark[connect]==${PYSPARK_VERSION}" zstandard \
        || { rm -f "$CONSTRAINTS"; echo "❌ uv pip install of the Spark Connect client deps failed" >&2; exit 1; }
    rm -f "$CONSTRAINTS"
fi

# Assert isolation: the venv must not see host site-packages.
"$VENV_DIR/bin/python" - <<'PY'
import sys
# Confirm rasterio resolves from the venv, not host.
import rasterio, pathlib
p = pathlib.Path(rasterio.__file__).resolve()
assert str(pathlib.Path(sys.prefix).resolve()) in str(p), f"rasterio leaked from host: {p}"
print("venv isolation OK:", sys.prefix)
print("rasterio", rasterio.__version__, "GDAL", rasterio.__gdal_version__)
# The Zarr set and the Spark line the Delta test jars need.
import pyspark, zarr
assert pyspark.__version__.startswith("4.0."), f"pyspark {pyspark.__version__}: Delta 4.0.x jars need Spark 4.0.x"
print("pyspark", pyspark.__version__, "zarr", zarr.__version__)
PY
EXIT_CODE=$?
if [[ $EXIT_CODE -eq 0 && $CONNECT -eq 1 ]]; then
    "$VENV_DIR/bin/python" -c 'import grpc, grpc_status, google.rpc, pyspark.sql.connect.session; print("Spark Connect client OK: grpcio", grpc.__version__)'
    EXIT_CODE=$?
fi
[[ $EXIT_CODE -eq 0 ]] && echo "✅ venv ready at $VENV_DIR" || echo "❌ venv sync failed"
exit $EXIT_CODE
