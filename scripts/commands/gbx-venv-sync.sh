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
# test results, and the `test` extra pins mapbox-vector-tile<2.2, which the env6 lock's 2.2 rejects.
LIGHT_LOCK="${PROJECT_ROOT}/python/geobrix/requirements-light-env6-ci.txt"

show_help() {
    show_banner "gbx:venv:sync — isolated pyrx venv"
    cat <<'EOF'
Create or refresh an isolated uv venv (no system site-packages) for the light-tier
tests (gbx:test:light / gbx:test:pyrx, zarr included). It installs what the light CI phase
installs: the hash-pinned lock python/geobrix/requirements-light-env6-ci.txt, then
python/geobrix itself (editable, --no-deps). Re-running refreshes an existing venv in place;
--recreate builds a new venv beside the old one and swaps it in only once everything installed
(and, with --match-container, only once the new venv matches the container); the old venv is
untouched on a failure, and restored if the swap itself is interrupted. GBX_PYRX_VENV=<dir> or --venv <dir> moves the venv
from .venv-pyrx.

Usage: bash scripts/commands/gbx-venv-sync.sh [--python 3.12] [--recreate] [--connect] [--match-container] [--venv <dir>] [--log <path>]
Options:
  --venv <dir>     Put the venv at <dir> instead of .venv-pyrx (same as GBX_PYRX_VENV)
  --python <ver>   Python version for a NEW venv (default 3.12). An existing venv keeps the
                   Python it was made with: asking for another version fails (use --recreate)
  --recreate       Build a fresh venv beside the old one and swap it in on success only
  --connect        Also install the Spark Connect client deps for the venv's own pyspark
                   (grpcio, grpcio-status, googleapis-common-protos, zstandard: pyspark's
                   `connect` extra), so the local Connect tier runs (`spark_connect_local`
                   in python/geobrix/test/conftest.py). Opt-in: the CI lock does not carry
                   them, and nothing already installed changes version. The names are recorded
                   in <venv>/.gbx-connect-extras for gbx:venv:check.
  --match-container
                   Build the venv to match the running dev container instead of the CI
                   lock: the container's exact Python patch version (uv fetches it) and the
                   container's exact version of every package the lock names (the lock's version
                   for the few it lacks). Installed by exact version, WITHOUT hashes: the lock's
                   hashes are for the CI platform, not this one. A local version (rasterio
                   1.5.2+gbx1 from the env wheel's payload) is on no index: the public base is
                   installed, and gbx:venv:check reports the payload GDAL stack as expected.
                   Ends with gbx:venv:check, and fails on any other drift. Needs the container running.
  --log <path>     Tee output to a log file (resolved under test-logs/)
  --help, -h       Show this help
EOF
}

RECREATE=0
CONNECT=0
MATCH=0
PYTHON_EXPLICIT=0
while [[ $# -gt 0 ]]; do case $1 in
    --python|--log|--venv) [[ $# -ge 2 ]] || { echo "Option $1 needs a value" >&2; exit 2; }
        case $1 in
            --python) PYTHON_VERSION="$2"; PYTHON_EXPLICIT=1 ;;
            --venv) VENV_DIR="$2" ;;
            *) LOG_PATH=$(resolve_log_path "$2") ;;
        esac
        shift 2 ;;
    --match-container) MATCH=1; shift ;;
    --recreate) RECREATE=1; shift ;;
    --connect) CONNECT=1; shift ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done
VENV_DIR="$(abs_path "$VENV_DIR")"

cd "$PROJECT_ROOT"
show_banner "gbx:venv:sync — isolated pyrx venv"
setup_log_file "$LOG_PATH"

# --recreate builds into STAGING and swaps it over the venv only when everything installed: the
# old venv is never deleted before its replacement is known to resolve and install.
FINAL_VENV="$VENV_DIR"
STAGING=""
OLD=""        # the old venv, moved aside during the swap
SWAPPED=0     # 1 once the staging venv is at FINAL_VENV
WORK=""   # scratch dir of --match-container; removed explicitly (an EXIT trap would replace the log's)
cleanup() {
    [[ -n "$STAGING" ]] && rm -rf "$STAGING"
    [[ -n "$WORK" ]] && rm -rf "$WORK"
    return 0
}
# restore_old: an interrupted or failed swap puts the old venv back where it was (only when nothing is there now);
# a swap that already moved the new venv in only has the old one left to remove.
restore_old() {
    [[ -n "$OLD" ]] || return 0
    if [[ $SWAPPED -eq 1 || ( -n "$STAGING" && ! -e "$STAGING" ) ]]; then  # the new venv is at FINAL_VENV
        rm -rf "$OLD"
    elif [[ -d "$OLD" && ! -e "$FINAL_VENV" ]]; then
        mv "$OLD" "$FINAL_VENV"
    fi
    return 0
}
# fail <what failed> <how to fix it>: clean up, say the old venv is untouched, exit non-zero.
fail() {
    echo "❌ $1" >&2
    cleanup
    [[ $RECREATE -eq 1 && -d "$FINAL_VENV" ]] && echo "The existing venv at $FINAL_VENV is untouched." >&2
    echo "Fix: $2" >&2
    exit 1
}
trap 'restore_old; cleanup; exit 130' INT TERM
INDEX_FIX="make sure the index (PIP_INDEX_URL or uv's configuration) serves every pinned version, then re-run"

if [[ ! -f "$LIGHT_LOCK" ]]; then
    echo "ERROR: CI light lock not found: $LIGHT_LOCK" >&2
    exit 1
fi

if ! command -v uv >/dev/null 2>&1; then
    echo "ERROR: uv not found on PATH. Install uv: https://docs.astral.sh/uv/" >&2
    exit 1
fi

REPORT="${PROJECT_ROOT}/scripts/ci/venv_env_report.py"
if [[ $MATCH -eq 1 ]]; then
    # The container decides the Python patch version and the package versions.
    check_docker
    WORK="$(mktemp -d "${TMPDIR:-/tmp}/gbx-venv-sync.XXXXXX")"
    docker exec -i "$GBX_DEV_CONTAINER" python3 - collect < "$REPORT" > "$WORK/container.json" \
        || fail "could not read the container's environment" "start the dev container (gbx:docker:start), then re-run"
    PYTHON_VERSION="$(sed -n 's/.*"python": "\([^"]*\)".*/\1/p' "$WORK/container.json")"
    [[ -n "$PYTHON_VERSION" ]] || fail "the container reported no Python version" "check the container's python3, then re-run"
    PYTHON_EXPLICIT=1
    echo "Matching the container: Python $PYTHON_VERSION, its package versions (no hashes: installed by exact version)"
fi

if [[ $RECREATE -eq 1 && -d "$FINAL_VENV" ]]; then
    STAGING="${FINAL_VENV}.staging.$$"
    rm -rf "$STAGING"
    VENV_DIR="$STAGING"
    echo "Recreating venv: building $STAGING; $FINAL_VENV is replaced only if the build succeeds"
fi

# An existing venv keeps its Python: say so instead of silently ignoring --python.
if [[ -x "$VENV_DIR/bin/python" && $PYTHON_EXPLICIT -eq 1 ]]; then
    HAVE_PYTHON="$("$VENV_DIR/bin/python" -c 'import platform; print(platform.python_version())')"
    case "$HAVE_PYTHON" in
        "$PYTHON_VERSION"|"$PYTHON_VERSION".*) ;;
        *) echo "❌ $VENV_DIR is Python $HAVE_PYTHON; --python $PYTHON_VERSION does not change an existing venv. Add --recreate." >&2
           exit 1 ;;
    esac
fi

# uv venvs are no-system-site-packages by default.
# `uv venv` refuses an existing venv, so only create a missing one; a re-run refreshes in place.
if [[ ! -x "$VENV_DIR/bin/python" ]]; then
    # --relocatable only for a staging venv: it is moved to its final path, so its scripts must not hold the old one.
    RELOCATABLE=()
    [[ -n "$STAGING" ]] && RELOCATABLE=(--relocatable)
    uv venv "$VENV_DIR" --python "$PYTHON_VERSION" ${RELOCATABLE[@]+"${RELOCATABLE[@]}"} \
        || fail "uv venv failed" "check that uv can fetch Python $PYTHON_VERSION, then re-run"
fi
if [[ $MATCH -eq 1 ]]; then
    # The lock's packages at the container's versions, plain pins (the lock's hashes are CI's).
    "$VENV_DIR/bin/python" "$REPORT" match-requirements --lock "$LIGHT_LOCK" --container "$WORK/container.json" \
        > "$WORK/requirements.txt" || fail "could not derive the container's requirements" "check scripts/ci/venv_env_report.py and the lock, then re-run"
    uv pip install --python "$VENV_DIR/bin/python" -r "$WORK/requirements.txt" \
        || fail "uv pip install of the container's versions failed" "$INDEX_FIX"
    # Older pins can pull in packages the lock does not name, at their newest release: pin every
    # package both environments have to the container's version (the container is consistent).
    "$VENV_DIR/bin/python" - collect < "$REPORT" > "$WORK/host.json" \
        && "$VENV_DIR/bin/python" "$REPORT" pin-drift "$WORK/host.json" "$WORK/container.json" > "$WORK/pins.txt" \
        || fail "could not compare the new venv with the container" "check scripts/ci/venv_env_report.py, then re-run"
    if [[ -s "$WORK/pins.txt" ]]; then
        echo "Pinning $(wc -l < "$WORK/pins.txt" | tr -d ' ') transitive packages to the container's versions"
        uv pip install --python "$VENV_DIR/bin/python" -r "$WORK/pins.txt" \
            || fail "uv pip install of the container's transitive versions failed" "$INDEX_FIX"
    fi
else
    uv pip install --python "$VENV_DIR/bin/python" --require-hashes -r "$LIGHT_LOCK" \
        || fail "uv pip install of the lock failed" "$INDEX_FIX"
fi
uv pip install --python "$VENV_DIR/bin/python" --no-deps -e ./python/geobrix \
    || fail "uv pip install of the package failed" "fix the package's build (error above), then re-run"

# Opt-in: the Spark Connect client deps. `pyspark[connect]==<installed>` asks pyspark's own
# metadata for them (grpcio, grpcio-status, googleapis-common-protos; zstandard is named too
# for the plan compression), so they match the venv's pyspark whatever its version. The
# constraints file is the venv as it is now: nothing the lock installed may move, and a
# conflict fails here, loudly, instead of silently changing the CI versions.
if [[ $CONNECT -eq 1 ]]; then
    PYSPARK_VERSION="$("$VENV_DIR/bin/python" -c 'import pyspark; print(pyspark.__version__)')" \
        || fail "pyspark is not importable in $VENV_DIR" "check the lock install, then re-run"
    CONSTRAINTS="$(mktemp "${TMPDIR:-/tmp}/gbx-connect-constraints.XXXXXX")"
    uv pip freeze --python "$VENV_DIR/bin/python" | grep -vE '^(-e |#)' > "$CONSTRAINTS"
    uv pip install --python "$VENV_DIR/bin/python" -c "$CONSTRAINTS" \
        "pyspark[connect]==${PYSPARK_VERSION}" zstandard \
        || { rm -f "$CONSTRAINTS"; fail "uv pip install of the Spark Connect client deps failed" "$INDEX_FIX"; }
    rm -f "$CONSTRAINTS"
    # Record what the connect tier declares, so gbx:venv:check can tell these host-only packages
    # from drift (the container does not carry them yet). pyspark's own metadata names them.
    "$VENV_DIR/bin/python" - > "$VENV_DIR/.gbx-connect-extras" <<'PY'
import importlib.metadata as m, re
names = set()
for r in m.requires("pyspark") or []:
    if re.search(r"""extra\s*==\s*["']connect["']""", r):
        names.add(re.split(r"[ ;<>=!~\[(]", r)[0].lower().replace("_", "-"))
names.add("zstandard")  # named explicitly by --connect (plan compression), not by pyspark's extra
print("\n".join(sorted(names)))
PY
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
if [[ $EXIT_CODE -ne 0 ]]; then
    fail "venv sync failed" "read the error above; re-run once it is resolved"
fi
CHECKED=0
if [[ -n "$STAGING" ]]; then
    if [[ $MATCH -eq 1 ]]; then
        # The drift check judges the replacement BEFORE it replaces anything: a drifting venv never displaces a good one.
        echo ""
        GBX_PYRX_VENV="$STAGING" bash "$SCRIPT_DIR/gbx-venv-check.sh" \
            || fail "the new venv drifts from the container" "see the DRIFT rows above (a package the index cannot serve at the container's version, say), then re-run"
        CHECKED=1
    fi
    # Everything installed in the staging venv: only now does it replace the old one.
    OLD="${FINAL_VENV}.old.$$"
    mv "$FINAL_VENV" "$OLD" || fail "could not move the old venv aside" "check permissions on $(dirname "$FINAL_VENV"), then re-run"
    if ! mv "$STAGING" "$FINAL_VENV"; then
        restore_old
        fail "could not move the new venv into place" "check permissions on $(dirname "$FINAL_VENV"), then re-run"
    fi
    SWAPPED=1
    rm -rf "$OLD"
    STAGING=""
    VENV_DIR="$FINAL_VENV"
fi
echo "✅ venv ready at $VENV_DIR"
cleanup
if [[ $MATCH -eq 1 && $CHECKED -eq 0 ]]; then
    echo ""
    GBX_PYRX_VENV="$VENV_DIR" bash "$SCRIPT_DIR/gbx-venv-check.sh"
    EXIT_CODE=$?
fi
exit $EXIT_CODE
