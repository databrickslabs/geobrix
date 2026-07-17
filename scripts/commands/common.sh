#!/bin/bash
# Common helper functions for GeoBrix Cursor commands

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Maven env for Docker runs: unset conflicting opts, set Jupyter dirs, and tune Maven/JVM for coverage
# MAVEN_OPTS speeds up builds and scoverage (G1GC + 4G heap when running in geobrix-dev)
export DOCKER_MAVEN_ENV="unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && export MAVEN_OPTS=\"-Xmx4G -XX:+UseG1GC\""

check_docker() {
    if ! docker ps &> /dev/null; then
        echo -e "${RED}❌ Error: Docker is not running${NC}"
        echo "Start Docker and try again."
        exit 1
    fi
    
    if ! docker ps -a --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
        echo -e "${RED}❌ Error: geobrix-dev container not found${NC}"
        echo "Start the development container first:"
        echo -e "  ${YELLOW}./scripts/docker/start_docker.sh${NC}"
        exit 1
    fi
    
    if ! docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
        echo -e "${YELLOW}⚠️  Container is not running. Starting...${NC}"
        docker start geobrix-dev
        sleep 2
    fi
}

resolve_log_path() {
    local log_arg="$1"
    
    if [ -z "$log_arg" ]; then
        echo ""
        return
    fi
    
    # Check if absolute path (starts with /)
    if [[ "$log_arg" == /* ]]; then
        echo "$log_arg"
        return
    fi
    
    # Check if it's just a filename (no directory separator)
    if [[ "$log_arg" != */* ]]; then
        echo "test-logs/$log_arg"
        return
    fi
    
    # It's a relative path - prepend test-logs/
    echo "test-logs/$log_arg"
}

# Central logging: truncate log on each run so every command gets a fresh file.
# Commands that use --log should call this (or setup_log); the only exception is
# scripts that tee a subprocess only—those must truncate explicitly (: > "$LOG_PATH").
#
# Tees all subsequent script output to BOTH the terminal and the log file, reliably under
# `bash` and `sh` alike. The previous implementation used bash-only process substitution
# (`exec > >(tee ...)`) which (a) is a parse error under POSIX sh, so it fell back to a
# file-only redirect that left the terminal silent, and (b) even under bash races the shell
# exit — bash does not wait for the tee in `>(...)`, so the last lines could be truncated.
#
# Mechanism here: a private FIFO drained by a backgrounded `tee`, plus an EXIT trap that
# closes the write end (so tee sees EOF and flushes) and waits for tee before the script
# exits. No process substitution → identical behavior in both shells, no lost tail output.
# Uses `printf '%b'` rather than `echo -e` (which prints a literal "-e" under /bin/sh).
setup_log_file() {
    local log_path="$1"
    [ -n "$log_path" ] || return 0

    mkdir -p "$(dirname "$log_path")"
    : > "$log_path"
    printf '%b\n' "${CYAN}📝 Logging to: ${YELLOW}${log_path}${NC}"

    # Private FIFO. If FIFOs are unavailable, degrade to file-only logging rather than fail.
    local fifo
    fifo="$(mktemp -u "${TMPDIR:-/tmp}/gbx-log.XXXXXX")" || { exec >>"$log_path" 2>&1; return 0; }
    if ! mkfifo "$fifo" 2>/dev/null; then
        exec >>"$log_path" 2>&1
        return 0
    fi

    exec 3>&1                            # save the real terminal stdout on fd 3
    tee -a "$log_path" <"$fifo" >&3 &    # tee drains the FIFO -> log file + terminal
    GBX_TEE_PID=$!                       # global on purpose: the EXIT trap below reads it
    exec >"$fifo" 2>&1                   # route all stdout+stderr into the FIFO
    rm -f "$fifo"                        # unlink now; open fds keep it alive until closed

    # Flush-safe teardown: capture the real exit code, restore stdout/stderr (closing the
    # FIFO write end so tee reaches EOF), wait for tee to finish, then exit with that code.
    # `exit` inside an EXIT trap does not re-run the trap, so this is not recursive.
    trap 'rc=$?; exec 1>&3 2>&3 3>&-; [ -n "${GBX_TEE_PID:-}" ] && wait "${GBX_TEE_PID}" 2>/dev/null; exit $rc' EXIT
}

show_banner() {
    local title="$1"
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}  ${CYAN}$title${NC}"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════╝${NC}"
    echo ""
}

show_separator() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

# Print a clickable file:// URL for the report. Plain URL is clickable in most terminals/IDEs.
# Usage: print_report_link "/absolute/path/to/index.html"
print_report_link() {
    local report_path="$1"
    local abs_path
    abs_path="$(cd "$(dirname "$report_path")" 2>/dev/null && pwd)/$(basename "$report_path")"
    [ -n "$abs_path" ] || abs_path="$report_path"
    # file:// URL (three slashes for absolute path) - most UIs make this clickable
    printf 'file://%s\n' "$abs_path"
}

open_report() {
    local report_path="$1"
    
    if [ ! -f "$report_path" ]; then
        echo -e "${YELLOW}⚠️  Report file not found: $report_path${NC}"
        return 1
    fi
    
    echo -e "${CYAN}📊 Opening report: ${YELLOW}$report_path${NC}"
    
    # Detect OS and open accordingly
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open "$report_path"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        xdg-open "$report_path" &>/dev/null || echo -e "${YELLOW}⚠️  Could not open browser. Open manually: $report_path${NC}"
    else
        echo -e "${YELLOW}⚠️  Unsupported OS. Open manually: $report_path${NC}"
    fi
}

generate_timestamp() {
    date +%Y%m%d-%H%M%S
}

# Warn if the assembly JAR that Spark tests load via spark.jars is stale relative to Scala
# sources. A stale JAR silently tests old behavior and surfaces as UNRESOLVED_ROUTINE for
# functions added since the last `mvn package`. Non-fatal — prints a hint and returns.
# Usage: warn_if_jar_stale "$PROJECT_ROOT"
warn_if_jar_stale() {
    local project_root="$1"
    local rebuild='gbx:docker:exec "mvn clean package -PskipScoverage -DskipTests"'
    local jar
    jar=$(ls -t "$project_root"/target/geobrix-*-jar-with-dependencies.jar 2>/dev/null | head -n 1)
    if [ -z "$jar" ]; then
        echo -e "${YELLOW}⚠️  No assembly JAR in target/ — Spark tests load geobrix-*-jar-with-dependencies.jar via spark.jars.${NC}"
        echo -e "${YELLOW}   Build it first: ${rebuild}${NC}"
        echo ""
        return
    fi
    local newer
    newer=$(find "$project_root/src/main/scala" -name '*.scala' -newer "$jar" -print 2>/dev/null | head -n 1)
    if [ -n "$newer" ]; then
        echo -e "${YELLOW}⚠️  Assembly JAR is older than Scala sources — tests may fail with UNRESOLVED_ROUTINE on newly added functions.${NC}"
        echo -e "${YELLOW}   Stale JAR: $(basename "$jar")${NC}"
        echo -e "${YELLOW}   Rebuild:   ${rebuild}${NC}"
        echo ""
    fi
}

# Aliases for backward compatibility
print_banner() { show_banner "$@"; }
print_separator() { show_separator "$@"; }
setup_log() { setup_log_file "$@"; }

# Validate a benchmark selection set ("core" or "full"). Exits non-zero on bad value.
# Usage: validate_set "$SET"
validate_set() {
    case "$1" in
        core|full) return 0 ;;
        *) echo "ERROR: --set must be 'core' or 'full' (got '$1')" >&2; return 1 ;;
    esac
}

# Assert the arca host GDAL environment is active (LD_LIBRARY_PATH points at the $HOME-local
# GDAL install and gdalinfo is on PATH). The --host test paths need native GDAL in the forked
# Spark JVM; that is provided by sourcing ~/.local/geobrix-gdal-env.sh (geobrix-arca plugin),
# NOT by this repo. We only assert it — we never source it (it's user/plugin-owned).
# Returns non-zero with a remediation message if the env is not active.
require_host_gdal_env() {
    if ! command -v gdalinfo >/dev/null 2>&1 || [[ "${LD_LIBRARY_PATH:-}" != *".local/gdal"* ]]; then
        echo -e "${RED}❌ Host GDAL environment not active.${NC}" >&2
        echo -e "${YELLOW}   --host mode needs native GDAL on the arca host. Source the env first:${NC}" >&2
        echo -e "${YELLOW}     source ~/.local/geobrix-gdal-env.sh${NC}" >&2
        echo -e "${YELLOW}   (provisioned by the geobrix-arca plugin's geobrix-gdal-env skill).${NC}" >&2
        return 1
    fi
    return 0
}

# Ensure a host test venv exists, built from one of CI's exact hash-pinned locks via uv, with the
# geobrix package installed editable+no-deps. Mirrors CI's TWO-environment split (CI never runs all
# Python tests in one env) — pick the venv by the "kind" arg:
#
#   ci    -> $PROJECT_ROOT/.venv-host-ci   from requirements-ci.txt  (~27 pkgs: pyspark, py4j,
#            numpy, pytest; NO rasterio/pandas/pdal). Matches CI's python_build "heavy" job, which
#            runs `pytest test -m "not integration" --ignore=test/pyrx --ignore=test/pyvx`.
#   pyrx  -> $PROJECT_ROOT/.venv-host-pyrx from requirements-pyrx-ci.txt (~104 pkgs: rasterio,
#            shapely, pandas, pyarrow, h3, mapbox-vector-tile, vizx stack). Matches CI's pyrx_build
#            "light" job (test/pyrx test/ds test/pyvx test/pygx test/pmtiles_light test/stac
#            test/vizx test/sample) and is also the right env for the doc-tests (rasterio/pandas).
#
# Both locks are pure wheels on arca (neither contains pdal, which is source-only and needs native
# PDAL the container builds but arca lacks — so no package filtering is required, unlike the
# container image lock). uv is required (stdlib `python3 -m venv` yields a pip-less venv on arca —
# ensurepip is absent). The index is taken from ambient PIP_INDEX_URL/UV_INDEX_URL — never hardcoded.
#
# gdal/osgeo are NOT in either lock (CI installs gdal[numpy] from the apt-matched sdist); on the host
# they are provided on PYTHONPATH by the sourced arca env, additive to the venv at runtime.
#
# A stamp holds the sha256 of the source lock so re-runs skip the install unless the lock changed.
# Set GBX_REBUILD_VENV=1 to force a rebuild. Echoes the venv bin dir; callers use
# "$(ensure_host_test_venv <kind>)/python -m pytest".
ensure_host_test_venv() {
    local kind="${1:-pyrx}"
    local venv_dir reqs
    case "$kind" in
        ci)   venv_dir="${PROJECT_ROOT}/.venv-host-ci";   reqs="${PROJECT_ROOT}/python/geobrix/requirements-ci.txt" ;;
        pyrx) venv_dir="${PROJECT_ROOT}/.venv-host-pyrx"; reqs="${PROJECT_ROOT}/python/geobrix/requirements-pyrx-ci.txt" ;;
        *)    echo -e "${RED}❌ ensure_host_test_venv: unknown kind '$kind' (expected ci|pyrx)${NC}" >&2; return 1 ;;
    esac
    local stamp="${venv_dir}/.gbx-reqs-stamp"
    local py_version="${GBX_HOST_PY_VERSION:-3.12}"

    if ! command -v uv >/dev/null 2>&1; then
        echo -e "${RED}❌ uv not found on PATH — required to build the host test venv.${NC}" >&2
        echo -e "${YELLOW}   Install uv: https://docs.astral.sh/uv/ (or use the geobrix-arca plugin).${NC}" >&2
        return 1
    fi
    if [ ! -f "$reqs" ]; then
        echo -e "${RED}❌ Pinned requirements not found: $reqs${NC}" >&2
        return 1
    fi

    # Stamp on the source lock's hash: if the committed lock changes, the venv rebuilds.
    local want_hash cur_hash
    want_hash="$(sha256sum "$reqs" | awk '{print $1}')"
    cur_hash="$(cat "$stamp" 2>/dev/null || true)"

    if [ "${GBX_REBUILD_VENV:-0}" = "1" ] || [ ! -x "${venv_dir}/bin/python" ] || [ "$want_hash" != "$cur_hash" ]; then
        echo -e "${CYAN}🐍 Building host test venv (${kind}) at ${YELLOW}${venv_dir}${CYAN} from $(basename "$reqs")...${NC}" >&2
        [ "${GBX_REBUILD_VENV:-0}" = "1" ] && rm -rf "$venv_dir"
        uv venv "$venv_dir" --python "$py_version" >&2 \
            || { echo -e "${RED}❌ uv venv failed${NC}" >&2; return 1; }
        uv pip install --python "${venv_dir}/bin/python" --require-hashes -r "$reqs" >&2 \
            || { echo -e "${RED}❌ uv pip install ($kind lock) failed — check PIP_INDEX_URL/proxy coverage${NC}" >&2; return 1; }
        uv pip install --python "${venv_dir}/bin/python" --no-deps -e "${PROJECT_ROOT}/python/geobrix" >&2 \
            || { echo -e "${RED}❌ editable geobrix install failed${NC}" >&2; return 1; }
        printf '%s\n' "$want_hash" > "$stamp"
        echo -e "${GREEN}✅ Host test venv (${kind}) ready.${NC}" >&2
    fi

    echo "${venv_dir}/bin"
}

# Prepare the process environment so a host --host pytest run drives Spark + rasterio correctly.
# Call with the venv bin dir (from ensure_host_test_venv) BEFORE launching pytest. Two exports the
# forked Spark JVM's Python workers and rasterio need:
#   - PYSPARK_PYTHON / PYSPARK_DRIVER_PYTHON = the venv interpreter, so Spark workers use the venv
#     (pandas/pyarrow for Arrow UDFs live there, not in system python3). These MUST be real exports
#     in the current shell — a `VAR=x eval "..."` command-prefix does NOT propagate to the python
#     grandchild, so workers would silently fall back to system python3 (ModuleNotFound: pandas).
#   - unset PROJ_DATA / PROJ_LIB so the venv rasterio uses its own bundled proj.db (layout >=6);
#     the arca env points these at the older $HOME GDAL proj.db (layout 3), which rasterio rejects
#     (CRSError "... another PROJ installation"). unset (not empty-string) is required — an empty
#     PROJ_DATA is a search path of "", not a fallback to bundled data. The JVM GDAL sets PROJ_LIB
#     internally via SetConfigOption (the /usr/share/proj bridge), so the heavy tier is unaffected.
# Usage:  activate_host_python_env "$VENV_BIN"
activate_host_python_env() {
    local venv_bin="$1"
    export PYSPARK_PYTHON="${venv_bin}/python"
    export PYSPARK_DRIVER_PYTHON="${venv_bin}/python"
    unset PROJ_DATA PROJ_LIB
}

# The light-tier test dirs (single source of truth: python/geobrix/test/conftest.py _LIGHT_TEST_DIRS).
# Their modules import light-only deps (rasterio/shapely/pandas/h3/…) at collection time, so they run
# only in the pyrx venv; the ci venv's conftest collect_ignore skips them (rasterio absent). Echoed
# space-separated. Falls back to a hardcoded list only if the conftest can't be parsed.
host_light_test_dirs() {
    local conftest="${PROJECT_ROOT}/python/geobrix/test/conftest.py"
    local dirs
    dirs="$(awk '/_LIGHT_TEST_DIRS *= *\[/{f=1;next} f&&/\]/{f=0} f{gsub(/[",[:space:]]/,""); if($0!="") print}' "$conftest" 2>/dev/null | tr '\n' ' ')"
    if [ -z "${dirs// /}" ]; then
        dirs="bench ds pyrx pyvx pygx pmtiles_light stac vizx sample"
    fi
    echo "$dirs"
}

# Run a command inside the isolated pyrx venv (host, no Docker).
# Usage: run_in_pyrx_venv "<command string>"
# Requires gbx:venv:sync to have been run (venv at $PROJECT_ROOT/.venv-pyrx).
run_in_pyrx_venv() {
    local _cmd="$1"
    local _venv="${PROJECT_ROOT}/.venv-pyrx"
    if [[ ! -x "${_venv}/bin/python" ]]; then
        echo "ERROR: pyrx venv not found at ${_venv}. Run: bash scripts/commands/gbx-venv-sync.sh" >&2
        return 1
    fi
    # shellcheck disable=SC1091
    source "${_venv}/bin/activate"
    bash -c "${_cmd}"
    local _rc=$?
    deactivate || true
    return $_rc
}

# Export helpers + color vars so they survive any subshell that doesn't re-source this file
# (observed on macOS bash 3.2: "show_separator: command not found" mid-script).
export RED GREEN YELLOW BLUE CYAN NC DOCKER_MAVEN_ENV
export -f check_docker resolve_log_path setup_log_file show_banner show_separator \
          print_report_link open_report generate_timestamp warn_if_jar_stale \
          print_banner print_separator setup_log run_in_pyrx_venv validate_set \
          require_host_gdal_env ensure_host_test_venv activate_host_python_env \
          host_light_test_dirs 2>/dev/null || true
