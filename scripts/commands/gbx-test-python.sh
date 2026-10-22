#!/bin/bash
# gbx:test:python - Run Python unit tests (non-docs)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "🐍 GeoBrix: Python Tests (Non-Docs)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:python${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--path <dir>${NC}           Specific test directory or file (single pytest run; repeatable)"
    echo -e "  ${GREEN}-k <expr>${NC}              Pytest keyword filter (e.g. 'merge or fileName')"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    echo -e "  ${GREEN}--with-integration${NC}     Include ${YELLOW}@pytest.mark.integration${NC} tests (network downloads, slow); excluded by default"
    echo -e "  ${GREEN}--markers <expr>${NC}        Override marker filter with a pytest expression (e.g. 'not slow'); disables the default 'not integration' filter"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Phases (no --path):${NC} the heavy and light tiers cannot share one pytest process, so the default"
    echo -e "  run mirrors CI as separate pytest invocations, each with its own log, and prints a per-phase summary:"
    echo -e "  ${YELLOW}heavy${NC}  test/ minus the light dirs (LIGHT_IGNORES in .github/actions/python_build/action.yml)"
    echo -e "  ${YELLOW}light${NC}  the light dirs (pytest list in .github/actions/pyrx_build/action.yml)"
    echo -e "  Exits non-zero if any phase fails. ${YELLOW}--path${NC} bypasses phasing (one process, as given)."
    echo ""
    echo -e "${CYAN}Staged JAR (heavy tier only):${NC} the heavy tests load ${YELLOW}python/geobrix/lib/geobrix-*-jar-with-dependencies.jar${NC}."
    echo -e "  Before the heavy phase (or a ${YELLOW}--path${NC} that selects heavy tests) the newest JAR from ${YELLOW}target/${NC} is staged there"
    echo -e "  when none is staged or ${YELLOW}target/${NC} is newer. Maven is never run; with no JAR in either place the heavy"
    echo -e "  phase fails at once with one ${YELLOW}PRECONDITION MISSING${NC} line. The light phase never needs the JAR."
    echo ""
    echo -e "${CYAN}Zarr tests (test/zarr):${NC} when test/zarr is among the selected tests the command installs the"
    echo -e "  ${YELLOW}zarr_dbr19${NC} extra and fetches + sha256-checks the OSS Delta Lake jars (scripts/ci/fetch-delta-test-jars.sh)"
    echo -e "  when missing; conftest puts the jars on the JVM classpath. Other runs skip both."
    echo ""
    echo -e "${CYAN}Default marker filter:${NC} ${YELLOW}not integration${NC} (matches CI; opt in with ${GREEN}--with-integration${NC} or override with ${GREEN}--markers${NC})"
    echo ""
    echo -e "${CYAN}Log Path Behavior:${NC}"
    echo -e "  ${YELLOW}filename.log${NC}           → test-logs/filename.log"
    echo -e "  ${YELLOW}subdir/file.log${NC}        → test-logs/subdir/file.log"
    echo -e "  ${YELLOW}/abs/path/file.log${NC}     → /abs/path/file.log"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:python${NC}                                     ${CYAN}# all tiers as CI-style phases (default)${NC}"
    echo -e "  ${YELLOW}gbx:test:python --with-integration${NC}                  ${CYAN}# unit + integration (network)${NC}"
    echo -e "  ${YELLOW}gbx:test:python --path python/geobrix/test/rasterx/${NC}"
    echo -e "  ${YELLOW}gbx:test:python --markers 'not slow' --log python-tests.log${NC}"
    echo ""
}

# Parse arguments
# TEST_PATH accumulates one or more --path values (pytest accepts multiple positional
# paths in a single run). PATH_GIVEN records whether any --path was passed: without one the
# command runs the CI-style phases below instead of a single process over the whole tree.
TEST_PATH=""
PATH_ARGS=()
PATH_GIVEN=0
LOG_PATH=""
KEYWORD=""
# Default: exclude integration tests (network downloads); matches CI's python_build action.
MARKERS="-m 'not integration'"

while [[ $# -gt 0 ]]; do
    case $1 in
        --path)
            # Append so repeated --path flags all run in one pytest invocation.
            TEST_PATH="$TEST_PATH /root/geobrix/$2"
            PATH_ARGS+=("$2")
            PATH_GIVEN=1
            shift 2
            ;;
        -k)
            KEYWORD="-k '$2'"
            shift 2
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --with-integration)
            MARKERS=""
            shift
            ;;
        --markers)
            MARKERS="-m '$2'"
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
done

# Trim the leading space from the --path accumulation.
TEST_PATH="${TEST_PATH# }"

cd "$PROJECT_ROOT"

show_banner "🐍 GeoBrix: Python Tests (Non-Docs)"
check_docker
setup_log_file "$LOG_PATH"

# Python tests run against the assembly JAR (spark.jars); warn if it predates Scala sources.
warn_if_jar_stale "$PROJECT_ROOT"

# Ensure geobrix is importable in Spark worker processes. pytest's pythonpath= in
# pyproject.toml covers the driver (pytest) process only; UDF workers start with the
# system sys.path, so without an editable install every @f.udf test fails with
# ModuleNotFoundError. The pip show check is idempotent — near-zero cost when present.
docker exec geobrix-dev /bin/bash -c \
    "pip show geobrix >/dev/null 2>&1 || \
     pip3 install -e /root/geobrix/python/geobrix --no-deps --break-system-packages --quiet"

# STOPGAP: optional test deps that the current ~4-month-old geobrix-dev image predates.
# Covers: rio-cogeo (COG write), scikit-image (contour/isoband), xarray-spatial
# (terrain/viewshed), pmtiles, h3, pyarrow, netCDF4, quadbin, numexpr, laspy/lazrs (LiDAR),
# rio-tiler, mapbox-vector-tile, exifread, tenacity, contextily, anywidget.
# All are pinned in requirements-dev-container.txt or the env5/env6-all CI lockfiles; a fresh
# image build bakes them in — but a local rebuild is blocked (no JFrog token; CI-only path;
# see dev-image-rebuild-is-ci-operation note in MEMORY.md).  Until the image is rebuilt, this
# targeted (NOT hash-locked) install keeps local suites collecting + running from the dev proxy.
# One import probe gates one install of all packages (versions match the lockfile); near-zero
# cost when present; transitive deps resolve off the dev proxy without drifting numpy/pandas pins.
# The probe also fails when the installed lazrs is older than 0.8.2 (the floor in pyproject.toml):
# 0.8.0 / 0.8.1 print Rust panics on a LAZ file that declares more points than it holds.
# Remove once the image ships these.
# NOTE: tippecanoe is intentionally excluded — it installs but segfaults natively in-container
# (exit 139); a pip install cannot fix a native crash.
docker exec geobrix-dev /bin/bash -c \
    "python3 -c 'import importlib.metadata as m, rio_cogeo, skimage, xrspatial, mapbox_vector_tile, netCDF4, rio_tiler, pmtiles, h3, pyarrow, laspy, lazrs, quadbin, numexpr, exifread, tenacity, contextily, anywidget; assert tuple(int(v) for v in m.version(\"lazrs\").split(\".\")[:3]) >= (0, 8, 2)' >/dev/null 2>&1 || \
     pip3 install --break-system-packages --quiet \
       rio-cogeo==7.0.2 scikit-image==0.26.0 xarray-spatial==0.9.9 \
       pmtiles==3.7.0 h3==4.5.0 pyarrow==19.0.1 netCDF4==1.7.4 \
       quadbin==0.2.2 numexpr==2.14.1 laspy==2.7.0 rio-tiler==9.0.6 \
       lazrs==0.8.2 mapbox-vector-tile==2.1.0 exifread==3.5.1 \
       tenacity==9.1.4 contextily==1.6.2 anywidget==0.11.0"

if [ -n "$MARKERS" ]; then
    echo -e "${CYAN}🏷️  Markers: ${YELLOW}$MARKERS${NC}"
else
    echo -e "${CYAN}🏷️  Markers: ${YELLOW}(none — including integration tests)${NC}"
fi

# ---------------------------------------------------------------------------------------
# Heavy-tier JAR precondition.
#
# The heavy tests glob python/geobrix/lib/geobrix-*-jar-with-dependencies.jar for spark.jars
# and index it at import time, so an empty lib/ makes EVERY heavy module die at collection
# with `IndexError: list index out of range`. CI stages it via the Maven package phase (pom
# `copy-files-on-build`); ensure_staged_jar (common.sh) does the same copy from target/ and
# never runs Maven. The light tier is pure Python and never needs it.
# ---------------------------------------------------------------------------------------
HEAVY_ACTION="$PROJECT_ROOT/.github/actions/python_build/action.yml"
LIGHT_ACTION="$PROJECT_ROOT/.github/actions/pyrx_build/action.yml"

read_light_ignores() {
    sed -nE 's/^[[:space:]]*LIGHT_IGNORES="([^"]*)".*$/\1/p' "$HEAVY_ACTION" | head -1
}

# True when any --path can select heavy-tier tests: anything under python/geobrix/test/ that
# is not inside a light dir (LIGHT_IGNORES), or the whole tree. Paths outside the test tree
# are not heavy tests. If the CI list cannot be read, assume heavy (fail safe).
paths_need_jar() {
    local ignores p rel first
    ignores=$(read_light_ignores)
    for p in "${PATH_ARGS[@]}"; do
        rel="${p%%::*}"; rel="${rel#./}"; rel="${rel%/}"
        case "$rel" in
            python/geobrix|python/geobrix/test) return 0 ;;
            python/geobrix/test/*)
                first="${rel#python/geobrix/test/}"; first="${first%%/*}"
                if [ -n "$ignores" ] && [[ " $ignores " == *" --ignore=test/$first "* ]]; then
                    continue
                fi
                return 0
                ;;
        esac
    done
    return 1
}

# True when any --path can select test/zarr: the tree, the test root, or the zarr dir itself.
paths_need_zarr() {
    local p rel
    for p in "${PATH_ARGS[@]}"; do
        rel="${p%%::*}"; rel="${rel#./}"; rel="${rel%/}"
        case "$rel" in
            python/geobrix|python/geobrix/test|python/geobrix/test/zarr|python/geobrix/test/zarr/*) return 0 ;;
        esac
    done
    return 1
}

# True when every pin of the [zarr_dbr19] extra, read from pyproject.toml (the single source),
# is satisfied by what is installed in the container.
zarr_extra_installed() {
    docker exec -i geobrix-dev python3 - >/dev/null 2>&1 <<'PY'
import importlib.metadata as md
import tomllib

from packaging.requirements import Requirement

with open("/root/geobrix/python/geobrix/pyproject.toml", "rb") as f:
    pins = tomllib.load(f)["project"]["optional-dependencies"]["zarr_dbr19"]
for pin in map(Requirement, pins):
    # PackageNotFoundError (not installed) is a non-zero exit too.
    if not pin.specifier.contains(md.version(pin.name), prereleases=True):
        raise SystemExit(f"{pin.name} {md.version(pin.name)} does not satisfy {pin}")
PY
}

# Everything test/zarr needs that the base dev image does not carry. Called only when test/zarr
# is among the selected tests, so other runs pay nothing.
#   * the [zarr_dbr19] extra. The pins are NOT repeated here: pip installs the extra from
#     pyproject.toml, so a pin change reaches the container without touching this script.
#     `-e` re-resolves only that extra for the already-editable geobrix. Remove once the dev
#     image bakes it in (requirements-dev-container.txt).
#   * the OSS Delta Lake jars, which the manifest tests run against in a local Spark session
#     (test/conftest.py puts them on the JVM classpath when test/zarr is collected). They are
#     not a pip package and Spark's Ivy resolver cannot reach the dev proxy, so
#     scripts/ci/fetch-delta-test-jars.sh fetches them with Maven and checks their sha256.
#     A missing jar is a warning (unrelated suites still run, and the Delta-backed zarr tests
#     fail loudly, naming it); a sha256 MISMATCH (exit 3) aborts, so tampered jars are never
#     put on a classpath.
ensure_zarr_test_env() {
    if ! zarr_extra_installed; then
        echo -e "${CYAN}📦 Installing the zarr_dbr19 extra into geobrix-dev${NC}"
        docker exec geobrix-dev /bin/bash -c \
            "pip3 install --break-system-packages --quiet -e '/root/geobrix/python/geobrix[zarr_dbr19]'" \
            || echo -e "${YELLOW}⚠️  zarr_dbr19 install failed: zarr tests will fail on import${NC}"
    fi
    local rc=0
    docker exec geobrix-dev /bin/bash /root/geobrix/scripts/ci/fetch-delta-test-jars.sh || rc=$?
    if [ "$rc" -eq 3 ]; then
        echo -e "${RED}❌ Delta jar sha256 mismatch; refusing to run (see the message above)${NC}"
        exit 1
    elif [ "$rc" -ne 0 ]; then
        echo -e "${YELLOW}⚠️  Delta jars unavailable: Delta-backed zarr tests will fail (see scripts/ci/fetch-delta-test-jars.sh)${NC}"
    fi
}

# ---------------------------------------------------------------------------------------
# Single run: --path given → one pytest process over exactly the paths asked for.
# ---------------------------------------------------------------------------------------
run_single() {
    echo -e "${CYAN}🎯 Test path: ${YELLOW}$TEST_PATH${NC}"
    echo ""

    if paths_need_jar && ! ensure_staged_jar "$PROJECT_ROOT"; then
        exit 1
    fi
    if paths_need_zarr; then
        ensure_zarr_test_env
    fi
    show_separator
    echo -e "${CYAN}Running tests...${NC}"
    show_separator
    echo ""

    # Build pytest command
    PYTEST_CMD="unset JAVA_TOOL_OPTIONS && \
        cd /root/geobrix && \
        python3 -m pytest $TEST_PATH -v --tb=short --color=yes $MARKERS $KEYWORD"

    docker exec geobrix-dev /bin/bash -c "$PYTEST_CMD"
    EXIT_CODE=$?

    echo ""
    show_separator
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}✅ Python tests passed!${NC}"
    else
        echo -e "${RED}❌ Python tests failed (exit code: $EXIT_CODE)${NC}"
    fi
    show_separator

    if [ -n "$LOG_PATH" ]; then
        echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
    fi

    exit $EXIT_CODE
}

# ---------------------------------------------------------------------------------------
# Phased run: no --path → mirror CI's two Python phases.
#
# WHY phases: the heavy tier (Scala JAR / GDAL natives registered in the Spark session) and
# the light tier (pure Python: pyrx/pyvx/pygx/ds/...) cannot share one pytest process — one
# process over test/ produces spurious collection/session errors. CI runs them as separate
# jobs; this runs them as separate pytest invocations with separate logs.
#
# SOURCE OF TRUTH is the CI actions themselves; the lists are read from them (not copied
# here) so this command cannot drift from CI:
#   heavy: LIGHT_IGNORES in .github/actions/python_build/action.yml
#   light: the `pytest test/... -m ...` line in .github/actions/pyrx_build/action.yml
# Both run from python/geobrix like CI. CI-only extras are skipped locally (the heavy
# phase's --cov and `python -m build`; the light phase's wheel build and fresh install).
# ---------------------------------------------------------------------------------------
load_ci_phases() {
    LIGHT_IGNORES=$(read_light_ignores)
    LIGHT_DIRS=$(sed -nE 's/^[[:space:]]*pytest[[:space:]]+((test\/[A-Za-z0-9_]+[[:space:]]+)+)-m[[:space:]].*$/\1/p' "$LIGHT_ACTION" | head -1)
    LIGHT_DIRS="${LIGHT_DIRS%"${LIGHT_DIRS##*[![:space:]]}"}"

    local tok bad=0
    [ -n "$LIGHT_IGNORES" ] && [ -n "$LIGHT_DIRS" ] || bad=1
    for tok in $LIGHT_IGNORES; do
        [[ "$tok" =~ ^--ignore=test/[A-Za-z0-9_]+$ ]] || bad=1
    done
    for tok in $LIGHT_DIRS; do
        [[ "$tok" =~ ^test/[A-Za-z0-9_]+$ ]] || bad=1
    done
    if [ "$bad" -ne 0 ]; then
        echo -e "${RED}❌ Could not read the CI phase lists:${NC}"
        echo -e "   LIGHT_IGNORES ($HEAVY_ACTION): '${LIGHT_IGNORES}'"
        echo -e "   light dirs    ($LIGHT_ACTION): '${LIGHT_DIRS}'"
        echo -e "${YELLOW}The CI action's shape changed; update load_ci_phases() in gbx-test-python.sh.${NC}"
        exit 2
    fi
}

# Strip ANSI colour codes ($'..' quoting: BSD sed has no \x escapes).
strip_ansi() { sed $'s/\033\\[[0-9;]*[A-Za-z]//g'; }

# run_phase <name> <pytest-args> <log>: one pytest process in python/geobrix; output is
# streamed to the terminal and tee'd to <log>. Returns pytest's exit code.
run_phase() {
    local name="$1" args="$2" log="$3"
    echo ""
    show_separator
    echo -e "${CYAN}▶ Phase: ${YELLOW}${name}${NC}  ${CYAN}(log: ${log})${NC}"
    echo -e "${CYAN}  pytest ${args}${NC}"
    show_separator
    echo ""
    docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && \
        cd /root/geobrix/python/geobrix && \
        python3 -m pytest $args -v --tb=short --color=yes -rfE $MARKERS $KEYWORD" 2>&1 | tee -a "$log"
    return "${PIPESTATUS[0]}"
}

run_phased() {
    load_ci_phases

    local base
    if [ -n "$LOG_PATH" ]; then
        base="${LOG_PATH%.log}"
    else
        base="test-logs/python-tests"
    fi
    mkdir -p "$(dirname "$base")"

    local names=() argsets=() needs_jar=() codes=() logs=()
    names+=("heavy");  argsets+=("test $LIGHT_IGNORES"); needs_jar+=(1)
    names+=("light");  argsets+=("$LIGHT_DIRS");         needs_jar+=(0)

    echo -e "${CYAN}🧩 Phases (no --path; mirrors CI): ${YELLOW}${names[*]}${NC}"

    # The light phase includes test/zarr whenever the CI list does.
    if [[ " $LIGHT_DIRS " == *" test/zarr "* ]]; then
        ensure_zarr_test_env
    fi

    local i code
    for i in "${!names[@]}"; do
        logs[$i]="${base}-${names[$i]}.log"
        : > "${logs[$i]}"
        if [ "${needs_jar[$i]}" -eq 1 ]; then
            # Stage (or fail with one clear line) BEFORE pytest; the light phase still runs.
            ensure_staged_jar "$PROJECT_ROOT" 2>&1 | tee -a "${logs[$i]}"
            code=${PIPESTATUS[0]}
        else
            code=0
        fi
        if [ "$code" -eq 0 ]; then
            run_phase "${names[$i]}" "${argsets[$i]}" "${logs[$i]}"
            code=$?
        fi
        # With -k, a phase that selects nothing exits 5; that is not a failure.
        if [ "$code" -eq 5 ] && [ -n "$KEYWORD" ]; then code=0; fi
        codes[$i]=$code
    done

    echo ""
    show_separator
    echo -e "${CYAN}Per-phase summary${NC}"
    show_separator
    local overall=0 line fails
    for i in "${!names[@]}"; do
        line=$(strip_ansi < "${logs[$i]}" \
            | grep -E '^=+ .* in [0-9.]+s( \([0-9:]+\))? =+$' | tail -1 \
            | sed -E 's/^=+ //; s/ =+$//')
        [ -n "$line" ] || line=$(strip_ansi < "${logs[$i]}" | grep -m1 '^PRECONDITION MISSING')
        [ -n "$line" ] || line="(no pytest summary line — see log)"
        if [ "${codes[$i]}" -eq 0 ]; then
            echo -e "  ${GREEN}PASS${NC}  ${names[$i]}: ${line}"
        else
            overall=1
            echo -e "  ${RED}FAIL${NC}  ${names[$i]} (exit ${codes[$i]}): ${line}"
            fails=$(strip_ansi < "${logs[$i]}" | grep -E '^(FAILED|ERROR) ' | sort -u)
            if [ -n "$fails" ]; then
                echo "$fails" | head -25 | sed 's/^/        /'
                [ "$(echo "$fails" | wc -l)" -gt 25 ] && echo "        ... ($(echo "$fails" | wc -l | tr -d ' ') total; full list in the log)"
            fi
        fi
        echo -e "        log: ${logs[$i]}"
    done

    echo ""
    show_separator
    if [ "$overall" -eq 0 ]; then
        echo -e "${GREEN}✅ Python tests passed (all phases)!${NC}"
    else
        echo -e "${RED}❌ Python tests failed (one or more phases; see summary above)${NC}"
    fi
    show_separator
    if [ -n "$LOG_PATH" ]; then
        echo -e "${CYAN}📝 Combined log saved to: ${YELLOW}$LOG_PATH${NC}"
    fi
    exit "$overall"
}

if [ "$PATH_GIVEN" -eq 1 ]; then
    run_single
else
    run_phased
fi
