#!/bin/bash
# gbx:test:pyrx - Run light-tier Python tests (pyrx by default; any light dir, zarr included) on the host
# venv (no JAR, no container). See gbx:test:light for the whole light tier.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Light-tier Tests (pyrx, zarr, ...; no JAR)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:pyrx${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--path <dir>${NC}           Test directory or file; repeatable. Any light dir (default: python/geobrix/test/pyrx/)"
    echo -e "                          e.g. python/geobrix/test/zarr/test_manifest.py. The light dirs are _LIGHT_TEST_DIRS in test/conftest.py"
    echo -e "  ${GREEN}--markers <expr>${NC}       Extra pytest marker expression, ANDed with the default '(not integration) and (not perf)',"
    echo -e "                          e.g. --markers 'not delta' (runs as -m '((not integration) and (not perf)) and (not delta)')"
    echo -e "  ${GREEN}--perf${NC}                 Also run the perf-marked cases (slow tessellate / geomk / streamed-merge cases, some of them"
    echo -e "                          correctness oracles); the default unit loop skips them, CI runs them. Pass it on the full run before a push"
    echo -e "  ${GREEN}--with-integration${NC}     Also run the integration-marked tests (they need the network or a long setup, e.g. installing a"
    echo -e "                          package into a scratch venv); CI and the default loop exclude them. Combine with --path/--markers to pick one"
    echo -e "  ${GREEN}--docker${NC}               Run in the dev container instead of the host venv (default). Only for GDAL"
    echo -e "                          natives, the JAR and parity"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    print_lock_help
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Log Path Behavior:${NC}"
    echo -e "  ${YELLOW}filename.log${NC}           -> test-logs/filename.log"
    echo -e "  ${YELLOW}subdir/file.log${NC}        -> test-logs/subdir/file.log"
    echo -e "  ${YELLOW}/abs/path/file.log${NC}     -> /abs/path/file.log"
    echo ""
    echo -e "${CYAN}Environment:${NC}"
    echo -e "  ${YELLOW}GBX_PYRX_VENV=<dir>${NC}  Use this venv instead of .venv-pyrx (create it with the same variable: gbx:venv:sync)"
    echo -e "  ${YELLOW}GBX_TEST_TZ=<zone>${NC}   Run under this time zone instead of UTC (the host run pins UTC, as the container and CI are), e.g. America/New_York, to check a timestamp test does not depend on UTC"
    echo ""
    echo -e "${CYAN}Other pytest options:${NC}"
    echo -e "  Set ${YELLOW}PYTEST_ADDOPTS${NC} in the calling shell (host and --docker runs both pass it on), e.g."
    echo -e "  ${YELLOW}PYTEST_ADDOPTS='--durations=20 --junitxml=test-logs/pyrx.xml' gbx:test:pyrx --path ...${NC}"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:pyrx${NC}"
    echo -e "  ${YELLOW}gbx:test:pyrx --path python/geobrix/test/zarr/test_manifest.py${NC}"
    echo -e "  ${YELLOW}gbx:test:pyrx --path python/geobrix/test/zarr/ --markers 'not delta'${NC}"
    echo -e "  ${YELLOW}gbx:test:pyrx --path python/geobrix/test/pyrx/test_functions_spark.py --log pyrx.log${NC}"
    echo ""
}

# Parse arguments
DEFAULT_TEST_PATH="/root/geobrix/python/geobrix/test/pyrx/"
TEST_PATHS=()
LOG_PATH=""
USE_DOCKER=0
EXTRA_MARKERS=""
WITH_PERF=0
WITH_INTEGRATION=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --path)
            [[ $# -ge 2 ]] || { echo -e "${RED}Option --path needs a value${NC}" >&2; exit 2; }
            # Accumulate: --path may be repeated to run several files/dirs.
            TEST_PATHS+=("/root/geobrix/$2")
            shift 2
            ;;
        --markers)
            [[ $# -ge 2 ]] || { echo -e "${RED}Option --markers needs a value${NC}" >&2; exit 2; }
            EXTRA_MARKERS="$2"
            shift 2
            ;;
        --docker)
            USE_DOCKER=1
            shift
            ;;
        --perf)
            WITH_PERF=1
            shift
            ;;
        --with-integration)
            WITH_INTEGRATION=1
            shift
            ;;
        --log)
            [[ $# -ge 2 ]] || { echo -e "${RED}Option --log needs a value${NC}" >&2; exit 2; }
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --no-wait)
            GBX_LOCK_NO_WAIT=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

# Default to the full pyrx suite when no --path was given.
if [[ ${#TEST_PATHS[@]} -eq 0 ]]; then
    TEST_PATHS=("$DEFAULT_TEST_PATH")
fi

show_banner "GeoBrix: Light-tier Tests (pyrx, zarr, ...; no JAR)"
setup_log_file "$LOG_PATH"

# The marker expression: integration tests and the perf-marked cases are excluded by default (the
# unit loop; --with-integration and --perf include them); --markers narrows further.
EXCLUDES=()
[[ $WITH_INTEGRATION -eq 1 ]] || EXCLUDES+=("not integration")
[[ $WITH_PERF -eq 1 ]] || EXCLUDES+=("not perf")
BASE_MARKERS=""
for e in "${EXCLUDES[@]}"; do BASE_MARKERS="${BASE_MARKERS:+$BASE_MARKERS and }($e)"; done
[[ ${#EXCLUDES[@]} -eq 1 ]] && BASE_MARKERS="${EXCLUDES[0]}"
MARKERS="$BASE_MARKERS"
if [[ -n "$EXTRA_MARKERS" ]]; then
    MARKERS="${BASE_MARKERS:+($BASE_MARKERS) and }($EXTRA_MARKERS)"
fi
# The tests that use the local Spark Connect session (the spark_connect_local fixture, marked `connect` by the test
# conftest) cannot share a process with the classic ones: the fixture skips while a classic SparkContext is live, and
# a live Connect session turns getOrCreate into a Connect client. So when the selected paths hold any, they run in a
# pytest process after the first, which leaves them out. Each file that uses the fixture gets a process of its own: a
# light module imported before the Connect session exists (any file that imports the bench modules does) binds its
# UDFs to the classic session.
CONNECT_FILES=()
for p in "${TEST_PATHS[@]}"; do
    HOST_P="${p#/root/geobrix/}"
    while IFS= read -r f; do
        [[ $(basename "$f") == conftest.py ]] && continue
        CONNECT_FILES+=("${f#"$PROJECT_ROOT"/}")
    done < <(grep -rls --include='*.py' spark_connect_local "$PROJECT_ROOT/$HOST_P" | sort)
done
HAS_CONNECT=0
[[ ${#CONNECT_FILES[@]} -gt 0 ]] && HAS_CONNECT=1
MAIN_MARKERS="$MARKERS"
CONNECT_MARKERS="connect"
if [[ $HAS_CONNECT -eq 1 ]]; then
    MAIN_MARKERS="${MARKERS:+($MARKERS) and }(not connect)"
    CONNECT_MARKERS="${MARKERS:+($MARKERS) and }(connect)"
fi

echo -e "${CYAN}Test path: ${YELLOW}${TEST_PATHS[*]}${NC}"
echo -e "${CYAN}Markers: ${YELLOW}${MAIN_MARKERS}${NC}"
[[ $HAS_CONNECT -eq 1 ]] && echo -e "${CYAN}Then, a process each for ${CONNECT_FILES[*]} (Spark Connect): ${YELLOW}${CONNECT_MARKERS}${NC}"
echo ""

# run_pass <marker expression> <path>...: one pytest process over the paths; its exit code.
run_pass() {
    local markers_q paths=() hp
    markers_q="$(printf '%q' "$1")"   # one shell word inside the `bash -c` command string below
    shift
    if [[ $USE_DOCKER -eq 1 ]]; then
        local pytest_cmd env_args=()
        for hp in "$@"; do paths+=("/root/geobrix/${hp#/root/geobrix/}"); done
        pytest_cmd="unset JAVA_TOOL_OPTIONS && cd /root/geobrix && \
            python3 -m pytest ${paths[*]} -v --tb=short --color=yes -m $markers_q"
        [[ -n "${PYTEST_ADDOPTS:-}" ]] && env_args+=(-e "PYTEST_ADDOPTS=$PYTEST_ADDOPTS")
        gbx_native_env_args
        env_args+=(${GBX_NATIVE_ENV_ARGS[@]+"${GBX_NATIVE_ENV_ARGS[@]}"})
        docker exec ${env_args[@]+"${env_args[@]}"} "$GBX_DEV_CONTAINER" $GBX_LOCK_RUN /bin/bash -c "$pytest_cmd"
    else
        # Host venv path (default): isolated from system site-packages. run_in_pyrx_venv also
        # points PYSPARK_PYTHON at the venv python so Spark workers match the driver.
        for hp in "$@"; do paths+=("${hp#/root/geobrix/}"); done   # strip the in-container prefix if present
        run_in_pyrx_venv "cd '$PROJECT_ROOT' && python -m pytest ${paths[*]} \
            -v --tb=short --color=yes -m $markers_q"
    fi
}

show_separator
echo -e "${CYAN}Running light-tier tests...${NC}"
show_separator
echo ""

if [[ $USE_DOCKER -eq 1 ]]; then
    check_docker
    gbx_acquire_test_lock   # only the container run shares Maven/Spark state; the host venv run does not
fi
run_pass "$MAIN_MARKERS" "${TEST_PATHS[@]}"
EXIT_CODE=$?
if [[ $HAS_CONNECT -eq 1 ]]; then
    for cf in "${CONNECT_FILES[@]}"; do
        echo ""
        show_separator
        echo -e "${CYAN}Running the Spark Connect tests of ${cf} in their own process...${NC}"
        show_separator
        echo ""
        run_pass "$CONNECT_MARKERS" "$cf"
        CONNECT_EXIT=$?
        # Exit 5 is "nothing collected": fine for one process while another ran (a path of only Connect tests, or
        # --markers that leave them out). A failure in any process wins; all 5 is 5.
        if [[ $EXIT_CODE -ne 0 && $EXIT_CODE -ne 5 ]]; then :
        elif [[ $CONNECT_EXIT -ne 0 && $CONNECT_EXIT -ne 5 ]]; then EXIT_CODE=$CONNECT_EXIT
        elif [[ $EXIT_CODE -eq 5 && $CONNECT_EXIT -eq 5 ]]; then EXIT_CODE=5
        else EXIT_CODE=0
        fi
    done
fi

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Light-tier tests passed!${NC}"
else
    echo -e "${RED}Light-tier tests failed (exit code: $EXIT_CODE)${NC}"
fi
if [ $EXIT_CODE -eq 5 ]; then
    # pytest exit 5: nothing was collected under the marker expression. A file whose only tests are
    # perf-marked (test_geomk_golden.py) or integration-marked lands here, with no other sign of why.
    OPT_INS=""
    [ $WITH_PERF -eq 0 ] && OPT_INS="--perf (the perf-marked cases)"
    [ $WITH_INTEGRATION -eq 0 ] && OPT_INS="${OPT_INS:+$OPT_INS or }--with-integration (the integration-marked tests)"
    if [ -n "$OPT_INS" ]; then
        echo -e "${YELLOW}No tests ran: every selected test is excluded by the markers. Skipped by default: add $OPT_INS.${NC}"
    else
        echo -e "${YELLOW}No tests ran: every selected test is excluded by --markers.${NC}"
    fi
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
