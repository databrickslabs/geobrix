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
    echo -e "  ${GREEN}--perf${NC}                 Also run the perf-marked cases (slow tessellate / geomk / streamed-merge timing and memory"
    echo -e "                          tests); the default unit loop skips them, CI runs them"
    echo -e "  ${GREEN}--docker${NC}               Run in the geobrix-dev container instead of the host venv (default). Only for GDAL"
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

# The marker expression: integration tests are always excluded, and so are the perf-marked cases
# (the unit loop; --perf includes them); --markers narrows further.
BASE_MARKERS="(not integration) and (not perf)"
[[ $WITH_PERF -eq 1 ]] && BASE_MARKERS="not integration"
MARKERS="$BASE_MARKERS"
[[ -n "$EXTRA_MARKERS" ]] && MARKERS="($BASE_MARKERS) and ($EXTRA_MARKERS)"
MARKERS_Q="$(printf '%q' "$MARKERS")"   # one shell word inside the `bash -c` command string below

echo -e "${CYAN}Test path: ${YELLOW}${TEST_PATHS[*]}${NC}"
echo -e "${CYAN}Markers: ${YELLOW}${MARKERS}${NC}"
echo ""
show_separator
echo -e "${CYAN}Running light-tier tests...${NC}"
show_separator
echo ""

if [[ $USE_DOCKER -eq 1 ]]; then
    check_docker
    gbx_acquire_test_lock   # only the container run shares Maven/Spark state; the host venv run does not
    PYTEST_CMD="unset JAVA_TOOL_OPTIONS && cd /root/geobrix && \
        python3 -m pytest ${TEST_PATHS[*]} -v --tb=short --color=yes -m $MARKERS_Q"
    ENV_ARGS=()
    [[ -n "${PYTEST_ADDOPTS:-}" ]] && ENV_ARGS+=(-e "PYTEST_ADDOPTS=$PYTEST_ADDOPTS")
    docker exec "${ENV_ARGS[@]}" geobrix-dev $GBX_LOCK_RUN /bin/bash -c "$PYTEST_CMD"
    EXIT_CODE=$?
else
    # Host venv path (default): isolated from system site-packages. run_in_pyrx_venv also
    # points PYSPARK_PYTHON at the venv python so Spark workers match the driver.
    HOST_PATHS=()
    for p in "${TEST_PATHS[@]}"; do
        HOST_PATHS+=("${p#/root/geobrix/}")   # strip the in-container prefix if present
    done
    run_in_pyrx_venv "cd '$PROJECT_ROOT' && python -m pytest ${HOST_PATHS[*]} \
        -v --tb=short --color=yes -m $MARKERS_Q"
    EXIT_CODE=$?
fi

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}Light-tier tests passed!${NC}"
else
    echo -e "${RED}Light-tier tests failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
