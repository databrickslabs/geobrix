#!/bin/bash
# gbx:test:pyrx - Run the pyrx lightweight raster API tests (no JAR required)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: pyrx Tests (lightweight, no JAR)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:pyrx${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--path <dir>${NC}           Specific test directory or file (default: python/geobrix/test/pyrx/)"
    echo -e "  ${GREEN}--docker${NC}               Run in the geobrix-dev container instead of the host venv (default)"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Log Path Behavior:${NC}"
    echo -e "  ${YELLOW}filename.log${NC}           -> test-logs/filename.log"
    echo -e "  ${YELLOW}subdir/file.log${NC}        -> test-logs/subdir/file.log"
    echo -e "  ${YELLOW}/abs/path/file.log${NC}     -> /abs/path/file.log"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:pyrx${NC}"
    echo -e "  ${YELLOW}gbx:test:pyrx --path python/geobrix/test/pyrx/test_functions_spark.py --log pyrx.log${NC}"
    echo ""
}

# Parse arguments
DEFAULT_TEST_PATH="/root/geobrix/python/geobrix/test/pyrx/"
TEST_PATHS=()
LOG_PATH=""
USE_DOCKER=0

while [[ $# -gt 0 ]]; do
    case $1 in
        --path)
            # Accumulate: --path may be repeated to run several files/dirs.
            TEST_PATHS+=("/root/geobrix/$2")
            shift 2
            ;;
        --docker)
            USE_DOCKER=1
            shift
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
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

show_banner "GeoBrix: pyrx Tests (lightweight, no JAR)"
setup_log_file "$LOG_PATH"

echo -e "${CYAN}Test path: ${YELLOW}${TEST_PATHS[*]}${NC}"
echo -e "${CYAN}Markers: ${YELLOW}not integration${NC}"
echo ""
show_separator
echo -e "${CYAN}Running pyrx tests...${NC}"
show_separator
echo ""

if [[ $USE_DOCKER -eq 1 ]]; then
    check_docker
    PYTEST_CMD="unset JAVA_TOOL_OPTIONS && cd /root/geobrix && \
        python3 -m pytest ${TEST_PATHS[*]} -v --tb=short --color=yes -m 'not integration'"
    docker exec geobrix-dev /bin/bash -c "$PYTEST_CMD"
    EXIT_CODE=$?
else
    # Host venv path (default): isolated from system site-packages.
    HOST_PATHS=()
    for p in "${TEST_PATHS[@]}"; do
        HOST_PATHS+=("${p#/root/geobrix/}")   # strip the in-container prefix if present
    done
    run_in_pyrx_venv "cd '$PROJECT_ROOT' && python -m pytest ${HOST_PATHS[*]} \
        -v --tb=short --color=yes -m 'not integration'"
    EXIT_CODE=$?
fi

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}pyrx tests passed!${NC}"
else
    echo -e "${RED}pyrx tests failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
