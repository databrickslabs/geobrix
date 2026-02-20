#!/bin/bash
# gbx:test:sql-docs - Run SQL (and Python API) documentation tests

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📚 GeoBrix: SQL Documentation Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:sql-docs${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Description:${NC}"
    echo -e "  Runs documentation tests for the SQL API Reference and related Python API examples"
    echo -e "  (docs/tests/python/api/). Includes test_sql_api.py, test_python_api.py, etc."
    echo ""
    echo -e "${CYAN}Targeting:${NC}"
    echo -e "  ${GREEN}--test <nodeid>${NC}        Single test (e.g. api/test_sql_api.py::test_constant_exists_and_is_string)"
    echo -e "  ${GREEN}--path <path>${NC}          File or dir relative to docs/tests/python/ (default: api/)"
    echo ""
    echo -e "${CYAN}Common options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log (filename → test-logs/<name>)"
    echo -e "  ${GREEN}--markers <marker>${NC}     Pytest markers (e.g. \"not slow\")"
    echo -e "  ${GREEN}--include-integration${NC}  Include integration tests (excluded by default)"
    echo -e "  ${GREEN}--skip-build${NC}           Skip Maven and Python build"
    echo -e "  ${GREEN}--no-sample-data-root${NC}   Do not set GBX_SAMPLE_DATA_ROOT (use env or path_config default)"
    echo -e "  ${GREEN}--help${NC}                 This help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:sql-docs --skip-build${NC}"
    echo -e "  ${YELLOW}gbx:test:sql-docs --test api/test_sql_api.py --skip-build${NC}"
    echo -e "  ${YELLOW}gbx:test:sql-docs --log sql-docs.log${NC}"
    echo ""
}

BASE="/root/geobrix/docs/tests/python"
TEST_PATH="${BASE}/api/"
LOG_PATH=""
MARKERS="-m 'not integration'"
INCLUDE_INTEGRATION=false
SKIP_BUILD=false
# Default: set sample data root so doc tests use minimal bundle (required for remote/CI)
SET_SAMPLE_DATA_ROOT=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --test)
            TEST_PATH="${BASE}/$2"
            shift 2
            ;;
        --path)
            TEST_PATH="${BASE}/$2"
            shift 2
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --markers)
            MARKERS="-m '$2'"
            shift 2
            ;;
        --include-integration)
            INCLUDE_INTEGRATION=true
            MARKERS=""
            shift
            ;;
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --no-sample-data-root)
            SET_SAMPLE_DATA_ROOT=false
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

show_banner "📚 GeoBrix: SQL Documentation Tests"
check_docker
setup_log_file "$LOG_PATH"

mkdir -p "$PROJECT_ROOT/sample-data/Volumes/main/default/geobrix_samples"
if ! docker exec geobrix-dev test -d /Volumes 2>/dev/null; then
    echo -e "${RED}❌ /Volumes not found in container. Start with: ./scripts/docker/start_docker_with_volumes.sh${NC}"
    exit 1
fi

echo -e "${CYAN}🎯 Test path: ${YELLOW}$TEST_PATH${NC}"
[ "$SKIP_BUILD" = true ] && echo -e "${CYAN}⏭️  Skipping build (--skip-build)${NC}"
echo ""

# Use minimal bundle path in container so doc tests pass on remote/CI (unless --no-sample-data-root)
SAMPLE_DATA_ROOT_EXPORT=""
[ "$SET_SAMPLE_DATA_ROOT" = true ] && SAMPLE_DATA_ROOT_EXPORT="export GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data"

RUN_CMD="set -e
unset JAVA_TOOL_OPTIONS
export JUPYTER_PLATFORM_DIRS=1
$SAMPLE_DATA_ROOT_EXPORT
cd /root/geobrix
if [ ! -d /Volumes ]; then echo '❌ /Volumes not found'; exit 1; fi
if [ \"$SKIP_BUILD\" != 'true' ]; then
    echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
    echo 'Building JAR and Python package...'
    echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
    mvn package -DskipTests -q
    cd /root/geobrix/python/geobrix && python3 -m build && cd /root/geobrix
    pip install -e /root/geobrix/python/geobrix --break-system-packages -q
    echo ''
fi
echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
echo 'Running SQL/API documentation tests...'
echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
python3 -m pytest $TEST_PATH -v $MARKERS --tb=short --color=yes
"

docker exec geobrix-dev /bin/bash -c "$RUN_CMD"
EXIT_CODE=$?

echo ""
# Short test summary when logging (pytest-style: FAILED/SKIPPED + totals)
if [ -n "$LOG_PATH" ] && [ -f "$LOG_PATH" ]; then
    echo -e "${BLUE}=== Short test summary (SQL) ===${NC}"
    (grep -E '^FAILED |^SKIPPED ' "$LOG_PATH" 2>/dev/null || true)
    (grep -E '(failed|passed|skipped|deselected).* in [0-9]+\.' "$LOG_PATH" 2>/dev/null | tail -1 || true)
    echo ""
fi
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ SQL documentation tests passed!${NC}"
else
    echo -e "${RED}❌ SQL documentation tests failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

[ -n "$LOG_PATH" ] && echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"

exit $EXIT_CODE
