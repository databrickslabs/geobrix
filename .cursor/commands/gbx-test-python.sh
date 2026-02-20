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
    echo -e "  ${GREEN}--path <dir>${NC}           Specific test directory or file"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    echo -e "  ${GREEN}--markers <marker>${NC}     Run tests with specific markers (e.g., '-m slow')"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Log Path Behavior:${NC}"
    echo -e "  ${YELLOW}filename.log${NC}           → test-logs/filename.log"
    echo -e "  ${YELLOW}subdir/file.log${NC}        → test-logs/subdir/file.log"
    echo -e "  ${YELLOW}/abs/path/file.log${NC}     → /abs/path/file.log"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:python${NC}"
    echo -e "  ${YELLOW}gbx:test:python --path python/geobrix/test/rasterx/${NC}"
    echo -e "  ${YELLOW}gbx:test:python --markers 'not slow' --log python-tests.log${NC}"
    echo ""
}

# Parse arguments
TEST_PATH="/root/geobrix/python/geobrix/test/"
LOG_PATH=""
MARKERS=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --path)
            TEST_PATH="/root/geobrix/$2"
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

cd "$PROJECT_ROOT"

show_banner "🐍 GeoBrix: Python Tests (Non-Docs)"
check_docker
setup_log_file "$LOG_PATH"

echo -e "${CYAN}🎯 Test path: ${YELLOW}$TEST_PATH${NC}"
if [ -n "$MARKERS" ]; then
    echo -e "${CYAN}🏷️  Markers: ${YELLOW}$MARKERS${NC}"
fi

echo ""
show_separator
echo -e "${CYAN}Running tests...${NC}"
show_separator
echo ""

# Build pytest command
PYTEST_CMD="unset JAVA_TOOL_OPTIONS && \
    cd /root/geobrix && \
    python3 -m pytest $TEST_PATH -v --tb=short --color=yes $MARKERS"

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
