#!/bin/bash
# gbx:coverage:python - Run Python unit tests with code coverage

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📊 GeoBrix: Python Code Coverage (Non-Docs)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:coverage:python${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--path <dir>${NC}              Specific test directory (default: python/geobrix/test/)"
    echo -e "  ${GREEN}--min-coverage <percent>${NC}  Minimum coverage threshold"
    echo -e "  ${GREEN}--log <path>${NC}              Write output to log file"
    echo -e "  ${GREEN}--open${NC}                    Open HTML report in browser after generation"
    echo -e "  ${GREEN}--help${NC}                    Show this help"
    echo ""
    echo -e "${CYAN}Log Path Behavior:${NC}"
    echo -e "  ${YELLOW}filename.log${NC}              → test-logs/filename.log"
    echo -e "  ${YELLOW}subdir/file.log${NC}           → test-logs/subdir/file.log"
    echo -e "  ${YELLOW}/abs/path/file.log${NC}        → /abs/path/file.log"
    echo ""
    echo -e "${CYAN}Output:${NC}"
    echo -e "  HTML Report:  ${YELLOW}python/coverage-report/index.html${NC}"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:coverage:python${NC}"
    echo -e "  ${YELLOW}gbx:coverage:python --min-coverage 80 --open${NC}"
    echo -e "  ${YELLOW}gbx:coverage:python --path python/geobrix/test/rasterx/${NC}"
    echo ""
}

# Parse arguments
TEST_PATH="/root/geobrix/python/geobrix/test/"
MIN_COVERAGE=""
LOG_PATH=""
OPEN_REPORT=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --path)
            TEST_PATH="/root/geobrix/$2"
            shift 2
            ;;
        --min-coverage)
            MIN_COVERAGE="--cov-fail-under=$2"
            shift 2
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --open)
            OPEN_REPORT=true
            shift
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

show_banner "📊 GeoBrix: Python Code Coverage (Non-Docs)"
check_docker
setup_log_file "$LOG_PATH"

echo -e "${CYAN}🎯 Test path: ${YELLOW}$TEST_PATH${NC}"
if [ -n "$MIN_COVERAGE" ]; then
    echo -e "${CYAN}📊 Minimum coverage: ${YELLOW}${MIN_COVERAGE#--cov-fail-under=}%${NC}"
fi

echo ""
show_separator
echo -e "${CYAN}Running tests with coverage...${NC}"
show_separator
echo ""

# Build pytest command
# Coverage measures source code in src/, not tests
PYTEST_CMD="unset JAVA_TOOL_OPTIONS && \
    cd /root/geobrix && \
    python3 -m pytest $TEST_PATH \
    -v --tb=short \
    --cov=/root/geobrix/python/geobrix/src/databricks/labs/gbx \
    --cov-report=html:/root/geobrix/python/coverage-report \
    --cov-report=term \
    $MIN_COVERAGE"

docker exec geobrix-dev /bin/bash -c "$PYTEST_CMD"
EXIT_CODE=$?

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Coverage analysis complete!${NC}"
    echo ""
    echo -e "${CYAN}📊 Report generated:${NC}"
    echo -e "  HTML: ${YELLOW}python/coverage-report/index.html${NC}"
    
    if [ "$OPEN_REPORT" = true ]; then
        echo ""
        open_report "$PROJECT_ROOT/python/coverage-report/index.html"
    fi
else
    echo -e "${RED}❌ Coverage analysis failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
