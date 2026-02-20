#!/bin/bash
# gbx:coverage:scala - Run Scala tests with code coverage

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📊 GeoBrix: Scala Code Coverage"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:coverage:scala${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--min-coverage <percent>${NC}  Minimum coverage threshold (default: 80)"
    echo -e "  ${GREEN}--report-only${NC}             Generate report from existing data (no re-test)"
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
    echo -e "  HTML Report:  ${YELLOW}target/scoverage-report/index.html${NC}"
    echo -e "  XML Report:   ${YELLOW}target/scoverage-report/scoverage.xml${NC}"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:coverage:scala${NC}"
    echo -e "  ${YELLOW}gbx:coverage:scala --min-coverage 75 --open${NC}"
    echo -e "  ${YELLOW}gbx:coverage:scala --report-only --open${NC}"
    echo ""
}

# Parse arguments
MIN_COVERAGE="80"
REPORT_ONLY=false
LOG_PATH=""
OPEN_REPORT=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --min-coverage)
            MIN_COVERAGE="$2"
            shift 2
            ;;
        --report-only)
            REPORT_ONLY=true
            shift
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

show_banner "📊 GeoBrix: Scala Code Coverage"
check_docker
setup_log_file "$LOG_PATH"

echo -e "${CYAN}🎯 Minimum coverage: ${YELLOW}$MIN_COVERAGE%${NC}"

echo ""
show_separator
if [ "$REPORT_ONLY" = true ]; then
    echo -e "${CYAN}Generating report from existing data...${NC}"
    MVN_CMD="unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn scoverage:report-only -Dminimum.coverage=$MIN_COVERAGE"
else
    echo -e "${CYAN}Running tests with coverage...${NC}"
    MVN_CMD="unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn clean package -DskipTests=false -Dminimum.coverage=$MIN_COVERAGE"
fi
show_separator
echo ""

docker exec geobrix-dev /bin/bash -c "$MVN_CMD"
EXIT_CODE=$?

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Coverage analysis complete!${NC}"
    echo ""
    echo -e "${CYAN}📊 Reports generated:${NC}"
    echo -e "  HTML: ${YELLOW}target/site/scoverage/index.html${NC}"
    echo -e "  XML:  ${YELLOW}target/scoverage.xml${NC}"
    
    if [ "$OPEN_REPORT" = true ]; then
        echo ""
        open_report "$PROJECT_ROOT/target/site/scoverage/index.html"
    fi
else
    echo -e "${RED}❌ Coverage analysis failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
