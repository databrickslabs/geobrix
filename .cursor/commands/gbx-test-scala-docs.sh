#!/bin/bash
# gbx:test:scala-docs - Run Scala documentation tests

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📚 GeoBrix: Scala Documentation Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:scala-docs${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log (filename → test-logs/<name>)"
    echo -e "  ${GREEN}--suite <pattern>${NC}      Maven suite pattern (default: tests.docs.scala.*)"
    echo -e "  ${GREEN}--skip-build${NC}           Skip Maven compile before test (mvn test still compiles)"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Log path:${NC}"
    echo -e "  ${YELLOW}filename.log${NC} → test-logs/filename.log  ${YELLOW}/abs/path${NC} → as-is"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:scala-docs${NC}"
    echo -e "  ${YELLOW}gbx:test:scala-docs --log scala-docs.log${NC}"
    echo -e "  ${YELLOW}gbx:test:scala-docs --suite 'docs.tests.scala.api.*'${NC}"
    echo ""
}

# Parse arguments
LOG_PATH=""
SUITE_PATTERN="tests.docs.scala.*"
SKIP_BUILD=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --suite)
            SUITE_PATTERN="$2"
            shift 2
            ;;
        --skip-build)
            SKIP_BUILD=true
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

show_banner "📚 GeoBrix: Scala Documentation Tests"
check_docker
setup_log_file "$LOG_PATH"

echo -e "${CYAN}🎯 Suite: ${YELLOW}$SUITE_PATTERN${NC}"
[ "$SKIP_BUILD" = true ] && echo -e "${CYAN}⏭️  Skipping pre-compile (--skip-build)${NC}"
echo ""
show_separator
echo -e "${CYAN}Running Scala doc tests...${NC}"
show_separator
echo ""

MVN_CMD="unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test -Dsuites='$SUITE_PATTERN'"

docker exec geobrix-dev /bin/bash -c "$MVN_CMD"
EXIT_CODE=$?

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Scala documentation tests passed!${NC}"
else
    echo -e "${RED}❌ Scala documentation tests failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
