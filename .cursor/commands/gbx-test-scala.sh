#!/bin/bash
# gbx:test:scala - Run Scala unit tests (non-docs)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "🧪 GeoBrix: Scala Tests (Non-Docs)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:scala${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--by-package${NC}           Run tests per package in sequence (rasterx, gridx, vectorx, ds, expressions, util); reports pass/fail per package"
    echo -e "  ${GREEN}--suite <pattern>${NC}       Run specific test suite (single pattern)"
    echo -e "  ${GREEN}--suites <list>${NC}        Run specific test suites (comma-separated class/package patterns)"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    echo -e "  ${GREEN}--verbose${NC}              Increase Maven verbosity (-X flag)"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Log Path Behavior:${NC}"
    echo -e "  ${YELLOW}filename.log${NC}           → test-logs/filename.log"
    echo -e "  ${YELLOW}subdir/file.log${NC}        → test-logs/subdir/file.log"
    echo -e "  ${YELLOW}/abs/path/file.log${NC}     → /abs/path/file.log"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:scala${NC}"
    echo -e "  ${YELLOW}gbx:test:scala --suite 'com.databricks.labs.gbx.gridx.*'${NC}"
    echo -e "  ${YELLOW}gbx:test:scala --suites '...SpatialRefOpsTest,...GTiff_DataSourceTest'${NC}"
    echo -e "  ${YELLOW}gbx:test:scala --log scala-tests.log --verbose${NC}"
    echo ""
}

# Parse arguments
SUITE_PATTERN=""
LOG_PATH=""
VERBOSE=""
BY_PACKAGE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --by-package)
            BY_PACKAGE=true
            shift
            ;;
        --suite)
            SUITE_PATTERN="$2"
            shift 2
            ;;
        --suites)
            SUITE_PATTERN="$2"
            shift 2
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --verbose)
            VERBOSE="-X"
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

show_banner "🧪 GeoBrix: Scala Tests (Non-Docs)"
check_docker
setup_log_file "$LOG_PATH"

# Package list for --by-package (same as CI matrix)
GBX_PACKAGES="rasterx gridx vectorx ds expressions util"

if [ "$BY_PACKAGE" = true ]; then
    echo -e "${CYAN}🎯 Running tests by package (sequence): $GBX_PACKAGES${NC}"
    echo ""
    EXIT_CODE=0
    for pkg in $GBX_PACKAGES; do
        show_separator
        echo -e "${CYAN}Package: ${GREEN}$pkg${NC}"
        show_separator
        SUITES="com.databricks.labs.gbx.${pkg}.*"
        MVN_CMD="$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn test -PskipScoverage -DskipTests=false -Dsuites='$SUITES' $VERBOSE"
        if docker exec geobrix-dev /bin/bash -c "$MVN_CMD"; then
            echo -e "${GREEN}✅ $pkg passed${NC}"
        else
            echo -e "${RED}❌ $pkg failed${NC}"
            EXIT_CODE=1
        fi
        echo ""
    done
    show_separator
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}✅ All packages passed!${NC}"
    else
        echo -e "${RED}❌ One or more packages failed${NC}"
    fi
    show_separator
else
    # Build Maven command (DOCKER_MAVEN_ENV sets MAVEN_OPTS for faster Maven in Docker)
    MVN_CMD="$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn test -PskipScoverage -DskipTests=false"

    if [ -n "$SUITE_PATTERN" ]; then
        echo -e "${CYAN}🎯 Running suite: ${YELLOW}$SUITE_PATTERN${NC}"
        MVN_CMD="$MVN_CMD -Dsuites='$SUITE_PATTERN'"
    else
        echo -e "${CYAN}🎯 Running all Scala unit tests (src/test/scala; excludes docs)${NC}"
        MVN_CMD="$MVN_CMD -Dsuites='com.databricks.labs.gbx.*'"
    fi

    if [ -n "$VERBOSE" ]; then
        MVN_CMD="$MVN_CMD $VERBOSE"
    fi

    echo ""
    show_separator
    echo -e "${CYAN}Running tests...${NC}"
    show_separator
    echo ""

    docker exec geobrix-dev /bin/bash -c "$MVN_CMD"
    EXIT_CODE=$?

    echo ""
    show_separator
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${GREEN}✅ Scala tests passed!${NC}"
    else
        echo -e "${RED}❌ Scala tests failed (exit code: $EXIT_CODE)${NC}"
    fi
    show_separator
fi

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
