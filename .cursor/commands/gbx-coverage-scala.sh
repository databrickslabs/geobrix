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
    echo -e "  ${GREEN}--clean${NC}                   Run 'mvn clean' before coverage (default: incremental, no clean)"
    echo -e "  ${GREEN}--parallel${NC}                Run tests in parallel (scoverage:test -T 1C then report-only; faster on multi-core)"
    echo -e "  ${GREEN}--by-package${NC}              Run coverage per package in sequence, merge, then report (same as CI package matrix; no parallel in one container)"
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
    echo -e "  HTML Report:  ${YELLOW}target/site/scoverage/index.html${NC} or ${YELLOW}target/scoverage-report/index.html${NC}"
    echo -e "  XML Report:   ${YELLOW}target/scoverage.xml${NC}"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:coverage:scala${NC}"
    echo -e "  ${YELLOW}gbx:coverage:scala --parallel${NC}   # parallel tests, then report"
    echo -e "  ${YELLOW}gbx:coverage:scala --clean${NC}      # full clean + coverage"
    echo -e "  ${YELLOW}gbx:coverage:scala --report-only --open${NC}"
    echo ""
}

# Parse arguments
MIN_COVERAGE="80"
REPORT_ONLY=false
CLEAN=false
PARALLEL=false
BY_PACKAGE=false
LOG_PATH=""
OPEN_REPORT=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --by-package)
            BY_PACKAGE=true
            shift
            ;;
        --min-coverage)
            MIN_COVERAGE="$2"
            shift 2
            ;;
        --report-only)
            REPORT_ONLY=true
            shift
            ;;
        --clean)
            CLEAN=true
            shift
            ;;
        --parallel)
            PARALLEL=true
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
SUITES='com.databricks.labs.gbx.*'
# report-only with one aggregated report (aggregateOnly avoids many per-module HTMLs)
SCOV_REPORT_ONLY="$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn -q scoverage:report-only -Druntime=standard -Dminimum.coverage=$MIN_COVERAGE -Dscoverage.aggregate=true -Dscoverage.aggregateOnly=true"
GBX_PACKAGES="rasterx gridx vectorx ds expressions util"

if [ "$REPORT_ONLY" = true ]; then
    echo -e "${CYAN}Generating report from existing data...${NC}"
    docker exec geobrix-dev /bin/bash -c "$SCOV_REPORT_ONLY"
    EXIT_CODE=$?
elif [ "$BY_PACKAGE" = true ]; then
    echo -e "${CYAN}Running coverage by package (sequence), then merge and report...${NC}"
    mkdir -p "$PROJECT_ROOT/coverage-artifacts"
    docker exec geobrix-dev /bin/bash -c "mkdir -p /root/geobrix/coverage-artifacts"
    CLEAN_PREFIX=""
    [ "$CLEAN" = true ] && CLEAN_PREFIX="clean "
    EXIT_CODE=0
    for pkg in $GBX_PACKAGES; do
        echo -e "${CYAN}Package: ${GREEN}$pkg${NC}"
        PKG_SUITE="com.databricks.labs.gbx.${pkg}.*"
        MVN_PKG="$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn ${CLEAN_PREFIX}scoverage:test -T 1C -Druntime=standard -Dsuites='$PKG_SUITE'"
        docker exec geobrix-dev /bin/bash -c "$MVN_PKG" || { EXIT_CODE=1; echo -e "${RED}$pkg failed${NC}"; break; }
        docker exec geobrix-dev /bin/bash -c "cp -f /root/geobrix/target/scoverage.xml /root/geobrix/coverage-artifacts/scoverage-${pkg}.xml 2>/dev/null || true"
        [ "$CLEAN" = true ] && CLEAN_PREFIX=""  # only clean first run
    done
    if [ $EXIT_CODE -eq 0 ] && [ -n "$(ls "$PROJECT_ROOT/coverage-artifacts"/scoverage-*.xml 2>/dev/null)" ]; then
        echo -e "${CYAN}Merging scoverage XMLs...${NC}"
        python3 "$PROJECT_ROOT/scripts/ci/merge_scoverage.py" -o "$PROJECT_ROOT/target/scoverage.xml" "$PROJECT_ROOT/coverage-artifacts/"
        echo -e "${CYAN}Generating report (report-only)...${NC}"
        docker exec geobrix-dev /bin/bash -c "$SCOV_REPORT_ONLY"
        EXIT_CODE=$?
        if [ $EXIT_CODE -eq 0 ] && [ -f "$PROJECT_ROOT/target/scoverage.xml" ]; then
            rate=$(sed -n 's/.*statement-rate="\([^"]*\)".*/\1/p' "$PROJECT_ROOT/target/scoverage.xml" | head -1)
            inv=$(sed -n 's/.*statements-invoked="\([^"]*\)".*/\1/p' "$PROJECT_ROOT/target/scoverage.xml" | head -1)
            tot=$(sed -n 's/.*statement-count="\([^"]*\)".*/\1/p' "$PROJECT_ROOT/target/scoverage.xml" | head -1)
            echo ""
            echo "============================== Scala coverage ==============================="
            echo "Scala coverage: ${rate}% (${inv}/${tot} statements)"
            echo "=============================================================================="
        fi
    fi
elif [ "$PARALLEL" = true ]; then
    echo -e "${CYAN}Step 1: Running tests in parallel (scoverage:test -T 1C)...${NC}"
    CLEAN_PREFIX=""
    [ "$CLEAN" = true ] && CLEAN_PREFIX="clean "
    MVN_TEST="$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn ${CLEAN_PREFIX}scoverage:test -T 1C -Druntime=standard -Dsuites='$SUITES'"
    docker exec geobrix-dev /bin/bash -c "$MVN_TEST"
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${CYAN}Step 2: Generating report (report-only, aggregate only)...${NC}"
        docker exec geobrix-dev /bin/bash -c "$SCOV_REPORT_ONLY"
        EXIT_CODE=$?
    fi
else
    echo -e "${CYAN}Step 1: Running tests with instrumentation (scoverage:test -T 1C)...${NC}"
    [ "$CLEAN" = true ] && echo -e "${YELLOW}Using 'clean' (use default for incremental)${NC}"
    CLEAN_PREFIX=""
    [ "$CLEAN" = true ] && CLEAN_PREFIX="clean "
    MVN_TEST="$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn ${CLEAN_PREFIX}scoverage:test -T 1C -Druntime=standard -Dsuites='$SUITES'"
    docker exec geobrix-dev /bin/bash -c "$MVN_TEST"
    EXIT_CODE=$?
    if [ $EXIT_CODE -eq 0 ]; then
        echo -e "${CYAN}Step 2: Generating report (report-only, aggregate only)...${NC}"
        docker exec geobrix-dev /bin/bash -c "$SCOV_REPORT_ONLY"
        EXIT_CODE=$?
    fi
fi
show_separator
echo ""

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Coverage analysis complete!${NC}"
    echo ""
    echo -e "${CYAN}📊 Reports generated:${NC}"
    # Plugin may write to target/site/scoverage (Maven report) or target/scoverage-report
    HTML_REPORT=""
    for candidate in "$PROJECT_ROOT/target/site/scoverage/index.html" "$PROJECT_ROOT/target/scoverage-report/index.html"; do
        if [ -f "$candidate" ]; then
            HTML_REPORT="$candidate"
            break
        fi
    done
    if [ -n "$HTML_REPORT" ]; then
        echo -e "  HTML: ${YELLOW}${HTML_REPORT#$PROJECT_ROOT/}${NC}"
        echo -e "  Link: $(print_report_link "$HTML_REPORT")"
        if [ "$OPEN_REPORT" = true ]; then
            echo ""
            open_report "$HTML_REPORT"
        fi
    else
        echo -e "  HTML: ${YELLOW}(check target/site/scoverage/ or target/scoverage-report/)${NC}"
    fi
    if [ -f "$PROJECT_ROOT/target/scoverage.xml" ]; then
        echo -e "  XML:  ${YELLOW}target/scoverage.xml${NC}"
    elif [ -f "$PROJECT_ROOT/target/scoverage-report/scoverage.xml" ]; then
        echo -e "  XML:  ${YELLOW}target/scoverage-report/scoverage.xml${NC}"
    fi
else
    echo -e "${RED}❌ Coverage analysis failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
