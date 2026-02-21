#!/bin/bash
# gbx:coverage:scala-package - Run Scala coverage for specific package only

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📦 GeoBrix: Package-Targeted Scala Coverage"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:coverage:scala-package${NC} ${YELLOW}<package>${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Packages:${NC}"
    echo -e "  ${GREEN}rasterx${NC}              All raster operations (~5-7 min, 5,788 statements)"
    echo -e "  ${GREEN}rasterx.operator${NC}     Raster operators (232 statements, ~30 sec) ⭐"
    echo -e "  ${GREEN}rasterx.ds${NC}           Raster data sources (147 statements, ~20 sec)"
    echo -e "  ${GREEN}rasterx.util${NC}         Raster utilities (278 statements, ~30 sec)"
    echo -e "  ${GREEN}rasterx.gdal${NC}         GDAL operations (737 statements, ~1 min)"
    echo -e "  ${GREEN}rasterx.operations${NC}   Raster operations (1,091 statements, ~1-2 min)"
    echo -e "  ${GREEN}rasterx.expressions${NC}  Raster expressions (2,943 statements, ~3-4 min)"
    echo -e ""
    echo -e "  ${GREEN}gridx${NC}                Grid systems (BNG, H3) (~1 min)"
    echo -e "  ${GREEN}gridx.bng${NC}            BNG grid operations (~1 min)"
    echo -e "  ${GREEN}gridx.grid${NC}           Grid framework (886 statements, ~1 min)"
    echo -e ""
    echo -e "  ${GREEN}vectorx${NC}              Vector operations (~1-2 min)"
    echo -e "  ${GREEN}vectorx.jts${NC}          JTS geometry operations (342 statements, ~40 sec)"
    echo -e "  ${GREEN}vectorx.ds${NC}           Vector data sources (535 statements, ~50 sec)"
    echo -e ""
    echo -e "  ${GREEN}ds${NC}                   Data sources (~30 sec)"
    echo -e "  ${GREEN}expressions${NC}          Expression framework (~30 sec)"
    echo -e "  ${GREEN}util${NC}                 Utilities (502 statements, ~45 sec)"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--class <name>${NC}             Run only this test class (e.g. GDALRasterizeTest); comma-separated for multiple"
    echo -e "  ${GREEN}--min-coverage <percent>${NC}  Minimum coverage threshold (default: 90)"
    echo -e "  ${GREEN}--clean${NC}                   Run 'mvn clean' before coverage (default: incremental, no clean)"
    echo -e "  ${GREEN}--parallel${NC}                Run tests in parallel (scoverage:test -T 1C then report-only)"
    echo -e "  ${GREEN}--log <path>${NC}              Write output to log file"
    echo -e "  ${GREEN}--open${NC}                    Open HTML report in browser"
    echo -e "  ${GREEN}--help${NC}                    Show this help"
    echo ""
    echo -e "${CYAN}Output:${NC}"
    echo -e "  HTML Report:  ${YELLOW}target/scoverage-report/index.html${NC}"
    echo -e "  XML Report:   ${YELLOW}target/scoverage.xml${NC}"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:coverage:scala-package rasterx --open${NC}"
    echo -e "  ${YELLOW}gbx:coverage:scala-package rasterx.operations --class GDALRasterizeTest${NC}  # one class only (fast)"
    echo -e "  ${YELLOW}gbx:coverage:scala-package gridx --min-coverage 85${NC}"
    echo -e "  ${YELLOW}gbx:coverage:scala-package vectorx --log vectorx-cov.log${NC}"
    echo ""
    echo -e "${CYAN}Time Savings:${NC}"
    echo -e "  Full coverage:   ~10 minutes (all tests)"
    echo -e "  Package-only:    ~1-3 minutes (filtered tests)"
    echo -e "  ${GREEN}--class <Name>${NC}:  ~10-30 sec (single test class) ⭐"
    echo ""
}

# Parse arguments
PACKAGE=""
CLASS_NAMES=""
MIN_COVERAGE="90"
CLEAN=false
PARALLEL=false
LOG_PATH=""
OPEN_REPORT=false

# First argument should be package (if not a flag)
if [[ $# -gt 0 ]] && [[ ! "$1" =~ ^-- ]]; then
    PACKAGE="$1"
    shift
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --class)
            CLASS_NAMES="$2"
            shift 2
            ;;
        --min-coverage)
            MIN_COVERAGE="$2"
            shift 2
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

# Validate package argument
if [ -z "$PACKAGE" ]; then
    echo -e "${RED}❌ Error: Package name required${NC}"
    echo ""
    show_help
    exit 1
fi

# Map package names to test suite patterns (bash 3.x compatible)
map_package_to_suite() {
    local pkg="$1"
    case "$pkg" in
        # Top-level packages
        rasterx)
            echo "com.databricks.labs.gbx.rasterx.*"
            ;;
        gridx)
            echo "com.databricks.labs.gbx.gridx.*"
            ;;
        vectorx)
            echo "com.databricks.labs.gbx.vectorx.*"
            ;;
        ds)
            echo "com.databricks.labs.gbx.ds.*"
            ;;
        expressions)
            echo "com.databricks.labs.gbx.expressions.*"
            ;;
        util)
            echo "com.databricks.labs.gbx.util.*"
            ;;
        # RasterX sub-packages
        rasterx.operator)
            echo "com.databricks.labs.gbx.rasterx.operator.*"
            ;;
        rasterx.operations)
            echo "com.databricks.labs.gbx.rasterx.operations.*"
            ;;
        rasterx.expressions)
            echo "com.databricks.labs.gbx.rasterx.expressions.*"
            ;;
        rasterx.gdal)
            echo "com.databricks.labs.gbx.rasterx.gdal.*"
            ;;
        rasterx.util)
            echo "com.databricks.labs.gbx.rasterx.util.*"
            ;;
        rasterx.ds)
            echo "com.databricks.labs.gbx.rasterx.ds.*"
            ;;
        # GridX sub-packages
        gridx.bng)
            echo "com.databricks.labs.gbx.gridx.bng.*"
            ;;
        gridx.grid)
            echo "com.databricks.labs.gbx.gridx.grid.*"
            ;;
        # VectorX sub-packages
        vectorx.jts)
            echo "com.databricks.labs.gbx.vectorx.jts.*"
            ;;
        vectorx.ds)
            echo "com.databricks.labs.gbx.vectorx.ds.*"
            ;;
        # DS sub-packages
        ds.register)
            echo "com.databricks.labs.gbx.ds.register.*"
            ;;
        ds.whitelist)
            echo "com.databricks.labs.gbx.ds.whitelist.*"
            ;;
        *)
            echo ""
            ;;
    esac
}

# Validate package and get suite pattern (or single-class pattern)
SUITE_PATTERN=$(map_package_to_suite "$PACKAGE")
if [ -z "$SUITE_PATTERN" ]; then
    echo -e "${RED}❌ Error: Unknown package '$PACKAGE'${NC}"
    echo ""
    echo -e "${CYAN}Available packages (use --help for full list):${NC}"
    echo -e "  ${GREEN}Top-level:${NC} rasterx, gridx, vectorx, ds, expressions, util"
    echo -e "  ${GREEN}RasterX:${NC} rasterx.operator, rasterx.expressions, rasterx.operations, etc."
    echo -e "  ${GREEN}GridX:${NC} gridx.bng, gridx.grid"
    echo -e "  ${GREEN}VectorX:${NC} vectorx.jts, vectorx.ds"
    echo ""
    exit 1
fi

# If --class given, restrict to those test class(es) for faster runs
if [ -n "$CLASS_NAMES" ]; then
    # Package prefix is suite pattern with ".*" stripped
    PKG_PREFIX="${SUITE_PATTERN%.*}"
    SUITE_PATTERN=""
    for name in $(echo "$CLASS_NAMES" | tr ',' ' '); do
        name=$(echo "$name" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
        [ -z "$name" ] && continue
        case "$name" in
            com.*) full="$name" ;;
            *)     full="${PKG_PREFIX}.${name}" ;;
        esac
        if [ -n "$SUITE_PATTERN" ]; then
            SUITE_PATTERN="$SUITE_PATTERN,$full"
        else
            SUITE_PATTERN="$full"
        fi
    done
fi

cd "$PROJECT_ROOT"

show_banner "📦 GeoBrix: Package-Targeted Scala Coverage"
check_docker
setup_log_file "$LOG_PATH"

echo -e "${CYAN}📦 Package: ${GREEN}$PACKAGE${NC}"
[ -n "$CLASS_NAMES" ] && echo -e "${CYAN}🎯 Class(es) only: ${YELLOW}$CLASS_NAMES${NC}"
echo -e "${CYAN}🎯 Test suite: ${YELLOW}$SUITE_PATTERN${NC}"
echo -e "${CYAN}🎯 Minimum coverage: ${YELLOW}$MIN_COVERAGE%${NC}"

echo ""
show_separator
echo -e "${CYAN}Running coverage for package: ${GREEN}$PACKAGE${NC}"
echo -e "${YELLOW}Note: This only runs tests for the specified package${NC}"
echo -e "${YELLOW}      Use 'gbx:coverage:scala' for full coverage${NC}"
show_separator
echo ""

# Run scoverage: test with instrumentation then report-only (one aggregated report). Parallel (-T 1C) is thread-safe for scoverage:test.
SCOV_REPORT_ONLY="$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn -q scoverage:report-only -Druntime=standard -Dminimum.coverage=$MIN_COVERAGE -Dscoverage.aggregate=true -Dscoverage.aggregateOnly=true"
CLEAN_PREFIX=""
[ "$CLEAN" = true ] && CLEAN_PREFIX="clean "
echo -e "${CYAN}Step 1: Running package tests (scoverage:test -T 1C)...${NC}"
MVN_TEST="$DOCKER_MAVEN_ENV && cd /root/geobrix && mvn ${CLEAN_PREFIX}scoverage:test -T 1C -Druntime=standard -Dsuites='$SUITE_PATTERN'"
docker exec geobrix-dev /bin/bash -c "$MVN_TEST"
EXIT_CODE=$?
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${CYAN}Step 2: Generating report (report-only, aggregate only)...${NC}"
    docker exec geobrix-dev /bin/bash -c "$SCOV_REPORT_ONLY"
    EXIT_CODE=$?
fi

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Package coverage complete!${NC}"
    echo ""
    echo -e "${CYAN}📊 Reports generated:${NC}"
    # Plugin may write to target/site/scoverage or target/scoverage-report (same as gbx-coverage-scala)
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
    echo ""
    echo -e "${CYAN}💡 Tips:${NC}"
    echo -e "  ${YELLOW}• Use 'gbx:coverage:scala --report-only' to view without re-running${NC}"
    echo -e "  ${YELLOW}• Use 'gbx:coverage:gaps scala' to analyze coverage gaps${NC}"
    echo -e "  ${YELLOW}• Target other packages to improve overall coverage${NC}"
else
    echo -e "${RED}❌ Package coverage failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
