#!/bin/bash
# gbx:test:docs - Run all documentation examples (Python, SQL API, and Scala)
# Consolidates: invokes gbx-test-python-docs, gbx-test-sql-docs, gbx-test-scala-docs.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📚 GeoBrix: All Documentation Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:docs${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Description:${NC}"
    echo -e "  Runs all documentation tests by invoking: python-docs, sql-docs, then scala-docs."
    echo ""
    echo -e "${CYAN}Targeting (Python only; Scala always runs full suite):${NC}"
    echo -e "  ${GREEN}--suite <name>${NC}         Python subset: quickstart|api|readers|rasterx|advanced|setup"
    echo -e "  ${GREEN}--path <path>${NC}           Python: dir/file relative to docs/tests/python/"
    echo -e "  ${GREEN}--test <nodeid>${NC}         Python: single test node id"
    echo ""
    echo -e "${CYAN}Common options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log (filename → test-logs/<name>)"
    echo -e "  ${GREEN}--markers <marker>${NC}     Pytest markers for Python (e.g. \"not slow\")"
    echo -e "  ${GREEN}--include-integration${NC}  Include Python integration tests (excluded by default)"
    echo -e "  ${GREEN}--skip-build${NC}           Skip Maven and Python build before Python tests"
    echo -e "  ${GREEN}--scala-suite <pattern>${NC} Scala suite pattern (default: tests.docs.scala.*)"
    echo -e "  ${GREEN}--python-only${NC}          Run only Python doc tests (skip SQL + Scala)"
    echo -e "  ${GREEN}--scala-only${NC}           Run only Scala doc tests (skip Python + SQL)"
    echo -e "  ${GREEN}--no-sample-data-root${NC}  Do not set GBX_SAMPLE_DATA_ROOT (use env or path_config default)"
    echo -e "  ${GREEN}--help${NC}                 This help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:docs --skip-build --log docs.log${NC}"
    echo -e "  ${YELLOW}gbx:test:docs --python-only --suite api${NC}"
    echo -e "  ${YELLOW}gbx:test:docs --scala-only --log scala-docs.log${NC}"
    echo ""
}

LOG_PATH=""
MARKERS="-m 'not integration'"
MARKERS_VAL="not integration"
INCLUDE_INTEGRATION=false
SKIP_BUILD=false
SCALA_SUITE="tests.docs.scala.*"
PYTHON_ONLY=false
SCALA_ONLY=false
SET_SAMPLE_DATA_ROOT=true
# Pass-through for Python phase (only one of these set)
SUITE_VAL=""
PATH_VAL=""
TEST_VAL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --suite)
            case "$2" in
                quickstart|api|readers|rasterx|advanced|setup)
                    SUITE_VAL="$2"
                    ;;
                integration)
                    SUITE_VAL="$2"
                    INCLUDE_INTEGRATION=true
                    MARKERS=""
                    ;;
                *)
                    echo -e "${RED}❌ Invalid suite: $2${NC}"
                    exit 1
                    ;;
            esac
            shift 2
            ;;
        --path)
            PATH_VAL="$2"
            shift 2
            ;;
        --test)
            TEST_VAL="$2"
            shift 2
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --markers)
            MARKERS_VAL="$2"
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
        --scala-suite)
            SCALA_SUITE="$2"
            shift 2
            ;;
        --python-only)
            PYTHON_ONLY=true
            shift
            ;;
        --scala-only)
            SCALA_ONLY=true
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

show_banner "📚 GeoBrix: All Documentation Tests"
check_docker
setup_log_file "$LOG_PATH"

mkdir -p "$PROJECT_ROOT/sample-data/Volumes/main/default/geobrix_samples"
if ! docker exec geobrix-dev test -d /Volumes 2>/dev/null; then
    echo -e "${RED}❌ /Volumes not found. Start with: ./scripts/docker/start_docker_with_volumes.sh${NC}"
    exit 1
fi

[ "$SKIP_BUILD" = true ] && echo -e "${CYAN}⏭️  Skipping build (--skip-build)${NC}"
echo ""

TOTAL_EXIT=0

# Build args for child scripts (do not pass --log; parent already has logging). Use arrays so multi-word args are preserved.
PYTHON_ARR=()
SQL_ARR=()
SCALA_ARR=()
[ "$SKIP_BUILD" = true ] && { PYTHON_ARR+=(--skip-build); SQL_ARR+=(--skip-build); SCALA_ARR+=(--skip-build); }
[ "$SET_SAMPLE_DATA_ROOT" = false ] && { PYTHON_ARR+=(--no-sample-data-root); SQL_ARR+=(--no-sample-data-root); SCALA_ARR+=(--no-sample-data-root); }
[ -n "$MARKERS_VAL" ] && [ "$INCLUDE_INTEGRATION" = false ] && { PYTHON_ARR+=(--markers "$MARKERS_VAL"); SQL_ARR+=(--markers "$MARKERS_VAL"); }
[ "$INCLUDE_INTEGRATION" = true ] && { PYTHON_ARR+=(--include-integration); SQL_ARR+=(--include-integration); }
[ -n "$SUITE_VAL" ] && PYTHON_ARR+=(--suite "$SUITE_VAL")
[ -n "$PATH_VAL" ] && PYTHON_ARR+=(--path "$PATH_VAL")
[ -n "$TEST_VAL" ] && PYTHON_ARR+=(--test "$TEST_VAL")
SCALA_ARR+=(--suite "$SCALA_SUITE")

# ---- 1) Python doc tests (excludes api/ by default) ----
if [ "$SCALA_ONLY" = false ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}1/3 Python documentation tests${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    bash "$SCRIPT_DIR/gbx-test-python-docs.sh" "${PYTHON_ARR[@]}" || TOTAL_EXIT=$?
    echo ""
fi

# ---- 2) SQL/API doc tests ----
if [ "$SCALA_ONLY" = false ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}2/3 SQL/API documentation tests${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    bash "$SCRIPT_DIR/gbx-test-sql-docs.sh" "${SQL_ARR[@]}" || TOTAL_EXIT=$?
    echo ""
fi

# ---- 3) Scala doc tests ----
if [ "$PYTHON_ONLY" = false ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}3/3 Scala documentation tests${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    bash "$SCRIPT_DIR/gbx-test-scala-docs.sh" "${SCALA_ARR[@]}" || TOTAL_EXIT=$?
    echo ""
    # pytest-style short summary when logging (dedupe by test name, explain minimal bundle)
    if [ -n "$LOG_PATH" ] && [ -f "$LOG_PATH" ]; then
        echo -e "${BLUE}=== Short test summary (Scala) ===${NC}"
        echo -e "${CYAN}Note: Canceled/failed tests on the minimal bundle usually mean limited raster coverage or data (e.g. FileGDB, Sentinel-2). Use full bundle or \`gbx:data:generate-minimal-bundle\` if you need these to pass.${NC}"
        echo ""
        (grep -E '\*\*\* FAILED \*\*\*' "$LOG_PATH" 2>/dev/null | sed -E 's/^[[:space:]]*- (.*) \*\*\* FAILED \*\*\*/\1/' | sort -u | while read -r name; do echo "FAILED   - $name"; done)
        (grep -E '!!! CANCELED !!!' "$LOG_PATH" 2>/dev/null | sed -E 's/^[[:space:]]*- (.*) !!! CANCELED !!!/\1/' | sort -u | while read -r name; do echo "CANCELED - $name"; done)
        (grep -E '^Tests: succeeded' "$LOG_PATH" 2>/dev/null | tail -1)
        echo ""
    fi
fi

show_separator
if [ $TOTAL_EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ All documentation tests passed!${NC}"
else
    echo -e "${RED}❌ Some documentation tests failed (exit code: $TOTAL_EXIT)${NC}"
fi
show_separator

[ -n "$LOG_PATH" ] && echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"

exit $TOTAL_EXIT
