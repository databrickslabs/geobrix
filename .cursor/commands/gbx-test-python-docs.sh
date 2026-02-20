#!/bin/bash
# gbx:test:python-docs - Run Python documentation tests

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📚 GeoBrix: Python Documentation Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:python-docs${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Targeting (run pinpointed tests; full suite is 10+ min):${NC}"
    echo -e "  ${GREEN}--test <nodeid>${NC}        Single test: file::testname (e.g. quickstart/test_examples.py::test_exec_sql_read_and_use_snippet)"
    echo -e "  ${GREEN}--suite <name>${NC}         Subset: quickstart|api|readers|rasterx|advanced|setup|integration (see table below)"
    echo -e "  ${GREEN}--path <path>${NC}          Directory or file (relative to docs/tests/python/)"
    echo ""
    echo -e "${CYAN}Suites (approx time with --skip-build):${NC}"
    echo -e "  ${YELLOW}quickstart${NC}  ~1–2 min   quickstart/"
    echo -e "  ${YELLOW}api${NC}          ~3–5 min   api/"
    echo -e "  ${YELLOW}readers${NC}      ~2–4 min   readers/"
    echo -e "  ${YELLOW}rasterx${NC}      ~1–2 min   rasterx/"
    echo -e "  ${YELLOW}advanced${NC}     ~1 min     advanced/"
    echo -e "  ${YELLOW}setup${NC}         ~0.5 min   setup/"
    echo -e "  ${YELLOW}integration${NC}   varies    integration/ (DBR or integration env; use with --include-integration or run alone)"
    echo ""
    echo -e "${CYAN}Other options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log (use timestamp for tracking: python-docs-\$(date +%Y%m%d-%H%M%S).log)"
    echo -e "  ${GREEN}--markers <marker>${NC}     Pytest markers (e.g. \"not slow\")"
    echo -e "  ${GREEN}--include-integration${NC}  Include integration tests (excluded by default)"
    echo -e "  ${GREEN}--skip-build${NC}            Skip Maven/Python build (use when already built)"
    echo -e "  ${GREEN}--no-sample-data-root${NC}   Do not set GBX_SAMPLE_DATA_ROOT (use env or path_config default)"
    echo -e "  ${GREEN}--help${NC}                 This help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:python-docs --suite quickstart --skip-build --log quickstart.log${NC}"
    echo -e "  ${YELLOW}gbx:test:python-docs --test quickstart/test_examples.py::test_sql_constants_are_valid_strings --skip-build${NC}"
    echo -e "  ${YELLOW}gbx:test:python-docs --path api/test_rasterx_functions_sql.py --skip-build${NC}"
    echo -e "  ${YELLOW}gbx:test:python-docs --log test-logs/python-docs-\$(date +%Y%m%d-%H%M%S).log${NC}   # full suite with timestamped log"
    echo ""
}

# Parse arguments
BASE="/root/geobrix/docs/tests/python"
TEST_PATH="${BASE}/"
LOG_PATH=""
MARKERS="-m 'not integration'"
INCLUDE_INTEGRATION=false
SKIP_BUILD=false
# Default: set sample data root so doc tests use minimal bundle (required for remote/CI)
SET_SAMPLE_DATA_ROOT=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --test)
            # Single test: path relative to docs/tests/python, e.g. quickstart/test_examples.py::test_foo
            TEST_PATH="${BASE}/$2"
            shift 2
            ;;
        --suite)
            case "$2" in
                quickstart|api|readers|rasterx|advanced|setup)
                    TEST_PATH="${BASE}/$2/"
                    ;;
                integration)
                    TEST_PATH="${BASE}/$2/"
                    INCLUDE_INTEGRATION=true
                    MARKERS=""
                    ;;
                *)
                    echo -e "${RED}❌ Invalid suite: $2${NC}"
                    echo "Must be: quickstart, api, readers, rasterx, advanced, setup, integration"
                    exit 1
                    ;;
            esac
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
            echo ""
            show_help
            exit 1
            ;;
    esac
done

# When running the default full suite (entire docs/tests/python/), exclude api/
# so gbx:test:python-docs and gbx:test:sql-docs stay isolated. Use --suite api to run SQL/API tests.
SKIP_SQL_TESTS=false
if [ "$TEST_PATH" = "${BASE}/" ]; then
    SKIP_SQL_TESTS=true
fi
IGNORE_API_ARG=""
if [ "$SKIP_SQL_TESTS" = true ]; then
    IGNORE_API_ARG="--ignore=${BASE}/api"
fi

cd "$PROJECT_ROOT"

show_banner "📚 GeoBrix: Python Documentation Tests"
check_docker
setup_log_file "$LOG_PATH"

# Ensure sample-data Volumes structure exists on host (mount target for start_docker_with_volumes.sh)
mkdir -p "$PROJECT_ROOT/sample-data/Volumes/main/default/geobrix_samples"

# Volumes must be mounted so tests can use sample data (minimal bundle in-repo or full at geobrix_samples)
if ! docker exec geobrix-dev test -d /Volumes 2>/dev/null; then
    echo -e "${RED}❌ /Volumes not found in container. Start the container with the Volumes mount:${NC}"
    echo -e "   ${YELLOW}./scripts/docker/start_docker_with_volumes.sh${NC}"
    echo ""
    echo "Then run this command again so tests can run."
    exit 1
fi

echo -e "${CYAN}🎯 Test path: ${YELLOW}$TEST_PATH${NC}"
if [ "$SKIP_SQL_TESTS" = true ]; then
    echo -e "${CYAN}🚫 Excluding api/ (SQL/API tests; use gbx:test:sql-docs or --suite api to run them)${NC}"
fi
if [ "$INCLUDE_INTEGRATION" = true ]; then
    echo -e "${CYAN}🐢 Including integration tests (may be slow)${NC}"
else
    echo -e "${CYAN}⚡ Excluding integration tests (fast mode)${NC}"
fi
if [ "$SKIP_BUILD" = true ]; then
    echo -e "${CYAN}⏭️  Skipping Maven and Python build (--skip-build)${NC}"
fi
echo ""

# Use minimal bundle path in container so doc tests pass on remote/CI (unless --no-sample-data-root)
SAMPLE_DATA_ROOT_EXPORT=""
[ "$SET_SAMPLE_DATA_ROOT" = true ] && SAMPLE_DATA_ROOT_EXPORT="export GBX_SAMPLE_DATA_ROOT=/Volumes/main/default/test-data"

# Run pre-steps and pytest inside Docker (single bash -c so env and cwd carry through)
RUN_CMD="set -e
unset JAVA_TOOL_OPTIONS
export JUPYTER_PLATFORM_DIRS=1
$SAMPLE_DATA_ROOT_EXPORT
cd /root/geobrix

# 1) Ensure Volumes/sample-data is mounted (doc tests use /Volumes/main/default/geobrix_samples/... or test-data)
if [ ! -d /Volumes ]; then
    echo '❌ /Volumes not found. Start the container with the Volumes mount:'
    echo '   ./scripts/docker/start_docker_with_volumes.sh'
    exit 1
fi
if [ ! -d /Volumes/main/default/geobrix_samples ] && [ ! -d /Volumes/main/default/test-data ]; then
    echo '⚠️  Sample data not found at geobrix_samples or test-data. Some doc tests may skip or fail.'
    echo '   Use gbx:data:generate-minimal-bundle and start_docker_with_volumes.sh, or gbx:data:download.'
fi

# 2) Maven package and Python build (unless --skip-build)
if [ \"$SKIP_BUILD\" != 'true' ]; then
    echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
    echo 'Building JAR (mvn package -DskipTests)...'
    echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
    mvn package -DskipTests -q
    echo ''
    echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
    echo 'Building Python package (python3 -m build)...'
    echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
    cd /root/geobrix/python/geobrix && python3 -m build && cd /root/geobrix
    echo ''
    echo 'Installing Python package (editable)...'
    pip install -e /root/geobrix/python/geobrix --break-system-packages -q
    echo ''
fi

# 3) Run pytest (python-docs default excludes api/ for isolation from sql-docs)
echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
echo 'Running tests...'
echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
python3 -m pytest $TEST_PATH -v $MARKERS --tb=short --color=yes $IGNORE_API_ARG
"

docker exec geobrix-dev /bin/bash -c "$RUN_CMD"
EXIT_CODE=$?

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Python documentation tests passed!${NC}"
else
    echo -e "${RED}❌ Python documentation tests failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
