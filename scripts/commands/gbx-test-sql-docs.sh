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
    echo -e "  ${GREEN}--host${NC}                 Run on the host (arca), not Docker. Requires ${YELLOW}source ~/.local/geobrix-gdal-env.sh${NC}"
    echo -e "                         first; builds/uses ${YELLOW}.venv-host${NC} from the pinned lock."
    echo -e "  ${GREEN}--rebuild-venv${NC}         (with --host) force-rebuild the host test venv"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log (filename → test-logs/<name>)"
    echo -e "  ${GREEN}--markers <marker>${NC}     Pytest markers (e.g. \"not slow\")"
    echo -e "  ${GREEN}--include-integration${NC}  Include integration tests (excluded by default)"
    echo -e "  ${GREEN}--skip-build${NC}           Skip Maven and Python build"
    echo -e "  ${GREEN}--no-sample-data-root${NC}   Do not set GBX_SAMPLE_DATA_ROOT (use env or path_config default)"
    echo -e "  ${GREEN}--help${NC}                 This help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:sql-docs --skip-build${NC}"
    echo -e "  ${YELLOW}gbx:test:sql-docs --host${NC}                            ${CYAN}# on the arca host (no Docker)${NC}"
    echo -e "  ${YELLOW}gbx:test:sql-docs --test api/test_sql_api.py --skip-build${NC}"
    echo -e "  ${YELLOW}gbx:test:sql-docs --log sql-docs.log${NC}"
    echo ""
}

# REL_PATH is relative to docs/tests/python; each mode prefixes it (container BASE vs host).
REL_PATH="api/"
LOG_PATH=""
MARKERS="-m 'not integration'"
INCLUDE_INTEGRATION=false
SKIP_BUILD=false
USE_HOST=false
# Default: set sample data root so doc tests use minimal bundle (required for remote/CI)
SET_SAMPLE_DATA_ROOT=true

while [[ $# -gt 0 ]]; do
    case $1 in
        --test)
            REL_PATH="$2"
            shift 2
            ;;
        --path)
            REL_PATH="$2"
            shift 2
            ;;
        --host)
            USE_HOST=true
            shift
            ;;
        --rebuild-venv)
            export GBX_REBUILD_VENV=1
            shift
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
setup_log_file "$LOG_PATH"

if [ "$USE_HOST" = true ]; then
    # --- Host (arca) path: no Docker; build JAR on host, run pytest from .venv-host ---
    require_host_gdal_env || exit 1
    echo -e "${CYAN}🎯 Test path: ${YELLOW}$PROJECT_ROOT/docs/tests/python/$REL_PATH${NC}  ${CYAN}(host)${NC}"
    [ "$SKIP_BUILD" = true ] && echo -e "${CYAN}⏭️  Skipping build (--skip-build)${NC}"
    echo ""

    # On host, sample data reads from the on-disk mirror via GBX_SAMPLE_DATA_ROOT (path_config honors it);
    # no /Volumes symlink needed.
    SAMPLE_DATA_ROOT="$PROJECT_ROOT/sample-data/Volumes/main/default/test-data"

    VENV_BIN=$(ensure_host_test_venv pyrx) || exit 1
    # Spark Python workers must use the venv interpreter (pandas/pyarrow for Arrow UDFs live there, not in system python3).
    export PYSPARK_PYTHON="$VENV_BIN/python"
    export PYSPARK_DRIVER_PYTHON="$VENV_BIN/python"
    # The venv rasterio bundles its own libproj/proj.db (layout >=6); the arca env points PROJ_DATA/PROJ_LIB
    # at the older $HOME GDAL proj.db (layout 3), which rasterio refuses. Unset them for the Python side so
    # rasterio uses its bundled data. The JVM GDAL sets PROJ_LIB internally via SetConfigOption (the
    # /usr/share/proj bridge), so this does not affect the heavy tier.
    unset PROJ_DATA PROJ_LIB

    if [ "$SKIP_BUILD" != true ]; then
        show_separator
        echo -e "${CYAN}Building JAR (mvn package -DskipTests -PskipScoverage)...${NC}"
        show_separator
        (cd "$PROJECT_ROOT" && mvn package -DskipTests -q -PskipScoverage) || exit $?
        echo ""
    fi

    unset JAVA_TOOL_OPTIONS
    export JUPYTER_PLATFORM_DIRS=1
    [ "$SET_SAMPLE_DATA_ROOT" = true ] && export GBX_SAMPLE_DATA_ROOT="$SAMPLE_DATA_ROOT"
    show_separator
    echo -e "${CYAN}Running SQL/API documentation tests (host)...${NC}"
    show_separator
    eval "\"$VENV_BIN/python\" -m pytest \"$PROJECT_ROOT/docs/tests/python/$REL_PATH\" -v $MARKERS --tb=short --color=yes"
    EXIT_CODE=$?
else
    # --- Docker path (unchanged) ---
    check_docker

    mkdir -p "$PROJECT_ROOT/sample-data/Volumes/main/default/geobrix_samples"
    if ! docker exec geobrix-dev test -d /Volumes 2>/dev/null; then
        echo -e "${RED}❌ /Volumes not found in container. Start with: ./scripts/docker/start_docker_with_volumes.sh${NC}"
        exit 1
    fi

    echo -e "${CYAN}🎯 Test path: ${YELLOW}/root/geobrix/docs/tests/python/$REL_PATH${NC}"
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
    pip install --no-deps -e /root/geobrix/python/geobrix --break-system-packages -q
    echo ''
fi
echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
echo 'Running SQL/API documentation tests...'
echo '━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━'
python3 -m pytest /root/geobrix/docs/tests/python/$REL_PATH -v $MARKERS --tb=short --color=yes
"

    docker exec geobrix-dev /bin/bash -c "$RUN_CMD"
    EXIT_CODE=$?
fi

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
