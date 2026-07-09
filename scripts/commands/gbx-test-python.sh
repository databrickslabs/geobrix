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
    echo -e "  ${GREEN}--path <dir>${NC}           Specific test directory or file (repo-relative)"
    echo -e "  ${GREEN}--host${NC}                 Run on the host (arca) instead of the Docker container. Requires"
    echo -e "                         ${YELLOW}source ~/.local/geobrix-gdal-env.sh${NC} first; builds/uses ${YELLOW}.venv-host${NC}."
    echo -e "  ${GREEN}--rebuild-venv${NC}         (with --host) force-rebuild the host test venv from the pinned lock"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    echo -e "  ${GREEN}--with-integration${NC}     Include ${YELLOW}@pytest.mark.integration${NC} tests (network downloads, slow); excluded by default"
    echo -e "  ${GREEN}--markers <expr>${NC}        Override marker filter with a pytest expression (e.g. 'not slow'); disables the default 'not integration' filter"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Default marker filter:${NC} ${YELLOW}not integration${NC} (matches CI; opt in with ${GREEN}--with-integration${NC} or override with ${GREEN}--markers${NC})"
    echo ""
    echo -e "${CYAN}Log Path Behavior:${NC}"
    echo -e "  ${YELLOW}filename.log${NC}           → test-logs/filename.log"
    echo -e "  ${YELLOW}subdir/file.log${NC}        → test-logs/subdir/file.log"
    echo -e "  ${YELLOW}/abs/path/file.log${NC}     → /abs/path/file.log"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:python${NC}                                     ${CYAN}# unit tests only (default, Docker)${NC}"
    echo -e "  ${YELLOW}gbx:test:python --host${NC}                              ${CYAN}# run on the arca host (no Docker)${NC}"
    echo -e "  ${YELLOW}gbx:test:python --with-integration${NC}                  ${CYAN}# unit + integration (network)${NC}"
    echo -e "  ${YELLOW}gbx:test:python --path python/geobrix/test/rasterx/${NC}"
    echo -e "  ${YELLOW}gbx:test:python --markers 'not slow' --log python-tests.log${NC}"
    echo ""
}

# Parse arguments. REL_PATH is repo-relative; each mode prefixes it (container /root/geobrix vs host $PROJECT_ROOT).
REL_PATH="python/geobrix/test/"
LOG_PATH=""
USE_HOST=false
# Default: exclude integration tests (network downloads); matches CI's python_build action.
MARKERS="-m 'not integration'"

while [[ $# -gt 0 ]]; do
    case $1 in
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
        --with-integration)
            MARKERS=""
            shift
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
setup_log_file "$LOG_PATH"

# Python tests run against the assembly JAR (spark.jars); warn if it predates Scala sources.
warn_if_jar_stale "$PROJECT_ROOT"

if [ "$USE_HOST" = true ]; then
    echo -e "${CYAN}🎯 Test path: ${YELLOW}$PROJECT_ROOT/$REL_PATH${NC}  ${CYAN}(host)${NC}"
else
    echo -e "${CYAN}🎯 Test path: ${YELLOW}/root/geobrix/$REL_PATH${NC}"
fi
if [ -n "$MARKERS" ]; then
    echo -e "${CYAN}🏷️  Markers: ${YELLOW}$MARKERS${NC}"
else
    echo -e "${CYAN}🏷️  Markers: ${YELLOW}(none — including integration tests)${NC}"
fi

echo ""
show_separator
echo -e "${CYAN}Running tests...${NC}"
show_separator
echo ""

if [ "$USE_HOST" = true ]; then
    require_host_gdal_env || exit 1
    unset JAVA_TOOL_OPTIONS
    export JUPYTER_PLATFORM_DIRS=1

    # Runs pytest for one leg in the given venv kind. Sets the Spark-worker interpreter to the venv
    # python (pandas/pyarrow for Arrow UDFs live there, not system python3) and unsets PROJ_DATA/PROJ_LIB
    # so the venv rasterio uses its own bundled proj.db (layout >=6) rather than the arca env's older one
    # (layout 3); the JVM GDAL sets PROJ_LIB internally via SetConfigOption, so the heavy tier is unaffected.
    _run_leg() {  # $1=kind  $2=pytest path/args string
        local vbin
        vbin=$(ensure_host_test_venv "$1") || return 1
        PYSPARK_PYTHON="$vbin/python" PYSPARK_DRIVER_PYTHON="$vbin/python" \
            eval "PROJ_DATA= PROJ_LIB= \"$vbin/python\" -m pytest $2 -v --tb=short --color=yes $MARKERS"
    }

    EXIT_CODE=0
    if [ "$REL_PATH" = "python/geobrix/test/" ]; then
        # Default full run — mirror CI's two-environment split (CI never runs all unit tests in one env):
        #   heavy (requirements-ci.txt): test/ minus the light dirs;  light (requirements-pyrx-ci.txt):
        #   the pyrx/ds/pyvx/pygx/pmtiles_light/stac/vizx/sample dirs. Each leg in its own venv.
        local_root="$PROJECT_ROOT/python/geobrix/test"
        LIGHT_DIRS="pyrx pyvx pygx pmtiles_light stac vizx ds sample"
        light_paths=""; ignore_args=""
        for d in $LIGHT_DIRS; do light_paths="$light_paths \"$local_root/$d\""; ignore_args="$ignore_args --ignore=$local_root/$d"; done

        echo -e "${CYAN}▶ Heavy leg (requirements-ci.txt): test/ minus light dirs${NC}"
        _run_leg ci "\"$local_root\" $ignore_args" || EXIT_CODE=$?
        echo ""
        echo -e "${CYAN}▶ Light leg (requirements-pyrx-ci.txt): $LIGHT_DIRS${NC}"
        _run_leg pyrx "$light_paths" || EXIT_CODE=$?
    else
        # Explicit --path: route to the light venv if it's a light dir, else the heavy venv.
        kind=ci
        case "$REL_PATH" in
            *test/pyrx*|*test/pyvx*|*test/pygx*|*test/pmtiles_light*|*test/stac*|*test/vizx*|*test/ds*|*test/sample*) kind=pyrx ;;
        esac
        echo -e "${CYAN}▶ Leg ($kind venv): $REL_PATH${NC}"
        _run_leg "$kind" "\"$PROJECT_ROOT/$REL_PATH\"" || EXIT_CODE=$?
    fi
else
    check_docker
    PYTEST_CMD="unset JAVA_TOOL_OPTIONS && \
        cd /root/geobrix && \
        python3 -m pytest /root/geobrix/$REL_PATH -v --tb=short --color=yes $MARKERS"
    docker exec geobrix-dev /bin/bash -c "$PYTEST_CMD"
    EXIT_CODE=$?
fi

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
