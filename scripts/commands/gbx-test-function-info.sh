#!/bin/bash
# gbx:test:function-info - Re-inventory function-info.json and run DESCRIBE/coverage tests (in Docker)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "🧪 GeoBrix: Function-Info Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:function-info${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Description:${NC}"
    echo -e "  Runs inside ${YELLOW}geobrix-dev${NC} Docker container:"
    echo -e "  1. ${YELLOW}docs/scripts/generate-function-info.py${NC} to build function-info.json from"
    echo -e "     doc SQL examples (fails if any registered function has no example; fix upstream in docs)."
    echo -e "  2. Pytest in ${YELLOW}docs/tests-function-info/${NC} to print DESCRIBE FUNCTION"
    echo -e "     / DESCRIBE FUNCTION EXTENDED output per package and assert coverage."
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--skip-generate${NC}  Skip the generator step; run only tests"
    echo -e "  ${GREEN}--host${NC}           Run on the host (arca), not Docker. Requires ${YELLOW}source ~/.local/geobrix-gdal-env.sh${NC}"
    echo -e "                    first; builds/uses ${YELLOW}.venv-host${NC} and a built JAR."
    echo -e "  ${GREEN}--rebuild-venv${NC}   (with --host) force-rebuild the host test venv"
    echo -e "  ${GREEN}--log <path>${NC}    Write output to log file"
    echo -e "  ${GREEN}--help${NC}          Show this help"
    echo ""
}

SKIP_GENERATE=false
LOG_PATH=""
USE_HOST=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-generate)
            SKIP_GENERATE=true
            shift
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

cd "$PROJECT_ROOT" || exit 1

show_banner "🧪 GeoBrix: Function-Info Tests"
setup_log_file "$LOG_PATH"

EXIT=0
if [ "$USE_HOST" = true ]; then
    # --- Host (arca) path: no Docker. The pytest registers functions via the built JAR. ---
    require_host_gdal_env || exit 1
    VENV_BIN=$(ensure_host_test_venv pyrx) || exit 1
    # Wire Spark workers to the venv python and unset PROJ_DATA/PROJ_LIB (see common.sh).
    activate_host_python_env "$VENV_BIN"
    warn_if_jar_stale "$PROJECT_ROOT"
    unset JAVA_TOOL_OPTIONS
    export JUPYTER_PLATFORM_DIRS=1
    if [ "$SKIP_GENERATE" = false ]; then
        echo -e "${CYAN}Step 1: Generate function-info.json from doc SQL examples...${NC}"
        "$VENV_BIN/python" docs/scripts/generate-function-info.py || EXIT=$?
        echo ""
    fi
    if [ $EXIT -eq 0 ]; then
        echo -e "${CYAN}Step 2: Run function-info tests (DESCRIBE output + coverage, host)...${NC}"
        "$VENV_BIN/python" -m pytest docs/tests-function-info/ -v -s --tb=short || EXIT=$?
    fi
else
    # --- Docker path (unchanged) ---
    check_docker

    # Run generator and pytest inside container (paths under /root/geobrix)
    RUN_CMD="set -e
unset JAVA_TOOL_OPTIONS
export JUPYTER_PLATFORM_DIRS=1
cd /root/geobrix
"

    if [ "$SKIP_GENERATE" = false ]; then
        RUN_CMD="$RUN_CMD
echo 'Step 1: Generate function-info.json from doc SQL examples (fails if any registered function has no doc example)...'
python3 docs/scripts/generate-function-info.py
echo ''
"
    fi

    RUN_CMD="$RUN_CMD
echo 'Step 2: Run function-info tests (DESCRIBE output + coverage)...'
python3 -m pytest docs/tests-function-info/ -v -s --tb=short
"

    docker exec geobrix-dev /bin/bash -c "$RUN_CMD" || EXIT=$?
fi

show_separator
if [ $EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ All function-info tests passed.${NC}"
else
    echo -e "${RED}❌ Some tests failed (exit code: $EXIT)${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo ""
    echo -e "${CYAN}📝 Log: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT
