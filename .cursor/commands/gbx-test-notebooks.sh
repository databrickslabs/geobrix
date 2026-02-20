#!/bin/bash
# gbx:test:notebooks - Run notebook execution tests (notebooks/tests/)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📓 GeoBrix: Notebook Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:notebooks${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--allow-absolute-reads${NC}   Do not remap absolute read paths under workdir (default: remap so reads go under temp workdir)"
    echo -e "  ${GREEN}--allow-absolute-writes${NC}  Do not remap absolute write paths under workdir (default: remap so writes go under temp workdir)"
    echo -e "  ${GREEN}--include-integration${NC}    Include full-notebook execution tests (pytest only; default: exclude)"
    echo -e "  ${GREEN}--log <path>${NC}             Write output to log (filename → test-logs/<name>)"
    echo -e "  ${GREEN}--path <path>${NC}            Limit scope: subdir (e.g. sample-data, fixtures), a .ipynb, or a test file (.py)"
    echo -e "  ${GREEN}--help${NC}                   This help"
    echo ""
    echo -e "${CYAN}Behavior:${NC}"
    echo -e "  By default, runs notebooks cell-by-cell (no kernel): fixtures + sample-data."
    echo -e "  Absolute paths in read/write (open, os.stat, Path.exists, shutil.copy, etc.) are remapped under the notebook workdir unless ${GREEN}--allow-absolute-reads${NC} or ${GREEN}--allow-absolute-writes${NC} is set."
    echo -e "  Use ${GREEN}--path sample-data${NC} to run only notebooks under sample-data/."
    echo -e "  Use ${GREEN}--path test_notebook_via_script.py${NC} to run pytest for that test file."
    echo -e "  Verbosity: ${GREEN}GBX_NOTEBOOK_VERBOSITY=quiet|truncated|full${NC} (default: truncated)."
    echo -e "  Isolation: ${GREEN}GBX_NOTEBOOK_ISOLATED_ENV=1${NC} (default) runs notebooks in a temp venv + workdir; inner process gets ${GREEN}GBX_NOTEBOOK_ISOLATED=1${NC}. Set to 0 to disable."
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:notebooks${NC}                        # cell-by-cell: fixtures + sample-data"
    echo -e "  ${YELLOW}gbx:test:notebooks --path sample-data${NC}     # cell-by-cell: only sample-data notebooks"
    echo -e "  ${YELLOW}gbx:test:notebooks --path test_notebook_via_script.py${NC}  # pytest that test file"
    echo -e "  ${YELLOW}gbx:test:notebooks --log notebooks.log${NC}"
    echo ""
}

LOG_PATH=""
INCLUDE_INTEGRATION=false
ALLOW_ABSOLUTE_READS=false
ALLOW_ABSOLUTE_WRITES=false
TEST_PATH="/root/geobrix/notebooks/tests"
PATH_ARG=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --allow-absolute-reads)
            ALLOW_ABSOLUTE_READS=true
            shift
            ;;
        --allow-absolute-writes)
            ALLOW_ABSOLUTE_WRITES=true
            shift
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --include-integration)
            INCLUDE_INTEGRATION=true
            shift
            ;;
        --path)
            # Path: subdir (e.g. sample-data, fixtures) or test file (e.g. test_setup_sample_data.py).
            # If user passes notebooks/tests/..., strip that prefix.
            PATH_ARG="$2"
            [[ "$PATH_ARG" == notebooks/tests/* ]] && PATH_ARG="${PATH_ARG#notebooks/tests/}"
            TEST_PATH="/root/geobrix/notebooks/tests/$PATH_ARG"
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

# If --path is a bare .py filename and it doesn't exist under notebooks/tests/, look under subdirs
if [[ -n "${PATH_ARG:-}" && "$PATH_ARG" == *.py && "$PATH_ARG" != */* ]]; then
    NBTEST="$PROJECT_ROOT/notebooks/tests"
    if [[ ! -f "$NBTEST/$PATH_ARG" ]]; then
        FOUND=
        for sub in sample-data fixtures; do
            if [[ -f "$NBTEST/$sub/$PATH_ARG" ]]; then
                PATH_ARG="$sub/$PATH_ARG"
                TEST_PATH="/root/geobrix/notebooks/tests/$PATH_ARG"
                FOUND=1
                break
            fi
        done
    fi
fi

show_banner "📓 GeoBrix: Notebook Tests"
check_docker
setup_log_file "$LOG_PATH"

# Ensure sample-data Volumes structure exists on host (mount target for start_docker_with_volumes.sh)
mkdir -p "$PROJECT_ROOT/sample-data/Volumes/main/default/geobrix_samples"

# Notebook tests need /Volumes so the notebook can use get_volumes_path() and run_*_bundle()
if ! docker exec geobrix-dev test -d /Volumes 2>/dev/null; then
    echo -e "${RED}❌ /Volumes not found in container. Start the container with the Volumes mount:${NC}"
    echo -e "   ${YELLOW}./scripts/docker/start_docker_with_volumes.sh${NC}"
    echo ""
    echo "Then run this command again so notebook tests can use sample-data paths."
    exit 1
fi

# Single entry point: always use isolation (venv + GBX_NOTEBOOK_ISOLATED=1). Path can be a test file (.py) or notebook scope.
PATH_ARG="${PATH_ARG:-}"
NOTEBOOK_VERBOSITY="${GBX_NOTEBOOK_VERBOSITY:-}"
PYTEST_EXTRA="-s"
[[ "$NOTEBOOK_VERBOSITY" = "quiet" ]] && PYTEST_EXTRA=""

if [[ -n "$PATH_ARG" ]]; then
    echo -e "${CYAN}🎯 Path: ${YELLOW}$PATH_ARG${NC}"
else
    echo -e "${CYAN}🎯 Path: ${YELLOW}default (fixtures + sample-data)${NC}"
fi
if [[ "$PATH_ARG" == *.py ]]; then
    if [ "$INCLUDE_INTEGRATION" = true ]; then
        echo -e "${CYAN}🐢 Including integration tests${NC}"
    else
        echo -e "${CYAN}⚡ Excluding integration tests (use --include-integration to include)${NC}"
    fi
fi
echo ""

RUN_CMD="set -e
unset JAVA_TOOL_OPTIONS
export JUPYTER_PLATFORM_DIRS=1
export GBX_NOTEBOOK_TESTS_DOCKER=1
export GBX_NOTEBOOK_ISOLATED_ENV=1
${NOTEBOOK_VERBOSITY:+export GBX_NOTEBOOK_VERBOSITY=\"$NOTEBOOK_VERBOSITY\"}
${INCLUDE_INTEGRATION:+export GBX_NOTEBOOK_INCLUDE_INTEGRATION=1}
${ALLOW_ABSOLUTE_READS:+export GBX_NOTEBOOK_ALLOW_ABSOLUTE_READS=1}
${ALLOW_ABSOLUTE_WRITES:+export GBX_NOTEBOOK_ALLOW_ABSOLUTE_WRITES=1}
cd /root/geobrix
pip install -e /root/geobrix/python/geobrix --break-system-packages -q 2>/dev/null || true
pip install nbformat nbconvert --break-system-packages -q 2>/dev/null || true
python3 /root/geobrix/notebooks/tests/run_notebooks_cell_by_cell.py ${PATH_ARG:+"$PATH_ARG"}
"

# -t allocates a pseudo-TTY so testbook's Jupyter kernel doesn't hang (common in Docker)
docker exec -t geobrix-dev /bin/bash -c "$RUN_CMD"
EXIT_CODE=$?

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Notebook tests passed!${NC}"
else
    echo -e "${RED}❌ Notebook tests failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
