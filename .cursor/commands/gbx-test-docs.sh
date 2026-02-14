#!/bin/bash
# gbx:test:docs - Run all documentation examples (Python, Scala, and SQL)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📚 GeoBrix: All Documentation Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:docs${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Description:${NC}"
    echo -e "  Runs all documentation tests: Python (includes SQL API examples), then Scala."
    echo -e "  Same as running gbx:test:python-docs then gbx:test:scala-docs in sequence."
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
    echo -e "  ${GREEN}--skip-download${NC}        Skip sample-data download"
    echo -e "  ${GREEN}--data-bundle <type>${NC}   essential|complete|both (default: complete)"
    echo -e "  ${GREEN}--scala-suite <pattern>${NC} Scala suite pattern (default: tests.docs.scala.*)"
    echo -e "  ${GREEN}--python-only${NC}          Run only Python doc tests (skip Scala)"
    echo -e "  ${GREEN}--scala-only${NC}           Run only Scala doc tests (skip Python)"
    echo -e "  ${GREEN}--help${NC}                 This help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:docs --skip-build --skip-download --log docs.log${NC}"
    echo -e "  ${YELLOW}gbx:test:docs --python-only --suite api${NC}"
    echo -e "  ${YELLOW}gbx:test:docs --scala-only --log scala-docs.log${NC}"
    echo ""
}

BASE="/root/geobrix/docs/tests/python"
PYTHON_PATH="${BASE}/"
LOG_PATH=""
MARKERS="-m 'not integration'"
INCLUDE_INTEGRATION=false
SKIP_BUILD=false
SKIP_DOWNLOAD=false
DATA_BUNDLE="complete"
SCALA_SUITE="tests.docs.scala.*"
PYTHON_ONLY=false
SCALA_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --suite)
            case "$2" in
                quickstart|api|readers|rasterx|advanced|setup)
                    PYTHON_PATH="${BASE}/$2/"
                    ;;
                integration)
                    PYTHON_PATH="${BASE}/$2/"
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
            PYTHON_PATH="${BASE}/$2"
            shift 2
            ;;
        --test)
            PYTHON_PATH="${BASE}/$2"
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
        --skip-download)
            SKIP_DOWNLOAD=true
            shift
            ;;
        --data-bundle)
            DATA_BUNDLE="$2"
            [[ "$DATA_BUNDLE" =~ ^(essential|complete|both)$ ]] || { echo -e "${RED}❌ Invalid data-bundle${NC}"; exit 1; }
            shift 2
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

if [ "$SKIP_DOWNLOAD" = false ]; then
    echo -e "${CYAN}📥 Ensuring sample data (--data-bundle $DATA_BUNDLE)...${NC}"
    show_separator
    if ! bash "$SCRIPT_DIR/gbx-data-download.sh" --bundle "$DATA_BUNDLE"; then
        echo -e "${RED}❌ Sample data download failed. Use --skip-download if data is present.${NC}"
        exit 1
    fi
    show_separator
    echo ""
else
    echo -e "${CYAN}⏭️  Skipping sample-data download (--skip-download)${NC}"
fi

[ "$SKIP_BUILD" = true ] && echo -e "${CYAN}⏭️  Skipping build (--skip-build)${NC}"
echo ""

TOTAL_EXIT=0

# ---- Python doc tests ----
if [ "$SCALA_ONLY" = false ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}1/2 Python documentation tests${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}🎯 Path: ${YELLOW}$PYTHON_PATH${NC}"
    echo ""

    RUN_PY="set -e
unset JAVA_TOOL_OPTIONS
export JUPYTER_PLATFORM_DIRS=1
cd /root/geobrix
if [ \"$SKIP_BUILD\" != 'true' ]; then
    echo 'Building JAR and Python package...'
    mvn package -DskipTests -q
    cd /root/geobrix/python/geobrix && python3 -m build && cd /root/geobrix
    pip install -e /root/geobrix/python/geobrix --break-system-packages -q
    echo ''
fi
python3 -m pytest $PYTHON_PATH -v $MARKERS --tb=short --color=yes
"
    docker exec geobrix-dev /bin/bash -c "$RUN_PY" || TOTAL_EXIT=$?
    echo ""
fi

# ---- Scala doc tests ----
if [ "$PYTHON_ONLY" = false ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}2/2 Scala documentation tests${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}🎯 Suite: ${YELLOW}$SCALA_SUITE${NC}"
    echo ""

    MVN_CMD="unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test -Dsuites='$SCALA_SUITE'"
    docker exec geobrix-dev /bin/bash -c "$MVN_CMD" || TOTAL_EXIT=$?
    echo ""
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
