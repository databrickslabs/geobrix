#!/bin/bash
# gbx:lint:scalastyle - Run ScalaStyle on main Scala sources (same as CI)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "Lint: ScalaStyle"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:lint:scalastyle${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}    Write output to log file (e.g. scalastyle.log → test-logs/scalastyle.log)"
    echo -e "  ${GREEN}--help${NC}          Show this help"
    echo ""
    echo -e "${CYAN}Notes:${NC}"
    echo -e "  Runs the same ScalaStyle check as CI (scalastyle-config.xml)."
    echo -e "  Reports 0 errors / N warnings; CI fails on errors, not warnings (failOnWarning: false)."
    echo ""
}

LOG_PATH=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"
show_banner "Lint: ScalaStyle"
check_docker
setup_log_file "$LOG_PATH"

echo -e "${CYAN}Running scalastyle on src/main/scala...${NC}"
echo ""
show_separator

docker exec geobrix-dev /bin/bash -c "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn scalastyle:check -q"
EXIT_CODE=$?

show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}ScalaStyle: no errors (warnings, if any, in output above).${NC}"
else
    echo -e "${RED}ScalaStyle reported errors (exit code: $EXIT_CODE).${NC}"
fi
show_separator

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
