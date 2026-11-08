#!/bin/bash
# gbx:test:function-info - Check DESCRIBE FUNCTION for every function with a JVM level (in Docker)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Function-Info Tests"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:function-info${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Description:${NC}"
    echo -e "  Runs the DESCRIBE contract row of ${YELLOW}python/geobrix/test/manager/test_contracts.py${NC} in the"
    echo -e "  ${YELLOW}dev${NC} Docker container: for every manifest row with an L1 companion,"
    echo -e "  ${YELLOW}DESCRIBE FUNCTION EXTENDED <name>${NC} shows ${YELLOW}name(<usage args>)${NC} as the row gives it."
    echo -e "  It needs the staged JAR. Regenerate function-info.json with ${YELLOW}gbx:docs:function-info${NC}."
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}    Write output to log file"
    echo -e "  ${GREEN}--no-wait${NC}       Fail at once (exit 75) if another test run holds the container lock"
    echo -e "  ${GREEN}--help${NC}          Show this help"
    echo ""
}

LOG_ARGS=()
WAIT_ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --log)
            [[ $# -ge 2 ]] || { echo -e "${RED}Option --log needs a value${NC}" >&2; exit 2; }
            LOG_ARGS=(--log "$2")
            shift 2
            ;;
        --no-wait)
            WAIT_ARGS=(--no-wait)
            shift
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

cd "$PROJECT_ROOT" || exit 1

exec bash "$SCRIPT_DIR/gbx-test-python.sh" \
    --path python/geobrix/test/manager/test_contracts.py \
    -k describe_function_shows_the_usage_args \
    --with-integration "${WAIT_ARGS[@]}" "${LOG_ARGS[@]}"
