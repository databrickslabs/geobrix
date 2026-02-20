#!/bin/bash
# gbx:docs:function-info - Generate function-info.json from doc SQL examples (DESCRIBE FUNCTION EXTENDED)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📋 GeoBrix: Generate Function Info"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:docs:function-info${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Description:${NC}"
    echo -e "  Generates ${YELLOW}src/main/resources/com/databricks/labs/gbx/function-info.json${NC}"
    echo -e "  from doc SQL examples in docs/tests/python/api/*_functions_sql.py."
    echo -e "  That file drives ${YELLOW}DESCRIBE FUNCTION EXTENDED <name>${NC} (one-copy pattern)."
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}   Write output to log file"
    echo -e "  ${GREEN}--help${NC}         Show this help"
    echo ""
    echo -e "${CYAN}When to run:${NC}"
    echo -e "  After adding or changing SQL examples in:"
    echo -e "  ${YELLOW}docs/tests/python/api/rasterx_functions_sql.py${NC}"
    echo -e "  ${YELLOW}docs/tests/python/api/gridx_functions_sql.py${NC}"
    echo -e "  Then commit the updated function-info.json."
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
            echo -e "${RED}❌ Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT" || exit 1

show_banner "📋 GeoBrix: Generate Function Info"
check_docker
setup_log_file "$LOG_PATH"

echo -e "${CYAN}Running generator inside Docker (doc examples → function-info.json)...${NC}"
show_separator

docker exec geobrix-dev /bin/bash -c "cd /root/geobrix && unset JAVA_TOOL_OPTIONS && python3 docs/scripts/generate-function-info.py"
EXIT=$?

show_separator
if [ $EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ function-info.json updated.${NC}"
    echo ""
    echo -e "${CYAN}Output:${NC} ${YELLOW}src/main/resources/com/databricks/labs/gbx/function-info.json${NC}"
    echo -e "${CYAN}Commit this file after changing doc SQL examples.${NC}"
else
    echo -e "${RED}❌ Generator failed (exit code: $EXIT)${NC}"
fi

if [ -n "$LOG_PATH" ]; then
    echo ""
    echo -e "${CYAN}📝 Log: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT
