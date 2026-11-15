#!/bin/bash
# gbx:docs:function-info - Generate function-info.json from the function manifest (DESCRIBE FUNCTION EXTENDED)

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
    echo -e "  from the function manifest (usage args) and the oracle and hand-written doc examples."
    echo -e "  That file drives ${YELLOW}DESCRIBE FUNCTION EXTENDED <name>${NC}. It runs${NC}"
    echo -e "  ${YELLOW}scripts/manifest/gen_surfaces.py --write --only function-info${NC} in Docker, so the file names no host path."
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}   Write output to log file"
    echo -e "  ${GREEN}--help${NC}         Show this help"
    echo ""
    echo -e "${CYAN}When to run:${NC}"
    echo -e "  After changing a manifest row, an oracle example or a hand-written SQL example in"
    echo -e "  ${YELLOW}docs/tests/python/api/*_functions_sql.py${NC}. Then commit the updated function-info.json."
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

echo -e "${CYAN}Running generator inside Docker (function manifest → function-info.json)...${NC}"
show_separator

# The checkout as mounted in the container (GBX_CONTAINER_ROOT, default /root/geobrix): check_docker has already
# refused a container that mounts another tree, so the generator reads and writes this checkout. The generator imports
# the package from this checkout's source: a new container (a worktree's) has no editable install of it.
docker exec "$GBX_DEV_CONTAINER" /bin/bash -c "cd \"$GBX_CONTAINER_ROOT\" && unset JAVA_TOOL_OPTIONS && PYTHONPATH=python/geobrix/src\${PYTHONPATH:+:\$PYTHONPATH} python3 scripts/manifest/gen_surfaces.py --write --only function-info"
EXIT=$?

show_separator
if [ $EXIT -eq 0 ]; then
    echo -e "${GREEN}✅ function-info.json updated.${NC}"
    echo ""
    echo -e "${CYAN}Output:${NC} ${YELLOW}src/main/resources/com/databricks/labs/gbx/function-info.json${NC}"
    echo -e "${CYAN}Commit this file after changing a manifest row or an oracle example.${NC}"
else
    echo -e "${RED}❌ Generator failed (exit code: $EXIT)${NC}"
fi

if [ -n "$LOG_PATH" ]; then
    echo ""
    echo -e "${CYAN}📝 Log: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT
