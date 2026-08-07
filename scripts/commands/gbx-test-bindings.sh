#!/bin/bash
# gbx:test:bindings - Verify every registered function exists across all bindings (Scala, Python, SQL/function-info)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "🔗 GeoBrix: Binding Parity"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:bindings${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}What it checks:${NC}"
    echo -e "  Every name in ${YELLOW}docs/tests-function-info/registered_functions.txt${NC} (the canonical"
    echo -e "  SQL surface) also exists as a Scala companion (${YELLOW}override def name${NC}), a Python"
    echo -e "  binding (${YELLOW}functions.py${NC}), and a ${YELLOW}function-info.json${NC} entry. Fails if any"
    echo -e "  registered function is missing from a binding."
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Notes:${NC} Runs on the host (pure file parsing — no Docker needed)."
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
            echo ""
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"

show_banner "🔗 GeoBrix: Binding Parity"
setup_log_file "$LOG_PATH"

python3 "$PROJECT_ROOT/docs/scripts/check-binding-parity.py"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    python3 "$PROJECT_ROOT/docs/scripts/check-param-names.py"
    EXIT_CODE=$?
fi

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
