#!/bin/bash
# gbx:test:docs-examples - Structural guards for the docs example tabs & output tables
# (host-only; no Docker, no Spark). Catches malformed ASCII output tables and
# incomplete/half-rendered FunctionExamples tabs before they ship.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📄 GeoBrix: Docs Example Guards"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:docs-examples${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}What it checks:${NC}"
    echo -e "  ${YELLOW}1. Output tables${NC} — every ${YELLOW}*_example_output${NC} constant in the doc-example"
    echo -e "     modules has width/tick-consistent ASCII result tables (no orphan"
    echo -e "     separators, no mismatched column widths)."
    echo -e "  ${YELLOW}2. Tab completeness${NC} — every ${YELLOW}<FunctionExamples>${NC} tab declared in"
    echo -e "     function-info.json bindings has BOTH an example and an output constant,"
    echo -e "     and no single-tab <CodeFromTest> is used on a multi-tier function."
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Notes:${NC} Runs on the host (pure ast/regex parsing — no Docker, no Spark)."
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

show_banner "📄 GeoBrix: Docs Example Guards"
setup_log_file "$LOG_PATH"

# Pure file-parsing guards — no Docker, no Spark. The same checks are also
# collected as pytest modules in the doc-test suite
# (docs/tests/python/api/test_example_output_tables.py and
# test_example_tab_completeness.py); this standalone script is the fast host/CI
# gate (and what the QC judge runs).
python3 "$PROJECT_ROOT/docs/scripts/check-docs-examples.py"
EXIT_CODE=$?

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
