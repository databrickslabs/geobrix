#!/bin/bash
# gbx:coverage:baseline - Generate baseline coverage data

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📊 GeoBrix: Baseline Coverage"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:coverage:baseline${NC} ${YELLOW}<language>${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Purpose:${NC}"
    echo -e "  Generate comprehensive baseline coverage data for weekly reference."
    echo -e "  Runs full test suite and saves coverage data for gap analysis."
    echo ""
    echo -e "${CYAN}Languages:${NC}"
    echo -e "  ${GREEN}scala${NC}    Full Scala coverage (~10 min)"
    echo -e "  ${GREEN}python${NC}   Full Python coverage (~30 sec)"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--min-coverage <percent>${NC}  Minimum coverage threshold (default: 90)"
    echo -e "  ${GREEN}--log <path>${NC}              Write output to log file"
    echo -e "  ${GREEN}--open${NC}                    Open HTML report in browser"
    echo -e "  ${GREEN}--help${NC}                    Show this help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:coverage:baseline scala --open${NC}"
    echo -e "  ${YELLOW}gbx:coverage:baseline python${NC}"
    echo ""
    echo -e "${CYAN}Frequency:${NC}"
    echo -e "  ${YELLOW}Recommended: Once per week (Monday morning)${NC}"
    echo -e "  ${YELLOW}Or: Before major PR/release${NC}"
    echo -e "  ${YELLOW}Or: When data is stale (>7 days)${NC}"
    echo ""
}

# Parse arguments
LANGUAGE=""
MIN_COVERAGE="90"
LOG_PATH=""
OPEN_REPORT=false

# First argument should be language
if [[ $# -gt 0 ]] && [[ ! "$1" =~ ^-- ]]; then
    LANGUAGE="$1"
    shift
fi

while [[ $# -gt 0 ]]; do
    case $1 in
        --min-coverage)
            MIN_COVERAGE="$2"
            shift 2
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --open)
            OPEN_REPORT=true
            shift
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

# Validate language
if [ -z "$LANGUAGE" ]; then
    echo -e "${RED}❌ Error: Language required (scala or python)${NC}"
    echo ""
    show_help
    exit 1
fi

if [ "$LANGUAGE" != "scala" ] && [ "$LANGUAGE" != "python" ]; then
    echo -e "${RED}❌ Error: Invalid language '$LANGUAGE'${NC}"
    exit 1
fi

cd "$PROJECT_ROOT"

show_banner "📊 GeoBrix: Baseline Coverage"
check_docker

echo -e "${CYAN}📊 Generating baseline ${LANGUAGE} coverage...${NC}"
echo -e "${YELLOW}⏱️  This runs the full test suite${NC}"
echo ""

# Dispatch to appropriate coverage command
if [ "$LANGUAGE" = "scala" ]; then
    echo -e "${CYAN}🎯 Scala: Full test suite (~10 minutes)${NC}"
    echo ""
    CMD="$SCRIPT_DIR/gbx-coverage-scala.sh"
    ARGS="--min-coverage $MIN_COVERAGE"
    [ "$OPEN_REPORT" = true ] && ARGS="$ARGS --open"
    [ -n "$LOG_PATH" ] && ARGS="$ARGS --log $LOG_PATH"
    
    $CMD $ARGS
    EXIT_CODE=$?
    
elif [ "$LANGUAGE" = "python" ]; then
    echo -e "${CYAN}🎯 Python: Full test suite (~30 seconds)${NC}"
    echo ""
    CMD="$SCRIPT_DIR/gbx-coverage-python.sh"
    ARGS="--min-coverage $MIN_COVERAGE"
    [ "$OPEN_REPORT" = true ] && ARGS="$ARGS --open"
    [ -n "$LOG_PATH" ] && ARGS="$ARGS --log $LOG_PATH"
    
    $CMD $ARGS
    EXIT_CODE=$?
fi

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Baseline coverage complete!${NC}"
    echo ""
    echo -e "${CYAN}📊 Next steps:${NC}"
    echo -e "  ${YELLOW}1. Check gaps:${NC} gbx:coverage:gaps $LANGUAGE"
    echo -e "  ${YELLOW}2. Target lowest package:${NC} gbx:coverage:${LANGUAGE}-package <pkg>"
    echo -e "  ${YELLOW}3. View report anytime:${NC} gbx:coverage:${LANGUAGE} --report-only --open"
    echo ""
    echo -e "${CYAN}💡 This baseline data is valid for ~7 days${NC}"
    echo -e "${CYAN}   Re-run baseline weekly or before major releases${NC}"
else
    echo -e "${RED}❌ Baseline coverage failed (exit code: $EXIT_CODE)${NC}"
fi
show_separator

exit $EXIT_CODE
