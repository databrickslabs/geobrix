#!/bin/bash
# gbx:security:codeql - Run CodeQL analysis locally (Python). No GitHub license required.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
DB_DIR="$PROJECT_ROOT/.codeql/databases/python"
DEFAULT_OUTPUT="$PROJECT_ROOT/test-logs/codeql-results.sarif"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "Security: CodeQL (local)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:security:codeql${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--output <path>${NC}   SARIF output path (default: test-logs/codeql-results.sarif)."
    echo -e "  ${GREEN}--help${NC}             Show this help."
    echo ""
    echo -e "${CYAN}Prerequisites:${NC}"
    echo -e "  CodeQL CLI on PATH. Install: https://codeql.github.com/docs/codeql-cli/getting-started-with-the-codeql-cli/"
    echo -e "  No special license needed for local runs; upload to GitHub requires Code Security enabled for the repo."
    echo ""
}

OUTPUT_PATH="$DEFAULT_OUTPUT"
while [[ $# -gt 0 ]]; do
    case $1 in
        --output)
            OUTPUT_PATH="$2"
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
show_banner "Security: CodeQL (local)"

if ! command -v codeql &>/dev/null; then
    echo -e "${RED}CodeQL CLI not found on PATH.${NC}"
    echo ""
    echo -e "${CYAN}Install:${NC}"
    echo -e "  ${YELLOW}gh extension install github/gh-codeql${NC}"
    echo ""
    echo -e "${CYAN}To overcome 'CodeQL CLI not found on PATH':${NC}"
    echo -e "  ${YELLOW}gh codeql install-stub ~/bin${NC}"
    echo -e "  ${YELLOW}echo 'export PATH=\"\$HOME/bin:\$PATH\"' >> ~/.zshrc${NC}"
    echo -e "  ${YELLOW}source ~/.zshrc${NC}"
    echo ""
    echo -e "Local analysis is free; no special license required. Upload to GitHub Code Scanning requires Code Security to be enabled for the repo."
    exit 1
fi

mkdir -p "$(dirname "$OUTPUT_PATH")"
mkdir -p "$(dirname "$DB_DIR")"

echo -e "${CYAN}Creating CodeQL database (Python)...${NC}"
show_separator
if ! codeql database create "$DB_DIR" --language=python --build-mode=none --overwrite; then
    echo -e "${RED}CodeQL database create failed.${NC}"
    exit 1
fi

echo ""
echo -e "${CYAN}Downloading CodeQL Python query pack (if needed)...${NC}"
show_separator
if ! codeql pack download codeql/python-queries; then
    echo -e "${RED}CodeQL pack download failed (network or auth?).${NC}"
    exit 1
fi

echo ""
echo -e "${CYAN}Running CodeQL analysis...${NC}"
show_separator
if ! codeql database analyze "$DB_DIR" --format=sarif-latest --output="$OUTPUT_PATH"; then
    echo -e "${RED}CodeQL analyze failed.${NC}"
    exit 1
fi

# Generate CSV from SARIF for easy reading (avoids empty 0-byte CSV from codeql --format=csv with no alerts)
CSV_PATH="${OUTPUT_PATH%.*}.csv"
if command -v jq &>/dev/null && [ -s "$OUTPUT_PATH" ]; then
    if jq -r '
        ["query","severity","message","path","line"],
        (
            .runs[]?.results[]?
            | [
                .ruleId,
                (.level // "warning"),
                (.message.text // ""),
                (.locations[0].physicalLocation.artifactLocation.uri // ""),
                (.locations[0].physicalLocation.region.startLine // "")
              ] | @csv
        )
        | @csv
    ' "$OUTPUT_PATH" > "$CSV_PATH" 2>/dev/null; then
        echo ""
        echo -e "${CYAN}Wrote readable CSV: ${YELLOW}$CSV_PATH${NC}"
    fi
fi

show_separator
echo -e "${GREEN}CodeQL analysis complete.${NC}"
echo -e "Results (SARIF): ${YELLOW}$OUTPUT_PATH${NC}"
if [ -s "$CSV_PATH" ]; then
    echo -e "Results (CSV):   ${YELLOW}$CSV_PATH${NC}"
    echo ""
    echo -e "${CYAN}How to view results:${NC}"
    echo -e "  ${GREEN}CSV${NC} (easiest):  Open ${YELLOW}$CSV_PATH${NC} in a text editor or Excel (query, severity, message, file, line)."
else
    echo ""
    echo -e "${CYAN}How to view results:${NC}"
    echo -e "  ${GREEN}SARIF${NC}:  In Cursor/VS Code install the ${YELLOW}SARIF Viewer${NC} extension, then open ${YELLOW}$OUTPUT_PATH${NC}."
fi
echo -e "  ${GREEN}GitHub${NC}:  Upload the SARIF to the repo’s Code scanning workflow to see results in the Security tab."
exit 0
