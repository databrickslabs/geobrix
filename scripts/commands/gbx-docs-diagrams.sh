#!/bin/bash
# gbx:docs:diagrams - Regenerate the GeoBrix-owned HTML diagrams and export their light and dark PNGs

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

GEN_DIR="resources/images/generators"
EXPORTER="$GEN_DIR/diagrams/export_diagrams.py"
# name -> generator script and the folder for its HTML and PNGs
DIAGRAMS=("rasterx-function-categories")
gen_script() { echo "$GEN_DIR/$1.py"; }
out_dir() { case "$1" in rasterx-*) echo "resources/images/diagrams/rasterx" ;; esac; }

show_help() {
    show_banner "GeoBrix: Docs Diagrams"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:docs:diagrams${NC} ${YELLOW}[options] [name ...]${NC}"
    echo ""
    echo -e "${CYAN}Description:${NC}"
    echo -e "  Fills each diagram's HTML template from the function manifest and its docs page."
    echo -e "  Then it exports ${YELLOW}<name>-light.png${NC} and ${YELLOW}<name>-dark.png${NC} with headless Google Chrome."
    echo -e "  Diagrams: ${YELLOW}${DIAGRAMS[*]}${NC} (default: all)."
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--html-only${NC}         Write the HTML only; skip the PNG export (needs no Chrome)"
    echo -e "  ${GREEN}--theme <t>${NC}         light, dark or both (default: both)"
    echo -e "  ${GREEN}--log <path>${NC}        Write output to a log file"
    echo -e "  ${GREEN}--help${NC}              Show this help"
    echo ""
    echo -e "${CYAN}Needs:${NC} python3; for the export, uv (uvx) and Google Chrome. Nothing is installed globally."
    echo ""
}

LOG_PATH=""
HTML_ONLY=false
THEME="both"
TARGETS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --html-only) HTML_ONLY=true; shift ;;
        --theme)
            THEME="${2:-}"
            [[ "$THEME" =~ ^(light|dark|both)$ ]] || { echo -e "${RED}--theme needs light, dark or both${NC}"; exit 2; }
            shift 2
            ;;
        --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        --help|-h) show_help; exit 0 ;;
        -*) echo -e "${RED}Unknown option: $1${NC}"; show_help; exit 2 ;;
        *) TARGETS+=("$1"); shift ;;
    esac
done
[[ ${#TARGETS[@]} -gt 0 ]] || TARGETS=("${DIAGRAMS[@]}")
for t in "${TARGETS[@]}"; do
    [[ " ${DIAGRAMS[*]} " == *" $t "* ]] || { echo -e "${RED}Unknown diagram: $t (have: ${DIAGRAMS[*]})${NC}"; exit 2; }
done

cd "$PROJECT_ROOT" || exit 1
show_banner "GeoBrix: Docs Diagrams"
setup_log_file "$LOG_PATH"

if ! $HTML_ONLY; then
    command -v uvx >/dev/null || { echo -e "${RED}uvx not found; install uv: https://docs.astral.sh/uv/${NC}"; exit 1; }
    [[ -d "/Applications/Google Chrome.app" ]] || command -v google-chrome >/dev/null \
        || { echo -e "${RED}Google Chrome not found (the exporter drives the system Chrome)${NC}"; exit 1; }
fi

set -o pipefail
for t in "${TARGETS[@]}"; do
    dir="$(out_dir "$t")"
    html="$dir/$t.html"
    echo -e "${CYAN}==> $t: HTML${NC}"
    python3 "$(gen_script "$t")" "$html" || exit 1
    if ! $HTML_ONLY; then
        echo -e "${CYAN}==> $t: PNG ($THEME)${NC}"
        uvx --quiet --with playwright python -I "$EXPORTER" "$html" --out-dir "$dir" --theme "$THEME" || exit 1
    fi
done
show_separator
echo -e "${GREEN}Done.${NC} Review the PNGs, run ${YELLOW}python3 docs/scripts/check-diagram-coverage.py${NC}, then commit the HTML and PNGs."
