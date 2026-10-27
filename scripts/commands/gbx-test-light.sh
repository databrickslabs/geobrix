#!/bin/bash
# gbx:test:light - Run the light-tier Python tests (pyrx, pyvx, pygx, ds, zarr, ...) natively on
# the host. A thin front for gbx:test:pyrx, whose default is only test/pyrx: with no --path this
# runs every light dir in one pytest process, in the order of the light CI phase's pytest line
# (.github/actions/pyrx_build/action.yml). The order matters: the dirs share a process, and
# test/pyvx registers PROJ grids before test/bench builds rasters on worker threads.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Light-tier Tests (host venv, no JAR)"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:light${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--path <dir>${NC}           Test directory or file; repeatable (default: every light dir, in the light CI phase order)"
    echo -e "  ${GREEN}--docker${NC}               Run in the geobrix-dev container instead of the host venv (see gbx:test:pyrx)"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to log file"
    print_lock_help
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:test:light --path python/geobrix/test/zarr/test_manifest.py${NC}"
    echo -e "  ${YELLOW}gbx:test:light --log light.log${NC}"
    echo ""
}

ARGS=()
HAS_PATH=0
while [[ $# -gt 0 ]]; do
    case $1 in
        --path|--log)
            if [[ $# -lt 2 ]]; then
                echo -e "${RED}Option $1 needs a value${NC}" >&2
                exit 2
            fi
            [[ $1 == --path ]] && HAS_PATH=1
            ARGS+=("$1" "$2")
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            ARGS+=("$1")   # --docker, --no-wait, and unknown options (gbx:test:pyrx rejects those)
            shift
            ;;
    esac
done

if [[ $HAS_PATH -eq 0 ]]; then
    # The CI light phase runs: pytest test/pyrx test/ds ... -m "not integration" -v (from python/geobrix).
    CI_ACTION="$PROJECT_ROOT/.github/actions/pyrx_build/action.yml"
    LIGHT_DIRS=$(grep -E '^[[:space:]]*pytest test/' "$CI_ACTION" 2>/dev/null | head -1 | grep -oE 'test/[A-Za-z0-9_]+' | sed 's|^test/||')
    if [[ -z "$LIGHT_DIRS" ]]; then
        echo -e "${RED}Could not read the light test dirs from the pytest line of $CI_ACTION${NC}" >&2
        exit 1
    fi
    while IFS= read -r d; do
        ARGS+=(--path "python/geobrix/test/$d/")
    done <<< "$LIGHT_DIRS"
fi

exec bash "$SCRIPT_DIR/gbx-test-pyrx.sh" "${ARGS[@]}"
