#!/bin/bash
# gbx:data:generate-minimal-bundle - Generate minimal doc-test bundle from full sample-data (bbox extraction)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📦 GeoBrix: Generate Minimal Bundle"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:data:generate-minimal-bundle${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--source <dir>${NC}     Full bundle root (default: sample-data/.../geobrix_samples/geobrix-examples)"
    echo -e "  ${GREEN}--out <dir>${NC}        Output root (default: sample-data/.../test-data/geobrix-examples)"
    echo -e "  ${GREEN}--nyc-lon <float>${NC}  NYC center longitude (default: -73.9857, Manhattan)"
    echo -e "  ${GREEN}--nyc-lat <float>${NC}  NYC center latitude (default: 40.7484)"
    echo -e "  ${GREEN}--london-lon <float>${NC}  London center longitude (default: -0.1276)"
    echo -e "  ${GREEN}--london-lat <float>${NC}  London center latitude (default: 51.5074)"
    echo -e "  ${GREEN}--bbox-size <float>${NC}  Half-width/height of bbox in degrees (default: 0.02)"
    echo -e "  ${GREEN}--max-rows <int>${NC}   Max vector features per layer (default: 10)"
    echo -e "  ${GREEN}--log <path>${NC}       Write output to log file"
    echo -e "  ${GREEN}--help${NC}             Show this help"
    echo ""
    echo -e "${CYAN}Output:${NC}"
    echo -e "  ${YELLOW}sample-data/Volumes/main/default/test-data/geobrix-examples/${NC}"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:data:generate-minimal-bundle${NC}"
    echo -e "  ${YELLOW}gbx:data:generate-minimal-bundle --bbox-size 0.03 --max-rows 20${NC}"
    echo ""
}

LOG_PATH=""
EXTRA_ARGS=()

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
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

cd "$PROJECT_ROOT"
show_banner "📦 GeoBrix: Generate Minimal Bundle"
check_docker
setup_log_file "$LOG_PATH"

docker exec geobrix-dev python3 /root/geobrix/sample-data/generate-minimal-bundle.py "${EXTRA_ARGS[@]}"
exit $?
