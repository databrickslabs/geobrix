#!/bin/bash
# gbx:data:stage-sample - stage a sample-data dataset (default: the Zarr ERA5-like cube) to a Volume
#
# Uploads the files of one dataset directory of the minimal bundle to
# <volume-root>/geobrix-examples/<path> with the Files API and checks every size. It deletes
# only stale files inside the *.zarr stores it uploads, nothing else. No Docker and no Maven;
# it talks to a workspace, so --profile is required (the workspace is never chosen for you)
# except with --dry-run, which needs neither a profile, uv nor the SDK.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

LAUNCHER="$PROJECT_ROOT/examples/tests/stage_sample_to_volume.py"

show_help() {
    show_banner "GeoBrix: Stage Sample Data"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:data:stage-sample${NC} ${YELLOW}--profile NAME [options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--profile NAME${NC}      Databricks config profile (required unless --dry-run)"
    echo -e "  ${GREEN}--path REL${NC}          Dataset directory under geobrix-examples (default: global/era5-weather);"
    echo -e "                        relative, no '..', no empty or '.' segment"
    echo -e "  ${GREEN}--src-root DIR${NC}      Local geobrix-examples root"
    echo -e "                        (default: sample-data/Volumes/main/default/test-data/geobrix-examples)"
    echo -e "  ${GREEN}--volume-root DIR${NC}   Volume to stage into (default: /Volumes/geospatial_docs/geobrix/sample-data)"
    echo -e "  ${GREEN}--dry-run${NC}           List what would be uploaded and pruned; no workspace call, no auth,"
    echo -e "                        no uv or SDK needed"
    echo -e "  ${GREEN}--log <path>${NC}        Write output to log (filename -> test-logs/<name>)"
    echo -e "  ${GREEN}--help${NC}              This help"
    echo ""
    echo -e "${CYAN}What it deletes:${NC}"
    echo -e "  Only stale files ${YELLOW}inside the uploaded part of each *.zarr store${NC} (a chunk left by an"
    echo -e "  older layout of the store): the whole store when it, or a directory above it, is uploaded;"
    echo -e "  with --path <store>.zarr/<array>, only that array (zarr.json and the other arrays stay)."
    echo -e "  Never other files in the dataset directory, other datasets, or the rest of the Volume."
    echo -e "  A path with no *.zarr store (for example --path nyc) is uploaded over what is there and"
    echo -e "  deletes nothing. Finder files (.DS_Store) are not uploaded."
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-data-stage-sample.sh --profile oauth-fe${NC}"
    echo -e "  ${YELLOW}bash scripts/commands/gbx-data-stage-sample.sh --dry-run${NC}"
    echo ""
}

LOG_PATH=""
LAUNCHER_ARGS=()

while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        *)
            LAUNCHER_ARGS+=("$1")
            shift
            ;;
    esac
done

cd "$PROJECT_ROOT" || exit 1
show_banner "GeoBrix: Stage Sample Data"
setup_log_file "$LOG_PATH"

# The upload uses databricks-sdk (Python >= 3.11): see gbx_python_with_sdk in common.sh. Chosen
# after --help so help works on a machine with no SDK. A dry run imports nothing from the SDK,
# so it runs on plain python3 and works with neither uv nor the SDK installed.
DRY_RUN=false
for arg in "${LAUNCHER_ARGS[@]}"; do
    [[ "$arg" == "--dry-run" ]] && DRY_RUN=true
done
if [[ "$DRY_RUN" == true ]]; then
    GBX_SDK_PYTHON=(python3)
else
    gbx_python_with_sdk || exit 1
fi

"${GBX_SDK_PYTHON[@]}" "$LAUNCHER" "${LAUNCHER_ARGS[@]}"
exit $?
