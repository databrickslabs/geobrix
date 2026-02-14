#!/bin/bash

# --- CONFIGURATION ---
FINAL_TAG="geobrix-dev:ubuntu24-gdal311-spark"
STAGES=(
    "base"
    "system-deps"
    "hadoop-builder"
    "gdal-builder"
    "pdal-builder"
    "final"
)

# --- COLORS FOR FEEDBACK ---
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# --- HELP MENU ---
usage() {
    echo -e "${CYAN}Geobrix Smart Builder${NC}"
    echo -e "Usage: $0 [OPTIONS] [-- DOCKER_FLAGS]"
    echo ""
    echo -e "${YELLOW}Options:${NC}"
    echo -e "  -h, --help        Show this help message and exit"
    echo -e "  -p, --pull        Force pull latest ubuntu:24.04 (default: true)"
    echo -e "  --no-pull         Do not pull base image (faster, may miss security updates)"
    echo ""
    echo -e "${YELLOW}Docker Flags:${NC}"
    echo -e "  Any arguments provided to this script (or following --) are passed"
    echo -e "  directly to 'docker build'. Use this for things like --no-cache."
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo -e "  $0 --no-cache                     Build everything from scratch"
    echo -e "  $0 --progress=plain               Show detailed build output"
    echo -e "  $0 --build-arg CORES=4            Override the CPU cores used (default=2)"
    echo -e "  $0 --build-arg BUILDPLATFORM=<?>  (default=linux/amd64)"
    exit 0
}

# --- ARGUMENT PARSING ---
EXTRA_ARGS=()
FORCE_PULL=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help)
            usage
            ;;
        -p|--pull)
            FORCE_PULL=true
            shift
            ;;
        --no-pull)
            FORCE_PULL=false
            shift
            ;;
        --) # End of our internal options, treat everything else as docker flags
            shift
            EXTRA_ARGS+=("$@")
            break
            ;;
        *) # Catch-all for docker flags (like --no-cache)
            EXTRA_ARGS+=("$1")
            shift
            ;;
    esac
done

# --- HELPER FUNCTIONS ---
log_info() { echo -e "${CYAN}[INFO]${NC} $1"; }
log_success() { echo -e "${GREEN}[SUCCESS]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# --- ERROR HANDLING ---
set -e
trap 'log_error "Build failed at stage: ${CURRENT_STAGE}"; exit 1' ERR

# --- INITIALIZATION ---
START_TIME=$(date +%s)
log_info "Starting smart build for ${FINAL_TAG}..."

# --- STEP 1: PULL LATEST BASE ---
if [ "$FORCE_PULL" = true ]; then
    log_info "Checking for latest Ubuntu 24.04 security patches..."
    docker pull ubuntu:24.04
fi

# --- STEP 2: MULTI-STAGE BUILD LOOP ---
for STAGE in "${STAGES[@]}"; do
    CURRENT_STAGE=$STAGE
    STAGE_TAG="geobrix-checkpoint:${STAGE}"

    # We always use the --pull flag on the 'base' stage to link to the latest
    # Ubuntu image we just pulled locally.
    PULL_FLAG=""
    if [ "$STAGE" == "base" ] && [ "$FORCE_PULL" = true ]; then
        PULL_FLAG="--pull"
    fi

    log_info "Building stage: ${YELLOW}${STAGE}${NC}..."
    STAGE_START=$(date +%s)

    # Note: Using "${EXTRA_ARGS[@]}" preserves spaces in arguments
    docker build \
        $PULL_FLAG \
        --target "$STAGE" \
        -t "$STAGE_TAG" \
        "${EXTRA_ARGS[@]}" .

    STAGE_END=$(date +%s)
    log_success "Stage ${STAGE} completed in $((STAGE_END - STAGE_START))s."
done

# --- STEP 3: FINAL TAGGING ---
log_info "Applying final tag: ${YELLOW}${FINAL_TAG}${NC}..."
docker tag "geobrix-checkpoint:final" "$FINAL_TAG"

# --- SUMMARY ---
END_TIME=$(date +%s)
TOTAL_RUNTIME=$((END_TIME - START_TIME))

echo -e "\n${GREEN}===========================================${NC}"
log_success "Smart Build Complete!"
log_info "Final Image: ${FINAL_TAG}"
log_info "Total Build Time: $((TOTAL_RUNTIME / 60))m $((TOTAL_RUNTIME % 60))s"
echo -e "${GREEN}===========================================${NC}"