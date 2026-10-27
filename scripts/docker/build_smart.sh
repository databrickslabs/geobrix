#!/bin/bash

# --- CONFIGURATION ---
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# Platform and image-tag resolution shared with the start scripts.
source "$SCRIPT_DIR/gbx-platform.sh"
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
    echo -e "  --platform <p>    linux/arm64 or linux/amd64 (default: \$GBX_DOCKER_PLATFORM, else this host's"
    echo -e "                    architecture). Each platform gets its own image tag and checkpoint tags."
    echo -e "  -p, --pull        Force pull latest ubuntu:24.04 (default: true)"
    echo -e "  --no-pull         Do not pull base image (faster, may miss security updates)"
    echo ""
    echo -e "${YELLOW}Package index:${NC}"
    echo -e "  Export PIP_INDEX_URL to build through a private mirror. It reaches the build as a BuildKit"
    echo -e "  secret (never a --build-arg), so it is not recorded in the image history."
    echo ""
    echo -e "${YELLOW}Docker Flags:${NC}"
    echo -e "  Any arguments provided to this script (or following --) are passed"
    echo -e "  directly to 'docker build'. Use this for things like --no-cache."
    echo ""
    echo -e "${YELLOW}Examples:${NC}"
    echo -e "  $0 --no-cache                     Build everything from scratch"
    echo -e "  $0 --progress=plain               Show detailed build output"
    echo -e "  $0 --build-arg CORES=4            Override the CPU cores used (default=2)"
    echo -e "  $0 --platform linux/amd64         Build the amd64 image (default: the host's architecture)"
    exit 0
}

# --- ARGUMENT PARSING ---
EXTRA_ARGS=()
FORCE_PULL=true
PLATFORM_ARG=""

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
        --platform)
            if [ $# -lt 2 ]; then echo "ERROR: --platform needs a value (linux/arm64 or linux/amd64)" >&2; exit 2; fi
            PLATFORM_ARG="$2"
            shift 2
            ;;
        --platform=*)
            PLATFORM_ARG="${1#--platform=}"
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

# --- PLATFORM AND TAGS ---
# The platform is passed to every stage build, so the image is native to it; one tag per
# architecture means the arm64 and amd64 images coexist.
PLATFORM="$(gbx_docker_platform "$PLATFORM_ARG")" || exit 2
ARCH="$(gbx_platform_arch "$PLATFORM")"
FINAL_TAG="$(gbx_dev_image_tag "$PLATFORM")"

# The platform is chosen above, and the package index travels as a secret (below): a docker flag
# that sets either again would build one thing and tag another, or put the index in the history.
for a in "${EXTRA_ARGS[@]}"; do
    case "$a" in
        --platform|--platform=*)
            log_error "Pass the platform as --platform <p> before any '--' (or set GBX_DOCKER_PLATFORM), not as a docker flag."
            exit 2
            ;;
        PIP_INDEX_URL=*|--build-arg=PIP_INDEX_URL=*)
            log_error "Do not pass the package index as a build-arg (it would be recorded in the image history). Export PIP_INDEX_URL; it is passed as a BuildKit secret."
            exit 2
            ;;
    esac
done

# --- ERROR HANDLING ---
set -e
SECRET_FILE=""
cleanup() { if [ -n "$SECRET_FILE" ]; then rm -f "$SECRET_FILE"; fi; }
trap cleanup EXIT
trap 'log_error "Build failed at stage: ${CURRENT_STAGE}"; exit 1' ERR

# --- INITIALIZATION ---
START_TIME=$(date +%s)
log_info "Starting smart build for ${FINAL_TAG} (${PLATFORM})..."

# Forward host PIP_INDEX_URL into the build if set. Leave unset to use public PyPI; export it to
# point at a private mirror if your network needs one. It goes in as a BuildKit secret from a
# private file that exists only for the duration of the build, never as a --build-arg or ENV
# (either is recorded in `docker history`). The URL is never printed.
SECRET_ARGS=()
if [ -n "${PIP_INDEX_URL:-}" ]; then
    log_info "Using PIP_INDEX_URL from host environment for the build (as a build secret)."
    SECRET_FILE=$(mktemp "${TMPDIR:-/tmp}/gbx-dev-pip-index.XXXXXX")
    chmod 600 "$SECRET_FILE"
    printf '%s' "$PIP_INDEX_URL" > "$SECRET_FILE"
    SECRET_ARGS=(--secret "id=pip_index_url,src=$SECRET_FILE")
fi

# --- STEP 1: PULL LATEST BASE ---
if [ "$FORCE_PULL" = true ]; then
    log_info "Checking for latest Ubuntu 24.04 security patches..."
    docker pull --platform "$PLATFORM" ubuntu:24.04
fi

# --- STEP 2: MULTI-STAGE BUILD LOOP ---
for STAGE in "${STAGES[@]}"; do
    CURRENT_STAGE=$STAGE
    STAGE_TAG="geobrix-checkpoint:${STAGE}-${ARCH}"

    # We always use the --pull flag on the 'base' stage to link to the latest
    # Ubuntu image we just pulled locally.
    PULL_FLAG=""
    if [ "$STAGE" == "base" ] && [ "$FORCE_PULL" = true ]; then
        PULL_FLAG="--pull"
    fi

    log_info "Building stage: ${YELLOW}${STAGE}${NC}..."
    STAGE_START=$(date +%s)

    # Note: Using "${EXTRA_ARGS[@]}" preserves spaces in arguments.
    # Context is the project root so the Dockerfile's COPY can reach
    # python/geobrix/requirements-dev-container{,-aarch64}.{in,txt} for the hash-pinned
    # pip install. Dockerfile path is explicit via -f.
    DOCKER_BUILDKIT=1 docker build \
        $PULL_FLAG \
        --platform "$PLATFORM" \
        --target "$STAGE" \
        -t "$STAGE_TAG" \
        -f "$PROJECT_ROOT/scripts/docker/Dockerfile" \
        "${SECRET_ARGS[@]}" \
        "${EXTRA_ARGS[@]}" "$PROJECT_ROOT"

    STAGE_END=$(date +%s)
    log_success "Stage ${STAGE} completed in $((STAGE_END - STAGE_START))s."
done

# --- STEP 3: CHECK THE IMAGE RECORDS NO PACKAGE INDEX, THEN TAG ---
# The index must not be in the image's history or environment. Compared as a fixed string read from a
# pipe, so the URL is neither printed nor placed on a command line.
FINAL_CHECKPOINT="geobrix-checkpoint:final-${ARCH}"
if [ -n "${PIP_INDEX_URL:-}" ]; then
    if { docker history --no-trunc "$FINAL_CHECKPOINT"; docker image inspect "$FINAL_CHECKPOINT"; } 2>/dev/null \
        | grep -qF -f <(printf '%s\n' "$PIP_INDEX_URL"); then
        docker rmi "$FINAL_CHECKPOINT" >/dev/null 2>&1 || true
        log_error "The package index was recorded in the image history or config; image not tagged as ${FINAL_TAG}."
        exit 1
    fi
    log_info "Image history and config hold no package index."
fi
log_info "Applying final tag: ${YELLOW}${FINAL_TAG}${NC}..."
docker tag "$FINAL_CHECKPOINT" "$FINAL_TAG"

# --- SUMMARY ---
END_TIME=$(date +%s)
TOTAL_RUNTIME=$((END_TIME - START_TIME))

echo -e "\n${GREEN}===========================================${NC}"
log_success "Smart Build Complete!"
log_info "Final Image: ${FINAL_TAG}"
log_info "Total Build Time: $((TOTAL_RUNTIME / 60))m $((TOTAL_RUNTIME % 60))s"
echo -e "${GREEN}===========================================${NC}"