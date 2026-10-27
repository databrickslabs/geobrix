#!/bin/bash
# gbx:docker:rebuild - Rebuild geobrix-dev Docker image via scripts/docker/build_smart.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"
source "$PROJECT_ROOT/scripts/docker/gbx-platform.sh"

# Defaults
NO_CACHE=false
PULL=true
START=false
ATTACH=false
KEEP_CONTAINER=false
PLATFORM_ARG=""
LOG_FILE=""

show_help() {
    cat << EOF
$(print_banner "🐳 GeoBrix: Rebuild Docker Image")

Rebuild geobrix-dev Docker image using scripts/docker/build_smart.sh (multi-stage build).
Optionally start the container after rebuild.

USAGE:
    bash scripts/commands/gbx-docker-rebuild.sh [OPTIONS] [-- DOCKER_FLAGS]

OPTIONS:
    --platform <p>       linux/arm64 or linux/amd64 (default: \$GBX_DOCKER_PLATFORM, else this host's
                         architecture). Each platform builds its own image tag, so both coexist.
    --no-cache           Build without Docker cache (passed to build_smart.sh)
    --no-pull            Do not force-pull latest ubuntu:24.04 (faster, may miss base updates)
    --keep-container     Do not stop or remove the running geobrix-dev container
    --start              Start container after rebuild
    --attach             Attach to container after start (implies --start)
    --log <path>         Write output to log file (filename → test-logs/<name>). Not for a build with
                         PIP_INDEX_URL set: pip prints the index host, so the log would contain it.
    --help               Display this help message

DOCKER FLAGS (after --):
    Arguments after -- are passed to docker build. Examples:
    --progress=plain     Show full build output
    --build-arg CORES=4  Override CPU cores used in build (default=2)

EXAMPLES:
    # Rebuild image (multi-stage, with pull) for this host's architecture
    bash scripts/commands/gbx-docker-rebuild.sh

    # Build the arm64 image next to a running amd64 container, leaving that container alone
    bash scripts/commands/gbx-docker-rebuild.sh --platform linux/arm64 --keep-container

    # Rebuild without cache
    bash scripts/commands/gbx-docker-rebuild.sh --no-cache

    # Rebuild without pulling base image (faster)
    bash scripts/commands/gbx-docker-rebuild.sh --no-pull

    # Rebuild and start container
    bash scripts/commands/gbx-docker-rebuild.sh --start

    # Rebuild, start, and attach
    bash scripts/commands/gbx-docker-rebuild.sh --start --attach

    # Rebuild with detailed build output
    bash scripts/commands/gbx-docker-rebuild.sh -- --progress=plain

NOTES:
    - Uses scripts/docker/build_smart.sh (multi-stage; final image geobrix-dev:ubuntu24-gdal311-spark-<arch>)
    - Platform: --platform, else \$GBX_DOCKER_PLATFORM, else this host's architecture. An arm64 host builds
      a native arm64 image (an amd64 image under emulation is about 10x slower).
    - A private package index: export PIP_INDEX_URL before running; it reaches the build as a BuildKit
      secret, never a build-arg, and is not recorded in the image history. The image does keep it in its
      pip/uv config files, so never push, docker save or share the image.
    - Stops and removes the existing container before rebuild so the next start uses the new image,
      but only when that container runs the SAME architecture as the rebuild; a container of the other
      architecture, and --keep-container, leave it alone.
    - Start script uses the same per-architecture tag (start_docker_with_volumes.sh)

EOF
    exit 0
}

# Parse arguments; collect any pass-through for build_smart after --
BUILD_EXTRA=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-cache)
            NO_CACHE=true
            shift
            ;;
        --no-pull)
            PULL=false
            shift
            ;;
        --keep-container)
            KEEP_CONTAINER=true
            shift
            ;;
        --platform)
            if [ $# -lt 2 ]; then
                echo -e "${RED}❌ --platform needs a value (linux/arm64 or linux/amd64)${NC}"
                exit 2
            fi
            PLATFORM_ARG="$2"
            shift 2
            ;;
        --platform=*)
            PLATFORM_ARG="${1#--platform=}"
            shift
            ;;
        --start)
            START=true
            shift
            ;;
        --attach)
            ATTACH=true
            START=true
            shift
            ;;
        --log)
            LOG_FILE=$(resolve_log_path "$2")
            shift 2
            ;;
        --help|-h)
            show_help
            ;;
        --)
            shift
            BUILD_EXTRA+=("$@")
            break
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

if [ -n "$LOG_FILE" ]; then
    setup_log_file "$LOG_FILE"
fi

# Resolve the platform now, so a bad value fails before anything is touched. Docker flags that would set
# the platform or the package index again are refused for the same reason (they are checked before the
# container is removed, and build_smart.sh would refuse them too).
PLATFORM="$(gbx_docker_platform "$PLATFORM_ARG")" || exit 2
gbx_check_docker_flags "${BUILD_EXTRA[@]}" || exit 2
IMAGE_TAG="$(gbx_dev_image_tag "$PLATFORM")"

print_banner "🐳 GeoBrix: Rebuild Docker Image"

# Require Docker daemon only (container may not exist yet)
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running or not accessible${NC}"
    exit 1
fi

# Stop and remove existing container so next start uses new image. Only a container running the
# architecture being rebuilt is replaced by this build; one of the other architecture is unaffected by
# it, and so is any container when --keep-container is given (swap later: gbx:docker:start --recreate).
print_separator
echo -e "${CYAN}🧹 Cleaning up existing container...${NC}"
print_separator

CONTAINER_KEPT=false
if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q '^geobrix-dev$'; then
    CONTAINER_ARCH="$(gbx_container_arch geobrix-dev)"
    if [ "$KEEP_CONTAINER" = true ]; then
        CONTAINER_KEPT=true
        echo -e "${YELLOW}   Keeping the existing geobrix-dev container (--keep-container).${NC}"
    elif [ "$CONTAINER_ARCH" != "$(gbx_platform_arch "$PLATFORM")" ]; then
        CONTAINER_KEPT=true
        echo -e "${YELLOW}   Keeping the existing geobrix-dev container (it runs ${CONTAINER_ARCH:-an unknown architecture}; this build is $(gbx_platform_arch "$PLATFORM")).${NC}"
    else
        if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^geobrix-dev$'; then
            echo -e "${CYAN}   Stopping running container...${NC}"
            docker stop geobrix-dev 2>/dev/null || true
        fi
        echo -e "${CYAN}   Removing container...${NC}"
        docker rm geobrix-dev 2>/dev/null || true
    fi
fi

# Rebuild using build_smart.sh
print_separator
if [ "$NO_CACHE" = true ]; then
    echo -e "${CYAN}🔨 Rebuilding image without cache (build_smart.sh)...${NC}"
    echo -e "${YELLOW}   (This may take several minutes)${NC}"
else
    echo -e "${CYAN}🔨 Rebuilding image (build_smart.sh)...${NC}"
fi
print_separator

cd "$PROJECT_ROOT/scripts/docker"

BUILD_ARGS=("--platform" "$PLATFORM")
[ "$PULL" = true ] && BUILD_ARGS+=("--pull")
[ "$PULL" = false ] && BUILD_ARGS+=("--no-pull")
[ "$NO_CACHE" = true ] && BUILD_ARGS+=("--no-cache")
# After `--`, so build_smart.sh passes them to docker and never parses one as its own option (a --platform
# there would otherwise override the platform decided above).
if [ ${#BUILD_EXTRA[@]} -gt 0 ]; then
    BUILD_ARGS+=("--" "${BUILD_EXTRA[@]}")
fi

bash build_smart.sh "${BUILD_ARGS[@]}"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    print_separator
    echo -e "${GREEN}✅ Image rebuilt successfully!${NC}"
    echo ""
    echo -e "${CYAN}📊 Image: ${YELLOW}${IMAGE_TAG}${NC} (${PLATFORM})"
    print_separator

    if [ "$START" = true ]; then
        echo ""
        echo -e "${CYAN}🚀 Starting new container...${NC}"
        bash "$SCRIPT_DIR/gbx-docker-start.sh" --platform "$PLATFORM" $([ "$ATTACH" = true ] && echo "--attach")
    elif [ "$CONTAINER_KEPT" = true ]; then
        echo ""
        echo -e "${CYAN}💡 The existing container was left running. To switch it to the new image at a quiet point:${NC}"
        echo -e "   ${YELLOW}bash scripts/commands/gbx-docker-start.sh --recreate --platform ${PLATFORM}${NC}"
        print_separator
    else
        echo ""
        echo -e "${CYAN}💡 Start container with: ${YELLOW}gbx:docker:start${NC}"
        print_separator
    fi
else
    echo ""
    print_separator
    echo -e "${RED}❌ Rebuild failed${NC}"
    print_separator
    exit $EXIT_CODE
fi
