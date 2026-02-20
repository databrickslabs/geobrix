#!/bin/bash
# gbx:docker:rebuild - Rebuild geobrix-dev Docker image via scripts/docker/build_smart.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

# Defaults
NO_CACHE=false
PULL=true
START=false
ATTACH=false
LOG_FILE=""

show_help() {
    cat << EOF
$(print_banner "🐳 GeoBrix: Rebuild Docker Image")

Rebuild geobrix-dev Docker image using scripts/docker/build_smart.sh (multi-stage build).
Optionally start the container after rebuild.

USAGE:
    bash .cursor/commands/gbx-docker-rebuild.sh [OPTIONS] [-- DOCKER_FLAGS]

OPTIONS:
    --no-cache           Build without Docker cache (passed to build_smart.sh)
    --no-pull            Do not force-pull latest ubuntu:24.04 (faster, may miss base updates)
    --start              Start container after rebuild
    --attach             Attach to container after start (implies --start)
    --log <path>         Write output to log file (filename → test-logs/<name>)
    --help               Display this help message

DOCKER FLAGS (after --):
    Arguments after -- are passed to docker build. Examples:
    --progress=plain     Show full build output
    --build-arg CORES=4  Override CPU cores used in build (default=2)

EXAMPLES:
    # Rebuild image (multi-stage, with pull)
    bash .cursor/commands/gbx-docker-rebuild.sh

    # Rebuild without cache
    bash .cursor/commands/gbx-docker-rebuild.sh --no-cache

    # Rebuild without pulling base image (faster)
    bash .cursor/commands/gbx-docker-rebuild.sh --no-pull

    # Rebuild and start container
    bash .cursor/commands/gbx-docker-rebuild.sh --start

    # Rebuild, start, and attach
    bash .cursor/commands/gbx-docker-rebuild.sh --start --attach

    # Rebuild with detailed build output
    bash .cursor/commands/gbx-docker-rebuild.sh -- --progress=plain

NOTES:
    - Uses scripts/docker/build_smart.sh (multi-stage; final image geobrix-dev:ubuntu24-gdal311-spark)
    - Stops and removes existing container before rebuild so next start uses new image
    - Start script uses same image tag (start_docker_with_volumes.sh)

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

print_banner "🐳 GeoBrix: Rebuild Docker Image"

# Require Docker daemon only (container may not exist yet)
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running or not accessible${NC}"
    exit 1
fi

# Stop and remove existing container so next start uses new image
print_separator
echo -e "${CYAN}🧹 Cleaning up existing container...${NC}"
print_separator

if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^geobrix-dev$'; then
    echo -e "${CYAN}   Stopping running container...${NC}"
    docker stop geobrix-dev 2>/dev/null || true
fi
if docker ps -a --format '{{.Names}}' 2>/dev/null | grep -q '^geobrix-dev$'; then
    echo -e "${CYAN}   Removing container...${NC}"
    docker rm geobrix-dev 2>/dev/null || true
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

BUILD_ARGS=()
[ "$PULL" = true ] && BUILD_ARGS+=("--pull")
[ "$PULL" = false ] && BUILD_ARGS+=("--no-pull")
[ "$NO_CACHE" = true ] && BUILD_ARGS+=("--no-cache")
BUILD_ARGS+=("${BUILD_EXTRA[@]}")

bash build_smart.sh "${BUILD_ARGS[@]}"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    print_separator
    echo -e "${GREEN}✅ Image rebuilt successfully!${NC}"
    echo ""
    echo -e "${CYAN}📊 Image: ${YELLOW}geobrix-dev:ubuntu24-gdal311-spark${NC}"
    print_separator

    if [ "$START" = true ]; then
        echo ""
        echo -e "${CYAN}🚀 Starting new container...${NC}"
        bash "$SCRIPT_DIR/gbx-docker-start.sh" $([ "$ATTACH" = true ] && echo "--attach")
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
