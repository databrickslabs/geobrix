#!/bin/bash
# gbx:docker:start - Start geobrix-dev container

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"
source "$PROJECT_ROOT/scripts/docker/gbx-platform.sh"

# Default values
ATTACH=false
LOG_FILE=""
PRIVILEGED=false
RECREATE=false
PLATFORM_ARG=""

# Help message
show_help() {
    cat << EOF
$(print_banner "🐳 GeoBrix: Start Docker Container")

Start geobrix-dev container with proper volume mounts

USAGE:
    bash scripts/commands/gbx-docker-start.sh [OPTIONS]

OPTIONS:
    --platform <p>       linux/arm64 or linux/amd64 (default: \$GBX_DOCKER_PLATFORM, else this host's
                         architecture): which per-architecture image a NEW container is created from
    --recreate           Stop and remove an existing container first, then create a new one from the
                         image of the requested platform (the way to switch architecture)
    --attach             Attach to container after start
    --privileged         Run container in privileged mode (only when creating new container)
    --log <path>         Write output to log file
    --help               Display this help message

EXAMPLES:
    # Start container
    bash scripts/commands/gbx-docker-start.sh

    # Start and attach
    bash scripts/commands/gbx-docker-start.sh --attach

    # Start with privileged mode (e.g. for kernel/ZMQ issues)
    bash scripts/commands/gbx-docker-start.sh --privileged

    # Switch an existing container to the native arm64 image (stops it; do it at a quiet point)
    bash scripts/commands/gbx-docker-start.sh --recreate --platform linux/arm64

    # Fall back to the amd64 image (runs under emulation on an arm64 host)
    GBX_DOCKER_PLATFORM=linux/amd64 bash scripts/commands/gbx-docker-start.sh --recreate

    # Start with logging
    bash scripts/commands/gbx-docker-start.sh --log docker-start.log

NOTES:
    - Uses scripts/docker/start_docker_with_volumes.sh
    - Image: geobrix-dev:ubuntu24-gdal311-spark-<arch>; the platform is --platform, else
      \$GBX_DOCKER_PLATFORM, else this host's architecture
    - An existing container is kept as is, even when it runs the other architecture (a warning says so);
      --recreate replaces it
    - Mounts sample-data/Volumes to /Volumes
    - Checks if container already running
    - Starts existing container if stopped

VOLUME MOUNTS:
    - sample-data/Volumes → /Volumes (Unity Catalog volumes)
    - Project root → /root/geobrix
    - scripts/docker/m2 → /root/geobrix/scripts/docker/m2 (Maven cache)

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --attach)
            ATTACH=true
            shift
            ;;
        --privileged)
            PRIVILEGED=true
            shift
            ;;
        --recreate)
            RECREATE=true
            shift
            ;;
        --platform)
            if [ $# -lt 2 ]; then
                echo "--platform needs a value (linux/arm64 or linux/amd64)"
                exit 2
            fi
            PLATFORM_ARG="$2"
            shift 2
            ;;
        --platform=*)
            PLATFORM_ARG="${1#--platform=}"
            shift
            ;;
        --log)
            LOG_FILE=$(resolve_log_path "$2")
            shift 2
            ;;
        --help)
            show_help
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Setup logging if requested
if [ -n "$LOG_FILE" ]; then
    setup_log "$LOG_FILE"
fi

# Resolve the platform now, so a bad value fails before anything is touched.
PLATFORM="$(gbx_docker_platform "$PLATFORM_ARG")" || exit 2

# Print banner
print_banner "🐳 GeoBrix: Start Docker Container"

# Require Docker daemon only (container may not exist yet after rebuild)
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running or not accessible${NC}"
    exit 1
fi

print_separator
echo -e "${CYAN}🔍 Checking container status...${NC}"
print_separator

# Worktree-aware mount-source check. If a geobrix-dev container exists,
# verify its /root/geobrix mount source matches the current worktree;
# otherwise recreate so the agent / user gets the right files in-container.
existing_mount_source() {
    docker inspect --format '{{ range .Mounts }}{{ if eq .Destination "/root/geobrix" }}{{ .Source }}{{ end }}{{ end }}' geobrix-dev 2>/dev/null
}

if [ "$RECREATE" = true ] && docker ps -a --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    echo ""
    echo -e "${CYAN}🔄 Recreating container (--recreate): stopping and removing the existing one...${NC}"
    docker stop geobrix-dev >/dev/null 2>&1 || true
    docker rm geobrix-dev >/dev/null 2>&1 || true
fi

if docker ps -a --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    EXISTING_MOUNT="$(existing_mount_source)"
    if [ -n "$EXISTING_MOUNT" ] && [ "$EXISTING_MOUNT" != "$PROJECT_ROOT" ]; then
        echo ""
        echo -e "${YELLOW}⚠️  Existing 'geobrix-dev' container is mounted to a different worktree:${NC}"
        echo -e "   existing: ${YELLOW}$EXISTING_MOUNT${NC}"
        echo -e "   current:  ${YELLOW}$PROJECT_ROOT${NC}"
        echo -e "${CYAN}🔄 Recreating container with current worktree mount...${NC}"
        docker stop geobrix-dev >/dev/null 2>&1 || true
        docker rm geobrix-dev >/dev/null 2>&1 || true
        # Fall through to the "create new container" branch below.
    fi
fi

# An existing container keeps the architecture it was created with. Say so when it is not the one asked
# for (never recreate silently: other runs may be using it).
warn_if_arch_differs() {
    local have want
    have="$(gbx_container_arch geobrix-dev)"
    want="$(gbx_platform_arch "$PLATFORM")"
    if [ -n "$have" ] && [ "$have" != "$want" ]; then
        echo -e "${YELLOW}⚠️  The existing 'geobrix-dev' container runs the ${have} image; ${want} was requested.${NC}"
        echo -e "   Keeping it. To switch (stops the container): ${YELLOW}bash scripts/commands/gbx-docker-start.sh --recreate --platform ${PLATFORM}${NC}"
    fi
}

# Check if container is already running (with the correct mount)
if docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    echo ""
    echo -e "${YELLOW}ℹ️  Container 'geobrix-dev' is already running${NC}"
    warn_if_arch_differs
    echo ""
    echo -e "${CYAN}⚙️  Applying Maven setup (.m2 in project, skipScoverage default)...${NC}"
    docker exec geobrix-dev /bin/bash -c "sh /root/geobrix/scripts/docker/extras/docker_maven_setup.sh"
    print_separator

    if [ "$ATTACH" = true ]; then
        echo ""
        echo -e "${CYAN}🔗 Attaching to container...${NC}"
        docker exec -it geobrix-dev bash
    fi

    exit 0
fi

# Check if container exists but is stopped (and mount matches; mismatch was handled above)
if docker ps -a --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    echo ""
    warn_if_arch_differs
    echo -e "${CYAN}🚀 Starting existing container...${NC}"
    print_separator

    docker start geobrix-dev
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✅ Container started successfully!${NC}"
        echo ""
        echo -e "${CYAN}⚙️  Applying Maven setup (.m2 in project, skipScoverage default)...${NC}"
        docker exec geobrix-dev /bin/bash -c "sh /root/geobrix/scripts/docker/extras/docker_maven_setup.sh"
        print_separator
        
        if [ "$ATTACH" = true ]; then
            echo ""
            echo -e "${CYAN}🔗 Attaching to container...${NC}"
            docker exec -it geobrix-dev bash
        fi
    else
        echo ""
        echo -e "${RED}❌ Failed to start container${NC}"
        print_separator
        exit $EXIT_CODE
    fi
else
    # Container doesn't exist, create it
    echo ""
    echo -e "${CYAN}🏗️  Creating new container with volume mounts...${NC}"
    [ "$PRIVILEGED" = true ] && echo -e "${YELLOW}   (privileged mode)${NC}"
    print_separator
    
    # Use the existing start script
    cd "$PROJECT_ROOT"
    if [ "$PRIVILEGED" = true ]; then
        bash scripts/docker/start_docker_with_volumes.sh --platform "$PLATFORM" --privileged
    else
        bash scripts/docker/start_docker_with_volumes.sh --platform "$PLATFORM"
    fi
    EXIT_CODE=$?
    
    if [ $EXIT_CODE -eq 0 ]; then
        echo ""
        echo -e "${GREEN}✅ Container created and started!${NC}"
        echo ""
        echo -e "${CYAN}⚙️  Applying Maven setup (.m2 in project, skipScoverage default)...${NC}"
        docker exec geobrix-dev /bin/bash -c "sh /root/geobrix/scripts/docker/extras/docker_maven_setup.sh"
        echo ""
        echo -e "${CYAN}📊 Container details:${NC}"
        echo -e "   Name:    ${YELLOW}geobrix-dev${NC} (${PLATFORM})"
        echo -e "   Status:  ${YELLOW}Running${NC}"
        echo ""
        echo -e "${CYAN}📁 Volume mounts:${NC}"
        echo -e "   sample-data/Volumes → /Volumes${NC}"
        echo -e "   . → /root/geobrix${NC}"
        echo -e "   scripts/docker/m2 → /root/geobrix/scripts/docker/m2${NC}"
        print_separator
        
        if [ "$ATTACH" = true ]; then
            echo ""
            echo -e "${CYAN}🔗 Attaching to container...${NC}"
            docker exec -it geobrix-dev bash
        fi
    else
        echo ""
        echo -e "${RED}❌ Failed to create container${NC}"
        print_separator
        exit $EXIT_CODE
    fi
fi
