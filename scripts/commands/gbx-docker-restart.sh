#!/bin/bash
# gbx:docker:restart - Restart geobrix-dev container

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"
source "$PROJECT_ROOT/scripts/docker/gbx-platform.sh"

# Default values
TIMEOUT=10
ATTACH=false
LOG_FILE=""
PRIVILEGED=false

# Help message
show_help() {
    cat << EOF
$(print_banner "🐳 GeoBrix: Restart Docker Container")

Restart geobrix-dev container

USAGE:
    bash scripts/commands/gbx-docker-restart.sh [OPTIONS]

OPTIONS:
    --timeout <seconds>  Timeout before force stop (default: 10)
    --attach             Attach to container after restart
    --privileged         Recreate container with privileged mode (stop, rm, create), on the same
                         architecture as the container it replaces
    --log <path>         Write output to log file
    --help               Display this help message

EXAMPLES:
    # Restart container
    bash scripts/commands/gbx-docker-restart.sh

    # Restart with custom timeout
    bash scripts/commands/gbx-docker-restart.sh --timeout 30

    # Restart and attach
    bash scripts/commands/gbx-docker-restart.sh --attach

    # Recreate with privileged mode
    bash scripts/commands/gbx-docker-restart.sh --privileged

NOTES:
    - Default timeout is 10 seconds
    - --privileged stops and removes the container, then creates a new one with --privileged
    - Creates container if it doesn't exist

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --timeout)
            TIMEOUT="$2"
            shift 2
            ;;
        --attach)
            ATTACH=true
            shift
            ;;
        --privileged)
            PRIVILEGED=true
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

# Print banner
print_banner "🐳 GeoBrix: Restart Docker Container"

# Check Docker
check_docker

print_separator

# Check if container exists
if ! docker ps -a --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    echo -e "${YELLOW}ℹ️  Container doesn't exist, creating new one...${NC}"
    print_separator
    
    # Create and start new container (pass through --privileged and --attach)
    bash "$SCRIPT_DIR/gbx-docker-start.sh" \
        $([ "$ATTACH" = true ] && echo "--attach") \
        $([ "$PRIVILEGED" = true ] && echo "--privileged")
    exit $?
fi

# If --privileged requested, recreate container instead of restarting
if [ "$PRIVILEGED" = true ]; then
    # Recreate on the architecture of the container being replaced, read from its image (never the host
    # default or GBX_DOCKER_PLATFORM: a restart must not silently switch architecture; use
    # `gbx:docker:start --recreate --platform <p>` for that). The image must exist BEFORE the working
    # container is stopped and removed.
    CONTAINER_ARCH="$(gbx_container_arch geobrix-dev)"
    if [ -z "$CONTAINER_ARCH" ]; then
        echo -e "${RED}❌ Cannot tell which architecture the existing geobrix-dev container runs; it was left untouched.${NC}"
        echo -e "   To replace it on a chosen architecture: ${YELLOW}bash scripts/commands/gbx-docker-start.sh --recreate --platform linux/arm64|linux/amd64${NC}"
        exit 1
    fi
    RESTART_PLATFORM="linux/$CONTAINER_ARCH"
    echo -e "${CYAN}   Keeping the container's architecture: ${RESTART_PLATFORM}${NC}"
    if ! gbx_require_dev_image "$RESTART_PLATFORM" >/dev/null; then
        echo -e "${RED}❌ Not recreating: the existing geobrix-dev container was left untouched.${NC}"
        exit 1
    fi
    echo -e "${CYAN}🔄 Recreating container with privileged mode...${NC}"
    print_separator
    docker stop -t $TIMEOUT geobrix-dev 2>/dev/null || true
    docker rm geobrix-dev 2>/dev/null || true
    bash "$SCRIPT_DIR/gbx-docker-start.sh" --privileged --platform "$RESTART_PLATFORM" \
        $([ "$ATTACH" = true ] && echo "--attach")
    exit $?
fi

# Restart the container
echo -e "${CYAN}🔄 Restarting container (timeout: ${TIMEOUT}s)...${NC}"
print_separator

docker restart -t $TIMEOUT geobrix-dev
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ Container restarted successfully${NC}"
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
    echo -e "${RED}❌ Failed to restart container${NC}"
    print_separator
    exit $EXIT_CODE
fi
