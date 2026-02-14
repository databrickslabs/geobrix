#!/bin/bash
# gbx:docker:start - Start geobrix-dev container

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Default values
ATTACH=false
LOG_FILE=""
PRIVILEGED=false

# Help message
show_help() {
    cat << EOF
$(print_banner "🐳 GeoBrix: Start Docker Container")

Start geobrix-dev container with proper volume mounts

USAGE:
    bash .cursor/commands/gbx-docker-start.sh [OPTIONS]

OPTIONS:
    --attach             Attach to container after start
    --privileged         Run container in privileged mode (only when creating new container)
    --log <path>         Write output to log file
    --help               Display this help message

EXAMPLES:
    # Start container
    bash .cursor/commands/gbx-docker-start.sh

    # Start and attach
    bash .cursor/commands/gbx-docker-start.sh --attach

    # Start with privileged mode (e.g. for kernel/ZMQ issues)
    bash .cursor/commands/gbx-docker-start.sh --privileged

    # Start with logging
    bash .cursor/commands/gbx-docker-start.sh --log docker-start.log

NOTES:
    - Uses scripts/docker/start_docker_with_volumes.sh
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
print_banner "🐳 GeoBrix: Start Docker Container"

# Require Docker daemon only (container may not exist yet after rebuild)
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}❌ Docker is not running or not accessible${NC}"
    exit 1
fi

print_separator
echo -e "${CYAN}🔍 Checking container status...${NC}"
print_separator

# Check if container is already running
if docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    echo ""
    echo -e "${YELLOW}ℹ️  Container 'geobrix-dev' is already running${NC}"
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

# Check if container exists but is stopped
if docker ps -a --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    echo ""
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
        bash scripts/docker/start_docker_with_volumes.sh --privileged
    else
        bash scripts/docker/start_docker_with_volumes.sh
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
        echo -e "   Name:    ${YELLOW}geobrix-dev${NC}"
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
