#!/bin/bash
# gbx:docker:stop - Stop geobrix-dev container

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Default values
FORCE=false
TIMEOUT=10

# Help message
show_help() {
    cat << EOF
$(print_banner "🐳 GeoBrix: Stop Docker Container")

Stop geobrix-dev container

USAGE:
    bash .cursor/commands/gbx-docker-stop.sh [OPTIONS]

OPTIONS:
    --force              Force stop (kill immediately)
    --timeout <seconds>  Timeout before force stop (default: 10)
    --help               Display this help message

EXAMPLES:
    # Stop container gracefully
    bash .cursor/commands/gbx-docker-stop.sh

    # Force stop immediately
    bash .cursor/commands/gbx-docker-stop.sh --force

    # Stop with custom timeout
    bash .cursor/commands/gbx-docker-stop.sh --timeout 30

NOTES:
    - Default timeout is 10 seconds
    - Force stop uses docker kill
    - Safe to run even if container not running

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --force)
            FORCE=true
            shift
            ;;
        --timeout)
            TIMEOUT="$2"
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

# Print banner
print_banner "🐳 GeoBrix: Stop Docker Container"

# Check Docker
check_docker

print_separator
echo -e "${CYAN}🔍 Checking container status...${NC}"
print_separator

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    echo ""
    echo -e "${YELLOW}ℹ️  Container 'geobrix-dev' is not running${NC}"
    print_separator
    exit 0
fi

# Stop the container
echo ""
if [ "$FORCE" = true ]; then
    echo -e "${CYAN}🛑 Force stopping container...${NC}"
    docker kill geobrix-dev
else
    echo -e "${CYAN}🛑 Stopping container (timeout: ${TIMEOUT}s)...${NC}"
    docker stop -t $TIMEOUT geobrix-dev
fi

EXIT_CODE=$?

print_separator

if [ $EXIT_CODE -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ Container stopped successfully${NC}"
    print_separator
else
    echo ""
    echo -e "${RED}❌ Failed to stop container${NC}"
    print_separator
    exit $EXIT_CODE
fi
