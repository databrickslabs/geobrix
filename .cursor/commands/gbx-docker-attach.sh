#!/bin/bash
# gbx:docker:attach - Attach to geobrix-dev container

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Default values
USER="root"

# Help message
show_help() {
    cat << EOF
$(print_banner "🐳 GeoBrix: Attach to Docker Container")

Attach to running geobrix-dev container with interactive bash shell

USAGE:
    bash .cursor/commands/gbx-docker-attach.sh [OPTIONS]

OPTIONS:
    --user <username>    Attach as specific user (default: root)
    --help               Display this help message

EXAMPLES:
    # Attach as root
    bash .cursor/commands/gbx-docker-attach.sh

    # Attach as specific user
    bash .cursor/commands/gbx-docker-attach.sh --user spark

NOTES:
    - Requires container to be running
    - Opens interactive bash shell
    - Exit with Ctrl+D or 'exit'
    - Use gbx:docker:start if container not running

SHORTCUTS:
    - Ctrl+D or 'exit' to detach
    - Container continues running after detach

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --user)
            USER="$2"
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

# Check Docker
check_docker

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    echo -e "${RED}❌ Container 'geobrix-dev' is not running${NC}"
    echo -e "${YELLOW}   Start with: gbx:docker:start${NC}"
    exit 1
fi

# Print info (before attaching to keep terminal clean)
echo -e "${CYAN}🔗 Attaching to geobrix-dev container...${NC}"
echo -e "${YELLOW}   (Exit with Ctrl+D or type 'exit')${NC}"
echo ""

# Attach to container
docker exec -it -u $USER geobrix-dev bash
