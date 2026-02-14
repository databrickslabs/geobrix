#!/bin/bash
# gbx:docs:restart - Restart Docusaurus documentation server

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Help message
show_help() {
    cat << EOF
$(print_banner "📚 GeoBrix: Restart Documentation Server")

Restart Docusaurus documentation server

USAGE:
    bash .cursor/commands/gbx-docs-restart.sh [OPTIONS]

OPTIONS:
    --skip-build         Skip npm build, serve existing build
    --port <number>      Custom port (default: 3000)
    --log <path>         Write output to log file
    --help               Display this help message

EXAMPLES:
    # Restart with rebuild
    bash .cursor/commands/gbx-docs-restart.sh

    # Restart without rebuild
    bash .cursor/commands/gbx-docs-restart.sh --skip-build

    # Restart on custom port
    bash .cursor/commands/gbx-docs-restart.sh --port 3001

NOTES:
    - Stops existing server (all ports)
    - Starts new server with specified options
    - Combines gbx:docs:stop + gbx:docs:start

EOF
    exit 0
}

# Parse arguments (just for help)
for arg in "$@"; do
    if [ "$arg" = "--help" ]; then
        show_help
    fi
done

# Print banner
print_banner "📚 GeoBrix: Restart Documentation Server"

# Stop existing server
echo -e "${CYAN}🛑 Stopping existing server...${NC}"
print_separator
bash "$SCRIPT_DIR/gbx-docs-stop.sh"

# Wait a moment
sleep 1

# Start new server
echo ""
echo -e "${CYAN}🚀 Starting new server...${NC}"
print_separator
bash "$SCRIPT_DIR/gbx-docs-start.sh" "$@"
