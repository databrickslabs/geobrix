#!/bin/bash
# gbx:docs:restart - Restart Docusaurus documentation server

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Help message
show_help() {
    cat << EOF
$(print_banner "📚 GeoBrix: Restart Documentation Server")

Restart Docusaurus documentation server

USAGE:
    bash scripts/commands/gbx-docs-restart.sh [OPTIONS]

OPTIONS:
    --skip-build         Skip npm build, serve existing build
    --port <number>      Custom port (default: 3000)
    --force              Replace a server on the port even if another checkout started it
    --log <path>         Write output to log file
    --help               Display this help message

EXAMPLES:
    # Restart with rebuild
    bash scripts/commands/gbx-docs-restart.sh

    # Restart without rebuild
    bash scripts/commands/gbx-docs-restart.sh --skip-build

    # Restart on custom port
    bash scripts/commands/gbx-docs-restart.sh --port 3001

NOTES:
    - Stops only the server on --port, never other ports
    - Refuses a server started from another checkout unless --force
    - Starts new server with specified options
    - Combines gbx:docs:stop + gbx:docs:start

EOF
    exit 0
}

# Parse arguments: --port and --force are ours; everything else goes to gbx-docs-start.sh
PORT=3000
FORCE_ARGS=()
START_ARGS=()
while [[ $# -gt 0 ]]; do
    case $1 in
        --help)
            show_help
            ;;
        --port)
            PORT="$2"
            START_ARGS+=("$1" "$2")
            shift 2
            ;;
        --force)
            FORCE_ARGS=(--force)
            shift
            ;;
        *)
            START_ARGS+=("$1")
            shift
            ;;
    esac
done

# Print banner
print_banner "📚 GeoBrix: Restart Documentation Server"

# Stop existing server
echo -e "${CYAN}🛑 Stopping existing server...${NC}"
print_separator
bash "$SCRIPT_DIR/gbx-docs-stop.sh" --port "$PORT" "${FORCE_ARGS[@]}" || exit $?

# Wait a moment
sleep 1

# Start new server
echo ""
echo -e "${CYAN}🚀 Starting new server...${NC}"
print_separator
bash "$SCRIPT_DIR/gbx-docs-start.sh" "${START_ARGS[@]}"
