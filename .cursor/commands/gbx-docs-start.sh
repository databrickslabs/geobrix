#!/bin/bash
# gbx:docs:start - Start Docusaurus documentation server

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Default values
SKIP_BUILD=false
PORT=3000
LOG_FILE=""

# Help message
show_help() {
    cat << EOF
$(print_banner "📚 GeoBrix: Start Documentation Server")

Start Docusaurus documentation server with live rebuild

USAGE:
    bash .cursor/commands/gbx-docs-start.sh [OPTIONS]

OPTIONS:
    --skip-build         Skip npm build, serve existing build
    --port <number>      Custom port (default: 3000)
    --log <path>         Write output to log file
    --help               Display this help message

EXAMPLES:
    # Build and serve docs
    bash .cursor/commands/gbx-docs-start.sh

    # Serve without rebuild
    bash .cursor/commands/gbx-docs-start.sh --skip-build

    # Use custom port
    bash .cursor/commands/gbx-docs-start.sh --port 3001

    # Build and log output
    bash .cursor/commands/gbx-docs-start.sh --log docs-server.log

NOTES:
    - Default port: 3000
    - Checks if server already running
    - Stores PID in /tmp/docusaurus.pid
    - Access at: http://localhost:<port>
    - Stop with: gbx:docs:stop

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --skip-build)
            SKIP_BUILD=true
            shift
            ;;
        --port)
            PORT="$2"
            shift 2
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
print_banner "📚 GeoBrix: Start Documentation Server"

# Check if server already running
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    echo -e "${RED}❌ Port $PORT is already in use!${NC}"
    echo -e "${YELLOW}   Stop the existing server with: gbx:docs:stop${NC}"
    exit 1
fi

# Change to docs directory
cd "$PROJECT_ROOT/docs" || exit 1

print_separator
if [ "$SKIP_BUILD" = true ]; then
    echo -e "${CYAN}⏭️  Skipping build...${NC}"
else
    echo -e "${CYAN}🔨 Building documentation...${NC}"
    print_separator
    
    npm run build
    BUILD_EXIT=$?
    
    if [ $BUILD_EXIT -ne 0 ]; then
        echo ""
        echo -e "${RED}❌ Build failed!${NC}"
        exit $BUILD_EXIT
    fi
fi

print_separator
echo -e "${CYAN}🚀 Starting documentation server...${NC}"
echo -e "${CYAN}   Port: ${YELLOW}$PORT${NC}"
print_separator

# Start the server in the background
nohup npm run serve -- --port $PORT > /tmp/docusaurus-$PORT.log 2>&1 &
SERVER_PID=$!

# Store PID for stop command
echo $SERVER_PID > /tmp/docusaurus-$PORT.pid

# Wait a moment and check if it started
sleep 2

if ps -p $SERVER_PID > /dev/null; then
    echo ""
    echo -e "${GREEN}✅ Documentation server started!${NC}"
    echo ""
    echo -e "${CYAN}📊 Server details:${NC}"
    echo -e "   PID:    ${YELLOW}$SERVER_PID${NC}"
    echo -e "   Port:   ${YELLOW}$PORT${NC}"
    echo -e "   URL:    ${YELLOW}http://localhost:$PORT${NC}"
    echo -e "   Logs:   ${YELLOW}/tmp/docusaurus-$PORT.log${NC}"
    echo ""
    echo -e "${CYAN}💡 Commands:${NC}"
    echo -e "   Stop:    ${YELLOW}gbx:docs:stop${NC}"
    echo -e "   Restart: ${YELLOW}gbx:docs:restart${NC}"
    echo -e "   Logs:    ${YELLOW}tail -f /tmp/docusaurus-$PORT.log${NC}"
    print_separator
else
    echo ""
    echo -e "${RED}❌ Server failed to start!${NC}"
    echo -e "${YELLOW}   Check logs: /tmp/docusaurus-$PORT.log${NC}"
    exit 1
fi
