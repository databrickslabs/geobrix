#!/bin/bash
# gbx:docs:dev - Start Docusaurus in development mode (hot reload / dynamic refresh)

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Default values
PORT=3000
STOP_FIRST=true
LOG_FILE=""

# Help message
show_help() {
    cat << EOF
$(print_banner "📚 GeoBrix: Docs Development (Hot Reload)")

Start Docusaurus with 'npm run start' for dynamic refresh when you edit files.

USAGE:
    bash .cursor/commands/gbx-docs-dev.sh [OPTIONS]

OPTIONS:
    --port <number>      Custom port (default: 3000)
    --no-stop-first      Do not stop existing server if port in use (default: stop first, then start)
    --log <path>         Write output to log file
    --help               Display this help message

EXAMPLES:
    # Start dev server (dynamic refresh)
    bash .cursor/commands/gbx-docs-dev.sh

    # Custom port (stop-first is default)
    bash .cursor/commands/gbx-docs-dev.sh --port 3001

NOTES:
    - Uses 'npm run start' (Docusaurus dev server), NOT 'npm run serve'
    - Edits to MDX, JS, CSS trigger automatic browser refresh
    - Stop with: gbx:docs:stop
    - For production-style static serve (no hot reload), use: gbx-docs-serve-local

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --port)
            PORT="$2"
            shift 2
            ;;
        --no-stop-first)
            STOP_FIRST=false
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
print_banner "📚 GeoBrix: Docs Development (Hot Reload)"

if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
    if [ "$STOP_FIRST" = true ]; then
        echo -e "${CYAN}Port $PORT in use; stopping existing server...${NC}"
        bash "$SCRIPT_DIR/gbx-docs-stop.sh"
        sleep 2
        if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
            echo -e "${RED}❌ Port $PORT still in use after stop. Free it manually: kill -9 \$(lsof -ti:$PORT)${NC}"
            exit 1
        fi
    else
        echo -e "${RED}❌ Port $PORT is already in use!${NC}"
        echo -e "${YELLOW}   Run: gbx:docs:stop   or   gbx:docs:dev (default: stop first)${NC}"
        exit 1
    fi
fi

cd "$PROJECT_ROOT/docs" || exit 1

print_separator
echo -e "${CYAN}🚀 Starting Docusaurus dev server (npm run start)...${NC}"
echo -e "${CYAN}   Port: ${YELLOW}$PORT${NC}"
echo -e "${CYAN}   Hot reload: ${GREEN}enabled${NC}"
print_separator

# Start dev server in background; same PID file pattern as serve so gbx-docs-stop works
nohup npm run start -- --port $PORT > /tmp/docusaurus-$PORT.log 2>&1 &
SERVER_PID=$!
echo $SERVER_PID > /tmp/docusaurus-$PORT.pid

sleep 3
if ps -p $SERVER_PID > /dev/null 2>&1; then
    echo ""
    echo -e "${GREEN}✅ Dev server started (dynamic refresh on file changes)${NC}"
    echo ""
    echo -e "${CYAN}📊 Server details:${NC}"
    echo -e "   PID:    ${YELLOW}$SERVER_PID${NC}"
    echo -e "   Port:   ${YELLOW}$PORT${NC}"
    echo -e "   URL:    ${YELLOW}http://localhost:$PORT${NC}"
    echo -e "   Logs:   ${YELLOW}/tmp/docusaurus-$PORT.log${NC}"
    echo ""
    echo -e "${CYAN}💡 Edit MDX/JS/CSS and the browser will refresh automatically.${NC}"
    echo -e "   Stop: ${YELLOW}gbx:docs:stop${NC}"
    print_separator
else
    echo -e "${RED}❌ Dev server failed to start. Check: /tmp/docusaurus-$PORT.log${NC}"
    exit 1
fi
