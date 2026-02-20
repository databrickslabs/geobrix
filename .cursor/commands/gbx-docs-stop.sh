#!/bin/bash
# gbx:docs:stop - Stop Docusaurus documentation server

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Help message
show_help() {
    cat << EOF
$(print_banner "📚 GeoBrix: Stop Documentation Server")

Stop running Docusaurus documentation server

USAGE:
    bash .cursor/commands/gbx-docs-stop.sh [OPTIONS]

OPTIONS:
    --help               Display this help message

EXAMPLES:
    # Stop docs server
    bash .cursor/commands/gbx-docs-stop.sh

NOTES:
    - Stops servers on all ports (3000, 3001, etc.)
    - Cleans up PID files
    - Safe to run even if no server running

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
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
print_banner "📚 GeoBrix: Stop Documentation Server"

print_separator
echo -e "${CYAN}🛑 Stopping documentation server(s)...${NC}"
print_separator

STOPPED=false

# Stop by PID files
for pid_file in /tmp/docusaurus-*.pid; do
    if [ -f "$pid_file" ]; then
        PID=$(cat "$pid_file")
        PORT=$(echo "$pid_file" | sed 's/.*docusaurus-\([0-9]*\)\.pid/\1/')
        
        if ps -p $PID > /dev/null 2>&1; then
            echo -e "${CYAN}   Stopping server (PID: $PID, Port: $PORT)...${NC}"
            kill $PID 2>/dev/null
            
            # Wait for graceful shutdown
            sleep 1
            
            # Force kill if still running
            if ps -p $PID > /dev/null 2>&1; then
                kill -9 $PID 2>/dev/null
            fi
            
            STOPPED=true
        fi
        
        # Clean up PID file
        rm "$pid_file"
        
        # Clean up log file
        if [ -f "/tmp/docusaurus-$PORT.log" ]; then
            rm "/tmp/docusaurus-$PORT.log"
        fi
    fi
done

# Backup: kill by process name
PIDS=$(pgrep -f "npm.*serve.*docs/build" 2>/dev/null)
if [ -n "$PIDS" ]; then
    echo -e "${CYAN}   Stopping lingering processes...${NC}"
    pkill -f "npm.*serve.*docs/build"
    STOPPED=true
fi

# Kill any process listening on docs ports (port-based is reliable when PID files are missing)
for PORT in 3000 3001 3002 3003; do
    PIDS=$(lsof -ti:$PORT 2>/dev/null)
    if [ -n "$PIDS" ]; then
        echo -e "${CYAN}   Stopping process(es) on port $PORT (PID(s): $PIDS)...${NC}"
        for pid in $PIDS; do
            kill $pid 2>/dev/null
        done
        sleep 1
        for pid in $PIDS; do
            if ps -p $pid > /dev/null 2>&1; then
                kill -9 $pid 2>/dev/null
            fi
        done
        STOPPED=true
    fi
done

print_separator

if [ "$STOPPED" = true ]; then
    echo ""
    echo -e "${GREEN}✅ Documentation server stopped!${NC}"
    print_separator
else
    echo ""
    echo -e "${YELLOW}ℹ️  No running documentation server found${NC}"
    print_separator
fi
