#!/bin/bash
# gbx:docs:stop - Stop Docusaurus documentation server

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd -P)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Default values
PORT=""
FORCE=false

# Help message
show_help() {
    cat << EOF
$(print_banner "📚 GeoBrix: Stop Documentation Server")

Stop Docusaurus documentation server(s)

USAGE:
    bash scripts/commands/gbx-docs-stop.sh [OPTIONS]

OPTIONS:
    --port <number>      Stop only the server listening on this port
    --force              With --port, also stop a server started from another checkout
    --help               Display this help message

EXAMPLES:
    # Stop the server on one port
    bash scripts/commands/gbx-docs-stop.sh --port 3001

    # Stop every docs server on this machine
    bash scripts/commands/gbx-docs-stop.sh

NOTES:
    - Without --port, stops servers on all docs ports (3000-3003) and prints each PID
    - With --port, refuses a server whose checkout is not this one unless --force
    - Cleans up PID files
    - Safe to run even if no server running

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --port)
            if [[ $# -lt 2 || ! "$2" =~ ^[0-9]+$ ]]; then
                echo "--port needs a port number (got: ${2:-nothing})" >&2
                (show_help) >&2
                exit 2
            fi
            PORT="$2"
            shift 2
            ;;
        --force)
            FORCE=true
            shift
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

# SIGTERM, then SIGKILL after a grace period, for the given PIDs.
terminate_pids() {
    local pid i alive
    for pid in "$@"; do kill "$pid" 2>/dev/null; done
    for i in 1 2 3 4 5 6; do
        sleep 0.5
        alive=false
        for pid in "$@"; do ps -p "$pid" > /dev/null 2>&1 && alive=true; done
        [ "$alive" = true ] || return 0
    done
    for pid in "$@"; do
        ps -p "$pid" > /dev/null 2>&1 && kill -9 "$pid" 2>/dev/null
    done
    return 0
}

# PIDs of this process and its ancestors (never stopped).
own_ancestry() {
    local pid=$$
    while [ -n "$pid" ] && [ "$pid" -gt 1 ]; do
        echo "$pid"
        pid=$(ps -o ppid= -p "$pid" 2>/dev/null | tr -d ' ')
    done
}

# Stop the docs server on one port: its listener PID and the npm/node/sh parents that launched it.
# Not the process group: a server started by gbx:docs:dev shares its group with the caller.
stop_one_port() {
    local port="$1" pid parent cmd owner mine pids
    local targets=()
    pids=$(gbx_docs_port_pids "$port")
    if [ -z "$pids" ]; then
        echo -e "${YELLOW}ℹ️  Nothing is listening on port $port${NC}"
        rm -f "/tmp/docusaurus-$port.pid" "/tmp/docusaurus-$port.log"
        return 0
    fi

    if [ "$FORCE" != true ] && ! gbx_docs_port_is_mine "$port" "$PROJECT_ROOT"; then
        owner=$(gbx_docs_pid_checkout "$(echo "$pids" | head -1)")
        echo -e "${RED}❌ Port $port is held by a docs server from ${owner:-an unknown checkout}, not this one ($PROJECT_ROOT).${NC}"
        echo -e "${YELLOW}   Refusing to stop it. To stop it anyway:${NC}"
        echo -e "${YELLOW}     bash $SCRIPT_DIR/gbx-docs-stop.sh --port $port --force${NC}"
        if [ -n "$owner" ]; then
            echo -e "${YELLOW}   Or use that checkout's own copy:${NC}"
            echo -e "${YELLOW}     bash $owner/scripts/commands/gbx-docs-stop.sh --port $port${NC}"
        fi
        return 3
    fi

    mine=$(own_ancestry)
    for pid in $pids; do
        targets+=("$pid")
        parent=$pid
        while :; do
            parent=$(ps -o ppid= -p "$parent" 2>/dev/null | tr -d ' ')
            [ -n "$parent" ] && [ "$parent" -gt 1 ] || break
            echo "$mine" | grep -qx "$parent" && break
            cmd=$(ps -o command= -p "$parent" 2>/dev/null)
            [[ "$(basename "${cmd%% *}")" =~ ^(npm|node|sh)$ ]] || break
            [[ "$cmd" =~ (npm|docusaurus|node) ]] || break
            targets+=("$parent")
        done
    done
    echo -e "${CYAN}   Stopping server on port $port (PID(s): ${targets[*]})...${NC}"
    terminate_pids "${targets[@]}"
    rm -f "/tmp/docusaurus-$port.pid" "/tmp/docusaurus-$port.log"
    return 0
}

# Print banner
print_banner "📚 GeoBrix: Stop Documentation Server"

print_separator

if [ -n "$PORT" ]; then
    echo -e "${CYAN}🛑 Stopping the documentation server on port $PORT...${NC}"
    print_separator
    stop_one_port "$PORT"
    RC=$?
    print_separator
    if [ $RC -eq 0 ]; then
        echo -e "${GREEN}✅ Port $PORT is free${NC}"
    fi
    exit $RC
fi

echo -e "${CYAN}🛑 Stopping ALL documentation servers on this machine...${NC}"
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
    echo -e "${CYAN}   Stopping lingering processes (PID(s): $(echo $PIDS))...${NC}"
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
    echo -e "${GREEN}✅ Documentation server(s) stopped!${NC}"
    print_separator
else
    echo ""
    echo -e "${YELLOW}ℹ️  No running documentation server found${NC}"
    print_separator
fi
