#!/bin/bash
# Common helper functions for GeoBrix Cursor commands

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

check_docker() {
    if ! docker ps &> /dev/null; then
        echo -e "${RED}❌ Error: Docker is not running${NC}"
        echo "Start Docker and try again."
        exit 1
    fi
    
    if ! docker ps -a --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
        echo -e "${RED}❌ Error: geobrix-dev container not found${NC}"
        echo "Start the development container first:"
        echo -e "  ${YELLOW}./scripts/docker/start_docker.sh${NC}"
        exit 1
    fi
    
    if ! docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
        echo -e "${YELLOW}⚠️  Container is not running. Starting...${NC}"
        docker start geobrix-dev
        sleep 2
    fi
}

resolve_log_path() {
    local log_arg="$1"
    
    if [ -z "$log_arg" ]; then
        echo ""
        return
    fi
    
    # Check if absolute path (starts with /)
    if [[ "$log_arg" == /* ]]; then
        echo "$log_arg"
        return
    fi
    
    # Check if it's just a filename (no directory separator)
    if [[ "$log_arg" != */* ]]; then
        echo "test-logs/$log_arg"
        return
    fi
    
    # It's a relative path - prepend test-logs/
    echo "test-logs/$log_arg"
}

# Central logging: truncate log on each run so every command gets a fresh file.
# Commands that use --log should call this (or setup_log); the only exception is
# scripts that tee a subprocess only—those must truncate explicitly (: > "$LOG_PATH").
setup_log_file() {
    local log_path="$1"
    
    if [ -n "$log_path" ]; then
        mkdir -p "$(dirname "$log_path")"
        : > "$log_path"
        echo -e "${CYAN}📝 Logging to: ${YELLOW}$log_path${NC}"
        exec > >(tee -a "$log_path") 2>&1
    fi
}

show_banner() {
    local title="$1"
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}  ${CYAN}$title${NC}"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════╝${NC}"
    echo ""
}

show_separator() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

open_report() {
    local report_path="$1"
    
    if [ ! -f "$report_path" ]; then
        echo -e "${YELLOW}⚠️  Report file not found: $report_path${NC}"
        return 1
    fi
    
    echo -e "${CYAN}📊 Opening report: ${YELLOW}$report_path${NC}"
    
    # Detect OS and open accordingly
    if [[ "$OSTYPE" == "darwin"* ]]; then
        open "$report_path"
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        xdg-open "$report_path" &>/dev/null || echo -e "${YELLOW}⚠️  Could not open browser. Open manually: $report_path${NC}"
    else
        echo -e "${YELLOW}⚠️  Unsupported OS. Open manually: $report_path${NC}"
    fi
}

generate_timestamp() {
    date +%Y%m%d-%H%M%S
}

# Aliases for backward compatibility
print_banner() { show_banner "$@"; }
print_separator() { show_separator "$@"; }
setup_log() { setup_log_file "$@"; }
