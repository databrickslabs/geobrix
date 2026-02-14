#!/bin/bash
# gbx:docker:clear-pycache - Clear Python bytecode cache in geobrix-dev container

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

# Default values
LOG_FILE=""
VERBOSE=false

# Help message
show_help() {
    cat << EOF
$(print_banner "🧹 GeoBrix: Clear Python Cache")

Clear Python bytecode cache in geobrix-dev Docker container

USAGE:
    bash .cursor/commands/gbx-docker-clear-pycache.sh [OPTIONS]

OPTIONS:
    --log <path>     Write output to log file
    --verbose        Show detailed output of files being removed
    --help           Display this help message

WHAT IT CLEARS:
    - All .pyc files (compiled Python bytecode)
    - All __pycache__ directories
    - All .pytest_cache directories
    - Locations:
      • /root/geobrix/docs/tests/python/
      • /root/geobrix/python/geobrix/

WHEN TO USE:
    - After editing Python code (examples.py, conftest.py, etc.)
    - Before running tests to ensure fresh imports
    - When seeing "AttributeError: module has no attribute" errors
    - After modifying Python test files

EXAMPLES:
    # Basic usage
    bash .cursor/commands/gbx-docker-clear-pycache.sh

    # With detailed output
    bash .cursor/commands/gbx-docker-clear-pycache.sh --verbose

    # With logging
    bash .cursor/commands/gbx-docker-clear-pycache.sh --log cache-clear.log

NOTES:
    - Safe to run anytime (only removes cache files)
    - Automatic after this command, re-run tests for fresh imports
    - Required when Docker volumes show stale cached code

EOF
    exit 0
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --log)
            LOG_FILE=$(resolve_log_path "$2")
            shift 2
            ;;
        --verbose)
            VERBOSE=true
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

# Setup logging if requested
if [ -n "$LOG_FILE" ]; then
    setup_log "$LOG_FILE"
fi

# Print banner
print_banner "🧹 GeoBrix: Clear Python Cache"

# Check Docker
check_docker

# Check if container is running
if ! docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
    echo -e "${RED}❌ Container 'geobrix-dev' is not running${NC}"
    echo -e "${YELLOW}   Start with: gbx:docker:start${NC}"
    exit 1
fi

print_separator
echo -e "${CYAN}🧹 Clearing Python bytecode cache...${NC}"
print_separator

# Build the find commands
if [ "$VERBOSE" = true ]; then
    FIND_FLAGS="-print"
    echo -e "${YELLOW}ℹ️  Verbose mode: showing all files being removed${NC}"
    echo ""
else
    FIND_FLAGS="-delete"
fi

# Clear cache from docs/tests/python
echo -e "${CYAN}📂 Clearing cache in docs/tests/python/...${NC}"
if [ "$VERBOSE" = true ]; then
    docker exec geobrix-dev bash -c "cd /root/geobrix && find docs/tests/python -type f -name '*.pyc' $FIND_FLAGS"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find docs/tests/python -type d -name '__pycache__' $FIND_FLAGS"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find docs/tests/python -type d -name '.pytest_cache' $FIND_FLAGS"
    
    # Now delete them
    echo -e "${YELLOW}   Deleting files...${NC}"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find docs/tests/python -type f -name '*.pyc' -delete"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find docs/tests/python -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find docs/tests/python -type d -name '.pytest_cache' -exec rm -rf {} + 2>/dev/null || true"
else
    docker exec geobrix-dev bash -c "cd /root/geobrix && find docs/tests/python -type f -name '*.pyc' -delete 2>/dev/null || true"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find docs/tests/python -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find docs/tests/python -type d -name '.pytest_cache' -exec rm -rf {} + 2>/dev/null || true"
fi

# Clear cache from python/geobrix
echo -e "${CYAN}📂 Clearing cache in python/geobrix/...${NC}"
if [ "$VERBOSE" = true ]; then
    docker exec geobrix-dev bash -c "cd /root/geobrix && find python/geobrix -type f -name '*.pyc' $FIND_FLAGS"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find python/geobrix -type d -name '__pycache__' $FIND_FLAGS"
    
    # Now delete them
    echo -e "${YELLOW}   Deleting files...${NC}"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find python/geobrix -type f -name '*.pyc' -delete"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find python/geobrix -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true"
else
    docker exec geobrix-dev bash -c "cd /root/geobrix && find python/geobrix -type f -name '*.pyc' -delete 2>/dev/null || true"
    docker exec geobrix-dev bash -c "cd /root/geobrix && find python/geobrix -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true"
fi

print_separator
echo -e "${GREEN}✅ Python bytecode cache cleared${NC}"
echo -e "${YELLOW}ℹ️  Next: Re-run your tests to ensure fresh imports${NC}"
print_separator

exit 0
