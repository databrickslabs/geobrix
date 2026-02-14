#!/bin/bash

# ============================================================================
# GeoBrix: Python Databricks Runtime (DBR) Integration Tests
# ============================================================================
# 
# Runs Databricks-specific integration tests that require DBR SQL functions.
# These tests are ISOLATED from regular unit tests and excluded from coverage.
#
# Requirements:
# - Databricks Runtime environment (st_* SQL functions, spatial types)
# - GeoBrix JAR with all dependencies
# - Sample data mounted at /Volumes/
#
# Note: These tests are NOT run in CI/CD pipelines automatically.
# ============================================================================

set -e  # Exit on error

# Source common utilities
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/common.sh"

# Default values
TEST_PATH=""
VERBOSE=false
LOG_FILE=""
MARKERS=""

# ============================================================================
# Help Message
# ============================================================================

show_help() {
    cat << EOF
${BLUE}╔═══════════════════════════════════════════════════════╗${NC}
${BLUE}║${NC}  ${CYAN}📚 GeoBrix: Python DBR Integration Tests${NC}
${BLUE}╚═══════════════════════════════════════════════════════╝${NC}

${BOLD}USAGE:${NC}
    gbx:test:python-dbr [OPTIONS]

${BOLD}OPTIONS:${NC}
    --path PATH          Run specific test path (file or directory)
    --markers MARKERS    Run tests matching pytest markers
    --verbose, -v        Show verbose output
    --log FILE           Save output to log file
    --help, -h           Show this help message

${BOLD}EXAMPLES:${NC}
    ${CYAN}# Run all DBR tests${NC}
    gbx:test:python-dbr

    ${CYAN}# Run specific test file${NC}
    gbx:test:python-dbr --path readers/test_dbr_examples.py

    ${CYAN}# Run tests with specific marker${NC}
    gbx:test:python-dbr --markers databricks

    ${CYAN}# Save output to log${NC}
    gbx:test:python-dbr --log dbr-tests.log

${BOLD}IMPORTANT:${NC}
    ${YELLOW}⚠️  These tests require Databricks Runtime environment${NC}
    ${YELLOW}⚠️  They are EXCLUDED from regular unit test runs${NC}
    ${YELLOW}⚠️  They are EXCLUDED from coverage reporting${NC}
    ${YELLOW}⚠️  They are EXCLUDED from CI/CD pipelines${NC}

${BOLD}MARKERS:${NC}
    databricks           Tests requiring DBR SQL functions
    spatial_sql          Tests using DBR spatial SQL functions
    integration          Full integration tests

${BOLD}LOCATION:${NC}
    Tests: docs/tests-dbr/python/
    Config: docs/tests-dbr/pytest.ini

EOF
}

# ============================================================================
# Parse Arguments
# ============================================================================

while [[ $# -gt 0 ]]; do
    case $1 in
        --path)
            TEST_PATH="$2"
            shift 2
            ;;
        --markers)
            MARKERS="$2"
            shift 2
            ;;
        --verbose|-v)
            VERBOSE=true
            shift
            ;;
        --log)
            LOG_FILE="$2"
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# ============================================================================
# Main Execution
# ============================================================================

print_header "📚 GeoBrix: Python DBR Integration Tests"

# Validate environment
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed or not in PATH"
    exit 1
fi

if ! docker ps | grep -q geobrix-dev; then
    print_error "geobrix-dev container is not running"
    echo -e "${YELLOW}ℹ️  Start it with: gbx:docker:start${NC}"
    exit 1
fi

# Build pytest command
PYTEST_CMD="python3 -m pytest /root/geobrix/docs/tests-dbr/python/"

# Add test path if specified
if [ -n "$TEST_PATH" ]; then
    PYTEST_CMD="${PYTEST_CMD}${TEST_PATH}"
    echo -e "${CYAN}📂 Running tests from: ${TEST_PATH}${NC}"
fi

# Add markers if specified
if [ -n "$MARKERS" ]; then
    PYTEST_CMD="${PYTEST_CMD} -m ${MARKERS}"
    echo -e "${CYAN}🏷️  Using markers: ${MARKERS}${NC}"
fi

# Add verbose flag
if [ "$VERBOSE" = true ]; then
    PYTEST_CMD="${PYTEST_CMD} -vv"
fi

# Add standard options
PYTEST_CMD="${PYTEST_CMD} --tb=short"

echo ""
print_section "Running DBR Integration Tests"

# Execute tests
if [ -n "$LOG_FILE" ]; then
    LOG_PATH=$(resolve_log_path "$LOG_FILE")
    echo -e "${CYAN}📝 Logging to: ${LOG_PATH}${NC}"
    docker exec geobrix-dev bash -c "$PYTEST_CMD" 2>&1 | tee "$LOG_PATH"
    EXIT_CODE=${PIPESTATUS[0]}
else
    docker exec geobrix-dev bash -c "$PYTEST_CMD"
    EXIT_CODE=$?
fi

# Report results
print_divider

if [ $EXIT_CODE -eq 0 ]; then
    print_success "DBR integration tests passed"
else
    print_error "DBR integration tests failed (exit code: $EXIT_CODE)"
fi

print_divider

exit $EXIT_CODE
