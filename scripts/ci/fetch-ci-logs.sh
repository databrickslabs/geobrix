#!/bin/bash
# fetch-ci-logs.sh - Fetch and save GitHub Actions CI logs

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$PROJECT_ROOT/ci-logs"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo -e "${RED}❌ Error: GitHub CLI (gh) is not installed${NC}"
    echo "Run: ./scripts/ci/setup-gh-cli.sh"
    exit 1
fi

cd "$PROJECT_ROOT"

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR"

CURRENT_BRANCH=$(git branch --show-current)
TIMESTAMP=$(date '+%Y%m%d-%H%M%S')

echo -e "${BLUE}📥 Fetching CI logs...${NC}"
echo ""

# If a run ID is provided, fetch that specific run
if [ -n "$1" ]; then
    RUN_ID="$1"
else
    # Get the latest run
    RUN_ID=$(gh run list --branch "$CURRENT_BRANCH" --limit 1 --json databaseId --jq '.[0].databaseId')
fi

if [ -z "$RUN_ID" ]; then
    echo -e "${RED}❌ No workflow runs found for branch: ${CURRENT_BRANCH}${NC}"
    exit 1
fi

echo -e "${BLUE}📋 Fetching logs for run ID: ${YELLOW}${RUN_ID}${NC}"

# Get run details
RUN_NAME=$(gh run view "$RUN_ID" --json name --jq '.name')
RUN_STATUS=$(gh run view "$RUN_ID" --json conclusion --jq '.conclusion')

echo -e "${BLUE}📝 Run name: ${CYAN}${RUN_NAME}${NC}"
echo -e "${BLUE}📊 Status: ${CYAN}${RUN_STATUS}${NC}"
echo ""

# Create output filename
OUTPUT_FILE="$LOG_DIR/ci-run-${RUN_ID}-${TIMESTAMP}.log"

# Fetch full logs
echo -e "${BLUE}💾 Saving logs to: ${GREEN}${OUTPUT_FILE}${NC}"
gh run view "$RUN_ID" --log > "$OUTPUT_FILE"

echo -e "${GREEN}✅ Logs saved successfully${NC}"
echo ""

# Analyze logs for common patterns
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}📊 Log Analysis:${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Count test results
PASSED_TESTS=$(grep -c "PASSED" "$OUTPUT_FILE" 2>/dev/null || echo "0")
FAILED_TESTS=$(grep -c "FAILED" "$OUTPUT_FILE" 2>/dev/null || echo "0")
ERROR_LINES=$(grep -c "ERROR" "$OUTPUT_FILE" 2>/dev/null || echo "0")
WARNING_LINES=$(grep -c "WARNING" "$OUTPUT_FILE" 2>/dev/null || echo "0")

echo -e "  ${GREEN}✅ Passed tests:${NC}  $PASSED_TESTS"
echo -e "  ${RED}❌ Failed tests:${NC}  $FAILED_TESTS"
echo -e "  ${RED}🔴 Errors:${NC}        $ERROR_LINES"
echo -e "  ${YELLOW}⚠️  Warnings:${NC}      $WARNING_LINES"
echo ""

# Extract failures if any
if [ "$FAILED_TESTS" -gt 0 ]; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}Failed Tests:${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    grep -A 5 "FAILED" "$OUTPUT_FILE" | head -50 || true
    echo ""
fi

# Extract errors
if [ "$ERROR_LINES" -gt 0 ]; then
    ERRORS_FILE="$LOG_DIR/ci-run-${RUN_ID}-errors-${TIMESTAMP}.log"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${RED}Extracting errors to: ${YELLOW}${ERRORS_FILE}${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    grep -i "error" "$OUTPUT_FILE" > "$ERRORS_FILE" || true
    echo ""
fi

# Extract Scala test summaries
if grep -q "Tests:" "$OUTPUT_FILE" 2>/dev/null; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Scala Test Summary:${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    grep "Tests:" "$OUTPUT_FILE" | tail -10 || true
    echo ""
fi

# Python test summaries
if grep -q "passed" "$OUTPUT_FILE" 2>/dev/null; then
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Python Test Summary:${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    grep "passed\|failed" "$OUTPUT_FILE" | tail -10 || true
    echo ""
fi

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Files Created:${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "  ${GREEN}Full logs:${NC}    $OUTPUT_FILE"
if [ -f "$ERRORS_FILE" ]; then
    echo -e "  ${RED}Errors only:${NC}  $ERRORS_FILE"
fi
echo ""

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}View Options:${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "  ${GREEN}View full log:${NC}     less $OUTPUT_FILE"
echo -e "  ${GREEN}View in browser:${NC}   gh run view $RUN_ID --web"
echo -e "  ${GREEN}Search log:${NC}        grep 'pattern' $OUTPUT_FILE"
echo ""
