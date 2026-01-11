#!/bin/bash
# watch-ci.sh - Watch GitHub Actions CI run in real-time

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Check if gh CLI is installed
if ! command -v gh &> /dev/null; then
    echo -e "${RED}❌ Error: GitHub CLI (gh) is not installed${NC}"
    echo "Run: ./scripts/ci/setup-gh-cli.sh"
    exit 1
fi

cd "$PROJECT_ROOT"

CURRENT_BRANCH=$(git branch --show-current)

echo -e "${BLUE}🔍 Watching CI runs for branch: ${YELLOW}${CURRENT_BRANCH}${NC}"
echo ""

# If a run ID is provided, watch that specific run
if [ -n "$1" ]; then
    RUN_ID="$1"
    echo -e "${BLUE}👀 Watching run ID: ${YELLOW}${RUN_ID}${NC}"
    gh run watch "$RUN_ID"
else
    # Watch the latest run
    LATEST_RUN_ID=$(gh run list --branch "$CURRENT_BRANCH" --limit 1 --json databaseId --jq '.[0].databaseId')
    
    if [ -n "$LATEST_RUN_ID" ]; then
        echo -e "${BLUE}👀 Watching latest run (ID: ${YELLOW}${LATEST_RUN_ID}${NC}${BLUE})${NC}"
        echo ""
        gh run watch "$LATEST_RUN_ID"
    else
        echo -e "${RED}❌ No workflow runs found for branch: ${CURRENT_BRANCH}${NC}"
        exit 1
    fi
fi

# After the run completes, show summary
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}Run completed! Fetching logs...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo "To fetch full logs, run:"
echo -e "${GREEN}  ./scripts/ci/fetch-ci-logs.sh${NC}"
