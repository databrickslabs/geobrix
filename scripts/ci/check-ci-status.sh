#!/bin/bash
# check-ci-status.sh - Check status of GitHub Actions CI runs

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

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

CURRENT_BRANCH=$(git branch --show-current)
LIMIT=${1:-10}

echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo -e "${CYAN}  GeoBriX CI Status Dashboard${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${BLUE}📍 Branch:${NC} ${YELLOW}${CURRENT_BRANCH}${NC}"
echo -e "${BLUE}📅 Date:${NC}   $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Show recent runs for current branch
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Recent CI Runs (Last ${LIMIT}):${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
gh run list --branch "$CURRENT_BRANCH" --limit "$LIMIT"

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Latest Run Details:${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

# Get the latest run ID
LATEST_RUN_ID=$(gh run list --branch "$CURRENT_BRANCH" --limit 1 --json databaseId --jq '.[0].databaseId')

if [ -n "$LATEST_RUN_ID" ]; then
    # Show detailed view of latest run
    gh run view "$LATEST_RUN_ID"
    
    # Get run status
    RUN_STATUS=$(gh run view "$LATEST_RUN_ID" --json status --jq '.status')
    RUN_CONCLUSION=$(gh run view "$LATEST_RUN_ID" --json conclusion --jq '.conclusion')
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Status Summary:${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    case "$RUN_STATUS" in
        "completed")
            case "$RUN_CONCLUSION" in
                "success")
                    echo -e "${GREEN}✅ Status: SUCCESS${NC}"
                    ;;
                "failure")
                    echo -e "${RED}❌ Status: FAILED${NC}"
                    ;;
                *)
                    echo -e "${YELLOW}⚠️  Status: $RUN_CONCLUSION${NC}"
                    ;;
            esac
            ;;
        "in_progress")
            echo -e "${YELLOW}⏳ Status: IN PROGRESS${NC}"
            ;;
        "queued")
            echo -e "${BLUE}📋 Status: QUEUED${NC}"
            ;;
        *)
            echo -e "${YELLOW}⚠️  Status: $RUN_STATUS${NC}"
            ;;
    esac
    
    echo ""
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Available Actions:${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo ""
    echo -e "  ${GREEN}Watch live:${NC}        ./scripts/ci/watch-ci.sh"
    echo -e "  ${GREEN}Fetch logs:${NC}        ./scripts/ci/fetch-ci-logs.sh"
    echo -e "  ${GREEN}Re-run failed:${NC}     gh run rerun $LATEST_RUN_ID"
    echo -e "  ${GREEN}View in browser:${NC}   gh run view $LATEST_RUN_ID --web"
    echo ""
else
    echo -e "${YELLOW}⚠️  No workflow runs found for branch: ${CURRENT_BRANCH}${NC}"
fi

echo -e "${BLUE}═══════════════════════════════════════════════════════${NC}"
