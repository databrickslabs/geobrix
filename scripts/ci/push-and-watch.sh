#!/bin/bash
# push-and-watch.sh - Push to remote and automatically monitor the triggered workflow

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

# Check if authenticated
if ! gh auth status &> /dev/null; then
    echo -e "${RED}❌ Error: Not authenticated with GitHub${NC}"
    echo "Run: gh auth login"
    exit 1
fi

cd "$PROJECT_ROOT"

# Get current branch
CURRENT_BRANCH=$(git branch --show-current)

echo -e "${BLUE}╔═══════════════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║${NC}  ${CYAN}🚀 Push and Watch CI${NC}                                  ${BLUE}║${NC}"
echo -e "${BLUE}╚═══════════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${BLUE}📍 Branch:${NC} ${YELLOW}${CURRENT_BRANCH}${NC}"
echo ""

# Check for uncommitted changes
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    echo -e "${YELLOW}⚠️  Warning: You have uncommitted changes${NC}"
    git status --short
    echo ""
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Aborted.${NC}"
        exit 1
    fi
fi

# Show what will be pushed
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Commits to push:${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
git log --oneline origin/${CURRENT_BRANCH}..HEAD 2>/dev/null || git log --oneline -5
echo ""

# Confirm push
read -p "Push these commits and watch CI? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${YELLOW}Aborted.${NC}"
    exit 0
fi

# Get the commit SHA before pushing (to identify the triggered run)
COMMIT_SHA=$(git rev-parse HEAD)
SHORT_SHA=$(git rev-parse --short HEAD)

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Pushing to remote...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Push to remote
if git push origin "$CURRENT_BRANCH"; then
    echo ""
    echo -e "${GREEN}✅ Push successful!${NC}"
else
    echo ""
    echo -e "${RED}❌ Push failed!${NC}"
    exit 1
fi

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Waiting for workflow to start...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${YELLOW}⏳ Waiting 10 seconds for GitHub to trigger workflows...${NC}"
sleep 10

# Find the workflow run triggered by this push
echo -e "${BLUE}🔍 Looking for workflow run for commit ${YELLOW}${SHORT_SHA}${NC}..."
echo ""

# Try to find the run for up to 30 seconds
MAX_ATTEMPTS=6
ATTEMPT=1
RUN_ID=""

while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    # Get the latest run ID for this branch
    RUN_ID=$(gh run list \
        --branch "$CURRENT_BRANCH" \
        --limit 5 \
        --json databaseId,headSha,status,createdAt \
        --jq "sort_by(.createdAt) | reverse | .[0] | select(.headSha == \"$COMMIT_SHA\") | .databaseId" 2>/dev/null || echo "")
    
    if [ -n "$RUN_ID" ]; then
        echo -e "${GREEN}✅ Found workflow run: ${YELLOW}#${RUN_ID}${NC}"
        break
    fi
    
    if [ $ATTEMPT -lt $MAX_ATTEMPTS ]; then
        echo -e "${YELLOW}⏳ Not found yet, waiting... (attempt $ATTEMPT/$MAX_ATTEMPTS)${NC}"
        sleep 5
    fi
    
    ATTEMPT=$((ATTEMPT + 1))
done

if [ -z "$RUN_ID" ]; then
    echo ""
    echo -e "${YELLOW}⚠️  Could not automatically find the workflow run.${NC}"
    echo ""
    echo -e "${CYAN}Recent runs for this branch:${NC}"
    gh run list --branch "$CURRENT_BRANCH" --limit 5
    echo ""
    echo "This might happen if:"
    echo "  - Workflow is still queuing (check GitHub Actions tab)"
    echo "  - No workflow is configured for push events"
    echo "  - There's a delay in GitHub's API"
    echo ""
    echo "You can manually check status with:"
    echo -e "  ${GREEN}./scripts/ci/check-ci-status.sh${NC}"
    echo ""
    echo "Or watch the latest run with:"
    echo -e "  ${GREEN}./scripts/ci/watch-ci.sh${NC}"
    exit 1
fi

# Show run details
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Workflow Run Details:${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
gh run view "$RUN_ID"

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Watching workflow run...${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "${YELLOW}👀 Monitoring run ${RUN_ID}... (Press Ctrl+C to stop watching)${NC}"
echo ""

# Watch the run
gh run watch "$RUN_ID"

# After run completes, show summary
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Run Complete!${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Get final status
RUN_STATUS=$(gh run view "$RUN_ID" --json conclusion --jq '.conclusion')

case "$RUN_STATUS" in
    "success")
        echo -e "${GREEN}✅ Status: SUCCESS${NC}"
        ;;
    "failure")
        echo -e "${RED}❌ Status: FAILED${NC}"
        echo ""
        echo -e "${YELLOW}To fetch detailed logs:${NC}"
        echo -e "  ${GREEN}./scripts/ci/fetch-ci-logs.sh $RUN_ID${NC}"
        ;;
    *)
        echo -e "${YELLOW}⚠️  Status: $RUN_STATUS${NC}"
        ;;
esac

echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Next Steps:${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""
echo -e "  ${GREEN}Fetch logs:${NC}        ./scripts/ci/fetch-ci-logs.sh $RUN_ID"
echo -e "  ${GREEN}View in browser:${NC}   gh run view $RUN_ID --web"
echo -e "  ${GREEN}Check status:${NC}      ./scripts/ci/check-ci-status.sh"
if [ "$RUN_STATUS" = "failure" ]; then
    echo -e "  ${GREEN}Re-run failed:${NC}     gh run rerun $RUN_ID --failed"
fi
echo ""
