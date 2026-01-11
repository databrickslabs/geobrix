#!/bin/bash
# trigger-remote-tests.sh - Trigger remote CI tests on GitHub Actions

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

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
echo -e "${BLUE}📍 Current branch: ${YELLOW}${CURRENT_BRANCH}${NC}"

# Check if there are uncommitted changes
if ! git diff-index --quiet HEAD --; then
    echo -e "${YELLOW}⚠️  Warning: You have uncommitted changes${NC}"
    echo "Commit or stash your changes before triggering remote tests."
    read -p "Continue anyway? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Push current branch to remote
echo -e "${BLUE}🚀 Pushing to remote...${NC}"
git push origin "$CURRENT_BRANCH"

# Check for workflow files
WORKFLOW_DIR="$PROJECT_ROOT/.github/workflows"
if [ ! -d "$WORKFLOW_DIR" ]; then
    echo -e "${RED}❌ Error: No .github/workflows directory found${NC}"
    exit 1
fi

# List available workflows
echo -e "${BLUE}📋 Available workflows:${NC}"
gh workflow list

echo ""
echo -e "${BLUE}🔄 Recent workflow runs:${NC}"
gh run list --limit 5

echo ""
read -p "Trigger a new workflow run? (y/N) " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${BLUE}🚀 Triggering workflow on branch: ${YELLOW}${CURRENT_BRANCH}${NC}"
    
    # Try to trigger the main CI workflow
    # Adjust workflow name based on your actual workflow file
    if gh workflow run "CI" --ref "$CURRENT_BRANCH" 2>/dev/null; then
        echo -e "${GREEN}✅ Workflow triggered successfully${NC}"
    elif gh workflow run "Test" --ref "$CURRENT_BRANCH" 2>/dev/null; then
        echo -e "${GREEN}✅ Workflow triggered successfully${NC}"
    elif gh workflow run "Build and Test" --ref "$CURRENT_BRANCH" 2>/dev/null; then
        echo -e "${GREEN}✅ Workflow triggered successfully${NC}"
    else
        echo -e "${YELLOW}⚠️  Could not auto-trigger workflow${NC}"
        echo "Please select a workflow manually:"
        gh workflow list
        echo ""
        read -p "Enter workflow name or ID: " WORKFLOW_NAME
        gh workflow run "$WORKFLOW_NAME" --ref "$CURRENT_BRANCH"
    fi
    
    echo ""
    echo -e "${BLUE}⏳ Waiting for workflow to start (5 seconds)...${NC}"
    sleep 5
    
    echo -e "${BLUE}📊 Latest workflow run:${NC}"
    gh run list --limit 1
    
    echo ""
    echo "To watch the workflow run:"
    echo -e "${GREEN}  gh run watch${NC}"
    echo ""
    echo "To view workflow logs:"
    echo -e "${GREEN}  ./scripts/ci/fetch-ci-logs.sh${NC}"
fi
