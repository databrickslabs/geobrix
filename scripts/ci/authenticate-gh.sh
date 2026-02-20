#!/bin/bash
# authenticate-gh.sh - Standalone GitHub CLI authentication helper

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}  GitHub CLI Authentication Helper${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

# Check if gh is installed
if ! command -v gh &> /dev/null; then
    echo -e "${RED}❌ Error: GitHub CLI (gh) is not installed${NC}"
    echo ""
    echo "Install first:"
    echo "  macOS:  brew install gh"
    echo "  Linux:  sudo apt-get install gh"
    echo ""
    exit 1
fi

# Check current auth status
if gh auth status &> /dev/null; then
    echo -e "${GREEN}✅ Already authenticated!${NC}"
    echo ""
    gh auth status
    echo ""
    read -p "Re-authenticate? (y/N) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 0
    fi
fi

# Show authentication options
echo -e "${CYAN}Choose authentication method:${NC}"
echo ""
echo -e "  ${GREEN}1)${NC} Browser Login (Recommended)"
echo -e "     - Opens browser automatically"
echo -e "     - You'll see a one-time code like: ${YELLOW}ABCD-1234${NC}"
echo -e "     - Enter that code on the GitHub page"
echo ""
echo -e "  ${GREEN}2)${NC} Token Login"
echo -e "     - Generate token: ${YELLOW}https://github.com/settings/tokens${NC}"
echo -e "     - Required scopes: ${YELLOW}repo, workflow, read:org${NC}"
echo -e "     - Paste token when prompted"
echo ""
echo -n -e "${YELLOW}Enter choice [1-2]:${NC} "
read -n 1 choice
echo ""
echo ""

case $choice in
    1)
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${CYAN}Browser Authentication${NC}"
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo ""
        echo -e "${YELLOW}⚠️  WATCH FOR THE ONE-TIME CODE!${NC}"
        echo ""
        echo "Look for a line like:"
        echo -e "${CYAN}  ! First copy your one-time code: ${GREEN}ABCD-1234${NC}"
        echo ""
        echo "That code needs to be entered on the GitHub page."
        echo ""
        read -p "Press Enter to continue..."
        echo ""
        
        # Run auth with explicit prompts
        gh auth login \
            --hostname github.com \
            --git-protocol https \
            --web
        ;;
    2)
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo -e "${CYAN}Token Authentication${NC}"
        echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
        echo ""
        echo "Steps to generate a token:"
        echo ""
        echo "  1. Visit: ${CYAN}https://github.com/settings/tokens${NC}"
        echo "  2. Click: 'Generate new token (classic)'"
        echo "  3. Give it a name (e.g., 'GeoBriX CLI')"
        echo "  4. Select scopes (${YELLOW}ALL THREE REQUIRED${NC}):"
        echo "     ${YELLOW}✓${NC} repo (Full control of private repositories)"
        echo "     ${YELLOW}✓${NC} workflow (Update GitHub Action workflows)"
        echo "     ${YELLOW}✓${NC} read:org (Read org and team membership)"
        echo "  5. Click 'Generate token'"
        echo "  6. Copy the token (${RED}you won't see it again!${NC})"
        echo ""
        read -p "Press Enter when you have your token..."
        echo ""
        
        gh auth login \
            --hostname github.com \
            --git-protocol https \
            --with-token
        ;;
    *)
        echo -e "${RED}Invalid choice${NC}"
        exit 1
        ;;
esac

# Verify authentication
echo ""
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${CYAN}Verification${NC}"
echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo ""

if gh auth status; then
    echo ""
    echo -e "${GREEN}✅ Authentication successful!${NC}"
    echo ""
    echo "You can now use GeoBriX CI scripts:"
    echo -e "  ${GREEN}./scripts/ci/check-ci-status.sh${NC}"
    echo -e "  ${GREEN}./scripts/ci/trigger-remote-tests.sh${NC}"
    echo -e "  ${GREEN}./scripts/ci/ci-manager.sh${NC}"
    echo ""
else
    echo ""
    echo -e "${RED}❌ Authentication failed${NC}"
    echo ""
    echo "Troubleshooting:"
    echo "  1. If browser didn't open, check your default browser settings"
    echo "  2. If code wasn't visible, try running in a different terminal"
    echo "  3. Try token method instead (option 2)"
    echo "  4. Check GitHub status: https://www.githubstatus.com/"
    echo ""
    exit 1
fi
