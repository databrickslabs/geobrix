#!/bin/bash
# ci-manager.sh - Master CI management interface for GeoBriX

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
MAGENTA='\033[0;35m'
NC='\033[0m'

# Banner
show_banner() {
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}  ${CYAN}🧊 GeoBriX CI Manager${NC}                             ${BLUE}║${NC}"
    echo -e "${BLUE}║${NC}  ${YELLOW}Manage GitHub Actions CI from the command line${NC}    ${BLUE}║${NC}"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════╝${NC}"
    echo ""
}

# Help message
show_help() {
    show_banner
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}./scripts/ci/ci-manager.sh${NC} ${YELLOW}[command]${NC}"
    echo ""
    echo -e "${CYAN}Commands:${NC}"
    echo -e "  ${GREEN}status${NC}       Check CI status and recent runs"
    echo -e "  ${GREEN}trigger${NC}      Push and trigger new CI workflow"
    echo -e "  ${GREEN}watch${NC}        Watch latest CI run in real-time"
    echo -e "  ${GREEN}logs${NC}         Fetch and analyze CI logs"
    echo -e "  ${GREEN}setup${NC}        Install and configure GitHub CLI"
    echo -e "  ${GREEN}help${NC}         Show this help message"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}./scripts/ci/ci-manager.sh status${NC}     # Check CI status"
    echo -e "  ${YELLOW}./scripts/ci/ci-manager.sh trigger${NC}    # Trigger new run"
    echo -e "  ${YELLOW}./scripts/ci/ci-manager.sh watch${NC}      # Watch latest run"
    echo ""
    echo -e "${CYAN}Interactive Mode:${NC}"
    echo -e "  ${YELLOW}./scripts/ci/ci-manager.sh${NC}            # Launch interactive menu"
    echo ""
}

# Check prerequisites
check_prerequisites() {
    if ! command -v gh &> /dev/null; then
        echo -e "${RED}❌ Error: GitHub CLI (gh) is not installed${NC}"
        echo ""
        echo "Would you like to install it now? (y/N)"
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            "$SCRIPT_DIR/setup-gh-cli.sh"
        else
            echo ""
            echo "Run: ./scripts/ci/setup-gh-cli.sh"
            exit 1
        fi
    fi
    
    if ! gh auth status &> /dev/null; then
        echo -e "${RED}❌ Error: Not authenticated with GitHub${NC}"
        echo ""
        echo "Would you like to authenticate now? (y/N)"
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            gh auth login
        else
            echo ""
            echo "Run: gh auth login"
            exit 1
        fi
    fi
}

# Interactive menu
show_menu() {
    show_banner
    echo -e "${CYAN}What would you like to do?${NC}"
    echo ""
    echo -e "  ${GREEN}1)${NC} Check CI status"
    echo -e "  ${GREEN}2)${NC} Trigger new CI run"
    echo -e "  ${GREEN}3)${NC} Watch latest CI run"
    echo -e "  ${GREEN}4)${NC} Fetch CI logs"
    echo -e "  ${GREEN}5)${NC} Setup GitHub CLI"
    echo -e "  ${GREEN}6)${NC} Exit"
    echo ""
    echo -n -e "${YELLOW}Enter choice [1-6]:${NC} "
}

# Execute command
execute_command() {
    local cmd="$1"
    
    case "$cmd" in
        status|1)
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            echo -e "${CYAN}Checking CI Status...${NC}"
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            "$SCRIPT_DIR/check-ci-status.sh"
            ;;
        trigger|2)
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            echo -e "${CYAN}Triggering CI Workflow...${NC}"
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            "$SCRIPT_DIR/trigger-remote-tests.sh"
            ;;
        watch|3)
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            echo -e "${CYAN}Watching CI Run...${NC}"
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            "$SCRIPT_DIR/watch-ci.sh"
            ;;
        logs|4)
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            echo -e "${CYAN}Fetching CI Logs...${NC}"
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            "$SCRIPT_DIR/fetch-ci-logs.sh"
            ;;
        setup|5)
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            echo -e "${CYAN}Setting Up GitHub CLI...${NC}"
            echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
            "$SCRIPT_DIR/setup-gh-cli.sh"
            ;;
        help|h|-h|--help)
            show_help
            ;;
        exit|6|q|quit)
            echo -e "${GREEN}👋 Goodbye!${NC}"
            exit 0
            ;;
        *)
            echo -e "${RED}❌ Unknown command: $cmd${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

# Main
main() {
    # If command provided, execute it
    if [ $# -gt 0 ]; then
        check_prerequisites
        execute_command "$1"
        exit 0
    fi
    
    # Otherwise, show interactive menu
    check_prerequisites
    
    while true; do
        show_menu
        read -r choice
        echo ""
        
        if [ "$choice" = "6" ] || [ "$choice" = "q" ] || [ "$choice" = "quit" ]; then
            echo -e "${GREEN}👋 Goodbye!${NC}"
            exit 0
        fi
        
        execute_command "$choice"
        
        echo ""
        echo -e "${YELLOW}Press Enter to continue...${NC}"
        read -r
        clear
    done
}

# Run main
main "$@"
