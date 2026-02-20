#!/bin/bash
# run-doc-tests.sh - Run documentation tests locally or check CI status

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

show_banner() {
    echo -e "${BLUE}╔═══════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║${NC}  ${CYAN}📚 GeoBriX Documentation Tests${NC}                        ${BLUE}║${NC}"
    echo -e "${BLUE}║${NC}  ${YELLOW}Test documentation code examples${NC}                     ${BLUE}║${NC}"
    echo -e "${BLUE}╚═══════════════════════════════════════════════════════╝${NC}"
    echo ""
}

show_help() {
    show_banner
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}./scripts/ci/run-doc-tests.sh${NC} ${YELLOW}[command] [language]${NC}"
    echo ""
    echo -e "${CYAN}Commands:${NC}"
    echo -e "  ${GREEN}local [python|scala|all]${NC}  Run tests locally in Docker"
    echo -e "  ${GREEN}python${NC}                    Run Python doc tests only"
    echo -e "  ${GREEN}scala${NC}                     Run Scala doc tests only"
    echo -e "  ${GREEN}status${NC}                    Check CI status for doc tests"
    echo -e "  ${GREEN}trigger${NC}                   Trigger doc tests in CI"
    echo -e "  ${GREEN}watch${NC}                     Watch latest doc test run"
    echo -e "  ${GREEN}logs${NC}                      Fetch doc test CI logs"
    echo -e "  ${GREEN}help${NC}                      Show this help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}./scripts/ci/run-doc-tests.sh local${NC}          # Run all tests"
    echo -e "  ${YELLOW}./scripts/ci/run-doc-tests.sh local python${NC}   # Python only"
    echo -e "  ${YELLOW}./scripts/ci/run-doc-tests.sh python${NC}         # Same as local python"
    echo -e "  ${YELLOW}./scripts/ci/run-doc-tests.sh scala${NC}          # Scala tests only"
    echo -e "  ${YELLOW}./scripts/ci/run-doc-tests.sh status${NC}         # Check CI"
    echo ""
}

run_python_tests() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Running Python Documentation Tests${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    # Run all Python documentation tests
    # Includes API tests (SQL/Python structure) and readers tests (executable examples)
    docker exec geobrix-dev python3 -m pytest /root/geobrix/docs/tests/python/ \
        -v \
        -m "not integration" \
        --tb=short \
        --color=yes
    
    PYTHON_EXIT=$?
    return $PYTHON_EXIT
}

run_scala_tests() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Running Scala Documentation Tests${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    docker exec geobrix-dev /bin/bash -c \
        "unset JAVA_TOOL_OPTIONS && export JUPYTER_PLATFORM_DIRS=1 && cd /root/geobrix && mvn test -Dsuites='tests.docs.scala.*'"
    
    SCALA_EXIT=$?
    return $SCALA_EXIT
}

check_docker() {
    # Check if Docker is running
    if ! docker ps &> /dev/null; then
        echo -e "${RED}❌ Error: Docker is not running${NC}"
        echo "Start Docker and try again."
        exit 1
    fi
    
    # Check if geobrix-dev container exists
    if ! docker ps -a --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
        echo -e "${RED}❌ Error: geobrix-dev container not found${NC}"
        echo "Start the development container first:"
        echo -e "  ${YELLOW}./scripts/docker/start_docker.sh${NC}"
        exit 1
    fi
    
    # Check if container is running
    if ! docker ps --format '{{.Names}}' | grep -q '^geobrix-dev$'; then
        echo -e "${YELLOW}⚠️  Container is not running. Starting...${NC}"
        docker start geobrix-dev
        sleep 2
    fi
}

run_local() {
    local lang="${1:-all}"
    
    echo -e "${CYAN}Running documentation tests locally...${NC}"
    echo ""
    
    check_docker
    
    case "$lang" in
        python)
            run_python_tests
            RESULT=$?
            ;;
        scala)
            run_scala_tests
            RESULT=$?
            ;;
        all|*)
            run_python_tests
            PYTHON_RESULT=$?
            echo ""
            run_scala_tests
            SCALA_RESULT=$?
            RESULT=$((PYTHON_RESULT + SCALA_RESULT))
            ;;
    esac
    
    echo ""
    if [ $RESULT -eq 0 ]; then
        echo -e "${GREEN}✅ Local documentation tests complete!${NC}"
    else
        echo -e "${YELLOW}⚠️  Some tests failed (see output above)${NC}"
    fi
    echo ""
    echo -e "${CYAN}Next steps:${NC}"
    echo "  - Python with coverage: docker exec geobrix-dev pytest docs/tests/python/ -v --cov=docs/tests/python"
    echo "  - Scala compile only: docker exec geobrix-dev mvn test-compile"
    echo "  - Push and trigger CI: ./scripts/ci/push-and-watch.sh"
    
    return $RESULT
}

check_status() {
    echo -e "${CYAN}Checking Documentation Tests CI status...${NC}"
    echo ""
    
    # Check if gh CLI is installed
    if ! command -v gh &> /dev/null; then
        echo -e "${RED}❌ Error: GitHub CLI (gh) is not installed${NC}"
        echo "Run: ./scripts/ci/setup-gh-cli.sh"
        exit 1
    fi
    
    cd "$PROJECT_ROOT"
    
    # Get runs for Documentation Tests workflow
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Recent Documentation Test Runs:${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    if gh run list --workflow="Documentation Tests" --limit 5 2>/dev/null; then
        echo ""
        echo -e "${GREEN}✅ Documentation Tests workflow found${NC}"
    else
        echo -e "${YELLOW}⚠️  No Documentation Tests runs found${NC}"
        echo "The workflow may not have run yet, or may not be configured."
        echo ""
        echo "To trigger a run:"
        echo -e "  ${GREEN}./scripts/ci/run-doc-tests.sh trigger${NC}"
    fi
}

trigger_tests() {
    echo -e "${CYAN}Triggering Documentation Tests in CI...${NC}"
    echo ""
    
    # Check if gh CLI is installed
    if ! command -v gh &> /dev/null; then
        echo -e "${RED}❌ Error: GitHub CLI (gh) is not installed${NC}"
        echo "Run: ./scripts/ci/setup-gh-cli.sh"
        exit 1
    fi
    
    cd "$PROJECT_ROOT"
    
    CURRENT_BRANCH=$(git branch --show-current)
    echo -e "${BLUE}📍 Current branch: ${YELLOW}${CURRENT_BRANCH}${NC}"
    
    # Check for uncommitted changes
    if ! git diff-index --quiet HEAD --; then
        echo -e "${YELLOW}⚠️  Warning: You have uncommitted changes${NC}"
        echo "Commit your changes first for them to be included in CI."
        read -p "Continue anyway? (y/N) " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            exit 1
        fi
    fi
    
    # Push to remote
    echo -e "${BLUE}🚀 Pushing to remote...${NC}"
    git push origin "$CURRENT_BRANCH"
    
    # Trigger workflow
    echo -e "${BLUE}🚀 Triggering Documentation Tests workflow...${NC}"
    
    if gh workflow run "Documentation Tests" --ref "$CURRENT_BRANCH" 2>/dev/null; then
        echo -e "${GREEN}✅ Documentation Tests workflow triggered successfully${NC}"
        echo ""
        echo "Waiting for workflow to start..."
        sleep 5
        
        echo -e "${BLUE}📊 Latest workflow run:${NC}"
        gh run list --workflow="Documentation Tests" --limit 1
        
        echo ""
        echo "To watch in real-time:"
        echo -e "  ${GREEN}./scripts/ci/run-doc-tests.sh watch${NC}"
    else
        echo -e "${RED}❌ Failed to trigger workflow${NC}"
        echo ""
        echo "The workflow may not have workflow_dispatch enabled."
        echo "Check .github/workflows/doc-tests.yml"
        exit 1
    fi
}

watch_tests() {
    echo -e "${CYAN}Watching Documentation Tests...${NC}"
    echo ""
    
    # Check if gh CLI is installed
    if ! command -v gh &> /dev/null; then
        echo -e "${RED}❌ Error: GitHub CLI (gh) is not installed${NC}"
        echo "Run: ./scripts/ci/setup-gh-cli.sh"
        exit 1
    fi
    
    cd "$PROJECT_ROOT"
    
    # Get latest run ID for Documentation Tests
    LATEST_RUN=$(gh run list --workflow="Documentation Tests" --limit 1 --json databaseId --jq '.[0].databaseId')
    
    if [ -z "$LATEST_RUN" ]; then
        echo -e "${YELLOW}⚠️  No Documentation Tests runs found${NC}"
        echo ""
        echo "Trigger a run first:"
        echo -e "  ${GREEN}./scripts/ci/run-doc-tests.sh trigger${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}📊 Watching run ID: ${YELLOW}${LATEST_RUN}${NC}"
    echo ""
    
    gh run watch "$LATEST_RUN"
    
    echo ""
    echo -e "${CYAN}Run complete!${NC}"
    echo ""
    echo "To view logs:"
    echo -e "  ${GREEN}./scripts/ci/run-doc-tests.sh logs${NC}"
}

fetch_logs() {
    echo -e "${CYAN}Fetching Documentation Tests logs...${NC}"
    echo ""
    
    # Check if gh CLI is installed
    if ! command -v gh &> /dev/null; then
        echo -e "${RED}❌ Error: GitHub CLI (gh) is not installed${NC}"
        echo "Run: ./scripts/ci/setup-gh-cli.sh"
        exit 1
    fi
    
    cd "$PROJECT_ROOT"
    
    # Get latest run ID for Documentation Tests
    LATEST_RUN=$(gh run list --workflow="Documentation Tests" --limit 1 --json databaseId --jq '.[0].databaseId')
    
    if [ -z "$LATEST_RUN" ]; then
        echo -e "${YELLOW}⚠️  No Documentation Tests runs found${NC}"
        exit 1
    fi
    
    echo -e "${BLUE}📊 Fetching logs for run ID: ${YELLOW}${LATEST_RUN}${NC}"
    
    # Create logs directory
    mkdir -p ci-logs
    
    # Download logs
    TIMESTAMP=$(date +%Y%m%d-%H%M%S)
    LOG_FILE="ci-logs/doc-tests-${LATEST_RUN}-${TIMESTAMP}.log"
    
    gh run view "$LATEST_RUN" --log > "$LOG_FILE"
    
    echo -e "${GREEN}✅ Logs saved to: ${YELLOW}${LOG_FILE}${NC}"
    echo ""
    
    # Show summary
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${CYAN}Test Summary:${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    
    if grep -q "passed" "$LOG_FILE"; then
        PASSED=$(grep -o "[0-9]* passed" "$LOG_FILE" | head -1 || echo "0 passed")
        echo -e "${GREEN}✅ ${PASSED}${NC}"
    fi
    
    if grep -q "failed" "$LOG_FILE"; then
        FAILED=$(grep -o "[0-9]* failed" "$LOG_FILE" | head -1 || echo "0 failed")
        echo -e "${RED}❌ ${FAILED}${NC}"
    fi
    
    if grep -q "skipped" "$LOG_FILE"; then
        SKIPPED=$(grep -o "[0-9]* skipped" "$LOG_FILE" | head -1 || echo "0 skipped")
        echo -e "${YELLOW}⏭️  ${SKIPPED}${NC}"
    fi
    
    echo ""
    echo "To view full logs:"
    echo -e "  ${GREEN}less ${LOG_FILE}${NC}"
}

# Main
main() {
    case "${1:-help}" in
        local)
            run_local "${2:-all}"
            ;;
        python)
            check_docker
            run_python_tests
            ;;
        scala)
            check_docker
            run_scala_tests
            ;;
        status)
            check_status
            ;;
        trigger)
            trigger_tests
            ;;
        watch)
            watch_tests
            ;;
        logs)
            fetch_logs
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            echo -e "${RED}❌ Unknown command: $1${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

main "$@"
