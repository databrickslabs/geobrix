#!/bin/bash
# gbx:docs:build - Verify the docs build (MDX compiles, links resolve) WITHOUT
# leaving a running gbx:docs:dev server (port 3000) in a corrupted state.
#
# `docusaurus build` and `docusaurus start` share the same <siteDir>/.docusaurus
# cache (registry.js, client-manifest.json, ...). Running a build while a dev
# server is up rewrites that cache out from under it, so its routes render stale
# or blank until it is restarted. This command detects a running dev server,
# stops it for the build, then restarts it on the same port — so you end on a
# healthy, freshly-cached server instead of a broken one.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

LOG_PATH=""
RESTART=true

show_help() {
    cat << EOF
$(print_banner "📚 GeoBrix: Docs Build (Dev-Server-Aware)")

Runs 'npm run build' (docusaurus build) to verify MDX compiles and internal
links resolve. If a gbx:docs:dev server is running, it is stopped for the
build and restarted on the same port afterward, because a build rewrites the
shared docs/.docusaurus/ cache that the dev server relies on.

USAGE:
    bash scripts/commands/gbx-docs-build.sh [OPTIONS]

OPTIONS:
    --no-restart         Do not restart the dev server after building
                         (still stops it first if running)
    --log <path>         Write output to log file (filename → test-logs/<name>)
    --help               Display this help message

EXAMPLES:
    # Verify docs compile; leave the dev server as it was found
    bash scripts/commands/gbx-docs-build.sh

    # Build for CI-style check without bringing a dev server back up
    bash scripts/commands/gbx-docs-build.sh --no-restart

NOTES:
    - Never run a bare 'npm run build' on the host while a dev server is up —
      it corrupts the server's rendered routes. Use this command instead.
    - Detects the dev server via /tmp/docusaurus-<port>.pid (written by
      gbx:docs:dev). If none is running, it just builds.
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --no-restart)
            RESTART=false
            shift
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --help|-h)
            show_help
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

show_banner "📚 GeoBrix: Docs Build (Dev-Server-Aware)"
setup_log_file "$LOG_PATH"

# Detect a running dev server (shared helper reads /tmp/docusaurus-<port>.pid).
# Capture the port(s) so we can bring the server back up on the same one.
RUNNING_PORTS=()
while IFS= read -r port; do
    [ -n "$port" ] && RUNNING_PORTS+=("$port")
done < <(detect_docs_dev_servers)

if [ ${#RUNNING_PORTS[@]} -gt 0 ]; then
    echo -e "${CYAN}Dev server running on port(s): ${YELLOW}${RUNNING_PORTS[*]}${NC}"
    echo -e "${CYAN}Stopping it so the build doesn't corrupt its .docusaurus cache...${NC}"
    bash "$SCRIPT_DIR/gbx-docs-stop.sh"
    sleep 2
fi

cd "$PROJECT_ROOT/docs" || exit 1

echo -e "${CYAN}Running npm run build...${NC}"
show_separator
npm run build
EXIT_CODE=$?

echo ""
show_separator
if [ $EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✅ Docs build succeeded (MDX compiles, links resolve).${NC}"
else
    echo -e "${RED}❌ Docs build failed (exit code: $EXIT_CODE).${NC}"
fi
show_separator

# Restart the dev server on the same port(s) it was found on so the user ends
# on a healthy, freshly-cached server.
if [ "$RESTART" = true ] && [ ${#RUNNING_PORTS[@]} -gt 0 ]; then
    for port in "${RUNNING_PORTS[@]}"; do
        echo -e "${CYAN}Restarting dev server on port ${YELLOW}$port${CYAN}...${NC}"
        bash "$SCRIPT_DIR/gbx-docs-dev.sh" --port "$port"
    done
elif [ ${#RUNNING_PORTS[@]} -gt 0 ]; then
    echo -e "${YELLOW}Dev server was stopped for the build and NOT restarted (--no-restart).${NC}"
    echo -e "${YELLOW}Restart it with: gbx:docs:dev${NC}"
fi

if [ -n "$LOG_PATH" ]; then
    echo -e "${CYAN}📝 Log saved to: ${YELLOW}$LOG_PATH${NC}"
fi

exit $EXIT_CODE
