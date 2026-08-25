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
STRICT=false

show_help() {
    cat << EOF
$(print_banner "📚 GeoBrix: Docs Build (Dev-Server-Aware)")

Runs 'npm run build' (docusaurus build) to verify MDX compiles and internal
links resolve. If a gbx:docs:dev server is running, it is stopped for the
build and restarted on the same port afterward, because a build rewrites the
shared docs/.docusaurus/ cache that the dev server relies on.

By default the build is PERMISSIVE: docusaurus.config.js sets
onBrokenLinks='warn', so broken internal links are reported but the build
still exits 0 (this keeps the routine build + dev-server-restart flow from
being blocked by a stray broken link). Pass --strict for a green validation
gate (pre-push / CI): the command then exits non-zero if Docusaurus reports
ANY broken link. Either way, a running dev server is still restarted so
port 3000 is never left down.

USAGE:
    bash scripts/commands/gbx-docs-build.sh [OPTIONS]

OPTIONS:
    --strict             Fail (exit non-zero) if Docusaurus reports broken
                         links. Use for validation gates. Default: permissive
                         (broken links warn but the build still passes).
    --no-restart         Do not restart the dev server after building
                         (still stops it first if running)
    --log <path>         Write output to log file (filename → test-logs/<name>)
    --help               Display this help message

EXAMPLES:
    # Verify docs compile; leave the dev server as it was found (permissive)
    bash scripts/commands/gbx-docs-build.sh

    # Strict validation gate — fail on any broken link (e.g. before a push)
    bash scripts/commands/gbx-docs-build.sh --strict

    # Build for CI-style check without bringing a dev server back up
    bash scripts/commands/gbx-docs-build.sh --strict --no-restart

NOTES:
    - Never run a bare 'npm run build' on the host while a dev server is up —
      it corrupts the server's rendered routes. Use this command instead.
    - Detects the dev server via /tmp/docusaurus-<port>.pid (written by
      gbx:docs:dev). If none is running, it just builds.
    - The '✅ ... all links resolve' banner is printed ONLY when Docusaurus
      reported no broken links; otherwise it warns (or fails under --strict).
EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case $1 in
        --strict)
            STRICT=true
            shift
            ;;
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

# Clear stale Docusaurus cache + prior build so a corrupt cache can't break a
# subsequent `gbx:docs:dev` (the user's port-3000 server). Both are gitignored.
rm -rf "$PROJECT_ROOT/docs/.docusaurus" "$PROJECT_ROOT/docs/build"

echo -e "${CYAN}Running npm run build...${NC}"
if [ "$STRICT" = true ]; then
    echo -e "${CYAN}(strict mode: broken links will FAIL the build)${NC}"
fi
show_separator

# Capture the build output so we can detect broken-link warnings. Docusaurus
# runs with onBrokenLinks='warn', so a broken link is reported but does NOT set
# a non-zero exit; the raw exit code alone cannot tell "clean" from "broken".
# tee keeps the live output flowing to the terminal (and the --log FIFO).
BUILD_OUT="$(mktemp "${TMPDIR:-/tmp}/gbx-docs-build.XXXXXX")"
npm run build 2>&1 | tee "$BUILD_OUT"
EXIT_CODE=${PIPESTATUS[0]}

# Docusaurus prints "found broken links!" / "broken links found:" / "Broken
# link on source page ..." when any internal link fails to resolve.
BROKEN_LINKS=false
if grep -qiE "broken link" "$BUILD_OUT"; then
    BROKEN_LINKS=true
fi
rm -f "$BUILD_OUT"

echo ""
show_separator
if [ $EXIT_CODE -ne 0 ]; then
    echo -e "${RED}❌ Docs build failed (exit code: $EXIT_CODE).${NC}"
elif [ "$BROKEN_LINKS" = true ]; then
    if [ "$STRICT" = true ]; then
        echo -e "${RED}❌ Docs build (--strict): Docusaurus reported BROKEN LINKS (listed above).${NC}"
        EXIT_CODE=1
    else
        echo -e "${YELLOW}⚠️  Docs build compiled, but Docusaurus reported BROKEN LINKS.${NC}"
        echo -e "${YELLOW}    onBrokenLinks='warn' → the build still exits 0. Re-run with --strict to fail on this,${NC}"
        echo -e "${YELLOW}    and grep the output above for 'broken link' to see them.${NC}"
    fi
else
    echo -e "${GREEN}✅ Docs build succeeded (MDX compiles, all links resolve).${NC}"
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
