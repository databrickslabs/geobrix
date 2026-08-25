#!/bin/bash
# gbx:mcp:auth - Show which configured MCP servers are authenticated vs need auth.
#
# Read-only. Runs `claude mcp list` (a health-check that reports status but does NOT
# launch any browser/OAuth flow), then groups servers into NEEDS AUTH / FAILED /
# PENDING APPROVAL / CONNECTED and prints the exact `claude mcp login <name>` line for
# each server that needs authenticating. Also surfaces the "claude.ai connectors are
# disabled" notice (that one is gated by the managed-settings model gateway and is an
# org-admin lever, not a per-server login).

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "MCP: Auth Status"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:mcp:auth${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--log <path>${NC}   Write output to a log file (e.g. mcp-auth.log → test-logs/mcp-auth.log)"
    echo -e "  ${GREEN}--raw${NC}          Also print the raw \`claude mcp list\` output"
    echo -e "  ${GREEN}--help${NC}         Show this help"
    echo ""
    echo -e "${CYAN}What it does:${NC}"
    echo -e "  Read-only. Groups configured MCP servers by connection/auth status and prints"
    echo -e "  the exact \`claude mcp login <name>\` line for each server that needs auth."
    echo -e "  Does NOT trigger any browser/OAuth flow itself."
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${GREEN}gbx:mcp:auth${NC}"
    echo -e "  ${GREEN}gbx:mcp:auth${NC} ${YELLOW}--raw --log mcp-auth.log${NC}"
    echo ""
    echo -e "${CYAN}Exit code:${NC} 0 on success (even when servers need auth); non-zero only if \`claude mcp list\` fails."
    echo ""
}

LOG_PATH=""
SHOW_RAW=0
while [[ $# -gt 0 ]]; do
    case $1 in
        --log)     LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
        --raw)     SHOW_RAW=1; shift ;;
        --help|-h) show_help; exit 0 ;;
        *)         echo -e "${RED}Unknown option: $1${NC}"; show_help; exit 1 ;;
    esac
done

[ -n "$LOG_PATH" ] && setup_log_file "$LOG_PATH"

show_banner "MCP: Auth Status"

if ! command -v claude >/dev/null 2>&1; then
    echo -e "${RED}✘ 'claude' CLI not found on PATH.${NC}"
    exit 1
fi

echo -e "${CYAN}Checking MCP server health (read-only; no auth flow is triggered)…${NC}"
echo ""

# Capture the health-checked listing (stdout+stderr). Strip ANSI colors for stable
# parsing using an embedded literal ESC byte (portable across GNU and BSD sed).
RAW="$(claude mcp list 2>&1)"
STATUS=$?
ESC=$'\033'
CLEAN="$(printf '%s\n' "$RAW" | sed "s/${ESC}\[[0-9;]*m//g")"

if [ "$STATUS" -ne 0 ] && [ -z "$CLEAN" ]; then
    echo -e "${RED}✘ \`claude mcp list\` failed (exit $STATUS).${NC}"
    exit "$STATUS"
fi

# Server entry lines look like:  name: <endpoint> - <status>
#   ✔ Connected | ✘ Failed to connect — … | ! Needs authentication | ⏸ Pending approval
# The name may contain colons (e.g. plugin:slack:slack); the name/endpoint delimiter is
# the first ': ' (colon+space), which plugin-name colons never contain.
server_lines="$(printf '%s\n' "$CLEAN" | grep -E ' - .*(Connected|Failed to connect|Needs authentication|Pending approval)')"

lines_with() { printf '%s\n' "$server_lines" | grep -F "$1"; }             # full lines matching keyword
names_with() { lines_with "$1" | sed 's/: .*//'; }                          # just the server names
count()      { [ -z "$1" ] && echo 0 || printf '%s\n' "$1" | grep -c .; }
reason_of()  { printf '%s\n' "$1" | sed 's/^[^ ]*: .* - //'; }              # status/reason after ' - '

NEEDS_AUTH_LINES="$(lines_with 'Needs authentication')"
FAILED_LINES="$(lines_with 'Failed to connect')"
PENDING_LINES="$(lines_with 'Pending approval')"
CONNECTED_NAMES="$(names_with 'Connected')"

n_auth=$(count "$NEEDS_AUTH_LINES")
n_fail=$(count "$FAILED_LINES")
n_pend=$(count "$PENDING_LINES")
n_conn=$(count "$CONNECTED_NAMES")

echo -e "${CYAN}Summary:${NC} ${GREEN}${n_conn} connected${NC} · ${YELLOW}${n_auth} need auth${NC} · ${RED}${n_fail} failed${NC} · ${n_pend} pending approval"
echo ""

# --- Needs authentication (actionable via login) ---
if [ "$n_auth" -gt 0 ]; then
    echo -e "${YELLOW}! NEEDS AUTHENTICATION${NC} — run the matching login command:"
    printf '%s\n' "$NEEDS_AUTH_LINES" | while IFS= read -r line; do
        [ -z "$line" ] && continue
        name="${line%%: *}"
        echo -e "    ${YELLOW}•${NC} $name"
        echo -e "        ${GREEN}claude mcp login \"$name\"${NC}"
    done
    echo ""
fi

# --- Failed to connect (may or may not be auth-related; show the reason) ---
if [ "$n_fail" -gt 0 ]; then
    echo -e "${RED}✘ FAILED TO CONNECT${NC} — inspect the reason (some are auth, some transport):"
    printf '%s\n' "$FAILED_LINES" | while IFS= read -r line; do
        [ -z "$line" ] && continue
        name="${line%%: *}"
        echo -e "    ${RED}•${NC} $name — $(reason_of "$line")"
    done
    echo -e "    ${CYAN}Tip:${NC} \`claude mcp get <name>\` for detail; if it is auth, \`claude mcp login <name>\`."
    echo ""
fi

# --- Pending approval (.mcp.json servers not yet approved for this project) ---
if [ "$n_pend" -gt 0 ]; then
    echo -e "${YELLOW}⏸ PENDING APPROVAL${NC} — approve the project's .mcp.json before it connects:"
    names_with 'Pending approval' | while IFS= read -r name; do
        [ -z "$name" ] && continue
        echo -e "    ${YELLOW}•${NC} $name"
    done
    echo -e "    ${CYAN}Tip:${NC} start Claude Code in this project and approve the server, or \`claude mcp get <name>\`."
    echo ""
fi

# --- Connected (nothing to do) ---
if [ "$n_conn" -gt 0 ]; then
    echo -e "${GREEN}✔ CONNECTED${NC} (no action needed):"
    printf '%s\n' "$CONNECTED_NAMES" | while IFS= read -r name; do
        [ -z "$name" ] && continue
        echo -e "    ${GREEN}•${NC} $name"
    done
    echo ""
fi

# --- claude.ai org connectors gated by the managed gateway (org-admin lever) ---
CONNECTORS_WARN="$(printf '%s\n' "$CLEAN" | grep -i 'connectors are disabled')"
if [ -n "$CONNECTORS_WARN" ]; then
    echo -e "${YELLOW}⚠ claude.ai org connectors are DISABLED${NC}"
    echo -e "    A model auth source (ANTHROPIC_BASE_URL / gateway in managed-settings.json)"
    echo -e "    takes precedence over your claude.ai login, so org connectors won't load."
    echo -e "    This is NOT a per-server login — it is set by enterprise managed settings"
    echo -e "    (root-owned) and only your Claude Code / gateway admins can change it."
    echo ""
fi

if [ "$server_lines" = "" ]; then
    echo -e "${CYAN}No MCP servers reported by \`claude mcp list\`.${NC}"
    echo ""
fi

if [ "$SHOW_RAW" -eq 1 ]; then
    show_separator
    echo -e "${CYAN}Raw \`claude mcp list\` output:${NC}"
    printf '%s\n' "$CLEAN"
    echo ""
fi

exit 0
