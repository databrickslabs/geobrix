#!/bin/bash
# gbx:prompt-session - Output agent context rule for the agent to review

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
CONTEXT_FILE="$PROJECT_ROOT/.cursor/rules/00-agent-context.mdc"

show_help() {
    cat << EOF
gbx:prompt-session - Paste agent context for review

Outputs the contents of .cursor/rules/00-agent-context.mdc so the agent
can review rules layout, topic→subagent mapping, and quick reference.

USAGE:
    bash .cursor/commands/gbx-prompt-session.sh [OPTIONS]

OPTIONS:
    --help    Display this help message

EXAMPLES:
    bash .cursor/commands/gbx-prompt-session.sh

EOF
    exit 0
}

case "${1:-}" in
    --help|-h) show_help ;;
esac

if [[ ! -f "$CONTEXT_FILE" ]]; then
    echo "Error: $CONTEXT_FILE not found." >&2
    exit 1
fi

cat "$CONTEXT_FILE"
