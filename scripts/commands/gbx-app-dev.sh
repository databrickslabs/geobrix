#!/bin/bash
# gbx:app:dev - Run the Genie Map app locally (pnpm dev)

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

APP_DIR="$PROJECT_ROOT/apps/genie_map"
ENV_FILE="$APP_DIR/databricks.env"

# Help message
show_help() {
    cat << EOF
$(print_banner "🗺️  GeoBrix: Genie Map dev server")

Run the Genie Map app locally with hot reload (client :5173, server :3000).

USAGE:
    bash scripts/commands/gbx-app-dev.sh [OPTIONS]

OPTIONS:
    --help, -h    Display this help message

NOTES:
    - Requires apps/genie_map/databricks.env (copy from databricks.env.example
      and fill it in). It is sourced into the shell so Vite's loadEnv('') and
      the server pick up the vars.
    - Runs 'pnpm install' automatically if node_modules is missing.
    - Ctrl+C to stop.

EXAMPLES:
    bash scripts/commands/gbx-app-dev.sh
EOF
    exit 0
}

# Parse arguments (before the env check so --help works without databricks.env)
while [[ $# -gt 0 ]]; do
    case $1 in
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

cd "$APP_DIR" || exit 1

# Require + source the populated env file
[ -f "$ENV_FILE" ] || { echo "❌ apps/genie_map/databricks.env missing — copy databricks.env.example and fill it"; exit 1; }
set -a; source "$ENV_FILE"; set +a

# Install deps if needed
[ -d node_modules ] || pnpm install

exec pnpm dev
