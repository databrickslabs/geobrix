#!/bin/bash
# gbx:app:deploy - Build + deploy the Genie Map app to Databricks Apps via DAB

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Source common utilities
source "$SCRIPT_DIR/common.sh"

APP_DIR="$PROJECT_ROOT/apps/genie_map"
ENV_FILE="$APP_DIR/databricks.env"

# Defaults
PROFILE="oauth-fe"
LOG_ARG=""

# Help message
show_help() {
    cat << EOF
$(print_banner "🚀 GeoBrix: Deploy Genie Map")

Build the app (pnpm build) and deploy to Databricks Apps via the DAB bundle.

USAGE:
    bash scripts/commands/gbx-app-deploy.sh [OPTIONS]

OPTIONS:
    --profile <p>    Databricks CLI profile (default: oauth-fe)
    --log <path>     Tee output to a log file (filename → test-logs/<name>)
    --help, -h       Display this help message

NOTES:
    - Requires apps/genie_map/databricks.env (copy from databricks.env.example
      and fill it in). It is sourced into the shell so the build picks up the vars.
    - Runs 'pnpm install' automatically if node_modules is missing.
    - Deploys the bundle at apps/genie_map (databricks.yml), then runs the app.

EXAMPLES:
    bash scripts/commands/gbx-app-deploy.sh
    bash scripts/commands/gbx-app-deploy.sh --profile oauth-fe --log deploy.log
EOF
    exit 0
}

# Parse arguments (before the env check so --help works without databricks.env)
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        --log)
            LOG_ARG="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Optional logging
LOG_PATH="$(resolve_log_path "$LOG_ARG")"
[ -n "$LOG_PATH" ] && setup_log_file "$LOG_PATH"

cd "$APP_DIR" || exit 1

# Require + source the populated env file
[ -f "$ENV_FILE" ] || { echo "❌ apps/genie_map/databricks.env missing — copy databricks.env.example and fill it"; exit 1; }
set -a; source "$ENV_FILE"; set +a

# Install deps if needed
[ -d node_modules ] || pnpm install

# Build (fail → abort before deploy)
pnpm build || exit 1

# Deploy + run the bundle (databricks.yml lives at the app root)
cd "$APP_DIR" || exit 1
databricks bundle deploy --profile "$PROFILE" || exit 1
databricks bundle run genie_map --profile "$PROFILE"
