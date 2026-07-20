#!/bin/bash
# gbx:app:seed-genie - Seed the Genie Space's instructions + example SQLs from
# docs/GENIE-SPACE.md via the Genie spaces update API. The DAB genie_space
# resource only re-applies the table set on deploy (wiping curated instructions
# and examples), so this restores that curation programmatically — no manual
# UI pasting. Idempotent: content-hash ids mean re-runs converge.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

APP_DIR="$PROJECT_ROOT/apps/genie_map"

# The Genie Space id + warehouse are resolved per-environment from `databricks
# bundle summary` (the space the bundle actually deployed), so this works for
# WHATEVER workspace you deploy to — no hardcoded ids and no profile assumptions
# (oauth-fe is perpetually at capacity; deploys go to other envs). --profile is
# still required (which workspace to talk to); --space-id / --warehouse override
# the bundle-resolved values if ever needed.
PROFILE=""
SPACE_ID=""
WAREHOUSE_ID=""
DRY_RUN=""
LOG_ARG=""

show_help() {
  cat <<EOF
$(print_banner "🌱 GeoBrix: Seed Genie Space")

Seed the Genie Space instructions + example SQL queries from the tracked source
of truth (apps/genie_map/docs/GENIE-SPACE.md, Paste blocks A and B) via the
Genie spaces update API. Run this AFTER every 'gbx:app:deploy' — the DAB
genie_space resource re-applies only the table set and wipes the curated
instructions/examples, and this restores them without manual UI pasting.

USAGE:
    bash scripts/commands/gbx-app-seed-genie.sh --profile <p> [OPTIONS]

OPTIONS:
    --profile <p>       CLI profile of the workspace you deployed to (REQUIRED)
    --space-id <id>     Genie Space id (default: resolved from 'bundle summary')
    --warehouse <id>    SQL warehouse id (default: resolved from 'bundle summary')
    --dry-run           Parse + print what would be written; do not PATCH
    --log <path>        Tee output to a log file (filename -> test-logs/<name>)
    --help, -h          Show this help

By default the Genie Space id and warehouse are read from
'databricks bundle summary' for the given profile — i.e. the space this bundle
deployed to THAT workspace — so this command follows your deploy target
wherever it is (oauth-fe is perpetually at capacity, so deploys go elsewhere).

EXAMPLES:
    bash scripts/commands/gbx-app-seed-genie.sh --profile genie-map-env
    bash scripts/commands/gbx-app-seed-genie.sh --profile genie-map-env --dry-run
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --help|-h) show_help ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --space-id) SPACE_ID="$2"; shift 2 ;;
    --warehouse) WAREHOUSE_ID="$2"; shift 2 ;;
    --dry-run) DRY_RUN="--dry-run"; shift ;;
    --log) LOG_ARG="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; echo "Use --help for usage information"; exit 1 ;;
  esac
done

if [ -z "$PROFILE" ]; then
  echo "❌ --profile is required (the CLI profile of the workspace you deployed to)."
  echo "   Use --help for usage information."
  exit 1
fi

LOG_PATH="$(resolve_log_path "$LOG_ARG")"
[ -n "$LOG_PATH" ] && setup_log_file "$LOG_PATH"

# Resolve the Genie Space id + warehouse from the deployed bundle unless the
# caller pinned them. `bundle summary` reflects the space THIS bundle deployed
# to THIS workspace, so seeding always targets the right space per-environment.
if [ -z "$SPACE_ID" ] || [ -z "$WAREHOUSE_ID" ]; then
  SUMMARY="$(cd "$APP_DIR" && databricks bundle summary --profile "$PROFILE" -o json 2>/dev/null)"
  if [ -z "$SUMMARY" ]; then
    echo "❌ Could not read 'databricks bundle summary' for profile '$PROFILE'."
    echo "   Deploy first (gbx:app:deploy), or pass --space-id and --warehouse explicitly."
    exit 1
  fi
  RESOLVED="$(printf '%s' "$SUMMARY" | python3 -c "
import sys, json
d = json.load(sys.stdin)
gs = d.get('resources', {}).get('genie_spaces', {}).get('vapor_eyes_genie', {})
print((gs.get('id') or ''), (gs.get('warehouse_id') or d.get('variables', {}).get('warehouse_id', {}).get('value') or ''))
")"
  [ -z "$SPACE_ID" ] && SPACE_ID="$(echo "$RESOLVED" | awk '{print $1}')"
  [ -z "$WAREHOUSE_ID" ] && WAREHOUSE_ID="$(echo "$RESOLVED" | awk '{print $2}')"
fi

if [ -z "$SPACE_ID" ] || [ -z "$WAREHOUSE_ID" ]; then
  echo "❌ Could not resolve Genie Space id / warehouse from the bundle."
  echo "   Pass --space-id and --warehouse explicitly."
  exit 1
fi

echo "Seeding Genie Space $SPACE_ID (warehouse $WAREHOUSE_ID) on profile $PROFILE"

python3 "$APP_DIR/scripts/seed_genie_space.py" \
  --space-id "$SPACE_ID" \
  --warehouse-id "$WAREHOUSE_ID" \
  --profile "$PROFILE" \
  --docs "$APP_DIR/docs/GENIE-SPACE.md" \
  $DRY_RUN
