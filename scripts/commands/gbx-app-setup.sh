#!/bin/bash
# gbx:app:setup - One-time, idempotent setup for the Genie Map app that the DAB
# bundle can't express: grant the app's service principal access to the gold
# catalog/schema, verify the required UC secrets exist, and print the manual
# Genie-UI curation step. Run AFTER `gbx:app:deploy` has created the app.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

APP_DIR="$PROJECT_ROOT/apps/genie_map"

# Defaults (reference deployment). Override via flags for another workspace.
PROFILE="genie-map-env"
APP_NAME="genie-map"
WAREHOUSE_ID="13e2ed4a49e74f6c"
CATALOG="serverless_stable_genie_map_catalog"
SCHEMA="vapor_eyes_lf"

show_help() {
  cat <<EOF
$(print_banner "🛠️  GeoBrix: Genie Map setup")

Idempotent setup for the parts of Genie Map that the DAB bundle cannot declare:
  - grants the app's service principal USE CATALOG / USE SCHEMA / SELECT on the
    gold schema (without these the viewport queries + Genie return 403/insufficient
    privileges)
  - verifies the Earthdata + Carbon Mapper UC secrets exist (pipeline needs them)
  - prints the manual Genie-Space UI curation step (docs/GENIE-SPACE.md)

USAGE:
    bash scripts/commands/gbx-app-setup.sh [OPTIONS]

OPTIONS:
    --profile <p>       CLI profile (default: $PROFILE)
    --app <name>        App name (default: $APP_NAME)
    --warehouse <id>    SQL warehouse id (default: $WAREHOUSE_ID)
    --catalog <c>       Gold catalog (default: $CATALOG)
    --schema <s>        Gold schema (default: $SCHEMA)
    --help              Show this help

EXAMPLE:
    bash scripts/commands/gbx-app-setup.sh --profile genie-map-env
EOF
  exit 0
}

while [ $# -gt 0 ]; do
  case "$1" in
    --help|-h) show_help ;;
    --profile) PROFILE="$2"; shift 2 ;;
    --app) APP_NAME="$2"; shift 2 ;;
    --warehouse) WAREHOUSE_ID="$2"; shift 2 ;;
    --catalog) CATALOG="$2"; shift 2 ;;
    --schema) SCHEMA="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

set -e
print_banner "🛠️  Genie Map setup (profile=$PROFILE, ${CATALOG}.${SCHEMA})"

# --- 1. Resolve the app's service principal ---------------------------------
echo "→ Resolving app service principal for '$APP_NAME'..."
SP=$(databricks apps get "$APP_NAME" -p "$PROFILE" -o json \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('service_principal_client_id') or d.get('service_principal_id') or '')")
if [ -z "$SP" ]; then
  echo "❌ Could not resolve the app SP. Has the app been deployed (gbx:app:deploy)?"
  exit 1
fi
echo "   app SP = $SP"

# --- 2. Grant the SP access to the gold catalog/schema ----------------------
run_sql() {
  databricks api post /api/2.0/sql/statements -p "$PROFILE" \
    --json "{\"warehouse_id\":\"$WAREHOUSE_ID\",\"wait_timeout\":\"30s\",\"statement\":\"$1\"}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); s=d.get('status',{}); e=s.get('error'); print('   '+('OK' if not e else 'ERR: '+e.get('message','')[:120]))"
}
echo "→ Granting the app SP access to $CATALOG.$SCHEMA (idempotent)..."
run_sql "GRANT USE CATALOG ON CATALOG $CATALOG TO \`$SP\`"
run_sql "GRANT USE SCHEMA ON SCHEMA $CATALOG.$SCHEMA TO \`$SP\`"
run_sql "GRANT SELECT ON SCHEMA $CATALOG.$SCHEMA TO \`$SP\`"

# --- 3. Verify the UC secrets exist -----------------------------------------
echo "→ Checking UC secrets in $CATALOG.$SCHEMA..."
SECRETS=$(databricks secrets-uc list-secrets --catalog-name "$CATALOG" --schema-name "$SCHEMA" -p "$PROFILE" -o json 2>/dev/null \
  | python3 -c "import sys,json;
try:
  d=json.load(sys.stdin); s=d if isinstance(d,list) else d.get('secrets',[]); print(' '.join(sorted(x.get('name','') for x in s)))
except: print('')" )
for want in earthdata_token carbon_mapper_token; do
  if echo "$SECRETS" | grep -qw "$want"; then echo "   ✓ $want present"; else
    echo "   ⚠ $want MISSING — create it with:"
    echo "     databricks secrets-uc create-secret $want $CATALOG $SCHEMA \"<TOKEN>\" -p $PROFILE"
  fi
done

set +e
# --- 4. Print the manual Genie-UI curation step -----------------------------
cat <<EOF

────────────────────────────────────────────────────────────────────────────
MANUAL STEP (not automatable): curate the Genie Space in the UI.
The bundle creates the space + table set + a short description, but the rich
instructions and example SQL must be pasted in the Genie Space editor.
  1. Open the "Vapor-Eyes — Permian Methane (Genie Map)" space.
  2. Instructions tab → paste the "General instructions" block from:
       apps/genie_map/docs/GENIE-SPACE.md
  3. Example SQL queries → add each verified example from the same doc.
See docs/SETUP.md for the full runbook.
────────────────────────────────────────────────────────────────────────────
EOF
echo "✅ Setup complete."
