#!/bin/bash
# gbx:uc:sql - Run a file of `-- cid: <id>` statements on a SQL warehouse (the Statement API) and write the results
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

CONFIG="examples/tests/databricks_cluster_config.env"

show_help() {
    show_banner "GeoBrix: UC statements on a SQL warehouse"
    cat <<'HELP'
Run a file of statements on a SQL warehouse through the Statement API, time each one, and write
what came back. Statements follow a `-- cid: <id>` line; the oracle's UC statements come from
bench.oracles.uc_sql, and bench.oracles.check_uc_results judges the file written by --out.

Usage: bash scripts/commands/gbx-uc-sql.sh --file <sql> --profile <name> [options]

Options:
  --file <sql>           Statements, each after a `-- cid: <id>` line (required)
  --profile <name>       Databricks config profile (required; never chosen for you: see
                         `databricks auth profiles`)
  --warehouse-id ID      SQL warehouse (default: GBX_UC_WAREHOUSE_ID in
                         examples/tests/databricks_cluster_config.env)
  --catalog C            Default catalog for the statements
  --schema S             Default schema for the statements
  --out <json>           Write {cid: {"value": ..., "wall_s": [...]} | {"error": ...}} here
  --repeat N             Run each statement N times and record every wall time (default 1)
  --no-decode-binary     Keep BINARY cells as the API's base64 text (decoded to bytes by default)
  --dry-run              Parse the file and print the plan: no workspace call, no auth
  --log <path>           Write output to a log file (filename -> test-logs/<name>)
  --help, -h             Show this help

Environment:
  GBX_PYRX_VENV=<dir>    Run on this venv instead of .venv-pyrx (Python 3.11+; databricks-sdk is
                         added on top when the venv lacks it)

Example:
  bash scripts/commands/gbx-uc-sql.sh --profile oauth-fe --file .superpowers/streamline/uc/mvp_s.sql \
    --out .superpowers/streamline/uc/mvp_s.json --repeat 3
HELP
}

LOG_PATH=""
HAVE_WAREHOUSE=false
ARGS=()
while [[ $# -gt 0 ]]; do case $1 in
    --help|-h) show_help; exit 0 ;;
    --log) [[ $# -ge 2 ]] || { echo "Option --log needs a value" >&2; exit 2; }
        LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --warehouse-id) [[ $# -ge 2 ]] || { echo "Option --warehouse-id needs a value" >&2; exit 2; }
        HAVE_WAREHOUSE=true; ARGS+=("$1" "$2"); shift 2 ;;
    --file|--profile|--catalog|--schema|--out|--repeat)
        [[ $# -ge 2 ]] || { echo "Option $1 needs a value" >&2; exit 2; }
        ARGS+=("$1" "$2"); shift 2 ;;
    --no-decode-binary|--dry-run) ARGS+=("$1"); shift ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT" || exit 1
show_banner "GeoBrix: UC statements on a SQL warehouse"
setup_log_file "$LOG_PATH"

# The warehouse id may live in the (untracked) cluster config; read that one key, never source the
# file (it can hold a host and a token).
if [[ "$HAVE_WAREHOUSE" == false && -f "$CONFIG" ]]; then
    WAREHOUSE_ID="$(grep -E '^GBX_UC_WAREHOUSE_ID=' "$CONFIG" | tail -1 | cut -d= -f2- | tr -d '[:space:]"'"'")"
    [[ -n "$WAREHOUSE_ID" ]] && ARGS+=(--warehouse-id "$WAREHOUSE_ID")
fi

# The launcher needs databricks-sdk and Python >= 3.11, and imports the bench package from this
# checkout: see gbx_python_with_sdk in common.sh. Chosen after --help, so help works anywhere.
gbx_python_with_sdk || exit 1
"${GBX_SDK_PYTHON[@]}" examples/tests/uc_sql.py "${ARGS[@]}"
exit $?
