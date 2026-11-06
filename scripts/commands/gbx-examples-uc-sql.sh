#!/bin/bash
# gbx:examples:uc-sql - Run the Unity Catalog SQL examples on a SQL warehouse, and upload them to your workspace
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

CONFIG="examples/tests/databricks_cluster_config.env"

show_help() {
    show_banner "GeoBrix: Unity Catalog SQL examples"
    cat <<'HELP'
Run every file of examples/uc-functions/*.sql, in name order, on a SQL warehouse. A statement must
succeed, and a query (SELECT or WITH) must return at least one row. The results of each file go to
test-logs/uc-examples/<file>.json. With --upload, the files are also imported into your workspace
folder /Workspace/Users/<you>/geobrix/uc-function-examples/ (the user comes from `current-user me`).

Usage: bash scripts/commands/gbx-examples-uc-sql.sh --profile <name> [options]

Options:
  --profile <name>       Databricks config profile (required; never chosen for you: see
                         `databricks auth profiles`)
  --warehouse-id ID      SQL warehouse (default: GBX_UC_WAREHOUSE_ID in
                         examples/tests/databricks_cluster_config.env)
  --catalog C            Catalog the functions are installed in (default geospatial_docs)
  --schema S             Schema the functions are installed in (default geobrix_uc)
  --dir <folder>         Folder of example files (default examples/uc-functions)
  --files <prefixes>     Comma list of file name prefixes to run, such as 00,05 (default: all)
  --out-dir <folder>     Folder for the per-file result JSON (default test-logs/uc-examples)
  --timeout-s N          Cancel a statement still running after N seconds (default 600)
  --parallel N           Run N example files side by side (default 1); file 00, the setup, always runs first
  --upload               After a clean run, import the files into your workspace folder
  --upload-only          Import the files and run nothing
  --ws-dir <path>        Workspace folder to import into (default the folder above)
  --dry-run              Parse the files and print the plan: no workspace call, no auth
  --log <path>           Write output to a log file (filename -> test-logs/<name>)
  --help, -h             Show this help

Exit code: 0 when every statement succeeded and every query returned a row, 1 otherwise, 2 for bad options.
Install the functions first: see docs/docs/api/uc-functions.mdx.

Example:
  bash scripts/commands/gbx-examples-uc-sql.sh --profile oauth-fe --upload
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
    --profile|--catalog|--schema|--dir|--files|--out-dir|--timeout-s|--ws-dir|--parallel)
        [[ $# -ge 2 ]] || { echo "Option $1 needs a value" >&2; exit 2; }
        ARGS+=("$1" "$2"); shift 2 ;;
    --upload|--upload-only|--dry-run) ARGS+=("$1"); shift ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 2 ;;
esac; done

cd "$PROJECT_ROOT" || exit 1
show_banner "GeoBrix: Unity Catalog SQL examples"
setup_log_file "$LOG_PATH"

# The warehouse id may live in the (untracked) cluster config; read that one key, never source the file.
if [[ "$HAVE_WAREHOUSE" == false && -f "$CONFIG" ]]; then
    WAREHOUSE_ID="$(grep -E '^GBX_UC_WAREHOUSE_ID=' "$CONFIG" | tail -1 | cut -d= -f2- | tr -d '[:space:]"'"'")"
    [[ -n "$WAREHOUSE_ID" ]] && ARGS+=(--warehouse-id "$WAREHOUSE_ID")
fi

# The launcher needs databricks-sdk and Python >= 3.11: see gbx_python_with_sdk in common.sh.
gbx_python_with_sdk || exit 1
"${GBX_SDK_PYTHON[@]}" examples/tests/uc_examples_sql.py "${ARGS[@]}"
exit $?
