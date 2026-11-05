#!/bin/bash
# gbx:test:uc-integration - Run the Unity Catalog integration gate on Databricks and write its evidence file
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

CONFIG="examples/tests/databricks_cluster_config.env"

show_help() {
    show_banner "GeoBrix: Unity Catalog integration gate"
    cat <<'HELP'
Build the env 6 wheel, stage it with its JAR into a fresh Volume folder, install the Unity Catalog
functions into a scratch schema from a Serverless environment 6 job, run the UC oracle slice and the
example files on a SQL warehouse against that schema, drop the schema, and write the evidence file
test-logs/uc-integration/evidence.json. Run it before a pull request or a push to main.

Usage: bash scripts/commands/gbx-test-uc-integration.sh --profile <name> [options]

Options:
  --profile <name>        Databricks config profile (required; never chosen for you: see
                          `databricks auth profiles`)
  --warehouse-id ID       SQL warehouse (default: GBX_UC_WAREHOUSE_ID in
                          examples/tests/databricks_cluster_config.env)
  --catalog C             Catalog of the scratch schema (default geospatial_docs)
  --volroot DIR           Volume folder that holds uc-it/<sha12>/ (default
                          /Volumes/geospatial_docs/geobrix/sample-data)
  --slice core|full       Oracle tables: grid and vector (core, default), or also the raster tables (full)
  --shards N              Parallel warehouse sessions for the slice (default 8)
  --rebuild               Build the wheel even when dist/env6 holds one that fits HEAD
  --keep                  Keep the schema and the Volume folder
  --install-timeout-min N Time limit of the install job (default 90)
  --smoke SCHEMA          Wiring check on an existing install in SCHEMA: no build, stage, install or cleanup;
                          12 slice statements and example files 00 and 01; never writes evidence.json
  --dry-run               Print the plan and counts; no build, no workspace call
  --log <path>            Write output to a log file (filename -> test-logs/<name>)
  --help, -h              Show this help

Behaviour:
  Refuses to run when a tracked file under python/geobrix/src, examples/uc-functions, src/main/scala or
  pom.xml differs from HEAD. The schema is geobrix_uc_it_<sha12>. Docker is needed to build the wheel;
  a wheel in dist/env6 is reused when no wheel input changed since its commit.
  Exit code: 0 when the result is pass, 1 on fail or refusal, 2 for bad options.
  Known failures: scripts/ci/uc_integration_known_failures.txt (empty).

Example:
  bash scripts/commands/gbx-test-uc-integration.sh --profile oauth-fe
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
    --profile|--catalog|--volroot|--slice|--shards|--install-timeout-min|--smoke)
        [[ $# -ge 2 ]] || { echo "Option $1 needs a value" >&2; exit 2; }
        ARGS+=("$1" "$2"); shift 2 ;;
    --rebuild|--keep|--dry-run) ARGS+=("$1"); shift ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 2 ;;
esac; done

cd "$PROJECT_ROOT" || exit 1
show_banner "GeoBrix: Unity Catalog integration gate"
setup_log_file "$LOG_PATH"

# The warehouse id may live in the (untracked) cluster config; read that one key, never source the file.
if [[ "$HAVE_WAREHOUSE" == false && -f "$CONFIG" ]]; then
    WAREHOUSE_ID="$(grep -E '^GBX_UC_WAREHOUSE_ID=' "$CONFIG" | tail -1 | cut -d= -f2- | tr -d '[:space:]"'"'")"
    [[ -n "$WAREHOUSE_ID" ]] && ARGS+=(--warehouse-id "$WAREHOUSE_ID")
fi

# The driver needs databricks-sdk and Python >= 3.11: see gbx_python_with_sdk in common.sh.
gbx_python_with_sdk || exit 1
"${GBX_SDK_PYTHON[@]}" examples/tests/uc_integration.py "${ARGS[@]}"
exit $?
