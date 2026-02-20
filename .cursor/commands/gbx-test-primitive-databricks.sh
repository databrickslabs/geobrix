#!/bin/bash
# gbx:test:primitive-databricks - Push primitive Volume-test notebook to Databricks and run it on the cluster.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Primitive Volume tests on Databricks"
    echo "Usage: gbx:test:primitive-databricks [--no-wait] [--help]"
    echo "Uploads the primitive runner notebook and runs it on CLUSTER_ID."
    echo "Options: --no-wait  Submit and exit without waiting."
    echo "Config: notebooks/tests/databricks_cluster_config.env (CLUSTER_ID, GBX_BUNDLE_VOLUME_*, optional GBX_RUNNER_DIR, GBX_PRIMITIVE_RUNNER_NOTEBOOK)"
}

NO_WAIT=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-wait) NO_WAIT="--no-wait"; shift ;;
        --help|-h) show_help; exit 0 ;;
        *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
    esac
done

cd "$PROJECT_ROOT" || exit 1
python notebooks/tests/push_and_run_primitive_on_cluster.py $NO_WAIT
exit $?
