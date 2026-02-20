#!/bin/bash
# gbx:test:bundle-databricks - Push runner notebook to Databricks and run Essential bundle ON the cluster (default). Use --local to run on host.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "📦 GeoBrix: Test bundle on Databricks"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:bundle-databricks${NC} ${YELLOW}[--local] [--no-wait] [--debug] [--help]${NC}"
    echo ""
    echo -e "${CYAN}Default (push and run on cluster):${NC}"
    echo -e "  Uploads the runner notebook (${YELLOW}GBX_BUNDLE_RUNNER_NOTEBOOK_PATH${NC}) and runs it as a one-off job on ${YELLOW}CLUSTER_ID${NC}."
    echo -e "  The bundle executes ${GREEN}on the cluster${NC}; Volume paths work without token/Volume API from your machine."
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--local${NC}    Run the bundle on this machine instead (requires token with UC volume access)."
    echo -e "  ${GREEN}--no-wait${NC}  Submit the job and exit without waiting (default: wait for completion)."
    echo -e "  ${GREEN}--debug${NC}    With --local: enable GBX_BUNDLE_DEBUG."
    echo ""
    echo -e "${CYAN}Config:${NC}"
    echo -e "  Copy ${YELLOW}notebooks/tests/databricks_cluster_config.example.env${NC} to ${YELLOW}notebooks/tests/databricks_cluster_config.env${NC}"
    echo -e "  Set: DATABRICKS_HOST, DATABRICKS_TOKEN (or profile), CLUSTER_ID, GBX_BUNDLE_VOLUME_*"
    echo -e "  Optional: GBX_BUNDLE_RUNNER_NOTEBOOK_PATH, GBX_BUNDLE_WHEEL_VOLUME_PATH"
    echo ""
}

MODE="cluster"
NO_WAIT=""
DEBUG=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --local)
            MODE="local"
            shift
            ;;
        --no-wait)
            NO_WAIT="--no-wait"
            shift
            ;;
        --debug)
            DEBUG=1
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            show_help
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT" || exit 1

if [[ "$MODE" == "cluster" ]]; then
    python notebooks/tests/push_and_run_bundle_on_cluster.py $NO_WAIT
    exit $?
fi

export GBX_NOTEBOOK_TESTS_ALLOW_HOST=1
[[ -n "$DEBUG" ]] && export GBX_BUNDLE_DEBUG=1
PYTEST_OPTS="-s -v"
[[ -n "$DEBUG" ]] && PYTEST_OPTS="-s -vv"
python -m pytest notebooks/tests/test_bundle_on_databricks.py $PYTEST_OPTS
exit $?
