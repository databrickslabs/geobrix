#!/bin/bash
# gbx:test:mutation-databricks - Run every mutant of some kernel modules on a large Databricks node (guidance, never a gate).

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Full-Mutant Guidance Run on Databricks"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:mutation-databricks${NC} ${YELLOW}--family <name> --modules <a,b> --tests \"<paths>\" --purpose \"<why>\" [options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--family <name>${NC}        Names the run and its output folder"
    echo -e "  ${GREEN}--modules <a,b,...>${NC}    Kernel modules under src/databricks/labs/gbx, e.g. pyvx/_validity,pyvx/_cleaning"
    echo -e "  ${GREEN}--tests \"<paths>\"${NC}      pytest args relative to python/geobrix, one quoted string"
    echo -e "  ${GREEN}--purpose \"<why>\"${NC}      Required on a launch; heads the notebook and rides on the run name"
    echo -e "  ${GREEN}--profile <name>${NC}       Databricks profile (default DATABRICKS_CONFIG_PROFILE)"
    echo -e "  ${GREEN}--node-type <t>${NC}        Node of a NEW cluster. Default m5d.16xlarge (64 vCPU)"
    echo -e "  ${GREEN}--pool <name>${NC}          Lease the single-node cluster of this pool (default gbx-test-1n; made once when the pool has none)"
    echo -e "  ${GREEN}--pool-wait-min <n>${NC}    Wait up to n minutes for a free pool cluster (default 30)"
    echo -e "  ${GREEN}--fresh-cluster${NC}        Run on a new job cluster that ends with the run (a cold start every time)"
    echo -e "  ${GREEN}--pip-extra \"<reqs>\"${NC}  More pip requirements the selected tests import (default \"mercantile>=1.2,<2\")"
    echo -e "  ${GREEN}--run-id <id>${NC}          Default mut-<date>-<time>-<4 hex>"
    echo -e "  ${GREEN}--sha <rev>${NC}            Commit to test (default HEAD)"
    echo -e "  ${GREEN}--wheel <path>${NC}         env6 wheel (default dist/env6/)"
    echo -e "  ${GREEN}--table <c.s.t>${NC}        Result table (default <catalog>.<schema>.mutation_results of the bundle Volume)"
    echo -e "  ${GREEN}--oracle-tables <T,T>${NC}  GBX_ORACLE_TABLES for the family harness"
    echo -e "  ${GREEN}--wall-cap-min <n>${NC}     Stop the mutants after n minutes (default 180)"
    echo -e "  ${GREEN}--setup-cap-min <n>${NC}    Stop when no mutant was tried n minutes after the work started (default 30)"
    echo -e "  ${GREEN}--stall-min <n>${NC}        Stop when no mutant finished for n minutes (default 15)"
    echo -e "  ${GREEN}--start-cap-min <n>${NC}    Cancel when the node has not beaten n minutes after the submit (default 20)"
    echo -e "  ${GREEN}--keep-stage${NC}           Keep the Volume folder and the runner notebook"
    echo -e "  ${GREEN}--explain-only${NC}         Print the plan and stop; no network"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to a log file"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Example:${NC}"
    echo -e "  ${YELLOW}gbx:test:mutation-databricks --profile oauth-fe --family pyvx --modules pyvx/_validity,pyvx/_cleaning --tests test/pyvx --purpose \"kill rates of the pyvx kernels\"${NC}"
    echo ""
}

PASS_ARGS=()
LOG_PATH=""
while [[ $# -gt 0 ]]; do
    case $1 in
        --help|-h)
            show_help
            exit 0
            ;;
        --log)
            [[ $# -ge 2 ]] || { echo -e "${RED}Option --log needs a value${NC}" >&2; exit 2; }
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --family|--modules|--tests|--purpose|--profile|--node-type|--run-id|--sha|--wheel|--table|--oracle-tables|--wall-cap-min|--setup-cap-min|--stall-min|--start-cap-min|--pool|--pool-wait-min|--pip-extra)
            [[ $# -ge 2 ]] || { echo -e "${RED}Option $1 needs a value${NC}" >&2; exit 2; }
            PASS_ARGS+=("$1" "$2")
            shift 2
            ;;
        --keep-stage|--explain-only|--fresh-cluster)
            PASS_ARGS+=("$1")
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}" >&2
            exit 2
            ;;
    esac
done

# Refuse a launch with no purpose before anything else (the launcher checks too, and --explain-only may omit it).
HAS_PURPOSE=0; EXPLAIN=0
for ((i = 0; i < ${#PASS_ARGS[@]}; i++)); do
    [[ "${PASS_ARGS[$i]}" == "--purpose" && -n "${PASS_ARGS[$((i + 1))]// /}" ]] && HAS_PURPOSE=1
    [[ "${PASS_ARGS[$i]}" == "--explain-only" ]] && EXPLAIN=1
done
if [[ $HAS_PURPOSE -eq 0 && $EXPLAIN -eq 0 ]]; then
    echo -e "${RED}--purpose \"<why>\" is required on a launch: say what the run tests and why.${NC}" >&2
    exit 2
fi

cd "$PROJECT_ROOT"
show_banner "GeoBrix: Full-Mutant Guidance Run on Databricks"
setup_log_file "$LOG_PATH"

CONFIG="examples/tests/databricks_cluster_config.env"
if GBX_CLUSTER_CONFIG_ENV="$(gbx_resolve_untracked "$CONFIG")" && [[ -f "$GBX_CLUSTER_CONFIG_ENV" ]]; then
    export GBX_CLUSTER_CONFIG_ENV
fi

# --explain-only needs no SDK; a launch does (the launcher imports databricks.sdk).
if [[ $EXPLAIN -eq 1 ]]; then
    VENV="${GBX_PYRX_VENV:-${PROJECT_ROOT}/.venv-pyrx}"
    if [[ -x "${VENV}/bin/python" ]]; then GBX_SDK_PYTHON=("${VENV}/bin/python"); else GBX_SDK_PYTHON=("$(command -v python3)"); fi
else
    echo "Starting a Databricks job cluster (this costs compute)..."
    gbx_python_with_sdk || exit 1
fi
"${GBX_SDK_PYTHON[@]}" "$PROJECT_ROOT/examples/tests/push_and_run_mutation_on_cluster.py" "${PASS_ARGS[@]}"
exit $?
