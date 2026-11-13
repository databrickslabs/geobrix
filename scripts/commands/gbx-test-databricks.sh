#!/bin/bash
# gbx:test:databricks - Run GeoBrix's test shards (CI's) on warm Databricks pool clusters, side by side.

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Tests on Databricks Pool Clusters"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:test:databricks${NC} ${YELLOW}--profile <name> --purpose \"<why>\" [--shards <spec>] [options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--profile <name>${NC}       Databricks profile (required on a launch; never picked for you)"
    echo -e "  ${GREEN}--purpose \"<why>\"${NC}      Required on a launch; heads each runner notebook and rides on the run name"
    echo -e "  ${GREEN}--shards <spec>${NC}        affected (default: the files and tables a change reaches), all-light, all-heavy, or names/groups (\"pyrx\", \"zarr 2/4\"), comma separated"
    echo -e "  ${GREEN}--base <rev>${NC}           For affected: files changed between this and --sha pick the tests, through the import graph (default origin/streamline/0.6.0)"
    echo -e "  ${GREEN}--allow-broad${NC}          Let affected run a selection over 25% of the light bank (refused without it)"
    echo -e "  ${GREEN}--sha <rev>${NC}            Commit to test (default HEAD); uncommitted edits are not in the run"
    echo -e "  ${GREEN}--wheel <path>${NC}         env6 wheel for dependencies and native payload (default dist/env6/)"
    echo -e "  ${GREEN}--jar-path <path>${NC}      JAR of the heavy shards, or its folder (default python/geobrix/lib/)"
    echo -e "  ${GREEN}--max-parallel <n>${NC}     Runners per cluster, each taking shards from one queue (default: the node's cores / 8, so 4 on 32 vCPU)"
    echo -e "  ${GREEN}--clusters <n>${NC}         Clusters to spread over (default: every free cluster the pool has; a busy pool gives fewer)"
    echo -e "  ${GREEN}--split auto|off${NC}       auto (default) cuts shards longer than --target-min by file; off keeps CI's shards"
    echo -e "  ${GREEN}--target-min <n>${NC}       Length a cut shard aims at (default: chosen for about 3.5 chunks per slot, at least 3.5 min)"
    echo -e "  ${GREEN}--timings <path>${NC}       Timing history every run merges into; it cuts and orders the queue (default test-logs/databricks-tests/timings-history.json)"
    echo -e "  ${GREEN}--serverless${NC}           Run light shards on Serverless env 6: 4 runner tasks by default, chunks of 3-5 min, its own timings"
    echo -e "  ${GREEN}--fresh-cluster${NC}        Use a new cluster that ends with the run, not the pools"
    echo -e "  ${GREEN}--pool-light <name>${NC}    Default gbx-test-1n (no JAR)"
    echo -e "  ${GREEN}--pool-jar <name>${NC}      Default gbx-test-1n-jar (with the JAR)"
    echo -e "  ${GREEN}--node-type <t>${NC}        Node of a pool cluster this run makes (default m5d.8xlarge)"
    echo -e "  ${GREEN}--shard-cap-min <n>${NC}    Stop a shard after n minutes (default 45)"
    echo -e "  ${GREEN}--stall-min <n>${NC}        Stop a shard when no test finished for n minutes (default 10)"
    echo -e "  ${GREEN}--start-cap-min <n>${NC}    Cancel when no shard has beaten n minutes after the submit (default 25)"
    echo -e "  ${GREEN}--pool-wait-min <n>${NC}    Wait up to n minutes for a free pool cluster (default 30)"
    echo -e "  ${GREEN}--first-failed <k>${NC}     Stop the run when the first k chunks to finish all failed (default 2; 0 off)"
    echo -e "  ${GREEN}--max-failed-chunks <n>${NC} Stop the run at n failed chunks (default 3; 0 off)"
    echo -e "  ${GREEN}--max-failures <n>${NC}     Stop the run at n failed or erroring tests (default 20; 0 off)"
    echo -e "  ${GREEN}--maxfail <n>${NC}          pytest --maxfail in each chunk (default 3; 0 off)"
    echo -e "  ${GREEN}--no-fail-fast${NC}         Run every chunk to its end: no stop rule, no --maxfail (the smoke stays)"
    echo -e "  ${GREEN}--no-smoke${NC}             Start the whole queue at once, without the smoke chunk first"
    echo -e "  ${GREEN}--serverless-cap <n>${NC}   Runner tasks of gbx Serverless test runs at once in the workspace; a launch waits for room (default 8; 0 off)"
    echo -e "  ${GREEN}--start-chunks <n>${NC}     Runners that start at once; the rest wait for n passed chunks (default 2; 0 all, pools only)"
    echo -e "  ${GREEN}--test-timeout <s>${NC}     Fail a test that runs longer than s seconds (default 120; 0 off)"
    echo -e "  ${GREEN}--overrun-x <x>${NC}        Stop a chunk at x times its estimate, at least 3 min (default 2; 0 off)"
    echo -e "  ${GREEN}--max-wall-min <n>${NC}     Stop the run at n minutes of wall (default 20; 0 off)"
    echo -e "  ${GREEN}--rerun-failed <run id>${NC} Run only the tests that failed in that earlier run (--maxfail 1)"
    echo -e "  ${GREEN}--pip-extra \"<reqs>\"${NC}  More pip requirements the tests import"
    echo -e "  ${GREEN}--env KEY=VAL${NC}          An environment variable every shard sets (repeatable; e.g. GBX_CAP_ARROW_TILE_TRANSPORT=0)"
    echo -e "  ${GREEN}--spark-conf KEY=VAL${NC}   A Spark conf every shard's session sets before its tests (repeatable)"
    echo -e "  ${GREEN}--run-id <id>${NC}          Default tst-<date>-<time>-<4 hex>"
    echo -e "  ${GREEN}--keep-stage${NC}           Keep the Volume folder, the notebooks and the queue table"
    echo -e "  ${GREEN}--explain-only${NC}         Print the plan and stop; no network"
    echo -e "  ${GREEN}--log <path>${NC}           Write output to a log file"
    echo -e "  ${GREEN}--help${NC}                 Show this help"
    echo ""
    echo -e "${CYAN}Example:${NC}"
    echo -e "  ${YELLOW}gbx:test:databricks --profile oauth-fe --shards affected --purpose \"pre-push pass of the dirs I touched\"${NC}"
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
        --profile|--purpose|--shards|--base|--sha|--wheel|--jar-path|--max-parallel|--clusters|--split|--target-min|--timings|--pool-light|--pool-jar|--node-type|--shard-cap-min|--stall-min|--start-cap-min|--pool-wait-min|--pip-extra|--env|--spark-conf|--run-id|--first-failed|--max-failed-chunks|--max-failures|--maxfail|--start-chunks|--serverless-cap|--test-timeout|--overrun-x|--max-wall-min|--rerun-failed)
            [[ $# -ge 2 ]] || { echo -e "${RED}Option $1 needs a value${NC}" >&2; exit 2; }
            PASS_ARGS+=("$1" "$2")
            shift 2
            ;;
        --serverless|--fresh-cluster|--keep-stage|--explain-only|--allow-broad|--no-fail-fast|--no-smoke)
            PASS_ARGS+=("$1")
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}" >&2
            exit 2
            ;;
    esac
done

# Refuse a launch with no purpose before anything else (the launcher checks too; --explain-only may omit it).
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
show_banner "GeoBrix: Tests on Databricks Pool Clusters"
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
    echo "Leasing Databricks pool clusters (this costs compute)..."
    gbx_python_with_sdk || exit 1
fi
"${GBX_SDK_PYTHON[@]}" "$PROJECT_ROOT/examples/tests/push_and_run_tests_on_cluster.py" "${PASS_ARGS[@]}"
exit $?
