#!/bin/bash
# gbx:bench:pool — a pool of long-lived bench clusters: up, status, down.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

PASS_ARGS=()
LOG_PATH=""
show_help() {
    show_banner "gbx:bench:pool"
    cat <<'EOT'
A pool caps the active DBR19 bench clusters and starts them on demand, one run per cluster at a time. A launch with
`gbx:bench:cluster --pool <name>` leases an idle one (starting it when it has ended), runs its smoke and its
main run on it, and gives it back: no cluster start per run. The clusters end themselves after 60 idle minutes.

Usage: bash scripts/commands/gbx-bench-pool.sh <up|status|down> --name <pool> [options]
  up       --n <cap>             the most clusters active at once; records the cap and the cluster shape
           --warm <k>            clusters to start now (default 1; 0 starts none)
           --spark-version <v>   default 19.x-scala2.13
           --node-type <t>       default m5d.2xlarge
           --workers <n>         default 4
           --single-node         the driver alone (--workers is ignored)
  status                         each pool cluster: state, idle time, jar fingerprint, lease holder
  down     [--delete]            terminate the pool's clusters (--delete removes them)
  --profile <name>               Databricks profile (default DATABRICKS_CONFIG_PROFILE)
  --log <path>                   write output to a log file

Only clusters tagged gbx_bench_pool=<name> are ever touched; cluster 1016-152458-i5qbli6q is on a deny-list.
The lease files live under $GBX_BUNDLE_VOLUME_*/bench-pool/<name>/.
EOT
}

while [[ $# -gt 0 ]]; do case $1 in
    --help|-h) show_help; exit 0 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    *) PASS_ARGS+=("$1"); shift ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:pool"
setup_log_file "$LOG_PATH"
CONFIG="examples/tests/databricks_cluster_config.env"
# A detached worktree has no untracked config: the main checkout's is used.
if GBX_CLUSTER_CONFIG_ENV="$(gbx_resolve_untracked "$CONFIG")" && [[ -f "$GBX_CLUSTER_CONFIG_ENV" ]]; then
    set -a; source "$GBX_CLUSTER_CONFIG_ENV"; set +a
fi
gbx_python_with_sdk || exit 1
"${GBX_SDK_PYTHON[@]}" "$PROJECT_ROOT/examples/tests/bench_pool.py" "${PASS_ARGS[@]}"
exit $?
