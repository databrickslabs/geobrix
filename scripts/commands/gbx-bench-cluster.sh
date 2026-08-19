#!/bin/bash
# gbx:bench:cluster — submit the heavy-vs-light benchmark as a one-off job to a Databricks cluster.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

PASS_ARGS=()
show_help() {
    show_banner "gbx:bench:cluster"
    cat <<'EOF'
Submit the heavy-vs-light benchmark as a one-off notebook job to a Databricks
cluster (the cluster + artifacts must be provisioned by the operator per the
installation docs). Results land in the bench_results Delta table + out_dir on
the configured Volume. Reads notebooks/tests/databricks_cluster_config.env.

Usage: bash scripts/commands/gbx-bench-cluster.sh [options]
Options:
  --cluster-id <id>     Target cluster (overrides CLUSTER_ID from config)
  --run-id <id>         Run id (default cluster)
  --functions <csv>     rst_* names (overrides --set)
  --set <core|full>     Selection tier: core (fast default) or full (default core)
  --modes <m>           pure-core | spark-path | both (default both)
  --row-counts <csv>    Spark-path row ladder (default 10,100,1000,10000)
  --warmup <n>          Warmup iters (default 2)
  --measured <n>        Measured iters (default 5)
  --heavyweight-only    Skip the lightweight leg
  --lightweight-only    Skip the heavyweight leg (use on ARM clusters)
  --benchmark-readers   Also run reader benchmarks (raster_gbx vs gdal)
  --readers-only        Run ONLY the reader benchmark (no fn benchmarks)
  --benchmark-pmtiles   Also run PMTiles writer benchmark (pmtiles_gbx vs pmtiles)
  --pmtiles-only        Run ONLY the PMTiles benchmark (no fn benchmarks)
  --benchmark-vector    Also run vector reader benchmark (light *_gbx vs heavy *_ogr)
  --vector-only         Run ONLY the vector reader benchmark (no fn benchmarks)
  --benchmark-netcdf    Also run NetCDF reader benchmark (light netcdf_gbx vs heavy netcdf_gdal)
  --netcdf-only         Run ONLY the NetCDF reader benchmark (no fn benchmarks)
  --input-tile <mode>   Input tile for the light spark-path leg: materialized (default) or virtual
  --disable-file        Set GBX_DISABLE_FILE=1 in the notebook (FILE-off A/B leg; use with --input-tile virtual)
  --serverless          Submit to Serverless compute (light-only, no JAR). Skips --cluster-id.
                        Profile must point to a workspace that supports Serverless jobs (e.g. dogfood).
  --env-version <N>     Serverless environment version to pin (default 6; env v6 = protobuf-6 regime).
                        Only used with --serverless.
  --no-wait             Submit without blocking on completion
  --help, -h            Show help

NOTE: this submits a job to a real cluster (or Serverless) and consumes compute. For classic
cluster runs the operator must have provisioned the cluster and filled databricks_cluster_config.env.
For --serverless, set DATABRICKS_CONFIG_PROFILE to a workspace with Serverless enabled (e.g. dogfood).
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --help|-h) show_help; exit 0 ;;
    --cluster-id) export CLUSTER_ID="$2"; shift 2 ;;
    *) PASS_ARGS+=("$1"); shift ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:cluster"

CONFIG="notebooks/tests/databricks_cluster_config.env"
if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: $CONFIG not found. Copy notebooks/tests/databricks_cluster_config.example.env and fill it." >&2
    exit 1
fi

echo "Submitting benchmark job (this runs on a real cluster and costs compute)..."
# The launcher imports the bench package (-> pyrx -> rasterio) and the
# databricks SDK, so it must run in the project's .venv-pyrx (which has both),
# not the ambient python. Fall back to system python only if the venv is absent.
PYBIN="python"
if [[ -x "$PROJECT_ROOT/.venv-pyrx/bin/python" ]]; then
    PYBIN="$PROJECT_ROOT/.venv-pyrx/bin/python"
fi
"$PYBIN" notebooks/tests/push_and_run_bench_on_cluster.py "${PASS_ARGS[@]}"
exit $?
