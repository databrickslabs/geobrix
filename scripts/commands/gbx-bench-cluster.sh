#!/bin/bash
# gbx:bench:cluster — submit the heavy-vs-light benchmark as a one-off job to a Databricks cluster.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"
gbx_snapshot_exec "${BASH_SOURCE[0]}" "$@"   # run from a copy: an edit during the run cannot change it
# In the snapshot's run the code is the snapshot's, the state (venv, logs, local build) the source tree's.
SNAP_ROOT="$PROJECT_ROOT"
PROJECT_ROOT="${GBX_SOURCE_ROOT:-$PROJECT_ROOT}"

PASS_ARGS=()
LOG_PATH=""
show_help() {
    show_banner "gbx:bench:cluster"
    cat <<'EOF'
Submit the heavy-vs-light benchmark as a one-off notebook job to a Databricks
cluster (the cluster + artifacts must be provisioned by the operator per the
installation docs). Results land in the bench_results Delta table + out_dir on
the configured Volume. Reads examples/tests/databricks_cluster_config.env.

Usage: bash scripts/commands/gbx-bench-cluster.sh [options]
Options:
  --cluster-id <id>     Target cluster (overrides CLUSTER_ID from config)
  --run-id <id>         Run id (default cluster)
  --functions <csv>     rst_* names (overrides --set); with --grid-bng-only, bng_* names
                        pick which BNG legs run (no bng_* name = all legs)
  --set <core|full>     Selection tier: core (fast default) or full (default core)
  --modes <m>           pure-core | spark-path | both (default both)
  --row-counts <csv>    Spark-path row ladder (default 10,100,1000,10000)
  --warmup <n>          Pure-core warmup iters (default 1)
  --measured <n>        Pure-core measured iters (default 3)
  --spark-warmup <n>    Spark-path warmup iters (default 1)
  --spark-measured <n>  Spark-path measured iters (default 1)
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
  --benchmark-lidar     Also run LiDAR reader+writer benchmark (light lidar_gbx only)
  --lidar-only          Run ONLY the LiDAR reader+writer benchmark (no fn benchmarks)
  --benchmark-exif      Also run EXIF reader benchmark (light exif_gbx only)
  --exif-only           Run ONLY the EXIF reader benchmark (no fn benchmarks)
  --benchmark-grid-bng  Also run the BNG grid benchmark (light pygx vs heavy gridx.bng)
  --grid-bng-only       Run ONLY the BNG grid benchmark; each explode leg checks its query
                        reaches its own tier before timing
  --input-tile <mode>   Input tile for the light spark-path leg: materialized (default) or virtual
  --disable-file        Set GBX_DISABLE_FILE=1 in the notebook (FILE-off A/B leg; use with --input-tile virtual)
  --grouped-file        Also run the grouped FILE-amortization benchmark (rst_clip_grouped +
                        pixel-op _grouped fns over a MULTIWINDOW COG corpus; materialized /
                        virtual+FILE-off / virtual+FILE-on modes). Toggles FILE per-mode internally.
  --grouped-file-only   Run ONLY the grouped FILE-amortization benchmark (no fn benchmarks)
  --multiwindow-corpus <path>  Multiwindow COG corpus dir (holds cog_multiwindow_manifest.json).
                        Default: <corpus>/bench-corpus-cog-multiwindow.
  --levels <S|G|A|H|SE|IO> Also run the levels leg for one set of functions (S session functions,
                        G generators, A aggregates, H DataFrame helpers, SE the GDAL spec engine's
                        rows; IO has no forms yet):
                        every form of that set at each execution level the compute offers
                        (L1 Catalyst, L2 Java UDF, L3 light, L4 Unity Catalog) at 1x/4x/16x, with
                        the attribution split (serialization, Python eval, shuffle, spill, memory,
                        GC, worker RSS) in the row. Tile inputs come from <corpus>/rows. L1/L2
                        need the geobrix JAR on the cluster.
  --levels-only         Run ONLY the levels leg (no fn benchmarks, no heavy tests JAR)
  --levels-scales <csv> Multiples of each form's base row count the levels leg times (default 1,4,16)
  --levels-uc <c.s>     catalog.schema holding the L4 (Unity Catalog) functions. Without it the L4
                        forms record a skipped row.
  --file-matrix         Also run the FILE-access matrix: GeoTIFF + GeoPackage reads across
                        file_mode in (fuse, external, managed). Light-only (pure Python).
  --file-matrix-only    Run ONLY the FILE matrix benchmark (no fn benchmarks)
  --gpkg-chunksize      Also run the GeoPackage chunkSize sweep (1k/10k/100k; fuse mode,
                        fanout-invariance check). Light-only. Stages GPKG corpus automatically.
  --gpkg-chunksize-only Run ONLY the chunkSize sweep (no fn benchmarks)
  --layout-sweep        Also run the FILE write layout sweep (order/cluster/plain) for GeoTIFF
                        + GeoPackage. Requires --file-filespace for FILE-table targets.
  --layout-sweep-only   Run ONLY the layout sweep (no fn benchmarks)
  --layout-scan         Also run the layout-scan comparison (sequential-scan + shuffle-input
                        cost) over the FILE tables written by the layout sweep.
  --layout-scan-only    Run ONLY the layout scan (no fn benchmarks)
  --file-filespace <id> Filespace identifier for FILE EXTERNAL/MANAGED table creation (e.g.
                        a TBLPROPERTIES filespace path under a Volume). Default: empty string
                        (managed/external legs yield na_by_design on FUSE-only tiers).
  --gpkg-corpus <path>  GeoPackage bench corpus base dir (staged by stage_gpkg_bench_corpus).
                        Default: <corpus>/bench-corpus-gpkg.
  --serverless          Submit to Serverless compute (light-only, no JAR). Skips --cluster-id.
                        Profile must point to a workspace that supports Serverless jobs (e.g. dogfood).
  --env-version <N>     Serverless environment version to pin (default 6; env v6 = protobuf-6 regime).
                        Only used with --serverless.
  --no-wait             Submit without blocking on completion
  --log <path>          Write the output to a log (filename -> test-logs/<name>)
  --no-smoke            Skip the smoke stage (logged). By default a SMOKE run goes first: each form of the
                        --levels set once at --smoke-rows rows plus a worker-side import check; only a clean
                        smoke submits the real run (exit 4 otherwise)
  --smoke-only          Run the smoke stage and stop (exit 0 when clean): proves a compute and a launch cheaply
  --smoke-rows <n>      Rows per form at the smoke (default 16)
  --smoke-timeout-min <n>  Time limit of the smoke run (default 15)
  --smoke-allow-errors  A form's error row at the smoke does not stop the real run
  --skip-preflight      Skip the local preflight (logged): the environment spec names the wheel, the staged wheel
                        is the local build, the probe SQL runs on a local Spark, no client input above the cap
                        (exit 5 when a check fails)
  --timeout-min <n>     Time limit of the real run (default 120 with --levels, else 360)
  --form-cap-min <n>    The watchdog cancels the run when one form runs longer (default 15; the smoke 5)
  --stale-min <n>       ... or when the notebook's heartbeat stops changing for this long (default 5);
                        either prints `STALLED: <form> after <m> min` and the driver stack, exit 3, no retry
  --inject-stall <s>    TEST ONLY: add a form that sleeps s seconds on the driver (proves the watchdog)
  --help, -h            Show help

Environment: GBX_PYRX_VENV=<dir> runs the launcher on that venv instead of .venv-pyrx (Python 3.11+;
databricks-sdk is added on top when the venv lacks it).

NOTE: this submits a job to a real cluster (or Serverless) and consumes compute. For classic
cluster runs the operator must have provisioned the cluster and filled databricks_cluster_config.env.
For --serverless, set DATABRICKS_CONFIG_PROFILE to a workspace with Serverless enabled (e.g. dogfood).
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --help|-h) show_help; exit 0 ;;
    --cluster-id) export CLUSTER_ID="$2"; shift 2 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    *) PASS_ARGS+=("$1"); shift ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:cluster"
setup_log_file "$LOG_PATH"

CONFIG="examples/tests/databricks_cluster_config.env"
if [[ ! -f "$CONFIG" ]]; then
    echo "ERROR: $CONFIG not found. Copy examples/tests/databricks_cluster_config.example.env and fill it." >&2
    exit 1
fi

echo "Submitting benchmark job (this runs on a real cluster and costs compute)..."
# The launcher imports the bench package (-> pyrx -> rasterio) and the databricks SDK: it runs
# on the project's .venv-pyrx with the SDK layered over it (see gbx_python_with_sdk in common.sh).
gbx_python_with_sdk || exit 1
"${GBX_SDK_PYTHON[@]}" "$SNAP_ROOT/examples/tests/push_and_run_bench_on_cluster.py" "${PASS_ARGS[@]}"
exit $?
