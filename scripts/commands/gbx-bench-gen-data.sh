#!/bin/bash
# gbx:bench:gen-data — generate the seeded benchmark corpus (in the pyrx venv).
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"
gbx_snapshot_exec "${BASH_SOURCE[0]}" "$@"   # run from a copy: an edit during the run cannot change it
# In the snapshot's run the code is the snapshot's, the state (venv, logs, corpus) the source tree's.
SNAP_ROOT="$PROJECT_ROOT"
PROJECT_ROOT="${GBX_SOURCE_ROOT:-$PROJECT_ROOT}"

OUT="${PROJECT_ROOT}/sample-data/Volumes/main/default/bench-corpus"
TILE_PX="256,512,1024,2048,4096"
BANDS="1,4,13"
DTYPES="uint8,int16,float32"
SRIDS="4326,3857,32618,27700"
NODATA_FRAC="0.02"
SEED="1234"
ROW_ROWS="10000"
ROW_TILE_PX=""
ROW_BANDS=""
ROW_DTYPE=""
SET="core"
LOG_PATH=""
ON_DATABRICKS=false
DBX_ARGS=()
ROWS_FROM=""
ROWS_TO=""

show_help() {
    show_banner "gbx:bench:gen-data"
    cat <<'EOF'
Generate the seeded benchmark corpus + corpus.json manifest in the pyrx venv.

Usage: bash scripts/commands/gbx-bench-gen-data.sh [options]
Options:
  --out <dir>          Corpus output dir (default sample-data/.../bench-corpus)
  --tile-px <list>     Tile-size sweep (default 256,512,1024,2048,4096)
  --bands <list>       Band counts (default 1,4,13)
  --dtypes <list>      Dtypes (default uint8,int16,float32)
  --srids <list>       SRIDs to cycle (default 4326,3857,32618,27700)
  --nodata-frac <list> NoData fraction(s), e.g. 0.02,0.25,0.5 (default 0.02)
  --row-rows <n>       Row-pool size for spark-path sweep (default 10000)
  --row-tile-px <n>    Row-pool tile size in px (default 1024) — the tile the
                       spark-path sweep actually benchmarks
  --row-bands <n>      Row-pool band count (default 4)
  --row-dtype <t>      Row-pool dtype (default float32)
  --seed <n>           RNG seed (default 1234)
  --set <core|full>    Accepted for pipeline symmetry (default core); the corpus
                       is function-agnostic, so this does not change its content.
  --log <path>         Tee output under test-logs/
  --help, -h           Show help

On Databricks (row pool only): generate rows/r<N>.tif on Serverless, byte-identical to the local ones.
  --on-databricks      Generate on Serverless into the corpus Volume instead of locally
  --profile <name>     Databricks config profile (required)
  --serverless         Accepted for symmetry with gbx:bench:cluster; Serverless is the only compute
  --rows-from <n>      First tile index (default 0)
  --rows-to <n>        Last tile index, included (default --row-rows minus 1)
  --skip-existing      Skip a tile whose file has the expected size (default); --no-skip-existing rewrites
  --partitions <n>     Spark partitions per chunk (default 64)
  --chunk <n>          Tiles per chunk; progress prints after each (default 512)
  --verify-indices <csv>  Tiles whose sha256 is checked against the local files (default: ends, middle, 0/999/1000/5000/9999 in range)
  --corpus-out <path>  Volume corpus dir (default <volume>/bench-corpus)
  --env-version <n>    Serverless environment version (default 6)
  --run-id <id>        Run id (default gen-<date>-<time>-<hex>)
  --no-smoke           Skip the smoke stage; --smoke-only stops after it
  --timeout-min <n>    Time limit of the real run (default 240)
  --status-file <path> Write the run's state and latest progress there on every poll
  --no-wait            Submit and return
Nothing on the Volume is deleted. Exit: 0 verified, 1 verification failed, 2 usage, 3 stalled, 4 smoke failed.
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --out) OUT="$2"; shift 2 ;;
    --tile-px) TILE_PX="$2"; shift 2 ;;
    --bands) BANDS="$2"; shift 2 ;;
    --dtypes) DTYPES="$2"; shift 2 ;;
    --srids) SRIDS="$2"; shift 2 ;;
    --nodata-frac) NODATA_FRAC="$2"; shift 2 ;;
    --row-rows) ROW_ROWS="$2"; shift 2 ;;
    --row-tile-px) ROW_TILE_PX="$2"; shift 2 ;;
    --row-bands) ROW_BANDS="$2"; shift 2 ;;
    --row-dtype) ROW_DTYPE="$2"; shift 2 ;;
    --seed) SEED="$2"; shift 2 ;;
    --set) SET="$2"; shift 2 ;;
    --log) LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --on-databricks) ON_DATABRICKS=true; shift ;;
    --serverless) shift ;;
    --profile) export DATABRICKS_CONFIG_PROFILE="$2"; DBX_ARGS+=(--profile "$2"); shift 2 ;;
    --rows-from) ROWS_FROM="$2"; shift 2 ;;
    --rows-to) ROWS_TO="$2"; shift 2 ;;
    --skip-existing|--no-skip-existing|--no-smoke|--smoke-only|--no-wait) DBX_ARGS+=("$1"); shift ;;
    --partitions|--chunk|--verify-indices|--env-version|--run-id|--timeout-min|--status-file) DBX_ARGS+=("$1" "$2"); shift 2 ;;
    --corpus-out) DBX_ARGS+=(--out "$2"); shift 2 ;;
    --help|-h) show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:gen-data"
validate_set "$SET" || exit 1
setup_log_file "$LOG_PATH"

if [[ "$ON_DATABRICKS" == true ]]; then
    [[ -z "$ROWS_FROM" ]] && ROWS_FROM=0
    [[ -z "$ROWS_TO" ]] && ROWS_TO=$((ROW_ROWS - 1))
    # The tiles are those of the local generator: the same seed, SRIDs and row-pool shape (defaults 1024px, 4 bands, float32).
    echo "Generating row tiles r${ROWS_FROM}..r${ROWS_TO} on Databricks Serverless (costs compute)..."
    gbx_python_with_sdk || exit 1
    EXTRA=(--seed "$SEED" --srids "$SRIDS")
    [[ -n "$ROW_TILE_PX" ]] && EXTRA+=(--row-tile-px "$ROW_TILE_PX")
    [[ -n "$ROW_BANDS" ]] && EXTRA+=(--row-bands "$ROW_BANDS")
    [[ -n "$ROW_DTYPE" ]] && EXTRA+=(--row-dtype "$ROW_DTYPE")
    "${GBX_SDK_PYTHON[@]}" "$SNAP_ROOT/examples/tests/push_and_run_corpus_gen_on_serverless.py" \
        --rows-from "$ROWS_FROM" --rows-to "$ROWS_TO" --local-corpus "$OUT" "${EXTRA[@]}" "${DBX_ARGS[@]}"
    exit $?
fi

# Row-pool knobs are optional; only pass the flag when set so datagen's own
# defaults (1024px / 4 bands / float32) still apply when unspecified.
ROW_OPTS=""
[[ -n "$ROW_TILE_PX" ]] && ROW_OPTS="$ROW_OPTS --row-tile-px '$ROW_TILE_PX'"
[[ -n "$ROW_BANDS" ]] && ROW_OPTS="$ROW_OPTS --row-bands '$ROW_BANDS'"
[[ -n "$ROW_DTYPE" ]] && ROW_OPTS="$ROW_OPTS --row-dtype '$ROW_DTYPE'"

run_in_pyrx_venv "python -m databricks.labs.gbx.bench.datagen \
    --out '$OUT' --tile-px '$TILE_PX' --bands '$BANDS' --dtypes '$DTYPES' \
    --srids '$SRIDS' --nodata-frac '$NODATA_FRAC' --row-rows '$ROW_ROWS' \
    --seed '$SEED'$ROW_OPTS"
EXIT_CODE=$?
exit $EXIT_CODE
