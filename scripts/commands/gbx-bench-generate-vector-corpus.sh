#!/bin/bash
# gbx:bench:generate-vector-corpus — generate the scaled vector benchmark corpus
# (polygon seed -> transcode to each *_gbx format -> replicate xN copies) in the pyrx venv.
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/common.sh"

ROWS="1000000"
COPIES="100"
FORMATS="geojson_gbx,shapefile_gbx,gpkg_gbx,file_gdb_gbx"
OUT="/Volumes/main/default/bench-corpus/vector-scale"
LOG_PATH=""
GPKG_BENCH=0
GPKG_BENCH_ROWS="100000"
GPKG_BENCH_COPIES="10,80,160"
GPKG_BENCH_OUT="/Volumes/main/default/bench-corpus/gpkg-bench"

show_help() {
    show_banner "gbx:bench:generate-vector-corpus"
    cat <<'EOF'
Generate the scaled vector benchmark corpus in the pyrx venv.

Pipeline: generate_polygon_seed -> transcode_vector_seed -> replicate_vector_seed
Output layout: <out>/<fmt>/seed.<ext>  and  <out>/<fmt>/copies/copy_<i>.<ext>

Note: file_gdb_gbx requires the heavyweight GDAL natives (osgeo) -- cluster only.
      For local validation, use --formats geojson_gbx,gpkg_gbx.

Usage: bash scripts/commands/gbx-bench-generate-vector-corpus.sh [OPTIONS]

Options:
  --rows <n>       Number of polygon rows in the seed (default 1000000)
  --copies <n>     Number of per-format replicas (default 100)
  --formats <list> Comma-separated *_gbx format names
                   (default geojson_gbx,shapefile_gbx,gpkg_gbx,file_gdb_gbx)
  --out <dir>      Output root directory
                   (default /Volumes/main/default/bench-corpus/vector-scale)
  --log <path>     Tee output under test-logs/
  --help, -h       Show this help and exit

  --gpkg-bench     GeoPackage bench preset: stage one copies-dir per copies value
                   via stage_gpkg_bench_corpus (few files x many rows, swept copies).
                   Uses the options below; all other options are ignored.
  --rows <n>       (--gpkg-bench) rows per .gpkg file (default 100000)
  --copies <list>  (--gpkg-bench) comma-separated copies ladder (default 10,80,160)
  --out <dir>      (--gpkg-bench) output root directory
                   (default /Volumes/main/default/bench-corpus/gpkg-bench)

Examples:
  # Small local smoke test (no osgeo needed):
  bash scripts/commands/gbx-bench-generate-vector-corpus.sh \
      --rows 500 --copies 2 --formats geojson_gbx,gpkg_gbx --out /tmp/vc_smoke

  # Full cluster corpus (all 4 formats, 1M x 100):
  bash scripts/commands/gbx-bench-generate-vector-corpus.sh \
      --rows 1000000 --copies 100 --log vector-corpus.log

  # GeoPackage bench preset (100k rows/file, copies ladder 10/80/160):
  bash scripts/commands/gbx-bench-generate-vector-corpus.sh \
      --gpkg-bench --out /Volumes/main/default/bench-corpus/gpkg-bench

  # GeoPackage bench preset, local smoke (tiny rows/copies):
  bash scripts/commands/gbx-bench-generate-vector-corpus.sh \
      --gpkg-bench --rows 500 --copies 2,3 --out /tmp/gpkg_bench_smoke
EOF
}

while [[ $# -gt 0 ]]; do case $1 in
    --rows)       ROWS="$2"; GPKG_BENCH_ROWS="$2"; shift 2 ;;
    --copies)     COPIES="$2"; GPKG_BENCH_COPIES="$2"; shift 2 ;;
    --formats)    FORMATS="$2"; shift 2 ;;
    --out)        OUT="$2"; GPKG_BENCH_OUT="$2"; shift 2 ;;
    --log)        LOG_PATH=$(resolve_log_path "$2"); shift 2 ;;
    --gpkg-bench) GPKG_BENCH=1; shift ;;
    --help|-h)    show_help; exit 0 ;;
    *) echo "Unknown option: $1" >&2; show_help; exit 1 ;;
esac; done

cd "$PROJECT_ROOT"
show_banner "gbx:bench:generate-vector-corpus"
setup_log_file "$LOG_PATH"

if [[ "$GPKG_BENCH" -eq 1 ]]; then
    run_in_pyrx_venv "python - <<'PYEOF'
import sys
sys.path.insert(0, 'python/geobrix/src')

from pyspark.sql import SparkSession
from databricks.labs.gbx.bench.corpus_vector import stage_gpkg_bench_corpus

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('gbx-bench-gpkg-bench-corpus')
    .config('spark.sql.shuffle.partitions', '4')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

rows    = int('$GPKG_BENCH_ROWS')
copies_ladder = tuple(int(c.strip()) for c in '$GPKG_BENCH_COPIES'.split(',') if c.strip())
out     = '$GPKG_BENCH_OUT'

print(f'Staging GeoPackage bench corpus: {rows:,} rows/file, copies ladder={copies_ladder}')
print(f'Output  : {out}')
print()

result = stage_gpkg_bench_corpus(spark, out, rows=rows, copies_ladder=copies_ladder)

for copies, copies_dir in sorted(result.items()):
    import os
    n = len([f for f in os.listdir(copies_dir) if f.endswith('.gpkg')])
    print(f'  copies={copies}: {copies_dir}  ({n} .gpkg files)')

print()
print('Done.')
PYEOF
"
else
    run_in_pyrx_venv "python - <<'PYEOF'
import sys
sys.path.insert(0, 'python/geobrix/src')

from pyspark.sql import SparkSession
from databricks.labs.gbx.bench.corpus_vector import build_vector_corpus

spark = (
    SparkSession.builder
    .master('local[*]')
    .appName('gbx-bench-generate-vector-corpus')
    .config('spark.sql.shuffle.partitions', '4')
    .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

rows    = int('$ROWS')
copies  = int('$COPIES')
formats = [f.strip() for f in '$FORMATS'.split(',') if f.strip()]
out     = '$OUT'

print(f'Generating vector corpus: {rows:,} rows x {copies} copies per format')
print(f'Formats : {formats}')
print(f'Output  : {out}')
print()

result = build_vector_corpus(spark, rows=rows, copies=copies, formats=formats, out_base=out)

for fmt, paths in result.items():
    n = len(paths['copies'])
    print(f'  {fmt}: seed={paths[\"seed\"]}  copies={n}')

print()
print('Done.')
PYEOF
"
fi

EXIT_CODE=$?
exit $EXIT_CODE
