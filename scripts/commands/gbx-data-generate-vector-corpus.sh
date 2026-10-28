#!/bin/bash
# gbx:data:generate-vector-corpus - Generate synthetic vector corpus via light writers (for benchmarking)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

source "$SCRIPT_DIR/common.sh"

show_help() {
    show_banner "GeoBrix: Generate Vector Corpus"
    echo -e "${CYAN}Usage:${NC}"
    echo -e "  ${GREEN}gbx:data:generate-vector-corpus${NC} ${YELLOW}[options]${NC}"
    echo ""
    echo -e "${CYAN}Options:${NC}"
    echo -e "  ${GREEN}--format <fmt>${NC}     Light-writer format to use (default: geojson_gbx)"
    echo -e "  ${GREEN}--features <N>${NC}     Number of synthetic features to generate (default: 1000)"
    echo -e "  ${GREEN}--out <path>${NC}       Output path inside the container (default: /tmp/vector_corpus.geojson)"
    echo -e "  ${GREEN}--log <path>${NC}       Write output to log file"
    echo -e "  ${GREEN}--help${NC}             Show this help"
    echo ""
    echo -e "${CYAN}Examples:${NC}"
    echo -e "  ${YELLOW}gbx:data:generate-vector-corpus${NC}"
    echo -e "  ${YELLOW}gbx:data:generate-vector-corpus --format geojson_gbx --features 5000 --out /tmp/bench_corpus.geojson${NC}"
    echo ""
}

FORMAT="geojson_gbx"
FEATURES="1000"
OUT="/tmp/vector_corpus.geojson"
LOG_PATH=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --format)
            FORMAT="$2"
            shift 2
            ;;
        --features)
            FEATURES="$2"
            shift 2
            ;;
        --out)
            OUT="$2"
            shift 2
            ;;
        --log)
            LOG_PATH=$(resolve_log_path "$2")
            shift 2
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

cd "$PROJECT_ROOT"
show_banner "GeoBrix: Generate Vector Corpus"
check_docker
setup_log_file "$LOG_PATH"

echo -e "${CYAN}Format:   ${YELLOW}${FORMAT}${NC}"
echo -e "${CYAN}Features: ${YELLOW}${FEATURES}${NC}"
echo -e "${CYAN}Output:   ${YELLOW}${OUT}${NC}"
echo ""

# Write the generator script to a temp file, copy it into the container, then run it.
TMPSCRIPT="$(mktemp /tmp/gbx-vec-corpus-XXXXXX.py)"
cat > "$TMPSCRIPT" << 'PYEOF'
import sys
from pyspark.sql import SparkSession
from shapely import Point, to_wkb
from databricks.labs.gbx.ds.register import register

fmt, n, out = sys.argv[1], int(sys.argv[2]), sys.argv[3]
spark = SparkSession.builder.getOrCreate()
register(spark)
rows = [
    (str(i), i,
     bytearray(to_wkb(Point(float(i % 360) - 180.0, float(i % 170) - 85.0))),
     "4326", "")
    for i in range(n)
]
df = spark.createDataFrame(
    rows,
    schema="name string, val int, geom_0 binary, geom_0_srid string, "
    "geom_0_srid_proj string",
)
df.write.format(fmt).mode("overwrite").save(out)
print(f"wrote {n} features to {out} as {fmt}")
PYEOF

docker cp "$TMPSCRIPT" "$GBX_DEV_CONTAINER:/tmp/gbx-vec-corpus-gen.py"
rm -f "$TMPSCRIPT"

docker exec "$GBX_DEV_CONTAINER" bash -lc \
    "cd /root/geobrix && python3 /tmp/gbx-vec-corpus-gen.py '$FORMAT' '$FEATURES' '$OUT'"

exit $?
