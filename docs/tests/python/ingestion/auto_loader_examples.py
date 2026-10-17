"""declare_ingest examples -- single source of truth for docs/docs/ingestion/auto-loader.mdx.

``declare_ingest`` is a declaration inside a Lakeflow pipeline, so the pipeline
snippets are checked against the real signature and their inputs are validated
by the same code ``declare_ingest`` runs first (names, file pattern, schema from
the sample data). The SQL runs against tables shaped like the pipeline's output.
"""

from path_config import SAMPLE_DATA_BASE

SAMPLE_BOROUGHS_DIR = f"{SAMPLE_DATA_BASE}/nyc/boroughs"
SAMPLE_SENTINEL_DIR = f"{SAMPLE_DATA_BASE}/nyc/sentinel2"

PIPELINE_SCD2 = """# Lakeflow pipeline source file (serverless)
from databricks.labs.gbx.ds.changes import declare_ingest

declare_ingest(
    "main.geo.boroughs",                       # rows table: catalog.schema.table
    "/Volumes/main/geo/landing/boroughs",      # folder Auto Loader watches
    "geojson_gbx",
    on_change="scd2",                          # default; or "replace" / "append"
)"""

PIPELINE_RASTER = """# Lakeflow pipeline source file (serverless)
from databricks.labs.gbx.ds.changes import declare_ingest

declare_ingest(
    "main.geo.sentinel2_tiles",
    "/Volumes/main/geo/landing/sentinel2",
    "gtiff_gbx",
    options={"tileSize": "512"},   # reader options, as on the GeoTIFF reader page
    on_change="replace",           # keep only the latest content of each file
    num_partitions=64,             # spread the tile windows across 64 tasks
)"""

QUERY_CURRENT = """-- latest content of every file
SELECT * FROM main.geo.boroughs_current;

-- what was current at a point in time
SELECT r.* FROM main.geo.boroughs r
JOIN main.geo.boroughs_file_versions v
  ON r._source_path = v._source_path AND r._source_hash = v._source_hash
WHERE v.__START_AT <= TIMESTAMP'2026-10-17 12:00:00'
  AND (v.__END_AT IS NULL OR v.__END_AT > TIMESTAMP'2026-10-17 12:00:00');"""

# The landing folder each pipeline snippet watches, and the sample folder that
# stands in for it when the snippet's inputs are validated.
SAMPLE_FOR_LANDING = {
    "/Volumes/main/geo/landing/boroughs": SAMPLE_BOROUGHS_DIR,
    "/Volumes/main/geo/landing/sentinel2": SAMPLE_SENTINEL_DIR,
}
