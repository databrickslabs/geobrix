"""Incremental ingestion examples — single source of truth for
docs/docs/readers/incremental-ingestion.mdx.

``read_paths`` runs in this suite against the sample data. ``declare_ingest`` needs a
Lakeflow pipeline (it is a declaration, not a call), so the pipeline snippet is
checked against the real signature instead of executed here.
"""

from path_config import SAMPLE_DATA_BASE

SAMPLE_BOROUGHS_DIR = f"{SAMPLE_DATA_BASE}/nyc/boroughs"
SAMPLE_SENTINEL_DIR = f"{SAMPLE_DATA_BASE}/nyc/sentinel2"

READ_PATHS_BATCH = """from databricks.labs.gbx.ds.paths import read_paths

files = (spark.read.format("binaryFile")
         .option("pathGlobFilter", "*.geojson")
         .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/boroughs")
         .select("path", "modificationTime"))   # file bytes are never loaded
df = read_paths(files, "geojson_gbx")
df.select("boroname", "_source_path").show()"""

READ_PATHS_BATCH_output = """+-------------+--------------------+
|     boroname|        _source_path|
+-------------+--------------------+
|Staten Island|/Volumes/main/def...|
|        Bronx|/Volumes/main/def...|
|          ...|                 ...|
+-------------+--------------------+"""

READ_PATHS_STREAM = """from databricks.labs.gbx.ds.paths import read_paths

landing = "/Volumes/main/geo/landing/boroughs"

files = (spark.readStream.format("cloudFiles")
         .option("cloudFiles.format", "binaryFile")
         .option("pathGlobFilter", "*.geojson")
         .load(landing)
         .select("path", "modificationTime"))
rows = read_paths(files, "geojson_gbx", schema_path=landing)   # required for a stream

(rows.writeStream
     .option("checkpointLocation", "/Volumes/main/geo/checkpoints/boroughs")
     .trigger(availableNow=True)
     .toTable("main.geo.boroughs"))"""

READ_PATHS_RASTER = """from databricks.labs.gbx.ds.paths import read_paths

files = (spark.read.format("binaryFile")
         .option("pathGlobFilter", "*.tif")
         .load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2")
         .select("path", "modificationTime"))
tiles = read_paths(
    files, "gtiff_gbx",
    options={"tileSize": "512"},   # cut each raster into 512 x 512 pixel windows
    num_partitions=8,              # spread the windows across 8 tasks
)
tiles.select("tile.window", "_source_path").show(3, truncate=False)"""

READ_PATHS_RASTER_output = """+---------------------+----------------------------------+
|window               |_source_path                      |
+---------------------+----------------------------------+
|{0, 0, 512, 512}     |/Volumes/.../nyc_sentinel2_red.tif|
|{1536, 0, 512, 512}  |/Volumes/.../nyc_sentinel2_red.tif|
|...                  |...                               |
+---------------------+----------------------------------+"""

PIPELINE_SCD2 = """# Lakeflow pipeline source file (serverless)
from databricks.labs.gbx.ds.changes import declare_ingest

declare_ingest(
    "main.geo.boroughs",                       # rows table (pipeline catalog/schema)
    "/Volumes/main/geo/landing/boroughs",      # folder Auto Loader watches
    "geojson_gbx",
    glob="*.geojson",
    on_change="scd2",                          # default; or "replace" / "append"
)"""

QUERY_CURRENT = """-- latest content of every file
SELECT * FROM main.geo.boroughs_current;

-- what was current at a point in time
SELECT r.* FROM main.geo.boroughs r
JOIN main.geo.boroughs_file_versions v
  ON r._source_path = v._source_path AND r._source_hash = v._source_hash
WHERE v.__START_AT <= TIMESTAMP'2026-10-17 12:00:00'
  AND (v.__END_AT IS NULL OR v.__END_AT > TIMESTAMP'2026-10-17 12:00:00');"""


def _files(spark, folder, glob):
    return (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", glob)
        .load(folder)
        .select("path", "modificationTime")
    )


def read_paths_batch(spark, folder=None):
    from databricks.labs.gbx.ds.paths import read_paths

    files = _files(spark, folder or SAMPLE_BOROUGHS_DIR, "*.geojson")
    return read_paths(files, "geojson_gbx")


def read_paths_stream(spark, folder, checkpoint, query_name="incremental_ingestion_doc"):
    """READ_PATHS_STREAM against a local folder: ``binaryFile`` stands in for Auto
    Loader (``cloudFiles``), which only exists on Databricks; a memory sink stands in
    for ``toTable``."""
    from databricks.labs.gbx.ds.paths import read_paths

    files = (
        spark.readStream.format("binaryFile")
        .schema("path string, modificationTime timestamp, length long, content binary")
        .option("pathGlobFilter", "*.geojson")
        .load(folder)
        .select("path", "modificationTime")
    )
    rows = read_paths(files, "geojson_gbx", schema_path=folder)
    (
        rows.writeStream.format("memory")
        .queryName(query_name)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )
    return spark.table(query_name)


def read_paths_raster(spark, folder=None, tile_size="512"):
    """READ_PATHS_RASTER; ``tile_size`` is 512 as displayed, and a test can pass a
    smaller size to cut the small sample rasters into several windows."""
    from databricks.labs.gbx.ds.paths import read_paths

    files = _files(spark, folder or SAMPLE_SENTINEL_DIR, "*.tif")
    return read_paths(
        files, "gtiff_gbx", options={"tileSize": tile_size}, num_partitions=8
    )
