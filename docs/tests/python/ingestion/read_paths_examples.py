"""read_paths examples -- single source of truth for docs/docs/ingestion/read-paths.mdx.

Every snippet runs in this suite against the sample data. Auto Loader
(``cloudFiles``) only exists on Databricks, so the stream example runs with
``binaryFile`` as its stand-in (see :func:`read_paths_stream`).
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


# Longest an availableNow stream may run before it is stopped and reported.
STREAM_TIMEOUT_S = 300


def read_paths_stream(spark, folder, checkpoint, query_name="read_paths_doc"):
    """READ_PATHS_STREAM against a local folder: ``binaryFile`` stands in for Auto
    Loader (``cloudFiles``), which only exists on Databricks; a memory sink stands in
    for ``toTable``. The query is always stopped; one still running after
    ``STREAM_TIMEOUT_S`` raises instead of blocking."""
    from databricks.labs.gbx.ds.paths import read_paths

    files = (
        spark.readStream.format("binaryFile")
        .schema("path string, modificationTime timestamp, length long, content binary")
        .option("pathGlobFilter", "*.geojson")
        .load(folder)
        .select("path", "modificationTime")
    )
    rows = read_paths(files, "geojson_gbx", schema_path=folder)
    query = (
        rows.writeStream.format("memory")
        .queryName(query_name)
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .start()
    )
    try:
        # Raises the query's error if it failed; False if still running.
        if not query.awaitTermination(STREAM_TIMEOUT_S):
            raise TimeoutError(
                f"stream {query_name} still running after {STREAM_TIMEOUT_S} s: "
                f"{query.status}"
            )
    finally:
        query.stop()
    return spark.table(query_name)


def read_paths_raster(spark, path=None, tile_size="512"):
    """READ_PATHS_RASTER over ``path`` (a folder or one file; default the sample
    folder). ``tile_size`` is 512 as displayed; a test can pass another size."""
    from databricks.labs.gbx.ds.paths import read_paths

    files = _files(spark, path or SAMPLE_SENTINEL_DIR, "*.tif")
    return read_paths(
        files, "gtiff_gbx", options={"tileSize": tile_size}, num_partitions=8
    )
