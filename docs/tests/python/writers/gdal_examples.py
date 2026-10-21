"""
GDAL Writer Examples - Single Source of Truth

All code examples shown in docs/docs/writers/gdal.mdx are imported from this file.
Uses sample-data Volumes path for input; writes to a scratch directory for output.
"""

from path_config import SAMPLE_DATA_BASE

SAMPLE_RASTER_PATH = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"
OUTPUT_DIR = "/Volumes/main/default/test-data/geobrix-examples/out/writer-docs-example"

# --- Basic write (minimum required options) ----------------------------------

WRITE_GDAL = """# Read, (optionally transform), then write back as raster files.
# Keep the reader's full schema (source, tile): the writer looks up both by name.
(
    spark.read.format("gdal").load("{SAMPLE_RASTER_PATH}")
        .write
            .format("gdal")
            .mode("append")           # required -- other modes are not supported
            .option("ext", "tif")     # file extension (default: 'tif')
        .save("{OUTPUT_DIR}")
)"""

WRITE_GDAL_output = """(no DataFrame is returned by .save(); list the output directory to inspect files)
$ ls /Volumes/.../out/writer-docs-example
946817315_0_0.tif
..."""

# --- Write with nameCol (reproducible filenames) -----------------------------

WRITE_WITH_NAMECOL = """# Overwrite the reader's 'source' column with your desired filename prefix,
# then point nameCol at it. The writer needs the fixed (source, tile) schema,
# so replacing an existing column is the only way to inject a name.
from pyspark.sql.functions import monotonically_increasing_id, concat, lit

(
    spark.read.format("gdal").load("{SAMPLE_RASTER_PATH}")
        .withColumn("source", concat(lit("tile_"), monotonically_increasing_id()))
        .write
            .format("gdal")
            .mode("append")
            .option("nameCol", "source")    # 'source' now carries the filename
            .option("ext", "tif")
        .save("{OUTPUT_DIR}")
)"""

# --- End-to-end materialization pattern --------------------------------------

MATERIALIZE_PIPELINE = """# Performance pattern: materialize intermediate results to avoid
# repeating expensive transforms. Spark is lazy; each .display()/.count()
# re-runs the plan unless the source is already materialized.
import databricks.labs.gbx.rasterx as rx
rx.register(spark)

stacked_df = (
    spark.read.format("gtiff_gdal").load("{SAMPLE_RASTER_PATH}")
        # ... add transforms here (reproject, retile, etc.) ...
)

# Materialize to a Volume directory before follow-on work
(
    stacked_df
        .filter(rx.rst_tryopen("tile"))   # skip invalid tiles before writing
        .write
            .format("gdal")
            .mode("append")
            .option("ext", "tif")
        .save("{OUTPUT_DIR}")
)

# Follow-on steps read the materialized output -- fast, no recompute
next_df = spark.read.format("gtiff_gdal").load("{OUTPUT_DIR}")"""


# --- Named GeoTIFF writer + encoding metadata --------------------------------

WRITE_GTIFF_GDAL = """# Named GeoTIFF writer (gtiff_gdal = gdal writer with driver preset)
spark.read.format("gtiff_gdal").load(SAMPLE_RASTER_PATH) \\
    .write.format("gtiff_gdal").mode("append").option("ext", "tif").save(OUT_DIR)"""

ENCODING_FROM_METADATA = """# Output encoding is read from tile.metadata, not writer options:
#   format/driver (default GTiff), compression (DEFLATE), blocksize (512),
#   zlevel (6), zstd_level (9). Set them upstream (e.g. RST_AsFormat), then write."""


# --- Test helpers -------------------------------------------------------------


def write_gdal(spark, in_path, out_dir):
    """Verify WRITE_GDAL pattern works."""
    (
        spark.read.format("gdal")
        .load(in_path)
        .write.format("gdal")
        .mode("append")
        .option("ext", "tif")
        .save(out_dir)
    )


def write_with_namecol(spark, in_path, out_dir):
    """Verify WRITE_WITH_NAMECOL pattern works."""
    from pyspark.sql.functions import monotonically_increasing_id, concat, lit

    (
        spark.read.format("gdal")
        .load(in_path)
        .withColumn("source", concat(lit("tile_"), monotonically_increasing_id()))
        .write.format("gdal")
        .mode("append")
        .option("nameCol", "source")
        .option("ext", "tif")
        .save(out_dir)
    )
