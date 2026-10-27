"""raster_gbx / gtiff_gbx (lightweight) Writer Examples — single source of truth.

Code shown in docs/docs/writers/raster.mdx is imported from here. Writer
options are path/nameCol/ext plus compress/compressLevel/predictor for GeoTIFF
output; the output driver and the encoding of non-GeoTIFF drivers come from
tile.metadata.
"""

import os
import tempfile

from path_config import SAMPLE_DATA_BASE

SAMPLE_RASTER_PATH = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

WRITE_RASTER_GBX = """# Catch-all lightweight writer (output driver from tile.metadata; default GTiff)
from databricks.labs.gbx.ds.register import register
register(spark)
df = spark.read.format("raster_gbx").load("{SAMPLE_RASTER_PATH}")
df.write.format("raster_gbx").mode("overwrite").save(OUT_DIR)"""

WRITE_GTIFF_GBX = """# Read then write GeoTIFF tiles (lightweight)
from databricks.labs.gbx.ds.register import register
register(spark)
df = spark.read.format("raster_gbx").load("{SAMPLE_RASTER_PATH}")
df.write.format("gtiff_gbx").mode("overwrite").save(OUT_DIR)"""

WRITE_WITH_NAMECOL = """# Control output filenames: overwrite 'source', set nameCol
from pyspark.sql.functions import concat, lit, monotonically_increasing_id
(df.withColumn("source", concat(lit("tile_"), monotonically_increasing_id()))
   .write.format("gtiff_gbx").mode("overwrite")
   .option("nameCol", "source").option("ext", "tif").save(OUT_DIR))"""

ENCODING_NOTE = """# GeoTIFF output: the writer options choose the codec
#   compress -> auto (default) | zstd | deflate | lzw | none
#   compressLevel, predictor -> refine an explicit codec (ignored under auto)
# Other drivers: the output driver and its encoding come from tile.metadata
#   driver/format -> output driver (default GTiff)
#   compression/blocksize/zlevel/zstd_level -> applied when encoding a non-GTiff driver (COG)
# Change the metadata via upstream transforms, then write."""

WRITE_WITH_COMPRESS = """# Choose the GeoTIFF codec at write time: DEFLATE level 9 instead of the default ZSTD
(df.write.format("gtiff_gbx").mode("overwrite")
   .option("compress", "deflate")
   .option("compressLevel", "9")
   .save(OUT_DIR))"""


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def write_gtiff_gbx(spark, path=None):
    """Verify WRITE_GTIFF_GBX: round-trip read -> write -> re-read, same pixels."""
    import numpy as np
    import rasterio

    _register(spark)
    df = spark.read.format("raster_gbx").load(path or SAMPLE_RASTER_PATH)
    with tempfile.TemporaryDirectory() as out_dir:
        df.write.format("gtiff_gbx").mode("overwrite").save(out_dir)
        files = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
        assert files, "no output written"
        with rasterio.open(os.path.join(out_dir, files[0])) as w:
            written = w.read()
        with rasterio.open(path or SAMPLE_RASTER_PATH) as src:
            truth = src.read()
        # whole-file GTiff pass-through -> identical pixels
        assert written.shape == truth.shape
        np.testing.assert_allclose(written, truth, rtol=1e-3, atol=1e-3)


def write_raster_gbx(spark, path=None):
    """Verify WRITE_RASTER_GBX: round-trip read -> write via catch-all format -> re-read, same pixels."""
    import numpy as np
    import rasterio

    _register(spark)
    df = spark.read.format("raster_gbx").load(path or SAMPLE_RASTER_PATH)
    with tempfile.TemporaryDirectory() as out_dir:
        df.write.format("raster_gbx").mode("overwrite").save(out_dir)
        files = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
        assert files, "no output written"
        with rasterio.open(os.path.join(out_dir, files[0])) as w:
            written = w.read()
        with rasterio.open(path or SAMPLE_RASTER_PATH) as src:
            truth = src.read()
        assert written.shape == truth.shape
        np.testing.assert_allclose(written, truth, rtol=1e-3, atol=1e-3)


def write_with_namecol(spark, path=None):
    """Verify WRITE_WITH_NAMECOL: nameCol controls output filenames."""
    from pyspark.sql.functions import lit

    _register(spark)
    df = spark.read.format("raster_gbx").load(path or SAMPLE_RASTER_PATH)
    with tempfile.TemporaryDirectory() as out_dir:
        (
            df.withColumn("source", lit("mytile"))
            .write.format("gtiff_gbx")
            .mode("overwrite")
            .option("nameCol", "source")
            .save(out_dir)
        )
        assert "mytile.tif" in os.listdir(out_dir)


def write_with_compress(spark, path=None):
    """Verify WRITE_WITH_COMPRESS: default is ZSTD; compress="deflate" is honoured."""
    import rasterio

    _register(spark)
    df = spark.read.format("raster_gbx").load(path or SAMPLE_RASTER_PATH)

    def _codec(out_dir):
        files = [f for f in os.listdir(out_dir) if f.endswith(".tif")]
        assert files, "no output written"
        with rasterio.open(os.path.join(out_dir, files[0])) as w:
            return w.compression.value.lower() if w.compression else None

    with tempfile.TemporaryDirectory() as out_dir:
        df.write.format("gtiff_gbx").mode("overwrite").save(out_dir)
        assert _codec(out_dir) == "zstd"
    with tempfile.TemporaryDirectory() as out_dir:
        (
            df.write.format("gtiff_gbx")
            .mode("overwrite")
            .option("compress", "deflate")
            .option("compressLevel", "9")
            .save(out_dir)
        )
        assert _codec(out_dir) == "deflate"
