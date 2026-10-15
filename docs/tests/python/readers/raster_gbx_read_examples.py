"""raster_gbx / gtiff_gbx (lightweight) Reader Examples — single source of truth.

Code shown in docs/docs/readers/raster_gbx.mdx is imported from here. Pure-Python
DataSource V2 readers; no JAR required (registered via gbx.ds.register).
"""

from path_config import SAMPLE_DATA_BASE

SAMPLE_RASTER_PATH = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

REGISTER = """# Register the lightweight raster DataSources (once per session)
from databricks.labs.gbx.ds.register import register
register(spark)"""

READ_RASTER_GBX = """# Catch-all lightweight reader (any rasterio-readable raster)
df = spark.read.format("raster_gbx").load("{SAMPLE_RASTER_PATH}")
df.show()"""

READ_RASTER_GBX_output = """+--------------------------------------------------+-----+
|source                                            |tile |
+--------------------------------------------------+-----+
|/Volumes/.../nyc_sentinel2_red.tif                |{...}|
+--------------------------------------------------+-----+"""

READ_GTIFF_GBX = """# Named lightweight GeoTIFF reader (preset for GeoTIFF)
df = spark.read.format("gtiff_gbx").load("{SAMPLE_RASTER_PATH}")"""

READ_WITH_OPTIONS = r"""# Options: sizeInMB (tile split threshold) + filterRegex (directory listing)
df = (spark.read.format("raster_gbx")
      .option("sizeInMB", "16")
      .option("filterRegex", r".*\.tif$")
      .load("{SAMPLE_RASTER_PATH}"))"""


def _register(spark):
    from databricks.labs.gbx.ds.register import register

    register(spark)


def read_raster_gbx(spark, path=None):
    """Verify READ_RASTER_GBX: catch-all reader yields (source, tile) rows."""
    _register(spark)
    df = spark.read.format("raster_gbx").load(path or SAMPLE_RASTER_PATH)
    assert [f.name for f in df.schema.fields] == ["source", "tile"]
    rows = df.collect()
    assert len(rows) >= 1
    assert rows[0]["tile"]["cellid"] == -1
    return df


def read_gtiff_gbx(spark, path=None):
    """Verify READ_GTIFF_GBX: named reader reads a GeoTIFF identically.

    Materialized tiles (``virtualTiles=false``) carry the GDAL ``driver`` in their
    metadata; the default virtual tiles are bytes-free references and do not."""
    _register(spark)
    df = (
        spark.read.format("gtiff_gbx")
        .option("virtualTiles", "false")
        .load(path or SAMPLE_RASTER_PATH)
    )
    assert df.count() >= 1
    assert df.collect()[0]["tile"]["metadata"]["driver"] == "GTiff"
    return df
