"""
Python code examples for the light (pyrx) tier of RasterX functions — per-function examples.
Single source of truth for per-function light-Python tabs in docs/docs/api/rasterx-functions.mdx.

All examples are self-contained and JAR-free: they build a synthetic in-memory GeoTIFF
using rasterio + numpy rather than reading from /Volumes sample data.
No path_config import is needed.
"""

try:
    from databricks.labs.gbx.pyrx import functions as rx
except ImportError:
    rx = None


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _make_geotiff_bytes(width=4, height=3, count=1, epsg=4326):
    """Return in-memory float32 GTiff bytes (width x height, count bands, EPSG:epsg)."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=count,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            for b in range(1, count + 1):
                ds.write(data + (b - 1) * 100, b)
        return mf.read()


def _tile_df(spark, **kw):
    """One-row DataFrame with a tile struct column named 'tile'."""
    from pyspark.sql import functions as f

    raster = _make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


# ---------------------------------------------------------------------------
# rst_avg — per-band average pixel values
# ---------------------------------------------------------------------------


def rst_avg_python_light_example(spark):
    """Get per-band average pixel values from a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, width=4, height=3, count=1)
    result = tile_df.select(rx.rst_avg("tile").alias("band_averages")).first()
    return result["band_averages"]


rst_avg_python_light_example_output = """
+-------------+
|band_averages|
+-------------+
|        [5.5]|
+-------------+
"""


# ---------------------------------------------------------------------------
# rst_boundingbox — bounding polygon (returns WKB binary)
# ---------------------------------------------------------------------------


def rst_boundingbox_python_light_example(spark):
    """Get the bounding box of a raster tile as WKB binary using the light pyrx tier.

    The light tier returns the bounding polygon as WKB binary. Use
    st_geomfromwkb / shapely.wkb.loads to decode to a geometry object.
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, width=4, height=3)
    result = tile_df.select(rx.rst_boundingbox("tile").alias("bbox")).first()
    return result["bbox"]


rst_boundingbox_python_light_example_output = """
+----+
|bbox|
+----+
|[...|
+----+
(WKB binary — bounding POLYGON of the raster extent in EPSG:32618)
"""


# ---------------------------------------------------------------------------
# rst_numbands — band count
# ---------------------------------------------------------------------------


def rst_numbands_python_light_example(spark):
    """Get the number of bands in a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, count=1)
    result = tile_df.select(rx.rst_numbands("tile").alias("num_bands")).first()
    return result["num_bands"]


rst_numbands_python_light_example_output = """
+---------+
|num_bands|
+---------+
|        1|
+---------+
"""


# ---------------------------------------------------------------------------
# rst_width — pixel width
# ---------------------------------------------------------------------------


def rst_width_python_light_example(spark):
    """Get the pixel width of a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, width=4)
    result = tile_df.select(rx.rst_width("tile").alias("width")).first()
    return result["width"]


rst_width_python_light_example_output = """
+-----+
|width|
+-----+
|    4|
+-----+
"""


# ---------------------------------------------------------------------------
# rst_fromfile — load a raster tile from a file path (light / heavy Python only)
# ---------------------------------------------------------------------------


def rst_fromfile_python_light_example(spark):
    """Load a raster tile from a file path using the light pyrx tier.

    rst_fromfile is available in the light (pyrx) and heavy (rasterx) Python tiers only.
    There is no Scala Column form: the JVM executor cannot read UC Volume FUSE paths,
    so this function is implemented exclusively by the Python worker.
    """
    import tempfile
    import os
    from pyspark.sql import functions as f
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)

    # Write a small synthetic GeoTIFF to a temp file on the driver
    raster_bytes = _make_geotiff_bytes(width=4, height=3, count=1)
    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
        tmp.write(raster_bytes)
        tmp_path = tmp.name

    try:
        path_df = spark.createDataFrame([(tmp_path,)], ["path"])
        tile_df = path_df.select(rx.rst_fromfile("path", f.lit("GTiff")).alias("tile"))
        result = tile_df.select(rx.rst_width("tile").alias("width")).first()
        return result["width"]
    finally:
        os.unlink(tmp_path)


rst_fromfile_python_light_example_output = """
+-----+
|width|
+-----+
|    4|
+-----+
"""
