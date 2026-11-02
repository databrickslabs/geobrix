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


# ---------------------------------------------------------------------------
# rst_boundingbox — bounding polygon (returns WKB binary)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_numbands — band count
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# rst_width — pixel width
# ---------------------------------------------------------------------------


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
    from databricks.labs.gbx.rasterx import functions as rx

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
