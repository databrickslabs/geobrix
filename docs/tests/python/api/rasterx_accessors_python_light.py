"""
Python code examples for the light (pyrx) tier of RasterX accessor functions.
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


def _make_netcdf_bytes(width=4, height=4):
    """Return NETCDF4 bytes with 'temperature' and 'precipitation' variables.

    Uses two variables so GDAL exposes them as subdatasets, enabling
    rst_getsubdataset to extract a named layer.  No real file path needed —
    rasterio MemoryFile can open NETCDF4 (HDF5-based) bytes from memory.
    """
    import numpy as np
    import netCDF4 as nc4
    import tempfile, os

    with tempfile.NamedTemporaryFile(suffix=".nc", delete=False) as fh:
        nc_path = fh.name
    with nc4.Dataset(nc_path, "w", format="NETCDF4") as ds:
        ds.createDimension("y", height)
        ds.createDimension("x", width)
        for vname in ("temperature", "precipitation"):
            v = ds.createVariable(vname, "f4", ("y", "x"))
            v[:] = np.arange(width * height, dtype="float32").reshape(height, width)
    with open(nc_path, "rb") as fh:
        nc_bytes = fh.read()
    os.unlink(nc_path)
    return nc_bytes


def _nc_tile_df(spark, width=4, height=4):
    """One-row DataFrame with a NetCDF tile struct column named 'tile'."""
    from pyspark.sql import functions as f

    raster = _make_netcdf_bytes(width=width, height=height)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("netCDF")).alias("tile"))


def _tile_df(spark, **kw):
    """One-row DataFrame with a tile struct column named 'tile'."""
    from pyspark.sql import functions as f

    raster = _make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


# ---------------------------------------------------------------------------
# rst_bandmetadata — per-band metadata map
# ---------------------------------------------------------------------------


def rst_bandmetadata_python_light_example(spark):
    """Get metadata for band 1 of a raster tile using the light pyrx tier."""
    from pyspark.sql import functions as f
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, width=4, height=3, count=1)
    result = tile_df.select(
        rx.rst_bandmetadata("tile", f.lit(1)).alias("band_meta")
    ).first()
    return result["band_meta"]


rst_bandmetadata_python_light_example_output = """
+---------+
|band_meta|
+---------+
|{}       |
+---------+
"""


# ---------------------------------------------------------------------------
# rst_format — GDAL format name
# ---------------------------------------------------------------------------


def rst_format_python_light_example(spark):
    """Get the GDAL driver/format name of a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_format("tile").alias("format")).first()
    return result["format"]


rst_format_python_light_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
"""


# ---------------------------------------------------------------------------
# rst_georeference — georeference parameters map
# ---------------------------------------------------------------------------


def rst_georeference_python_light_example(spark):
    """Get georeference parameters (scale, skew, origin) using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_georeference("tile").alias("georeference")).first()
    return result["georeference"]


rst_georeference_python_light_example_output = """
+------------------------------------------------------------+
|                                                georeference|
+------------------------------------------------------------+
|{scaleX -> 0.5, scaleY -> -0.5, upperLeftY -> 50.0, skewX...|
+------------------------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_getnodata — NoData values per band
# ---------------------------------------------------------------------------


def rst_getnodata_python_light_example(spark):
    """Get the NoData values for each band using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, count=1)
    result = tile_df.select(rx.rst_getnodata("tile").alias("nodata")).first()
    return result["nodata"]


rst_getnodata_python_light_example_output = """
+----------+
|nodata    |
+----------+
|[-9999.0] |
+----------+
"""


# ---------------------------------------------------------------------------
# rst_getsubdataset — extract a named subdataset
# ---------------------------------------------------------------------------


def rst_getsubdataset_python_light_example(spark):
    """Extract a named subdataset from a NetCDF raster tile using the light pyrx tier.

    Multi-layer formats such as NetCDF expose each variable as a subdataset.
    rst_getsubdataset extracts one layer by name and returns it as a new tile.
    """
    from pyspark.sql import functions as f
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    nc_df = _nc_tile_df(spark, width=4, height=4)
    result = nc_df.select(
        rx.rst_width(
            rx.rst_getsubdataset("tile", f.lit("temperature"))
        ).alias("width")
    ).first()
    return result["width"]


rst_getsubdataset_python_light_example_output = """
+-----+
|width|
+-----+
|4    |
+-----+
"""


# ---------------------------------------------------------------------------
# rst_height — raster height in pixels
# ---------------------------------------------------------------------------


def rst_height_python_light_example(spark):
    """Get the height in pixels of a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, width=4, height=3)
    result = tile_df.select(rx.rst_height("tile").alias("height")).first()
    return result["height"]


rst_height_python_light_example_output = """
+------+
|height|
+------+
|3     |
+------+
"""


# ---------------------------------------------------------------------------
# rst_max — maximum pixel values per band
# ---------------------------------------------------------------------------


def rst_max_python_light_example(spark):
    """Get the maximum pixel value for each band using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, width=4, height=3, count=1)
    result = tile_df.select(rx.rst_max("tile").alias("band_max")).first()
    return result["band_max"]


rst_max_python_light_example_output = """
+--------+
|band_max|
+--------+
|[11.0]  |
+--------+
"""


# ---------------------------------------------------------------------------
# rst_median — median pixel values per band
# ---------------------------------------------------------------------------


def rst_median_python_light_example(spark):
    """Get the median pixel value for each band using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, width=4, height=3, count=1)
    result = tile_df.select(rx.rst_median("tile").alias("band_median")).first()
    return result["band_median"]


rst_median_python_light_example_output = """
+-----------+
|band_median|
+-----------+
|[5.5]      |
+-----------+
"""


# ---------------------------------------------------------------------------
# rst_memsize — in-memory size in bytes
# ---------------------------------------------------------------------------


def rst_memsize_python_light_example(spark):
    """Get the in-memory size of a raster tile in bytes using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_memsize("tile").alias("memsize")).first()
    return result["memsize"]


rst_memsize_python_light_example_output = """
+-------+
|memsize|
+-------+
|432    |
+-------+
"""


# ---------------------------------------------------------------------------
# rst_metadata — raster metadata map
# ---------------------------------------------------------------------------


def rst_metadata_python_light_example(spark):
    """Get the metadata map for a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_metadata("tile").alias("metadata")).first()
    return result["metadata"]


rst_metadata_python_light_example_output = """
+--------------------------------------------------+
|                                          metadata|
+--------------------------------------------------+
|{driver -> GTiff, crs -> EPSG:4326, count -> 1,...|
+--------------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_min — minimum pixel values per band
# ---------------------------------------------------------------------------


def rst_min_python_light_example(spark):
    """Get the minimum pixel value for each band using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, width=4, height=3, count=1)
    result = tile_df.select(rx.rst_min("tile").alias("band_min")).first()
    return result["band_min"]


rst_min_python_light_example_output = """
+--------+
|band_min|
+--------+
|[0.0]   |
+--------+
"""


# ---------------------------------------------------------------------------
# rst_pixelcount — total pixel count
# ---------------------------------------------------------------------------


def rst_pixelcount_python_light_example(spark):
    """Get the total pixel count for a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, width=4, height=3)
    result = tile_df.select(rx.rst_pixelcount("tile").alias("pixel_count")).first()
    return result["pixel_count"]


rst_pixelcount_python_light_example_output = """
+-----------+
|pixel_count|
+-----------+
|[12]       |
+-----------+
"""


# ---------------------------------------------------------------------------
# rst_pixelheight — pixel height in ground units
# ---------------------------------------------------------------------------


def rst_pixelheight_python_light_example(spark):
    """Get the pixel height in ground units (degrees for EPSG:4326) using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_pixelheight("tile").alias("pixel_height")).first()
    return result["pixel_height"]


rst_pixelheight_python_light_example_output = """
+------------+
|pixel_height|
+------------+
|0.5         |
+------------+
"""


# ---------------------------------------------------------------------------
# rst_pixelwidth — pixel width in ground units
# ---------------------------------------------------------------------------


def rst_pixelwidth_python_light_example(spark):
    """Get the pixel width in ground units (degrees for EPSG:4326) using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_pixelwidth("tile").alias("pixel_width")).first()
    return result["pixel_width"]


rst_pixelwidth_python_light_example_output = """
+-----------+
|pixel_width|
+-----------+
|0.5        |
+-----------+
"""


# ---------------------------------------------------------------------------
# rst_rotation — rotation in radians
# ---------------------------------------------------------------------------


def rst_rotation_python_light_example(spark):
    """Get the rotation angle of a raster tile in radians using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_rotation("tile").alias("rotation")).first()
    return result["rotation"]


rst_rotation_python_light_example_output = """
+--------+
|rotation|
+--------+
|0.0     |
+--------+
"""


# ---------------------------------------------------------------------------
# rst_scalex — scale (pixel size) in X
# ---------------------------------------------------------------------------


def rst_scalex_python_light_example(spark):
    """Get the pixel scale in the X direction using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_scalex("tile").alias("scale_x")).first()
    return result["scale_x"]


rst_scalex_python_light_example_output = """
+-------+
|scale_x|
+-------+
|0.5    |
+-------+
"""


# ---------------------------------------------------------------------------
# rst_scaley — scale (pixel size) in Y
# ---------------------------------------------------------------------------


def rst_scaley_python_light_example(spark):
    """Get the pixel scale in the Y direction using the light pyrx tier.

    For north-up rasters, the Y scale is negative (top row corresponds to the
    maximum latitude). The absolute value equals the pixel height.
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_scaley("tile").alias("scale_y")).first()
    return result["scale_y"]


rst_scaley_python_light_example_output = """
+-------+
|scale_y|
+-------+
|-0.5   |
+-------+
"""


# ---------------------------------------------------------------------------
# rst_skewx — skew in X
# ---------------------------------------------------------------------------


def rst_skewx_python_light_example(spark):
    """Get the skew coefficient in the X direction using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_skewx("tile").alias("skew_x")).first()
    return result["skew_x"]


rst_skewx_python_light_example_output = """
+------+
|skew_x|
+------+
|0.0   |
+------+
"""


# ---------------------------------------------------------------------------
# rst_skewy — skew in Y
# ---------------------------------------------------------------------------


def rst_skewy_python_light_example(spark):
    """Get the skew coefficient in the Y direction using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_skewy("tile").alias("skew_y")).first()
    return result["skew_y"]


rst_skewy_python_light_example_output = """
+------+
|skew_y|
+------+
|0.0   |
+------+
"""


# ---------------------------------------------------------------------------
# rst_srid — spatial reference ID (integer)
# ---------------------------------------------------------------------------


def rst_srid_python_light_example(spark):
    """Get the EPSG SRID integer for a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, epsg=4326)
    result = tile_df.select(rx.rst_srid("tile").alias("srid")).first()
    return result["srid"]


rst_srid_python_light_example_output = """
+----+
|srid|
+----+
|4326|
+----+
"""


# ---------------------------------------------------------------------------
# rst_crs — CRS as authority string or WKT
# ---------------------------------------------------------------------------


def rst_crs_python_light_example(spark):
    """Get the CRS string (e.g. 'EPSG:4326') for a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, epsg=4326)
    result = tile_df.select(rx.rst_crs("tile").alias("crs")).first()
    return result["crs"]


rst_crs_python_light_example_output = """
+---------+
|crs      |
+---------+
|EPSG:4326|
+---------+
"""


# ---------------------------------------------------------------------------
# rst_subdatasets — list of subdataset names
# ---------------------------------------------------------------------------


def rst_subdatasets_python_light_example(spark):
    """Get the subdatasets map for a raster tile using the light pyrx tier.

    Plain GeoTIFFs have no subdatasets and return an empty map.
    Multi-dataset formats (NetCDF, HDF5, etc.) return a map of dataset names
    to descriptions.
    """
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_subdatasets("tile").alias("subdatasets")).first()
    return result["subdatasets"]


rst_subdatasets_python_light_example_output = """
+-----------+
|subdatasets|
+-----------+
|{}         |
+-----------+
"""


# ---------------------------------------------------------------------------
# rst_summary — statistical summary as JSON string
# ---------------------------------------------------------------------------


def rst_summary_python_light_example(spark):
    """Get a statistical summary of a raster tile as a JSON string using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_summary("tile").alias("summary")).first()
    return result["summary"]


rst_summary_python_light_example_output = """
+------------------------------------------------------------+
|                                                     summary|
+------------------------------------------------------------+
|{driverShortName: GTiff, size: [4, 3], coordinateSystem: ...|
+------------------------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_type — data type per band
# ---------------------------------------------------------------------------


def rst_type_python_light_example(spark):
    """Get the data type string for each band using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, count=1)
    result = tile_df.select(rx.rst_type("tile").alias("band_types")).first()
    return result["band_types"]


rst_type_python_light_example_output = """
+----------+
|band_types|
+----------+
|[Float32] |
+----------+
"""


# ---------------------------------------------------------------------------
# rst_upperleftx — upper-left corner X coordinate
# ---------------------------------------------------------------------------


def rst_upperleftx_python_light_example(spark):
    """Get the X coordinate of the upper-left corner of a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_upperleftx("tile").alias("upper_left_x")).first()
    return result["upper_left_x"]


rst_upperleftx_python_light_example_output = """
+------------+
|upper_left_x|
+------------+
|        10.0|
+------------+
"""


# ---------------------------------------------------------------------------
# rst_upperlefty — upper-left corner Y coordinate
# ---------------------------------------------------------------------------


def rst_upperlefty_python_light_example(spark):
    """Get the Y coordinate of the upper-left corner of a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_upperlefty("tile").alias("upper_left_y")).first()
    return result["upper_left_y"]


rst_upperlefty_python_light_example_output = """
+------------+
|upper_left_y|
+------------+
|        50.0|
+------------+
"""


# ---------------------------------------------------------------------------
# rst_isempty — check if raster is empty
# ---------------------------------------------------------------------------


def rst_isempty_python_light_example(spark):
    """Check whether a raster tile is empty using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_isempty("tile").alias("is_empty")).first()
    return result["is_empty"]


rst_isempty_python_light_example_output = """
+--------+
|is_empty|
+--------+
|   false|
+--------+
"""


# ---------------------------------------------------------------------------
# rst_tryopen — validate raster can be opened
# ---------------------------------------------------------------------------


def rst_tryopen_python_light_example(spark):
    """Validate that a raster tile can be opened successfully using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark)
    result = tile_df.select(rx.rst_tryopen("tile").alias("try_open")).first()
    return result["try_open"]


rst_tryopen_python_light_example_output = """
+--------+
|try_open|
+--------+
|    true|
+--------+
"""


# ---------------------------------------------------------------------------
# rst_histogram — per-band histogram as MAP<STRING, ARRAY<LONG>>
# ---------------------------------------------------------------------------


def rst_histogram_python_light_example(spark):
    """Compute a per-band histogram with default settings using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx

    rx.register(spark)
    tile_df = _tile_df(spark, count=1)
    result = tile_df.select(rx.rst_histogram("tile").alias("histogram")).first()
    return result["histogram"]


rst_histogram_python_light_example_output = """
+--------------------------------------------------+
|                                         histogram|
+--------------------------------------------------+
|{band_1 -> [1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,...|
+--------------------------------------------------+
"""
