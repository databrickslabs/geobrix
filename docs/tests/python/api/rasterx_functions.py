"""
Python code examples for RasterX Function Reference documentation.
Single source of truth for docs/docs/api/rasterx-functions.mdx

Imports and registration are in the common setup only. SQL examples are in rasterx_functions_sql.py.
"""

try:
    from databricks.labs.gbx.rasterx import functions as rx
except ImportError:
    rx = None

# Sample data path at runtime (path_config)
from path_config import SAMPLE_DATA_BASE
SAMPLE_RASTER_PATH = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"


def rasterx_setup_example(spark):
    """Common setup: import, register RasterX, and load sample rasters. Run once before examples."""
    from databricks.labs.gbx.rasterx import functions as rx
    rx.register(spark)
    rasters = spark.read.format("gdal").load(SAMPLE_RASTER_PATH)
    rasters.createOrReplaceTempView("rasters")
    return rasters


rasterx_setup_example_output = """
RasterX registered. Temp view `rasters` created from sample raster.
"""


# ---------------------------------------------------------------------------
# Shared helper — in-memory GeoTIFF (no sample data needed)
# ---------------------------------------------------------------------------

def _make_geotiff_bytes(width=4, height=3, count=1, epsg=4326):
    """Return in-memory float32 GTiff bytes (width x height, count bands)."""
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


def _heavy_tile_df(spark, **kw):
    """One-row heavy-tier tile DataFrame built from in-memory synthetic bytes."""
    from pyspark.sql import functions as f
    from databricks.labs.gbx.rasterx import functions as rx

    raster = _make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


# ---------------------------------------------------------------------------
# rst_avg — per-band average pixel values (heavy tier)
# ---------------------------------------------------------------------------

def rst_avg_python_heavy_example(spark):
    """Get per-band average pixel values via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, width=4, height=3, count=1)
    result = tile_df.select(rx.rst_avg("tile").alias("band_averages")).first()
    return result["band_averages"]


rst_avg_python_heavy_example_output = """
[5.5]
"""


# ---------------------------------------------------------------------------
# rst_boundingbox — bounding polygon (heavy tier; returns WKB binary)
# ---------------------------------------------------------------------------

def rst_boundingbox_python_heavy_example(spark):
    """Get the bounding box of a raster tile as WKB binary via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, width=4, height=3)
    result = tile_df.select(rx.rst_boundingbox("tile").alias("bbox")).first()
    return result["bbox"]


rst_boundingbox_python_heavy_example_output = """
[WKB binary bytes — bounding POLYGON of the raster extent]
"""


# ---------------------------------------------------------------------------
# rst_numbands — band count (heavy tier)
# ---------------------------------------------------------------------------

def rst_numbands_python_heavy_example(spark):
    """Get the number of bands in a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, count=1)
    result = tile_df.select(rx.rst_numbands("tile").alias("num_bands")).first()
    return result["num_bands"]


rst_numbands_python_heavy_example_output = """
1
"""


# ---------------------------------------------------------------------------
# rst_width — pixel width (heavy tier)
# ---------------------------------------------------------------------------

def rst_width_python_heavy_example(spark):
    """Get the pixel width of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, width=4)
    result = tile_df.select(rx.rst_width("tile").alias("width")).first()
    return result["width"]


rst_width_python_heavy_example_output = """
4
"""


# ---------------------------------------------------------------------------
# rst_fromfile — load raster from a file path (heavy Python tier; no Scala Column form)
# ---------------------------------------------------------------------------

def rst_fromfile_python_heavy_example(spark):
    """Load a raster tile from a file path using the heavy rasterx Python tier.

    rst_fromfile is available in the light (pyrx) and heavy (rasterx) Python tiers only.
    There is no Scala Column form: the JVM executor cannot read UC Volume FUSE paths,
    so this function delegates to the Python worker.
    """
    import tempfile
    import os
    from pyspark.sql import functions as f
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)

    # Write a synthetic GeoTIFF to a temp file on the driver
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


rst_fromfile_python_heavy_example_output = """
4
"""


# ---------------------------------------------------------------------------
# rst_bandmetadata — per-band metadata map (heavy tier)
# ---------------------------------------------------------------------------


def rst_bandmetadata_python_heavy_example(spark):
    """Get metadata for band 1 of a raster tile via the heavy rasterx tier."""
    from pyspark.sql import functions as f
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, width=4, height=3, count=1)
    result = tile_df.select(
        rx.rst_bandmetadata("tile", f.lit(1)).alias("band_meta")
    ).first()
    return result["band_meta"]


rst_bandmetadata_python_heavy_example_output = """
{'STATISTICS_MAXIMUM': '11.0', 'STATISTICS_MEAN': '5.5', 'STATISTICS_MINIMUM': '0.0', ...}
"""


# ---------------------------------------------------------------------------
# rst_format — GDAL format name (heavy tier)
# ---------------------------------------------------------------------------


def rst_format_python_heavy_example(spark):
    """Get the GDAL driver/format name of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_format("tile").alias("format")).first()
    return result["format"]


rst_format_python_heavy_example_output = """
GTiff
"""


# ---------------------------------------------------------------------------
# rst_georeference — georeference parameters map (heavy tier)
# ---------------------------------------------------------------------------


def rst_georeference_python_heavy_example(spark):
    """Get georeference parameters (scale, skew, origin) via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_georeference("tile").alias("georeference")).first()
    return result["georeference"]


rst_georeference_python_heavy_example_output = """
{'scaleX': 0.5, 'scaleY': -0.5, 'skewX': 0.0, 'skewY': 0.0, 'upperLeftX': 10.0, 'upperLeftY': 50.0}
"""


# ---------------------------------------------------------------------------
# rst_getnodata — NoData values per band (heavy tier)
# ---------------------------------------------------------------------------


def rst_getnodata_python_heavy_example(spark):
    """Get the NoData values for each band via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, count=1)
    result = tile_df.select(rx.rst_getnodata("tile").alias("nodata")).first()
    return result["nodata"]


rst_getnodata_python_heavy_example_output = """
[-9999.0]
"""


# ---------------------------------------------------------------------------
# rst_getsubdataset — extract named subdataset (heavy tier)
# ---------------------------------------------------------------------------


def rst_getsubdataset_python_heavy_example(spark):
    """Get the subdataset map for a raster tile via the heavy rasterx tier.

    Plain GeoTIFFs have no subdatasets so rst_subdatasets returns an empty map.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_subdatasets("tile").alias("subs")).first()
    return result["subs"]


rst_getsubdataset_python_heavy_example_output = """
{}
"""


# ---------------------------------------------------------------------------
# rst_height — raster height in pixels (heavy tier)
# ---------------------------------------------------------------------------


def rst_height_python_heavy_example(spark):
    """Get the height in pixels of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, width=4, height=3)
    result = tile_df.select(rx.rst_height("tile").alias("height")).first()
    return result["height"]


rst_height_python_heavy_example_output = """
3
"""


# ---------------------------------------------------------------------------
# rst_max — maximum pixel values per band (heavy tier)
# ---------------------------------------------------------------------------


def rst_max_python_heavy_example(spark):
    """Get the maximum pixel value for each band via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, width=4, height=3, count=1)
    result = tile_df.select(rx.rst_max("tile").alias("band_max")).first()
    return result["band_max"]


rst_max_python_heavy_example_output = """
[11.0]
"""


# ---------------------------------------------------------------------------
# rst_median — median pixel values per band (heavy tier)
# ---------------------------------------------------------------------------


def rst_median_python_heavy_example(spark):
    """Get the median pixel value for each band via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, width=4, height=3, count=1)
    result = tile_df.select(rx.rst_median("tile").alias("band_median")).first()
    return result["band_median"]


rst_median_python_heavy_example_output = """
[5.5]
"""


# ---------------------------------------------------------------------------
# rst_memsize — in-memory size in bytes (heavy tier)
# ---------------------------------------------------------------------------


def rst_memsize_python_heavy_example(spark):
    """Get the in-memory size of a raster tile in bytes via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_memsize("tile").alias("memsize")).first()
    return result["memsize"]


rst_memsize_python_heavy_example_output = """
> 0
"""


# ---------------------------------------------------------------------------
# rst_metadata — raster metadata map (heavy tier)
# ---------------------------------------------------------------------------


def rst_metadata_python_heavy_example(spark):
    """Get the metadata map for a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_metadata("tile").alias("metadata")).first()
    return result["metadata"]


rst_metadata_python_heavy_example_output = """
{'AREA_OR_POINT': 'Area', ...}
"""


# ---------------------------------------------------------------------------
# rst_min — minimum pixel values per band (heavy tier)
# ---------------------------------------------------------------------------


def rst_min_python_heavy_example(spark):
    """Get the minimum pixel value for each band via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, width=4, height=3, count=1)
    result = tile_df.select(rx.rst_min("tile").alias("band_min")).first()
    return result["band_min"]


rst_min_python_heavy_example_output = """
[0.0]
"""


# ---------------------------------------------------------------------------
# rst_pixelcount — total pixel count (heavy tier)
# ---------------------------------------------------------------------------


def rst_pixelcount_python_heavy_example(spark):
    """Get the total pixel count for a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, width=4, height=3)
    result = tile_df.select(rx.rst_pixelcount("tile").alias("pixel_count")).first()
    return result["pixel_count"]


rst_pixelcount_python_heavy_example_output = """
[12]
"""


# ---------------------------------------------------------------------------
# rst_pixelheight — pixel height in ground units (heavy tier)
# ---------------------------------------------------------------------------


def rst_pixelheight_python_heavy_example(spark):
    """Get the pixel height in ground units via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_pixelheight("tile").alias("pixel_height")).first()
    return result["pixel_height"]


rst_pixelheight_python_heavy_example_output = """
0.5
"""


# ---------------------------------------------------------------------------
# rst_pixelwidth — pixel width in ground units (heavy tier)
# ---------------------------------------------------------------------------


def rst_pixelwidth_python_heavy_example(spark):
    """Get the pixel width in ground units via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_pixelwidth("tile").alias("pixel_width")).first()
    return result["pixel_width"]


rst_pixelwidth_python_heavy_example_output = """
0.5
"""


# ---------------------------------------------------------------------------
# rst_rotation — rotation in radians (heavy tier)
# ---------------------------------------------------------------------------


def rst_rotation_python_heavy_example(spark):
    """Get the rotation angle of a raster tile in radians via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_rotation("tile").alias("rotation")).first()
    return result["rotation"]


rst_rotation_python_heavy_example_output = """
0.0
"""


# ---------------------------------------------------------------------------
# rst_scalex — scale in X (heavy tier)
# ---------------------------------------------------------------------------


def rst_scalex_python_heavy_example(spark):
    """Get the pixel scale in the X direction via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_scalex("tile").alias("scale_x")).first()
    return result["scale_x"]


rst_scalex_python_heavy_example_output = """
0.5
"""


# ---------------------------------------------------------------------------
# rst_scaley — scale in Y (heavy tier)
# ---------------------------------------------------------------------------


def rst_scaley_python_heavy_example(spark):
    """Get the pixel scale in the Y direction via the heavy rasterx tier.

    For north-up rasters, the Y scale is negative (top row = maximum latitude).
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_scaley("tile").alias("scale_y")).first()
    return result["scale_y"]


rst_scaley_python_heavy_example_output = """
-0.5
"""


# ---------------------------------------------------------------------------
# rst_skewx — skew in X (heavy tier)
# ---------------------------------------------------------------------------


def rst_skewx_python_heavy_example(spark):
    """Get the skew coefficient in the X direction via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_skewx("tile").alias("skew_x")).first()
    return result["skew_x"]


rst_skewx_python_heavy_example_output = """
0.0
"""


# ---------------------------------------------------------------------------
# rst_skewy — skew in Y (heavy tier)
# ---------------------------------------------------------------------------


def rst_skewy_python_heavy_example(spark):
    """Get the skew coefficient in the Y direction via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_skewy("tile").alias("skew_y")).first()
    return result["skew_y"]


rst_skewy_python_heavy_example_output = """
0.0
"""


# ---------------------------------------------------------------------------
# rst_srid — spatial reference ID integer (heavy tier)
# ---------------------------------------------------------------------------


def rst_srid_python_heavy_example(spark):
    """Get the EPSG SRID integer for a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, epsg=4326)
    result = tile_df.select(rx.rst_srid("tile").alias("srid")).first()
    return result["srid"]


rst_srid_python_heavy_example_output = """
4326
"""


# ---------------------------------------------------------------------------
# rst_crs — CRS as authority string or WKT (heavy tier)
# ---------------------------------------------------------------------------


def rst_crs_python_heavy_example(spark):
    """Get the CRS string for a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, epsg=4326)
    result = tile_df.select(rx.rst_crs("tile").alias("crs")).first()
    return result["crs"]


rst_crs_python_heavy_example_output = """
EPSG:4326
"""


# ---------------------------------------------------------------------------
# rst_subdatasets — list of subdataset names (heavy tier)
# ---------------------------------------------------------------------------


def rst_subdatasets_python_heavy_example(spark):
    """Get the subdatasets map for a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_subdatasets("tile").alias("subdatasets")).first()
    return result["subdatasets"]


rst_subdatasets_python_heavy_example_output = """
{}
"""


# ---------------------------------------------------------------------------
# rst_summary — statistical summary as JSON string (heavy tier)
# ---------------------------------------------------------------------------


def rst_summary_python_heavy_example(spark):
    """Get a statistical summary of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_summary("tile").alias("summary")).first()
    return result["summary"]


rst_summary_python_heavy_example_output = """
{"driver": "GTiff", "size": [4, 3], "crs": "EPSG:4326", "bands": [...]}
"""


# ---------------------------------------------------------------------------
# rst_type — data type per band (heavy tier)
# ---------------------------------------------------------------------------


def rst_type_python_heavy_example(spark):
    """Get the data type string for each band via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, count=1)
    result = tile_df.select(rx.rst_type("tile").alias("band_types")).first()
    return result["band_types"]


rst_type_python_heavy_example_output = """
['Float32']
"""


# ---------------------------------------------------------------------------
# rst_upperleftx — upper-left corner X coordinate (heavy tier)
# ---------------------------------------------------------------------------


def rst_upperleftx_python_heavy_example(spark):
    """Get the X coordinate of the upper-left corner of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_upperleftx("tile").alias("upper_left_x")).first()
    return result["upper_left_x"]


rst_upperleftx_python_heavy_example_output = """
10.0
"""


# ---------------------------------------------------------------------------
# rst_upperlefty — upper-left corner Y coordinate (heavy tier)
# ---------------------------------------------------------------------------


def rst_upperlefty_python_heavy_example(spark):
    """Get the Y coordinate of the upper-left corner of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_upperlefty("tile").alias("upper_left_y")).first()
    return result["upper_left_y"]


rst_upperlefty_python_heavy_example_output = """
50.0
"""


# ---------------------------------------------------------------------------
# rst_isempty — check if raster is empty (heavy tier)
# ---------------------------------------------------------------------------


def rst_isempty_python_heavy_example(spark):
    """Check whether a raster tile is empty via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_isempty("tile").alias("is_empty")).first()
    return result["is_empty"]


rst_isempty_python_heavy_example_output = """
False
"""


# ---------------------------------------------------------------------------
# rst_tryopen — validate raster can be opened (heavy tier)
# ---------------------------------------------------------------------------


def rst_tryopen_python_heavy_example(spark):
    """Validate that a raster tile can be opened successfully via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark)
    result = tile_df.select(rx.rst_tryopen("tile").alias("try_open")).first()
    return result["try_open"]


rst_tryopen_python_heavy_example_output = """
True
"""


# ---------------------------------------------------------------------------
# rst_histogram — per-band histogram (heavy tier)
# ---------------------------------------------------------------------------


def rst_histogram_python_heavy_example(spark):
    """Compute a per-band histogram with explicit min/max via the heavy rasterx tier."""
    from pyspark.sql import functions as f
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _heavy_tile_df(spark, count=1)
    # Pass explicit n_buckets, min_val, max_val to avoid null-sentinel short-circuit.
    result = tile_df.select(
        rx.rst_histogram("tile", f.lit(16), f.lit(0.0), f.lit(11.0)).alias("histogram")
    ).first()
    return result["histogram"]


rst_histogram_python_heavy_example_output = """
{'band_1': [1, 1, 1, ..., 1, 0, ...]}
"""
