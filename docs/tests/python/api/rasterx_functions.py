"""
Python code examples for RasterX Function Reference documentation.
Single source of truth for docs/docs/api/rasterx-functions.mdx

Imports and registration are in the common setup only. SQL examples are in rasterx_functions_sql.py.
All accessor examples use the shared canonical fixtures from _fixtures.py.
"""

try:
    from databricks.labs.gbx.rasterx import functions as rx
except ImportError:
    rx = None

# Sample data path at runtime (path_config)
from path_config import SAMPLE_DATA_BASE

SAMPLE_RASTER_PATH = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"


# ---------------------------------------------------------------------------
# Shared helpers — imported from _fixtures.py (canonical fixture builders)
# ---------------------------------------------------------------------------


def _get_single_band_df_heavy(spark):
    from _fixtures import single_band_tile_df_heavy  # noqa: PLC0415
    return single_band_tile_df_heavy(spark)


def _get_multiband_df_heavy(spark):
    from _fixtures import multiband_tile_df_heavy  # noqa: PLC0415
    return multiband_tile_df_heavy(spark)


def _get_netcdf_df_heavy(spark):
    from _fixtures import netcdf_tile_df_heavy  # noqa: PLC0415
    return netcdf_tile_df_heavy(spark)


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


def _make_netcdf_bytes(width=4, height=4):
    """Return NETCDF4 bytes with 'temperature' and 'precipitation' variables.

    Two variables cause GDAL to expose them as subdatasets, enabling
    rst_getsubdataset to extract a named layer by name.
    """
    import os
    import tempfile

    import netCDF4 as nc4
    import numpy as np

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


def _make_tagged_geotiff_bytes(width=4, height=3, epsg=4326):
    """Return in-memory float32 GTiff bytes with band-level metadata tags.

    Band 1 carries GDAL_METADATA tags ``units`` and ``source`` so that
    rst_bandmetadata returns a non-empty map.
    """
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    transform = from_origin(10.0, 50.0, 0.5, 0.5)
    profile = dict(
        driver="GTiff",
        width=width,
        height=height,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=transform,
        nodata=-9999.0,
    )
    data = np.arange(width * height, dtype="float32").reshape(height, width)
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(data, 1)
            ds.update_tags(1, units="m", source="synthetic")
        return mf.read()


def _heavy_tagged_tile_df(spark, **kw):
    """One-row heavy-tier tile DataFrame built from a tagged GeoTIFF."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    raster = _make_tagged_geotiff_bytes(**kw)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


def _heavy_nc_tile_df(spark, width=4, height=4):
    """One-row heavy-tier tile DataFrame built from NetCDF bytes."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    raster = _make_netcdf_bytes(width=width, height=height)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("netCDF")).alias("tile"))


def _heavy_tile_df(spark, **kw):
    """One-row heavy-tier tile DataFrame built from in-memory synthetic bytes."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    raster = _make_geotiff_bytes(**kw)
    df = spark.createDataFrame([(raster,)], ["raster"])
    return df.select(rx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile"))


# ---------------------------------------------------------------------------
# rst_avg — per-band average pixel values (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — single-band is all-NoData)
# ---------------------------------------------------------------------------


def rst_avg_python_heavy_example(spark):
    """Get per-band average pixel values via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) because the
    canonical single-band sentinel2 tile is all-NoData (rst_avg returns [None]).
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_avg("tile").alias("band_averages")).first()
    return result["band_averages"]


rst_avg_python_heavy_example_output = """
+------------------------------------+
|band_averages                       |
+------------------------------------+
|[83.59375, 153.125, 114.3125]       |
+------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_boundingbox — bounding polygon (heavy tier; returns WKB binary)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_boundingbox_python_heavy_example(spark):
    """Get the bounding box of a raster tile as WKB binary via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_boundingbox("tile").alias("bbox")).first()
    return result["bbox"]


rst_boundingbox_python_heavy_example_output = """
+----+
|bbox|
+----+
|[...|
+----+
(WKB binary — bounding POLYGON of the raster extent in EPSG:32618)
"""


# ---------------------------------------------------------------------------
# rst_numbands — band count (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands)
# ---------------------------------------------------------------------------


def rst_numbands_python_heavy_example(spark):
    """Get the number of bands in a raster tile via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) to show a
    meaningful multi-band result.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_numbands("tile").alias("num_bands")).first()
    return result["num_bands"]


rst_numbands_python_heavy_example_output = """
+---------+
|num_bands|
+---------+
|3        |
+---------+
"""


# ---------------------------------------------------------------------------
# rst_width — pixel width (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 236 columns)
# ---------------------------------------------------------------------------


def rst_width_python_heavy_example(spark):
    """Get the pixel width of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_width("tile").alias("width")).first()
    return result["width"]


rst_width_python_heavy_example_output = """
+-----+
|width|
+-----+
|236  |
+-----+
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
    import os
    import tempfile

    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

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
+-----+
|width|
+-----+
|    4|
+-----+
"""


# ---------------------------------------------------------------------------
# rst_bandmetadata — per-band metadata map (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — has per-band GDAL metadata tags)
# ---------------------------------------------------------------------------


def rst_bandmetadata_python_heavy_example(spark):
    """Get metadata for band 1 of a raster tile via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif) which carries per-band
    GDAL metadata tags (name, wavelength_nm, band_index) written at fixture
    creation time.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(
        rx.rst_bandmetadata("tile", f.lit(1)).alias("band_meta")
    ).first()
    return result["band_meta"]


rst_bandmetadata_python_heavy_example_output = """
+----------------------------------------------+
|band_meta                                     |
+----------------------------------------------+
|{name -> red, wavelength_nm -> 665, band_in...|
+----------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_format — GDAL format name (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_format_python_heavy_example(spark):
    """Get the GDAL driver/format name of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_format("tile").alias("format")).first()
    return result["format"]


rst_format_python_heavy_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
"""


# ---------------------------------------------------------------------------
# rst_georeference — georeference parameters map (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_georeference_python_heavy_example(spark):
    """Get georeference parameters (scale, skew, origin) via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_georeference("tile").alias("georeference")).first()
    return result["georeference"]


rst_georeference_python_heavy_example_output = """
+--------------------------------------------------------------+
|georeference                                                  |
+--------------------------------------------------------------+
|{scaleX -> 10.0, scaleY -> -10.0, upperLeftX -> 2121950.0,...|
+--------------------------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_getnodata — NoData values per band (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — nodata=0.0)
# ---------------------------------------------------------------------------


def rst_getnodata_python_heavy_example(spark):
    """Get the NoData values for each band via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_getnodata("tile").alias("nodata")).first()
    return result["nodata"]


rst_getnodata_python_heavy_example_output = """
+--------+
|nodata  |
+--------+
|[0.0]   |
+--------+
"""


# ---------------------------------------------------------------------------
# rst_getsubdataset — extract named subdataset (heavy tier)
# Fixture: NETCDF (prAdjust_day_HadGEM2-CC_*.nc — has time_bnds and prAdjust)
# ---------------------------------------------------------------------------


def rst_getsubdataset_python_heavy_example(spark):
    """Extract a named subdataset from a NetCDF raster tile via the heavy rasterx tier.

    Uses the committed CMIP5 NetCDF fixture which has two subdatasets: time_bnds
    and prAdjust. Subdatasets require a multi-layer format such as NetCDF.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    nc_df = _get_netcdf_df_heavy(spark)
    result = nc_df.select(
        rx.rst_getsubdataset("tile", f.lit("prAdjust")).alias("subdataset")
    ).first()
    return result["subdataset"]


rst_getsubdataset_python_heavy_example_output = """
+-------------+
|subdataset   |
+-------------+
|{..., ..., ..|
+-------------+
(tile struct for the prAdjust subdataset — 31 bands, 360x720 pixels)
"""


# ---------------------------------------------------------------------------
# rst_height — raster height in pixels (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 161 rows)
# ---------------------------------------------------------------------------


def rst_height_python_heavy_example(spark):
    """Get the height in pixels of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_height("tile").alias("height")).first()
    return result["height"]


rst_height_python_heavy_example_output = """
+------+
|height|
+------+
|161   |
+------+
"""


# ---------------------------------------------------------------------------
# rst_max — maximum pixel values per band (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — single-band is all-NoData)
# ---------------------------------------------------------------------------


def rst_max_python_heavy_example(spark):
    """Get the maximum pixel value for each band via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) because the
    canonical single-band sentinel2 tile is all-NoData (rst_max returns [None]).
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_max("tile").alias("band_max")).first()
    return result["band_max"]


rst_max_python_heavy_example_output = """
+---------------------+
|band_max             |
+---------------------+
|[119.0, 197.0, 148.0]|
+---------------------+
"""


# ---------------------------------------------------------------------------
# rst_median — median pixel values per band (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — single-band is all-NoData)
# ---------------------------------------------------------------------------


def rst_median_python_heavy_example(spark):
    """Get the median pixel value for each band via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) because the
    canonical single-band sentinel2 tile is all-NoData (rst_median returns [None]).
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_median("tile").alias("band_median")).first()
    return result["band_median"]


rst_median_python_heavy_example_output = """
+---------------------+
|band_median          |
+---------------------+
|[85.0, 157.5, 111.5] |
+---------------------+
"""


# ---------------------------------------------------------------------------
# rst_memsize — in-memory size in bytes (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_memsize_python_heavy_example(spark):
    """Get the in-memory size of a raster tile in bytes via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_memsize("tile").alias("memsize")).first()
    return result["memsize"]


rst_memsize_python_heavy_example_output = """
+-------+
|memsize|
+-------+
|71749  |
+-------+
"""


# ---------------------------------------------------------------------------
# rst_metadata — raster metadata map (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_metadata_python_heavy_example(spark):
    """Get the metadata map for a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_metadata("tile").alias("metadata")).first()
    return result["metadata"]


rst_metadata_python_heavy_example_output = """
+--------------------------------------------------+
|metadata                                          |
+--------------------------------------------------+
|{driver -> GTiff, crs -> EPSG:32618, count -> 1,..|
+--------------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_min — minimum pixel values per band (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — single-band is all-NoData)
# ---------------------------------------------------------------------------


def rst_min_python_heavy_example(spark):
    """Get the minimum pixel value for each band via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) because the
    canonical single-band sentinel2 tile is all-NoData (rst_min returns [None]).
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_min("tile").alias("band_min")).first()
    return result["band_min"]


rst_min_python_heavy_example_output = """
+------------------+
|band_min          |
+------------------+
|[50.0, 102.0, 82.0]|
+------------------+
"""


# ---------------------------------------------------------------------------
# rst_pixelcount — total pixel count (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — 8x8=64 valid pixels per band)
# ---------------------------------------------------------------------------


def rst_pixelcount_python_heavy_example(spark):
    """Get the count of valid (non-NoData) pixels per band via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 8x8, no NoData set) so
    each band yields 64 valid pixels. The single-band sentinel2 tile has
    NoData=0 and all pixels equal zero, making pixelcount return [0].
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_pixelcount("tile").alias("pixel_count")).first()
    return result["pixel_count"]


rst_pixelcount_python_heavy_example_output = """
+------------+
|pixel_count |
+------------+
|[64, 64, 64]|
+------------+
"""


# ---------------------------------------------------------------------------
# rst_pixelheight — pixel height in ground units (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 10.0 m pixels in EPSG:32618)
# ---------------------------------------------------------------------------


def rst_pixelheight_python_heavy_example(spark):
    """Get the pixel height in ground units via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_pixelheight("tile").alias("pixel_height")).first()
    return result["pixel_height"]


rst_pixelheight_python_heavy_example_output = """
+------------+
|pixel_height|
+------------+
|10.0        |
+------------+
"""


# ---------------------------------------------------------------------------
# rst_pixelwidth — pixel width in ground units (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 10.0 m pixels in EPSG:32618)
# ---------------------------------------------------------------------------


def rst_pixelwidth_python_heavy_example(spark):
    """Get the pixel width in ground units via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_pixelwidth("tile").alias("pixel_width")).first()
    return result["pixel_width"]


rst_pixelwidth_python_heavy_example_output = """
+-----------+
|pixel_width|
+-----------+
|10.0       |
+-----------+
"""


# ---------------------------------------------------------------------------
# rst_rotation — rotation in radians (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — axis-aligned, rotation=0.0)
# ---------------------------------------------------------------------------


def rst_rotation_python_heavy_example(spark):
    """Get the rotation angle of a raster tile in radians via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_rotation("tile").alias("rotation")).first()
    return result["rotation"]


rst_rotation_python_heavy_example_output = """
+--------+
|rotation|
+--------+
|0.0     |
+--------+
"""


# ---------------------------------------------------------------------------
# rst_scalex — scale in X (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — scalex=10.0)
# ---------------------------------------------------------------------------


def rst_scalex_python_heavy_example(spark):
    """Get the pixel scale in the X direction via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_scalex("tile").alias("scale_x")).first()
    return result["scale_x"]


rst_scalex_python_heavy_example_output = """
+-------+
|scale_x|
+-------+
|10.0   |
+-------+
"""


# ---------------------------------------------------------------------------
# rst_scaley — scale in Y (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — scaley=-10.0 for north-up)
# ---------------------------------------------------------------------------


def rst_scaley_python_heavy_example(spark):
    """Get the pixel scale in the Y direction via the heavy rasterx tier.

    For north-up rasters, the Y scale is negative (top row = maximum Y).
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_scaley("tile").alias("scale_y")).first()
    return result["scale_y"]


rst_scaley_python_heavy_example_output = """
+-------+
|scale_y|
+-------+
|-10.0  |
+-------+
"""


# ---------------------------------------------------------------------------
# rst_skewx — skew in X (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — axis-aligned, skewx=0.0)
# ---------------------------------------------------------------------------


def rst_skewx_python_heavy_example(spark):
    """Get the skew coefficient in the X direction via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_skewx("tile").alias("skew_x")).first()
    return result["skew_x"]


rst_skewx_python_heavy_example_output = """
+------+
|skew_x|
+------+
|0.0   |
+------+
"""


# ---------------------------------------------------------------------------
# rst_skewy — skew in Y (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — axis-aligned, skewy=0.0)
# ---------------------------------------------------------------------------


def rst_skewy_python_heavy_example(spark):
    """Get the skew coefficient in the Y direction via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_skewy("tile").alias("skew_y")).first()
    return result["skew_y"]


rst_skewy_python_heavy_example_output = """
+------+
|skew_y|
+------+
|0.0   |
+------+
"""


# ---------------------------------------------------------------------------
# rst_srid — spatial reference ID integer (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# ---------------------------------------------------------------------------


def rst_srid_python_heavy_example(spark):
    """Get the EPSG SRID integer for a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_srid("tile").alias("srid")).first()
    return result["srid"]


rst_srid_python_heavy_example_output = """
+-----+
|srid |
+-----+
|32618|
+-----+
"""


# ---------------------------------------------------------------------------
# rst_crs — CRS as authority string or WKT (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# ---------------------------------------------------------------------------


def rst_crs_python_heavy_example(spark):
    """Get the CRS string for a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_crs("tile").alias("crs")).first()
    return result["crs"]


rst_crs_python_heavy_example_output = """
+----------+
|crs       |
+----------+
|EPSG:32618|
+----------+
"""


# ---------------------------------------------------------------------------
# rst_subdatasets — list of subdataset names (heavy tier)
# Fixture: NETCDF (prAdjust_day_HadGEM2-CC_*.nc — has time_bnds and prAdjust)
# ---------------------------------------------------------------------------


def rst_subdatasets_python_heavy_example(spark):
    """Get the subdatasets map for a NetCDF raster tile via the heavy rasterx tier.

    Uses the committed CMIP5 NetCDF fixture which has two subdatasets: time_bnds
    and prAdjust. Plain GeoTIFFs return an empty map.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_netcdf_df_heavy(spark)
    result = tile_df.select(rx.rst_subdatasets("tile").alias("subdatasets")).first()
    return result["subdatasets"]


rst_subdatasets_python_heavy_example_output = """
+------------------------------------------------------+
|subdatasets                                           |
+------------------------------------------------------+
|{SUBDATASET_1_NAME -> ..., SUBDATASET_1_DESC -> [31...|
+------------------------------------------------------+
(map with SUBDATASET_1_NAME/DESC for time_bnds and SUBDATASET_2_NAME/DESC for prAdjust)
"""


# ---------------------------------------------------------------------------
# rst_summary — statistical summary as JSON string (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — 8x8, 3 bands; single-band is all-NoData)
# ---------------------------------------------------------------------------


def rst_summary_python_heavy_example(spark):
    """Get a statistical summary of a raster tile via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) which has real pixel data.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_summary("tile").alias("summary")).first()
    return result["summary"]


rst_summary_python_heavy_example_output = """
+------------------------------------------------------------+
|summary                                                     |
+------------------------------------------------------------+
|{"driverShortName": "GTiff", "size": [8, 8], "coordinateS...|
+------------------------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_type — data type per band (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — UInt16 per band)
# ---------------------------------------------------------------------------


def rst_type_python_heavy_example(spark):
    """Get the data type string for each band via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands, UInt16).
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_type("tile").alias("band_types")).first()
    return result["band_types"]


rst_type_python_heavy_example_output = """
+-----------------------------+
|band_types                   |
+-----------------------------+
|[UInt16, UInt16, UInt16]     |
+-----------------------------+
"""


# ---------------------------------------------------------------------------
# rst_upperleftx — upper-left corner X coordinate (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — upperleftx=2121950.0 in EPSG:32618)
# ---------------------------------------------------------------------------


def rst_upperleftx_python_heavy_example(spark):
    """Get the X coordinate of the upper-left corner of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_upperleftx("tile").alias("upper_left_x")).first()
    return result["upper_left_x"]


rst_upperleftx_python_heavy_example_output = """
+------------+
|upper_left_x|
+------------+
|2121950.0   |
+------------+
"""


# ---------------------------------------------------------------------------
# rst_upperlefty — upper-left corner Y coordinate (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — upperlefty=-10790470.0 in EPSG:32618)
# ---------------------------------------------------------------------------


def rst_upperlefty_python_heavy_example(spark):
    """Get the Y coordinate of the upper-left corner of a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_upperlefty("tile").alias("upper_left_y")).first()
    return result["upper_left_y"]


rst_upperlefty_python_heavy_example_output = """
+---------------+
|upper_left_y   |
+---------------+
|-10790470.0    |
+---------------+
"""


# ---------------------------------------------------------------------------
# rst_isempty — check if raster is empty (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — has real pixel data, not empty)
# ---------------------------------------------------------------------------


def rst_isempty_python_heavy_example(spark):
    """Check whether a raster tile is empty via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif) which carries real pixel data.
    The canonical single-band sentinel2 tile has NoData=0 and all pixels equal zero,
    causing rst_isempty to return True.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_isempty("tile").alias("is_empty")).first()
    return result["is_empty"]


rst_isempty_python_heavy_example_output = """
+--------+
|is_empty|
+--------+
|false   |
+--------+
"""


# ---------------------------------------------------------------------------
# rst_tryopen — validate raster can be opened (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — committed, always openable)
# ---------------------------------------------------------------------------


def rst_tryopen_python_heavy_example(spark):
    """Validate that a raster tile can be opened successfully via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_tryopen("tile").alias("try_open")).first()
    return result["try_open"]


rst_tryopen_python_heavy_example_output = """
+--------+
|try_open|
+--------+
|true    |
+--------+
"""


# ---------------------------------------------------------------------------
# rst_histogram — per-band histogram (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands with real pixel data)
# ---------------------------------------------------------------------------


def rst_histogram_python_heavy_example(spark):
    """Compute a per-band histogram with explicit bounds via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) so the histogram
    has entries for each band. Explicit min/max bounds are required for the
    heavy tier to avoid the null-sentinel short-circuit (GDAL auto-detect
    returns None when statistics are not pre-computed). Bounds cover the
    UInt16 data range: 50–200.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(
        rx.rst_histogram("tile", f.lit(256), f.lit(0.0), f.lit(255.0)).alias("histogram")
    ).first()
    return result["histogram"]


rst_histogram_python_heavy_example_output = """
+--------------------------------------------------+
|histogram                                         |
+--------------------------------------------------+
|{band_1 -> [1, 0, 0, ...], band_2 -> [1, 0, 1,...|
+--------------------------------------------------+
"""
