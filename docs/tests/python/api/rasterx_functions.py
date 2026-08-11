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


def _get_dem_df_heavy(spark):
    from _fixtures import dem_tile_df_heavy  # noqa: PLC0415

    return dem_tile_df_heavy(spark)


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
    Returns the extracted subdataset tile directly.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    nc_df = _get_netcdf_df_heavy(spark)
    result = nc_df.select(
        rx.rst_getsubdataset("tile", f.lit("prAdjust")).alias("tile")
    ).first()
    return result["tile"]


rst_getsubdataset_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(extracted prAdjust subdataset — 720 pixels wide, 31 bands, 360 rows)
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
        rx.rst_histogram("tile", f.lit(256), f.lit(0.0), f.lit(255.0)).alias(
            "histogram"
        )
    ).first()
    return result["histogram"]


rst_histogram_python_heavy_example_output = """
+--------------------------------------------------+
|histogram                                         |
+--------------------------------------------------+
|{band_1 -> [1, 0, 0, ...], band_2 -> [1, 0, 1,...|
+--------------------------------------------------+
"""


# ===========================================================================
# Tile ops & constructors family (heavy tier)
# ===========================================================================


# ---------------------------------------------------------------------------
# rst_asformat — convert a raster to another GDAL format (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_asformat_python_heavy_example(spark):
    """Convert a raster tile to another GDAL format via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_asformat("tile", f.lit("GTiff")).alias("tile")
    ).first()
    return result["tile"]


rst_asformat_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(re-encoded tile in the requested GDAL format)
"""


# ---------------------------------------------------------------------------
# rst_band — extract a single band from a multi-band raster (heavy tier)
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands)
# ---------------------------------------------------------------------------


def rst_band_python_heavy_example(spark):
    """Extract band 1 from a multi-band raster tile via the heavy rasterx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) to demonstrate
    band extraction. The result is a new single-band tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    result = tile_df.select(rx.rst_band("tile", f.lit(1)).alias("tile")).first()
    return result["tile"]


rst_band_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single band extracted from the 3-band multiband fixture)
"""


# ---------------------------------------------------------------------------
# rst_buildoverviews — add internal overview pyramid (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_buildoverviews_python_heavy_example(spark):
    """Add internal overview levels to a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_buildoverviews("tile", f.array(f.lit(2), f.lit(4))).alias("tile")
    ).first()
    return result["tile"]


rst_buildoverviews_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(tile with internal overviews at levels [2, 4] embedded)
"""


# ---------------------------------------------------------------------------
# rst_clip — clip a raster to a geometry cutline (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif, EPSG:32618)
# Clip geometry: EWKT EPSG:4326 lon/lat over NYC area (auto-reprojected)
# ---------------------------------------------------------------------------


def rst_clip_python_heavy_example(spark):
    """Clip a raster tile to a geometry cutline via the heavy rasterx tier.

    Uses a plain WKT polygon in the raster's native CRS (EPSG:32618), covering
    the upper-left half of the raster extent (upperleftx=2121950, width=236px at 10m).
    Omitting an SRID means the geometry is assumed to be in the raster's CRS.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    clip_geom = "POLYGON((2121950 -10791280, 2123140 -10791280, 2123140 -10790470, 2121950 -10790470, 2121950 -10791280))"
    result = tile_df.select(
        rx.rst_clip("tile", f.lit(clip_geom), f.lit(True)).alias("tile")
    ).first()
    return result["tile"]


rst_clip_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(clipped tile; polygon is in the raster's native CRS (no SRID = no reprojection))
"""


# ---------------------------------------------------------------------------
# rst_cog_convert — re-layout tile as Cloud Optimized GeoTIFF (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_cog_convert_python_heavy_example(spark):
    """Re-layout a raster tile as a Cloud Optimized GeoTIFF via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_cog_convert("tile").alias("tile")).first()
    return result["tile"]


rst_cog_convert_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(COG tile; a COG is a valid GeoTIFF with tiled internal layout)
"""


# ---------------------------------------------------------------------------
# rst_convolve — apply a convolution kernel (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# Kernel: 3x3 identity
# ---------------------------------------------------------------------------


def rst_convolve_python_heavy_example(spark):
    """Apply a 3x3 identity convolution kernel to a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    kernel = f.array(
        f.array(f.lit(0.0), f.lit(0.0), f.lit(0.0)),
        f.array(f.lit(0.0), f.lit(1.0), f.lit(0.0)),
        f.array(f.lit(0.0), f.lit(0.0), f.lit(0.0)),
    )
    result = tile_df.select(rx.rst_convolve("tile", kernel).alias("tile")).first()
    return result["tile"]


rst_convolve_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(convolved tile; kernel is a 3x3 identity)
"""


# ---------------------------------------------------------------------------
# rst_fillnodata — fill NoData pixels via inverse-distance interpolation (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — nodata=0.0)
# ---------------------------------------------------------------------------


def rst_fillnodata_python_heavy_example(spark):
    """Fill NoData pixels in a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_fillnodata("tile", f.lit(100.0), f.lit(0)).alias("tile")
    ).first()
    return result["tile"]


rst_fillnodata_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(filled tile; NoData holes searched within 100 pixels)
"""


# ---------------------------------------------------------------------------
# rst_filter — spatial filter (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_filter_python_heavy_example(spark):
    """Apply a 3x3 median spatial filter to a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_filter("tile", f.lit(3), f.lit("median")).alias("tile")
    ).first()
    return result["tile"]


rst_filter_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(filtered tile; 3x3 median filter applied)
"""


# ---------------------------------------------------------------------------
# rst_fromcontent — create a tile from binary content (heavy tier, constructor)
# Fuller example: read bytes via binaryFile reader, construct tile, verify format.
# ---------------------------------------------------------------------------


def rst_fromcontent_python_heavy_example(spark):
    """Build a tile from binary raster bytes via the heavy rasterx tier.

    Constructor: reads bytes via Spark's binaryFile reader and constructs a tile
    column via rst_fromcontent. This is the canonical tier-agnostic pattern.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    from path_config import SAMPLE_DATA_BASE  # noqa: PLC0415

    path = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"
    binary_df = spark.read.format("binaryFile").load(path)
    tile_df = binary_df.select(
        rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )
    result = tile_df.select(rx.rst_format("tile").alias("format")).first()
    return result["format"]


rst_fromcontent_python_heavy_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
(format of the tile loaded from binary content via binaryFile reader)
"""


# ---------------------------------------------------------------------------
# rst_frombands — stack band tiles into a multi-band tile (heavy tier, constructor)
# Fuller example: extract per-band tiles, re-stack them.
# ---------------------------------------------------------------------------


def rst_frombands_python_heavy_example(spark):
    """Stack an array of single-band tiles into a multi-band tile via the heavy rasterx tier.

    Constructor: takes ARRAY<tile> of single-band tiles and stacks them in
    array order (element 0 → band 1). Splits the multiband fixture into per-band
    tiles via rst_band, then re-stacks into a 3-band tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_multiband_df_heavy(spark)
    with_bands = tile_df.select(
        f.array(
            rx.rst_band("tile", f.lit(1)),
            rx.rst_band("tile", f.lit(2)),
            rx.rst_band("tile", f.lit(3)),
        ).alias("bands")
    )
    result = with_bands.select(rx.rst_frombands("bands").alias("tile")).first()
    return result["tile"]


rst_frombands_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(3 per-band tiles stacked back into a 3-band tile)
"""


# ---------------------------------------------------------------------------
# rst_initnodata — initialize NoData values (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_initnodata_python_heavy_example(spark):
    """Initialize NoData values on a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_initnodata("tile").alias("tile")).first()
    return result["tile"]


rst_initnodata_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(tile with NoData initialized)
"""


# ---------------------------------------------------------------------------
# rst_resample — resample by a multiplicative factor (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 236x161 px)
# factor=2.0 bilinear → 472x322 px
# ---------------------------------------------------------------------------


def rst_resample_python_heavy_example(spark):
    """Resample a raster tile by a 2x factor via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_resample("tile", f.lit(2.0), f.lit("bilinear")).alias("tile")
    ).first()
    return result["tile"]


rst_resample_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(2x bilinear upsampled tile; source is 236x161 px)
"""


# ---------------------------------------------------------------------------
# rst_resample_to_res — resample to an explicit ground resolution (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 10 m pixels)
# target res=20.0 m → 118 px wide
# ---------------------------------------------------------------------------


def rst_resample_to_res_python_heavy_example(spark):
    """Resample a raster tile to an explicit ground resolution via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_resample_to_res(
            "tile", f.lit(20.0), f.lit(20.0), f.lit("average")
        ).alias("tile")
    ).first()
    return result["tile"]


rst_resample_to_res_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(downsampled tile; 10 m to 20 m resolution)
"""


# ---------------------------------------------------------------------------
# rst_resample_to_size — resample to an explicit pixel grid size (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# target 100x100 px
# ---------------------------------------------------------------------------


def rst_resample_to_size_python_heavy_example(spark):
    """Resample a raster tile to an explicit pixel grid size via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_resample_to_size("tile", f.lit(100), f.lit(100), f.lit("near")).alias(
            "tile"
        )
    ).first()
    return result["tile"]


rst_resample_to_size_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(resampled tile forced to 100x100 pixels)
"""


# ---------------------------------------------------------------------------
# rst_setcrs — stamp a CRS string onto a raster without reprojecting (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# ---------------------------------------------------------------------------


def rst_setcrs_python_heavy_example(spark):
    """Stamp a CRS string onto a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_crs(rx.rst_setcrs("tile", f.lit("EPSG:32618"))).alias("crs")
    ).first()
    return result["crs"]


rst_setcrs_python_heavy_example_output = """
+----------+
|crs       |
+----------+
|EPSG:32618|
+----------+
(CRS string after stamping; does NOT reproject — use rst_transformcrs to reproject)
"""


# ---------------------------------------------------------------------------
# rst_setsrid — stamp an EPSG SRID integer onto a raster (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — SRID=32618)
# ---------------------------------------------------------------------------


def rst_setsrid_python_heavy_example(spark):
    """Stamp an EPSG SRID onto a raster tile via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_setsrid("tile", f.lit(32618)).alias("tile")).first()
    return result["tile"]


rst_setsrid_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(tile with SRID stamped to 32618; does NOT reproject — use rst_transform to reproject)
"""


# ---------------------------------------------------------------------------
# rst_threshold — binarize a raster (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_threshold_python_heavy_example(spark):
    """Binarize a raster tile using a threshold condition via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_threshold("tile", f.lit(">"), f.lit(0.0)).alias("tile")
    ).first()
    return result["tile"]


rst_threshold_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(binary mask tile; pixels > 0.0 → 1, others → 0)
"""


# ---------------------------------------------------------------------------
# rst_transform — reproject to a target EPSG SRID (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# Reproject to EPSG:4326
# ---------------------------------------------------------------------------


def rst_transform_python_heavy_example(spark):
    """Reproject a raster tile to a target EPSG SRID via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(rx.rst_transform("tile", f.lit(4326)).alias("tile")).first()
    return result["tile"]


rst_transform_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(reprojected tile; source EPSG:32618 (UTM Zone 18N) to EPSG:4326)
"""


# ---------------------------------------------------------------------------
# rst_transformcrs — reproject to a target CRS string (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# Reproject to EPSG:3857
# ---------------------------------------------------------------------------


def rst_transformcrs_python_heavy_example(spark):
    """Reproject a raster tile to a CRS string target via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_crs(rx.rst_transformcrs("tile", f.lit("EPSG:3857"))).alias("crs")
    ).first()
    return result["crs"]


rst_transformcrs_python_heavy_example_output = """
+----------+
|crs       |
+----------+
|EPSG:3857 |
+----------+
(CRS string of the reprojected tile; accepts authority codes, WKT, or PROJ4)
"""


# ---------------------------------------------------------------------------
# rst_updatetype — convert the raster data type (heavy tier)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_updatetype_python_heavy_example(spark):
    """Convert a raster tile's data type via the heavy rasterx tier."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    tile_df = _get_single_band_df_heavy(spark)
    result = tile_df.select(
        rx.rst_updatetype("tile", f.lit("Float32")).alias("tile")
    ).first()
    return result["tile"]


rst_updatetype_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(type-converted tile; use rst_type to confirm the new data type)
"""


# ===========================================================================
# Aggregators family (heavy / rasterx tier)
# ===========================================================================


def _get_multi_band_tiles_df_heavy(spark):
    from _fixtures import multi_band_tiles_df_heavy  # noqa: PLC0415

    return multi_band_tiles_df_heavy(spark)


# ---------------------------------------------------------------------------
# rst_combineavg_agg -- per-pixel mean across aligned tiles (same grid/CRS)
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif, same grid)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_combineavg_agg_python_heavy_example(spark):
    """Average aligned raster tiles per group using the heavy rasterx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif split by rst_band.
    All 3 tiles share the same grid, satisfying combineavg_agg's alignment requirement.
    Grouped by region, producing 1 averaged tile row.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multi_band_tiles_df_heavy(spark)
    result = (
        df.groupBy("region")
        .agg(rx.rst_combineavg_agg("tile").alias("avg_tile"))
        .first()
    )
    return result["avg_tile"]


rst_combineavg_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|avg_tile                                                   |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(returns a v2 Tile)
"""


# ---------------------------------------------------------------------------
# rst_derivedband_agg -- apply a Python pixel function across a group's tiles
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif)
# Pixel function: identity (returns band 0, i.e. the first input band)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_derivedband_agg_python_heavy_example(spark):
    """Apply a Python pixel function across a group's band tiles using the heavy rasterx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif.  Each tile
    contributes one band to the VRT; the pixel function selects the first band.
    Grouped by region, producing 1 derived tile row.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    rx.register(spark)
    pyfunc = (
        "def fn(in_ar, out_ar, xoff, yoff, xsize, ysize, "
        "raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n"
        "    out_ar[:] = in_ar[0]\n"
    )
    df = _get_multi_band_tiles_df_heavy(spark)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_derivedband_agg("tile", f.lit(pyfunc), f.lit("fn")).alias("derived")
        )
        .first()
    )
    return result["derived"]


rst_derivedband_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|derived                                                    |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(returns a v2 Tile)
"""


# ---------------------------------------------------------------------------
# rst_frombands_agg -- stack per-band tiles into one multi-band tile
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_frombands_agg_python_heavy_example(spark):
    """Stack per-band tiles into one multi-band tile per group using the heavy rasterx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif, each with
    band_index=1/2/3.  Grouped by region, stacks ascending by band_index,
    producing 1 three-band tile row.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multi_band_tiles_df_heavy(spark)
    result = (
        df.groupBy("region")
        .agg(rx.rst_frombands_agg("tile", "band_index").alias("stacked"))
        .first()
    )
    return result["stacked"]


rst_frombands_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|stacked                                                    |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(returns a v2 Tile)
"""


# ---------------------------------------------------------------------------
# rst_merge_agg -- spatial mosaic of a group's tiles (union extent)
# Fixture: MULTI-TILE (3 per-band rows from rgb_nir_small.tif, same extent)
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_merge_agg_python_heavy_example(spark):
    """Merge a group's raster tiles into one spatial mosaic using the heavy rasterx tier.

    Multi-tile fixture: 3 per-band rows from rgb_nir_small.tif.  Each tile
    covers the same extent, so the merged result has the same bounding box.
    Grouped by region.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    df = _get_multi_band_tiles_df_heavy(spark)
    result = df.groupBy("region").agg(rx.rst_merge_agg("tile").alias("mosaic")).first()
    return result["mosaic"]


rst_merge_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|mosaic                                                     |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(returns a v2 Tile)
"""


# ---------------------------------------------------------------------------
# rst_rasterize_agg -- burn geometry/value rows into one tile per group
# Fixture: synthesized 3-row DataFrame of WKB polygon + value + extent constants
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_rasterize_agg_python_heavy_example(spark):
    """Burn geometry/value rows into one tile per group using the heavy rasterx tier.

    Multi-row fixture: 3 rows of (geom_wkb, burn_value) over a shared 4x4 extent
    in EPSG:4326.  Grouped by region, producing 1 rasterized tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415
    from pyspark.sql.types import (
        BinaryType,
        DoubleType,  # noqa: PLC0415
        StringType,
        StructField,
        StructType,
    )

    rx.register(spark)
    # WKB POLYGON((0 0, 4 0, 4 4, 0 4, 0 0)) in EPSG:4326
    poly = bytes.fromhex(
        "0103000000010000000500000000000000000000000000000000000000"
        "0000000000001040000000000000000000000000000010400000000000001040"
        "000000000000000000000000000010400000000000000000"
        "0000000000000000"
    )
    rows = [(poly, 1.0, "R1"), (poly, 2.0, "R1"), (poly, 3.0, "R1")]
    schema = StructType(
        [
            StructField("geom", BinaryType()),
            StructField("value", DoubleType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_rasterize_agg(
                "geom",
                "value",
                f.lit(0.0),
                f.lit(0.0),
                f.lit(4.0),
                f.lit(4.0),
                f.lit(8),
                f.lit(8),
                f.lit(4326),
            ).alias("burned")
        )
        .first()
    )
    return result["burned"]


rst_rasterize_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|burned                                                     |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(returns a v2 Tile)
"""


# ---------------------------------------------------------------------------
# rst_gridfrompoints_agg -- IDW interpolation: one point/value per row -> one tile
# Fixture: synthesized 4 point rows, EPSG:4326 small extent
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_gridfrompoints_agg_python_heavy_example(spark):
    """IDW-interpolate point/value rows into one tile per group using the heavy rasterx tier.

    Multi-row fixture: 4 rows of (WKB point, observation) over a shared [0,0,1,1]
    EPSG:4326 extent.  Grouped by region, producing 1 Float64 IDW tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415
    from pyspark.sql.types import (
        BinaryType,
        DoubleType,  # noqa: PLC0415
        StringType,
        StructField,
        StructType,
    )

    rx.register(spark)

    def _wkb_point(x, y):
        import struct  # noqa: PLC0415

        return struct.pack("<bIdd", 1, 1, x, y)

    rows = [
        (_wkb_point(0.1, 0.1), 10.0, "R1"),
        (_wkb_point(0.9, 0.1), 20.0, "R1"),
        (_wkb_point(0.1, 0.9), 30.0, "R1"),
        (_wkb_point(0.9, 0.9), 40.0, "R1"),
    ]
    schema = StructType(
        [
            StructField("pt", BinaryType()),
            StructField("val", DoubleType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_gridfrompoints_agg(
                "pt",
                "val",
                f.lit(0.0),
                f.lit(0.0),
                f.lit(1.0),
                f.lit(1.0),
                f.lit(8),
                f.lit(8),
                f.lit(4326),
            ).alias("idw")
        )
        .first()
    )
    return result["idw"]


rst_gridfrompoints_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|idw                                                        |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(returns a v2 Tile)
"""


# ---------------------------------------------------------------------------
# rst_dtmfromgeoms_agg -- Delaunay TIN DTM: one Z-point per row -> one tile
# Fixture: synthesized 4 Z-valued WKB points, small EPSG:4326 extent
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_dtmfromgeoms_agg_python_heavy_example(spark):
    """Build a Delaunay TIN DTM from Z-valued points per group using the heavy rasterx tier.

    Multi-row fixture: 4 rows of WKB POINT Z with elevation values, over a
    [0,0,1,1] EPSG:4326 extent.  Grouped by region, producing 1 DTM tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415
    from pyspark.sql.types import (
        BinaryType,
        StringType,  # noqa: PLC0415
        StructField,
        StructType,
    )

    rx.register(spark)

    def _wkb_point_z(x, y, z):
        import struct  # noqa: PLC0415

        return struct.pack("<bIddd", 1, 1001, x, y, z)

    rows = [
        (_wkb_point_z(0.1, 0.1, 100.0), "R1"),
        (_wkb_point_z(0.9, 0.1, 200.0), "R1"),
        (_wkb_point_z(0.1, 0.9, 150.0), "R1"),
        (_wkb_point_z(0.9, 0.9, 250.0), "R1"),
    ]
    schema = StructType(
        [
            StructField("pt", BinaryType()),
            StructField("region", StringType()),
        ]
    )
    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_dtmfromgeoms_agg(
                "pt",
                f.lit(None).cast("array<binary>"),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(0.0),
                f.lit(1.0),
                f.lit(1.0),
                f.lit(8),
                f.lit(8),
                f.lit(4326),
            ).alias("dtm")
        )
        .first()
    )
    return result["dtm"]


rst_dtmfromgeoms_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|dtm                                                        |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(returns a v2 Tile)
"""


# ---------------------------------------------------------------------------
# rst_h3_rasterize_agg -- burn H3 cells into one tile per group
# Fixture: synthesized 3 H3 resolution-9 cell rows, burn values 1.0/2.0/3.0
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_h3_rasterize_agg_python_heavy_example(spark):
    """Rasterize H3 cell/value rows into one tile per group using the heavy rasterx tier.

    Multi-row fixture: 3 rows of (H3 cell id BIGINT, burn value) at resolution 9
    near lon/lat (0.01, 0.01).  Grouped by region, producing 1 rasterized tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    import h3  # noqa: PLC0415

    res = 9
    cell_strs = [
        h3.latlng_to_cell(0.01, 0.01, res),
        h3.latlng_to_cell(0.02, 0.01, res),
        h3.latlng_to_cell(0.01, 0.02, res),
    ]
    cells = [h3.str_to_int(c) for c in cell_strs]
    rows = [
        (int(cells[0]), 1.0, "R1"),
        (int(cells[1]), 2.0, "R1"),
        (int(cells[2]), 3.0, "R1"),
    ]
    from pyspark.sql.types import (
        DoubleType,
        LongType,  # noqa: PLC0415
        StringType,
        StructField,
        StructType,
    )

    schema = StructType(
        [
            StructField("cellid", LongType()),
            StructField("value", DoubleType()),
            StructField("region", StringType()),
        ]
    )
    from pyspark.sql import functions as f  # noqa: PLC0415

    df = spark.createDataFrame(rows, schema)
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_h3_rasterize_agg(
                df["cellid"],
                df["value"],
                f.lit(4326),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("int"),
                f.lit(None).cast("int"),
                f.lit("centroids"),
                f.lit(1),
            ).alias("tile")
        )
        .first()
    )
    return result["tile"]


rst_h3_rasterize_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(returns a v2 Tile)
"""


# ---------------------------------------------------------------------------
# rst_quadbin_rasterize_agg -- burn quadbin cells into one tile per group
# Fixture: synthesized 3 quadbin zoom-12 cell rows near central London
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_quadbin_rasterize_agg_python_heavy_example(spark):
    """Rasterize quadbin cell/value rows into one tile per group using the heavy rasterx tier.

    Multi-row fixture: 3 rows of (quadbin cell id BIGINT, burn value) at zoom 12
    near central London.  Grouped by region, producing 1 rasterized tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    from databricks.labs.gbx.gridx.quadbin import functions as qbx  # noqa: PLC0415

    qbx.register(spark)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW _qb_cells_heavy AS
        SELECT region,
               gbx_quadbin_pointascell(cast(lon as double), cast(lat as double), 12) AS cellid,
               cast(val as double) AS value
        FROM (VALUES
            ('R1', -0.10, 51.50, 1.0),
            ('R1', -0.11, 51.51, 2.0),
            ('R1', -0.09, 51.49, 3.0)
        ) AS t(region, lon, lat, val)
    """)
    from pyspark.sql import functions as f  # noqa: PLC0415

    df = spark.table("_qb_cells_heavy")
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_quadbin_rasterize_agg(
                df["cellid"],
                df["value"],
                f.lit(4326),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("int"),
                f.lit(None).cast("int"),
                f.lit("centroids"),
                f.lit(1),
            ).alias("tile")
        )
        .first()
    )
    spark.catalog.dropTempView("_qb_cells_heavy")
    return result["tile"]


rst_quadbin_rasterize_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(returns a v2 Tile)
"""


# ---------------------------------------------------------------------------
# rst_bng_rasterize_agg -- burn BNG cells into one tile per group
# Fixture: synthesized 3 BNG 1km cell STRING rows near central London
# Output: tile struct (returns a raster tile)
# ---------------------------------------------------------------------------


def rst_bng_rasterize_agg_python_heavy_example(spark):
    """Rasterize BNG cell/value rows into one tile per group using the heavy rasterx tier.

    Multi-row fixture: 3 rows of (BNG STRING cell id, burn value) at 1km resolution
    near central London (EPSG:27700).  Grouped by region, producing 1 rasterized tile.
    """
    from databricks.labs.gbx.rasterx import functions as rx  # noqa: PLC0415

    rx.register(spark)
    from databricks.labs.gbx.gridx.bng import functions as bngx  # noqa: PLC0415

    bngx.register(spark)

    spark.sql("""
        CREATE OR REPLACE TEMP VIEW _bng_cells_heavy AS
        SELECT region,
               gbx_bng_eastnorthasbng(e, n, 3) AS cellid,
               val AS value
        FROM (VALUES
            ('R1', cast(530000.0 as double), cast(180000.0 as double), cast(1.0 as double)),
            ('R1', cast(531000.0 as double), cast(181000.0 as double), cast(2.0 as double)),
            ('R1', cast(529000.0 as double), cast(179000.0 as double), cast(3.0 as double))
        ) AS t(region, e, n, val)
    """)
    from pyspark.sql import functions as f  # noqa: PLC0415

    df = spark.table("_bng_cells_heavy")
    result = (
        df.groupBy("region")
        .agg(
            rx.rst_bng_rasterize_agg(
                df["cellid"],
                df["value"],
                f.lit(27700),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("double"),
                f.lit(None).cast("int"),
                f.lit(None).cast("int"),
                f.lit("centroids"),
                f.lit(1),
            ).alias("tile")
        )
        .first()
    )
    spark.catalog.dropTempView("_bng_cells_heavy")
    return result["tile"]


rst_bng_rasterize_agg_python_heavy_example_output = """
+------+-----------------------------------------------------------+
|region|tile                                                       |
+------+-----------------------------------------------------------+
|R1    |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+
(returns a v2 Tile)
"""


# ---------------------------------------------------------------------------
# rst_ndvi -- compute NDVI from red and NIR bands
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


def rst_ndvi_python_heavy_example(spark):
    """Compute NDVI from multiband tile using red (band 1) and NIR (band 2)."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_multiband_df_heavy(spark)
    result = df.select(rx.rst_ndvi("tile", f.lit(1), f.lit(2)).alias("tile")).first()
    return result["tile"]


rst_ndvi_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NDVI raster: (NIR-Red)/(NIR+Red))
"""


# ---------------------------------------------------------------------------
# rst_evi -- Enhanced Vegetation Index from red, NIR, blue
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


def rst_evi_python_heavy_example(spark):
    """Compute EVI using red (band 1), NIR (band 2), and green (band 3) as blue."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_multiband_df_heavy(spark)
    result = df.select(
        rx.rst_evi("tile", f.lit(1), f.lit(2), f.lit(3)).alias("tile")
    ).first()
    return result["tile"]


rst_evi_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band EVI raster: G*(NIR-Red)/(NIR+C1*Red-C2*Blue+L))
"""


# ---------------------------------------------------------------------------
# rst_savi -- Soil-Adjusted Vegetation Index
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


def rst_savi_python_heavy_example(spark):
    """Compute SAVI from red (band 1) and NIR (band 2) bands."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_multiband_df_heavy(spark)
    result = df.select(rx.rst_savi("tile", f.lit(1), f.lit(2)).alias("tile")).first()
    return result["tile"]


rst_savi_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band SAVI raster: (NIR-Red)/(NIR+Red+L)*(1+L))
"""


# ---------------------------------------------------------------------------
# rst_ndwi -- Normalized Difference Water Index
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


def rst_ndwi_python_heavy_example(spark):
    """Compute NDWI from green (band 3) and NIR (band 2) bands."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_multiband_df_heavy(spark)
    result = df.select(rx.rst_ndwi("tile", f.lit(3), f.lit(2)).alias("tile")).first()
    return result["tile"]


rst_ndwi_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NDWI raster: (Green-NIR)/(Green+NIR))
"""


# ---------------------------------------------------------------------------
# rst_nbr -- Normalized Burn Ratio (NIR - SWIR)
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Note: fixture has no SWIR; using green (band 3) as substitute for demo
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


def rst_nbr_python_heavy_example(spark):
    """Compute NBR using NIR (band 2) and green (band 3) as SWIR substitute."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_multiband_df_heavy(spark)
    result = df.select(rx.rst_nbr("tile", f.lit(2), f.lit(3)).alias("tile")).first()
    return result["tile"]


rst_nbr_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NBR raster: (NIR-SWIR)/(NIR+SWIR))
"""


# ---------------------------------------------------------------------------
# rst_index -- generic dispatcher for named spectral indices
# Fixture: multiband_tile_df_heavy(spark) (3 bands: red=1, NIR=2, green=3)
# Output: tile struct (single-band raster)
# ---------------------------------------------------------------------------


def rst_index_python_heavy_example(spark):
    """Compute NDVI via the generic rst_index dispatcher with band map."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_multiband_df_heavy(spark)
    band_map = f.create_map(f.lit("red"), f.lit(1), f.lit("nir"), f.lit(2))
    result = df.select(
        rx.rst_index("tile", f.lit("ndvi"), band_map).alias("tile")
    ).first()
    return result["tile"]


rst_index_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band index raster computed from named formula)
"""


# ---------------------------------------------------------------------------
# rst_combineavg -- combine multiple tiles by averaging
# Fixture: multi_band_tiles_df_heavy(spark) (3 rows: one per band)
# Output: tile struct (merged raster)
# ---------------------------------------------------------------------------


def rst_combineavg_python_heavy_example(spark):
    """Combine 3 aligned band tiles by averaging."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    from _fixtures import multi_band_tiles_df_heavy  # noqa: PLC0415

    df = multi_band_tiles_df_heavy(spark)
    result = (
        df.groupBy("region")
        .agg(rx.rst_combineavg(f.collect_list("tile")).alias("tile"))
        .first()
    )
    return result["tile"]


rst_combineavg_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(averaged combined raster from 3 input tiles)
"""


# ---------------------------------------------------------------------------
# rst_derivedband -- apply Python UDF to produce derived band
# Fixture: multiband_tile_df_heavy(spark) (3 bands)
# Output: tile struct (raster with derived band)
# ---------------------------------------------------------------------------


def rst_derivedband_python_heavy_example(spark):
    """Apply a Python pixel-function to derive a band (doubles band 1).

    ``python_func`` follows GDAL's VRT pixel-function signature; ``func_name``
    names the callable within it.
    """
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_multiband_df_heavy(spark)
    python_func = (
        "def double(in_ar, out_ar, xoff, yoff, xsize, ysize, "
        "raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\n"
        "    out_ar[:] = in_ar[0] * 2\n"
    )
    result = df.select(
        rx.rst_derivedband("tile", f.lit(python_func), f.lit("double")).alias("tile")
    ).first()
    return result["tile"]


rst_derivedband_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(raster with derived band from Python UDF)
"""


# ---------------------------------------------------------------------------
# rst_mapalgebra -- apply map algebra expression
# Fixture: multiband_tile_df_heavy(spark) (3 bands)
# Output: tile struct (result raster)
# ---------------------------------------------------------------------------


def rst_mapalgebra_python_heavy_example(spark):
    """Apply map algebra expression 'A * 2' to scale band values."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_multiband_df_heavy(spark)
    result = df.select(
        rx.rst_mapalgebra(f.array("tile"), f.lit("A * 2")).alias("tile")
    ).first()
    return result["tile"]


rst_mapalgebra_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(result raster from map algebra expression A * 2)
"""


# ---------------------------------------------------------------------------
# rst_merge -- merge/mosaic multiple raster tiles
# Fixture: multi_band_tiles_df_heavy(spark) (3 rows: one per band)
# Output: tile struct (merged raster)
# ---------------------------------------------------------------------------


def rst_merge_python_heavy_example(spark):
    """Merge 3 aligned band tiles into a single raster."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    from _fixtures import multi_band_tiles_df_heavy  # noqa: PLC0415

    df = multi_band_tiles_df_heavy(spark)
    result = (
        df.groupBy("region")
        .agg(rx.rst_merge(f.collect_list("tile")).alias("tile"))
        .first()
    )
    return result["tile"]


rst_merge_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(merged raster from co-registered input tiles)
"""


# ============================================================================
# Terrain Analysis Functions
# ============================================================================
# Fixture: dem_tile_df_heavy(spark) (SRTM elevation, NYC area)
# All return tile struct


def rst_slope_python_heavy_example(spark):
    """Compute slope (degrees or percent rise/run) from a DEM tile."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    result = df.select(
        rx.rst_slope("tile", f.lit("degrees"), f.lit(1.0), f.lit(1.0)).alias("tile")
    ).first()
    return result["tile"]


rst_slope_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(slope in degrees; auto-scaled from raster CRS units)
"""


def rst_aspect_python_heavy_example(spark):
    """Compute aspect (compass direction of steepest slope) from a DEM tile."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    result = df.select(
        rx.rst_aspect("tile", f.lit(False), f.lit(False)).alias("tile")
    ).first()
    return result["tile"]


rst_aspect_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(aspect in compass degrees: 0=N, 90=E, 180=S, 270=W)
"""


def rst_hillshade_python_heavy_example(spark):
    """Compute hillshade (8-bit shaded relief) from a DEM tile."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    result = df.select(
        rx.rst_hillshade("tile", f.lit(315.0), f.lit(45.0), f.lit(1.0)).alias("tile")
    ).first()
    return result["tile"]


rst_hillshade_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(8-bit hillshade: 0..255, NW azimuth 45-degree altitude)
"""


def rst_tri_python_heavy_example(spark):
    """Compute Terrain Ruggedness Index (TRI) from a DEM tile."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    result = df.select(rx.rst_tri("tile").alias("tile")).first()
    return result["tile"]


rst_tri_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(TRI: mean absolute neighbour difference)
"""


def rst_tpi_python_heavy_example(spark):
    """Compute Topographic Position Index (TPI) from a DEM tile."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    result = df.select(rx.rst_tpi("tile").alias("tile")).first()
    return result["tile"]


rst_tpi_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(TPI: positive=ridge, negative=valley)
"""


def rst_roughness_python_heavy_example(spark):
    """Compute Roughness (max neighbour delta in 3x3 window) from a DEM tile."""
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    result = df.select(rx.rst_roughness("tile").alias("tile")).first()
    return result["tile"]


rst_roughness_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(roughness: max absolute difference in 3x3 window)
"""


def rst_color_relief_python_heavy_example(spark):
    """Apply a color relief mapping (elevation → RGBA) from a DEM tile."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f
    from _fixtures import color_table_path  # noqa: PLC0415

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    clr_path = str(color_table_path())
    result = df.select(
        rx.rst_color_relief("tile", f.lit(clr_path)).alias("tile")
    ).first()
    return result["tile"]


rst_color_relief_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(4-band RGBA tile mapped via gdaldem color table)
"""


def rst_proximity_python_heavy_example(spark):
    """Compute per-pixel distance to the nearest non-NoData pixel."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    result = df.select(
        rx.rst_proximity("tile", f.lit(""), f.lit("PIXEL"), f.lit(100.0)).alias("tile")
    ).first()
    return result["tile"]


rst_proximity_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(distance in pixels to nearest non-NoData pixel, capped at 100)
"""


def rst_contour_python_heavy_example(spark):
    """Generate contour LineStrings at equal intervals from a DEM tile."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    result = df.select(
        rx.rst_contour("tile", f.lit([]), f.lit(50.0), f.lit(0.0), f.lit("elev")).alias(
            "contours"
        )
    ).first()
    return result["contours"]


rst_contour_python_heavy_example_output = """
+--------------------------------------+
|contours                              |
+--------------------------------------+
|[{[BINARY], 50.0}, {[BINARY], 100.0}, {[BINARY], 150.0}, {[BINARY], 200.0}, {[BINARY], 250.0}, {[BINARY], 300.0}]|
+--------------------------------------+
(array of contour features: LineString geometry + elevation)
"""


def rst_viewshed_python_heavy_example(spark):
    """Compute binary viewshed mask from a DEM and observer point."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    result = df.select(
        rx.rst_viewshed(
            "tile",
            f.lit("POINT(500320 4500320)"),
            f.lit(100.0),
            f.lit(1.6),
            f.lit(500.0),
        ).alias("tile")
    ).first()
    return result["tile"]


rst_viewshed_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(binary viewshed: 1=visible, 0=not visible)
"""


def rst_sample_python_heavy_example(spark):
    """Sample raster pixel values at a POINT geometry."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_dem_df_heavy(spark)
    result = df.select(
        rx.rst_sample("tile", f.lit("SRID=32618;POINT(500320 4500320)")).alias("values")
    ).first()
    return result["values"]


rst_sample_python_heavy_example_output = """
+-------+
|values |
+-------+
|[302.0]|
+-------+
(array of sampled values, one per band)
"""


def rst_gridfrompoints_python_heavy_example(spark):
    """IDW (Inverse Distance Weighting) interpolation from arrays of points and values."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f

    rx.register(spark)

    # Create synthetic point cloud: 2 points with WKB and values
    point_data = [
        (
            [
                bytes.fromhex("010100000000000000000004400000000000000040"),
                bytes.fromhex("010100000000000000000008400000000000000040"),
            ],
            [100.0, 110.0],
        )
    ]
    df = spark.createDataFrame(point_data, ["points_wkb_array", "values_array"])

    result = df.select(
        rx.rst_gridfrompoints(
            f.col("points_wkb_array"),
            f.col("values_array"),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(1000.0),
            f.lit(1000.0),
            f.lit(256),
            f.lit(256),
            f.lit(32633),
        ).alias("tile")
    ).first()
    return result["tile"]


rst_gridfrompoints_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(IDW-interpolated tile over specified extent)
"""


def rst_dtmfromgeoms_python_heavy_example(spark):
    """TIN (Triangulated Irregular Network) from Z-valued points via Delaunay interpolation."""
    from databricks.labs.gbx.rasterx import functions as rx
    from pyspark.sql import functions as f
    from pyspark.sql.types import (
        StructType,
        StructField,
        ArrayType,
        BinaryType,
    )  # noqa: PLC0415

    rx.register(spark)

    # Create synthetic survey point data with 4 points inset from the grid edges
    # POINT Z geometries: (100,100,50), (900,100,80), (900,900,120), (100,900,60)
    # Using explicit schema to avoid type inference errors
    survey_data = [
        (
            [
                bytes.fromhex(
                    "0101000080000000000000594000000000000059400000000000004940"
                ),
                bytes.fromhex(
                    "01010000800000000000208c4000000000000059400000000000005440"
                ),
                bytes.fromhex(
                    "01010000800000000000208c400000000000208c400000000000005e40"
                ),
                bytes.fromhex(
                    "010100008000000000000059400000000000208c400000000000004e40"
                ),
            ],
            [],
        )
    ]
    schema = StructType(
        [
            StructField("points_wkb_array", ArrayType(BinaryType()), True),
            StructField("breaklines_wkb_array", ArrayType(BinaryType()), True),
        ]
    )
    df = spark.createDataFrame(survey_data, schema=schema)

    result = df.select(
        rx.rst_dtmfromgeoms(
            f.col("points_wkb_array"),
            f.col("breaklines_wkb_array"),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(1000.0),
            f.lit(1000.0),
            f.lit(100),
            f.lit(100),
            f.lit(32618),
        ).alias("tile")
    ).first()
    return result["tile"]


rst_dtmfromgeoms_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(TIN-interpolated DTM over specified extent and pixel count)
"""


# ============================================================================
# Coordinate Transforms & Tiling (Heavy Tier)
# ============================================================================


def rst_rastertoworldcoord_python_heavy_example(spark):
    """Convert pixel coordinates (col, row) to world coordinates (x, y)."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # Pixel (100, 80) → world struct {x: <easting>, y: <northing>}
    result = df.select(
        rx.rst_rastertoworldcoord("tile", f.lit(100), f.lit(80)).alias("world_coord")
    ).first()
    return result["world_coord"]


rst_rastertoworldcoord_python_heavy_example_output = """
+-------------------+
|world_coord        |
+-------------------+
|{500980.0, ...}    |
+-------------------+
(struct with x: DOUBLE, y: DOUBLE)
"""


def rst_rastertoworldcoordx_python_heavy_example(spark):
    """Convert pixel column to world X coordinate (easting)."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # Pixel col=100 → easting
    result = df.select(
        rx.rst_rastertoworldcoordx("tile", f.lit(100), f.lit(80)).alias("easting")
    ).first()
    return result["easting"]


rst_rastertoworldcoordx_python_heavy_example_output = """
+-------+
|easting|
+-------+
|500980 |
+-------+
"""


def rst_rastertoworldcoordy_python_heavy_example(spark):
    """Convert pixel row to world Y coordinate (northing)."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # Pixel row=80 → northing
    result = df.select(
        rx.rst_rastertoworldcoordy("tile", f.lit(100), f.lit(80)).alias("northing")
    ).first()
    return result["northing"]


rst_rastertoworldcoordy_python_heavy_example_output = """
+--------+
|northing|
+--------+
|4599220 |
+--------+
"""


def rst_worldtorastercoord_python_heavy_example(spark):
    """Convert world coordinates (x, y) to pixel coordinates (col, row)."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # World (2122955, -10791275) in the raster CRS → pixel (100, 80)
    result = df.select(
        rx.rst_worldtorastercoord("tile", f.lit(2122955.0), f.lit(-10791275.0)).alias(
            "pixel_coord"
        )
    ).first()
    return result["pixel_coord"]


rst_worldtorastercoord_python_heavy_example_output = """
+-----------+
|pixel_coord|
+-----------+
|{5490, ...}|
+-----------+
(struct with x: INT, y: INT)
"""


def rst_worldtorastercoordx_python_heavy_example(spark):
    """Convert world X coordinate to pixel column."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # World (2122955, -10791275) → pixel column 100
    result = df.select(
        rx.rst_worldtorastercoordx("tile", f.lit(2122955.0), f.lit(-10791275.0)).alias(
            "pixel_col"
        )
    ).first()
    return result["pixel_col"]


rst_worldtorastercoordx_python_heavy_example_output = """
+---------+
|pixel_col|
+---------+
|5490     |
+---------+
"""


def rst_worldtorastercoordy_python_heavy_example(spark):
    """Convert world Y coordinate to pixel row."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # World (2122955, -10791275) → pixel row 80
    result = df.select(
        rx.rst_worldtorastercoordy("tile", f.lit(2122955.0), f.lit(-10791275.0)).alias(
            "pixel_row"
        )
    ).first()
    return result["pixel_row"]


rst_worldtorastercoordy_python_heavy_example_output = """
+---------+
|pixel_row|
+---------+
|5490     |
+---------+
"""


def rst_to_webmercator_python_heavy_example(spark):
    """Transform raster from its native CRS to Web Mercator (EPSG:3857)."""
    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # Reproject to Web Mercator (default bilinear resampling)
    result = df.select(rx.rst_to_webmercator("tile").alias("tile")).first()
    return result["tile"]


rst_to_webmercator_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_tilexyz_python_heavy_example(spark):
    """Render a single Web-Mercator XYZ tile (z, x, y) as PNG bytes."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # Render z=12, x=1234, y=1523 as a 256x256 PNG (rescale='none' keeps the raw
    # dtype mapping; an off-footprint slippy tile renders transparent, never null).
    result = df.select(
        rx.rst_tilexyz(
            "tile",
            f.lit(12),
            f.lit(1234),
            f.lit(1523),
            f.lit("PNG"),
            f.lit(256),
            f.lit("bilinear"),
            f.lit("none"),
        ).alias("png_bytes")
    ).first()
    return result["png_bytes"]


rst_tilexyz_python_heavy_example_output = """
+----------+
|png_bytes |
+----------+
|[BINARY]  |
+----------+
(PNG image bytes, 256×256 pixels)
"""


def rst_xyzpyramid_python_heavy_example(spark):
    """Generate XYZ tiles across a zoom range."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # Explode raster into PNG tiles for z=10..12 (returns array via LATERAL VIEW)
    result = df.select(
        rx.rst_xyzpyramid("tile", f.lit(10), f.lit(12)).alias("tile_array")
    ).first()
    return result["tile_array"]


rst_xyzpyramid_python_heavy_example_output = """
+----------+
|tile_array|
+----------+
|[tile, ...|
+----------+
(array of tile structs: [{z, x, y, bytes}, ...])
"""


def rst_h3_tessellate_python_heavy_example(spark):
    """Tessellate raster into H3 hexagonal grid cells (resolution=7, ~1km)."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # Partition into H3 cells (covering mode, returns array of structs)
    result = df.select(rx.rst_h3_tessellate("tile", f.lit(7)).alias("h3_cells")).first()
    return result["h3_cells"]


rst_h3_tessellate_python_heavy_example_output = """
+---+
|h3_|
+---+
|[{ |
+---+
(array of structs: [{cellid: LONG, raster: BINARY}, ...])
"""


def rst_bng_tessellate_python_heavy_example(spark):
    """Tessellate raster into British National Grid cells (1km resolution)."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # rst_bng_tessellate is a generator (one row per BNG cell). The raster is
    # warped to EPSG:27700 first, so a raster outside Great Britain (like this
    # NYC-area sample) yields no rows — an empty result, not an error.
    return df.select(
        rx.rst_bng_tessellate("tile", f.lit(3)).alias("bng_cells")
    ).collect()


rst_bng_tessellate_python_heavy_example_output = """
+---+
|bng|
+---+
|[{ |
+---+
(array of tile structs per BNG cell; raster rewarped to EPSG:27700)
"""


def rst_quadbin_tessellate_python_heavy_example(spark):
    """Tessellate raster into CARTO quadbin grid cells (zoom 12)."""
    from pyspark.sql import functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    # Partition into quadbin cells at zoom 12 (covering mode)
    result = df.select(
        rx.rst_quadbin_tessellate("tile", f.lit(12)).alias("qb_cells")
    ).first()
    return result["qb_cells"]


rst_quadbin_tessellate_python_heavy_example_output = """
+---+
|qb_|
+---+
|[{ |
+---+
(array of tile structs per quadbin cell, zoom 12)
"""


# ============================================================================
# Generator Functions (Heavy Tier)
# ============================================================================


def rst_retile_python_heavy_example(spark):
    """Retile a raster into uniform dimensions (UDTF via SQL LATERAL)."""
    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    df.createOrReplaceTempView("rasters")
    # Retile into 64x64-pixel sub-tiles using SQL LATERAL
    return spark.sql(
        "SELECT t.* FROM rasters, " "LATERAL gbx_rst_retile(tile, 64, 64) t"
    ).take(3)


rst_retile_python_heavy_example_output = """
+---+---+---+
|z  |x  |y  |
+---+---+---+
(one row per sub-tile: each sub-tile is a v2-Tile struct)
"""


def rst_tooverlappingtiles_python_heavy_example(spark):
    """Create overlapping tiles for edge-aware processing (UDTF via SQL LATERAL)."""
    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    df.createOrReplaceTempView("rasters")
    # 64x64 tiles with 8% overlap using SQL LATERAL
    return spark.sql(
        "SELECT t.* FROM rasters, "
        "LATERAL gbx_rst_tooverlappingtiles(tile, 64, 64, 8) t"
    ).take(3)


rst_tooverlappingtiles_python_heavy_example_output = """
+---+---+---+
|z  |x  |y  |
+---+---+---+
(one row per overlapping tile: each is a v2-Tile struct)
"""


def rst_separatebands_python_heavy_example(spark):
    """Separate multi-band raster into individual bands (UDTF via SQL LATERAL)."""
    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_multiband_df_heavy(spark)
    df.createOrReplaceTempView("multiband_rasters")
    # Separate 3-band raster into individual band tiles using SQL LATERAL
    return spark.sql(
        "SELECT t.* FROM multiband_rasters, " "LATERAL gbx_rst_separatebands(tile) t"
    ).collect()


rst_separatebands_python_heavy_example_output = """
+---+
|z  |
+---+
(one row per band: each is a v2-Tile struct)
"""


def rst_polygonize_python_heavy_example(spark):
    """Extract polygons from raster regions (heavy tier).

    Heavy rst_polygonize is a scalar returning an ARRAY<struct(geom_wkb, value)>
    (one element per contiguous-value region) — not a generator; call it in a
    plain select and explode the array if you want one row per feature.
    """
    if rx is None:
        raise ImportError("rasterx not installed")
    from pyspark.sql import functions as f

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    return df.select(
        rx.rst_polygonize("tile", f.lit(1), f.lit(4)).alias("features")
    ).first()["features"]


rst_polygonize_python_heavy_example_output = """
[{geom_wkb: ..., value: 365.0}, {geom_wkb: ..., value: 366.0}, ...]
(ARRAY<struct(geom_wkb BINARY (WKB), value DOUBLE)> — one element per region)
"""


def rst_maketiles_python_heavy_example(spark):
    """Subdivide raster into tiles by approximate size (UDTF, BROKEN in heavy tier)."""
    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    df = _get_single_band_df_heavy(spark)
    df.createOrReplaceTempView("rasters")
    # Subdivide into ~1.0 MB tiles (this crashes in heavy tier; see xfail in test)
    return spark.sql(
        "SELECT t.* FROM rasters, " "LATERAL gbx_rst_maketiles(tile, 1.0) t"
    ).collect()


rst_maketiles_python_heavy_example_output = """
+---+---+---+
|z  |x  |y  |
+---+---+---+
(one row per sub-tile; NOT WORKING IN HEAVY TIER)
"""


def rst_rasterize_python_heavy_example(spark):
    """Burn geometry into a raster tile (column-returning, not a generator)."""
    from pyspark.sql import Row, functions as f

    if rx is None:
        raise ImportError("rasterx not installed")

    rx.register(spark)
    # Create a synthetic geometry DataFrame with WKT polygon
    df = spark.createDataFrame([Row(geom="POLYGON((2 2, 8 2, 8 8, 2 8, 2 2))")])
    # Rasterize a square polygon (value=1.0, extent=(0,0,10,10), 10x10 pixels, EPSG:4326)
    result = df.select(
        rx.rst_rasterize(
            f.col("geom"),
            f.lit(1.0),
            f.lit(0.0),
            f.lit(0.0),
            f.lit(10.0),
            f.lit(10.0),
            f.lit(10),
            f.lit(10),
            f.lit(4326),
        ).alias("tile")
    ).collect()
    return result[0]["tile"]


rst_rasterize_python_heavy_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(rasterized tile: pixels inside the polygon carry the burn value)
"""
