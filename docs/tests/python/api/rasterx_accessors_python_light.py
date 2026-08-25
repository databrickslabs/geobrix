"""
Python code examples for the light (pyrx) tier of RasterX accessor functions.
Single source of truth for per-function light-Python tabs in docs/docs/api/raster-functions.mdx.

All examples use the shared canonical fixtures from _fixtures.py (single-band, multiband,
or NetCDF) so every function's four tabs show the SAME example — the same fixture, operation,
and argument values expressed in each tier's language.
"""

try:
    from databricks.labs.gbx.pyrx import functions as rx
except ImportError:
    rx = None


# ---------------------------------------------------------------------------
# Shared helpers — imported from _fixtures.py
# ---------------------------------------------------------------------------


def _get_single_band_df(spark):
    from ._fixtures import single_band_tile_df  # noqa: PLC0415

    return single_band_tile_df(spark)


def _get_multiband_df(spark):
    from ._fixtures import multiband_tile_df  # noqa: PLC0415

    return multiband_tile_df(spark)


def _get_netcdf_df(spark):
    from ._fixtures import netcdf_tile_df  # noqa: PLC0415

    return netcdf_tile_df(spark)


# ---------------------------------------------------------------------------
# rst_avg — per-band average pixel values
# Fixture: MULTIBAND (single-band nyc_sentinel2_red is all-NoData → returns [None])
# ---------------------------------------------------------------------------


def rst_avg_python_light_example(spark):
    """Get per-band average pixel values using the light pyrx tier.

    Reads the `multiband_rasters` Setup view (rgb_nir_small.tif, 3 bands)
    because the canonical single-band sentinel2 tile has NoData = 0 and all
    pixels equal zero, so rst_avg returns [None]. The multiband fixture carries
    real pixel data for each band.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_avg("tile").alias("band_averages")).first()
    return result["band_averages"]


rst_avg_python_light_example_output = """
+------------------------------------+
|band_averages                       |
+------------------------------------+
|[83.59375, 153.125, 114.3125]       |
+------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_bandmetadata — per-band metadata map
# Fixture: MULTIBAND (rgb_nir_small.tif has per-band GDAL metadata tags)
# ---------------------------------------------------------------------------


def rst_bandmetadata_python_light_example(spark):
    """Get metadata for band 1 of a raster tile using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif) which carries per-band
    GDAL metadata tags (name, wavelength_nm, band_index) written at fixture
    creation time.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_bandmetadata("tile", f.lit(1)).alias("band_meta")).first()
    return result["band_meta"]


rst_bandmetadata_python_light_example_output = """
+----------------------------------------------+
|band_meta                                     |
+----------------------------------------------+
|{name -> red, wavelength_nm -> 665, band_in...|
+----------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_boundingbox — bounding polygon (returns WKB binary)
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_boundingbox_python_light_example(spark):
    """Get the bounding box of a raster tile as WKB binary using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_boundingbox("tile").alias("bbox")).first()
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
# rst_crs — CRS as authority string or WKT
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# ---------------------------------------------------------------------------


def rst_crs_python_light_example(spark):
    """Get the CRS string for a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_crs("tile").alias("crs")).first()
    return result["crs"]


rst_crs_python_light_example_output = """
+----------+
|crs       |
+----------+
|EPSG:32618|
+----------+
"""


# ---------------------------------------------------------------------------
# rst_format — GDAL format name
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_format_python_light_example(spark):
    """Get the GDAL driver/format name of a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_format("tile").alias("format")).first()
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
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_georeference_python_light_example(spark):
    """Get georeference parameters (scale, skew, origin) using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_georeference("tile").alias("georeference")).first()
    return result["georeference"]


rst_georeference_python_light_example_output = """
+--------------------------------------------------------------+
|georeference                                                  |
+--------------------------------------------------------------+
|{scaleX -> 10.0, scaleY -> -10.0, upperLeftX -> 2121950.0,... |
+--------------------------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_getnodata — NoData values per band
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — nodata=0.0)
# ---------------------------------------------------------------------------


def rst_getnodata_python_light_example(spark):
    """Get the NoData values for each band using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_getnodata("tile").alias("nodata")).first()
    return result["nodata"]


rst_getnodata_python_light_example_output = """
+--------+
|nodata  |
+--------+
|[0.0]   |
+--------+
"""


# ---------------------------------------------------------------------------
# rst_getsubdataset — extract a named subdataset
# Fixture: NETCDF (prAdjust_day_HadGEM2-CC_*.nc — has time_bnds and prAdjust subdatasets)
# ---------------------------------------------------------------------------


def rst_getsubdataset_python_light_example(spark):
    """Extract a named subdataset from a NetCDF raster tile using the light pyrx tier.

    Uses the committed CMIP5 NetCDF fixture (prAdjust_day_HadGEM2-CC_*.nc) which
    has two subdatasets: time_bnds and prAdjust. Subdatasets require a multi-layer
    format such as NetCDF; plain GeoTIFFs return no subdatasets.
    Returns the extracted subdataset tile directly.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415
    from pyspark.sql import functions as f  # noqa: PLC0415

    df = spark.table("netcdf_rasters")
    result = df.select(
        rx.rst_getsubdataset("tile", f.lit("prAdjust")).alias("tile")
    ).first()
    return result["tile"]


rst_getsubdataset_python_light_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(light tier returns a materialized v2 Tile; extracted prAdjust subdataset — 720 pixels wide, 31 bands, 360 rows)
"""


# ---------------------------------------------------------------------------
# rst_height — raster height in pixels
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 161 rows)
# ---------------------------------------------------------------------------


def rst_height_python_light_example(spark):
    """Get the height in pixels of a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_height("tile").alias("height")).first()
    return result["height"]


rst_height_python_light_example_output = """
+------+
|height|
+------+
|161   |
+------+
"""


# ---------------------------------------------------------------------------
# rst_histogram — per-band histogram as MAP<STRING, ARRAY<LONG>>
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands with real pixel data)
# ---------------------------------------------------------------------------


def rst_histogram_python_light_example(spark):
    """Compute a per-band histogram with default settings using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) so the histogram
    has entries for each band. The default bucket count is 256.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_histogram("tile").alias("histogram")).first()
    return result["histogram"]


rst_histogram_python_light_example_output = """
+--------------------------------------------------+
|histogram                                         |
+--------------------------------------------------+
|{band_1 -> [1, 0, 0, ...], band_2 -> [1, 0, 1,... |
+--------------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_isempty — check if raster is empty (all NoData)
# Fixture: MULTIBAND (rgb_nir_small.tif — has real pixel data, not empty)
# ---------------------------------------------------------------------------


def rst_isempty_python_light_example(spark):
    """Check whether a raster tile is empty using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif) which carries real pixel data
    across all three bands. The canonical single-band sentinel2 tile has NoData=0
    and all pixels equal zero, causing rst_isempty to return True.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_isempty("tile").alias("is_empty")).first()
    return result["is_empty"]


rst_isempty_python_light_example_output = """
+--------+
|is_empty|
+--------+
|false   |
+--------+
"""


# ---------------------------------------------------------------------------
# rst_max — maximum pixel values per band
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands with real pixel data)
# ---------------------------------------------------------------------------


def rst_max_python_light_example(spark):
    """Get the maximum pixel value for each band using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) because the
    canonical single-band sentinel2 tile is all-NoData (rst_max returns [None]).
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_max("tile").alias("band_max")).first()
    return result["band_max"]


rst_max_python_light_example_output = """
+---------------------+
|band_max             |
+---------------------+
|[119.0, 197.0, 148.0]|
+---------------------+
"""


# ---------------------------------------------------------------------------
# rst_median — median pixel values per band
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands with real pixel data)
# ---------------------------------------------------------------------------


def rst_median_python_light_example(spark):
    """Get the median pixel value for each band using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) because the
    canonical single-band sentinel2 tile is all-NoData (rst_median returns [None]).
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_median("tile").alias("band_median")).first()
    return result["band_median"]


rst_median_python_light_example_output = """
+---------------------+
|band_median          |
+---------------------+
|[85.0, 157.5, 111.5] |
+---------------------+
"""


# ---------------------------------------------------------------------------
# rst_memsize — in-memory size in bytes
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_memsize_python_light_example(spark):
    """Get the in-memory size of a raster tile in bytes using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_memsize("tile").alias("memsize")).first()
    return result["memsize"]


rst_memsize_python_light_example_output = """
+-------+
|memsize|
+-------+
|71749  |
+-------+
"""


# ---------------------------------------------------------------------------
# rst_metadata — raster metadata map
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif)
# ---------------------------------------------------------------------------


def rst_metadata_python_light_example(spark):
    """Get the metadata map for a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_metadata("tile").alias("metadata")).first()
    return result["metadata"]


rst_metadata_python_light_example_output = """
+--------------------------------------------------+
|metadata                                          |
+--------------------------------------------------+
|{driver -> GTiff, crs -> EPSG:32618, count -> 1,..|
+--------------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_min — minimum pixel values per band
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands with real pixel data)
# ---------------------------------------------------------------------------


def rst_min_python_light_example(spark):
    """Get the minimum pixel value for each band using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) because the
    canonical single-band sentinel2 tile is all-NoData (rst_min returns [None]).
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_min("tile").alias("band_min")).first()
    return result["band_min"]


rst_min_python_light_example_output = """
+-------------------+
|band_min           |
+-------------------+
|[50.0, 102.0, 82.0]|
+-------------------+
"""


# ---------------------------------------------------------------------------
# rst_numbands — number of bands
# Fixture: MULTIBAND (rgb_nir_small.tif — 3 bands)
# ---------------------------------------------------------------------------


def rst_numbands_python_light_example(spark):
    """Get the number of bands in a raster tile using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands: red, NIR, green)
    to show a meaningful multi-band result.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_numbands("tile").alias("num_bands")).first()
    return result["num_bands"]


rst_numbands_python_light_example_output = """
+---------+
|num_bands|
+---------+
|3        |
+---------+
"""


# ---------------------------------------------------------------------------
# rst_pixelcount — total pixel count per band
# Fixture: MULTIBAND (rgb_nir_small.tif — 8x8 = 64 valid pixels per band)
# ---------------------------------------------------------------------------


def rst_pixelcount_python_light_example(spark):
    """Get the count of valid (non-NoData) pixels per band using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 8x8 pixels, no NoData set)
    so each band yields 64 valid pixels. The single-band sentinel2 tile has
    NoData=0 and all pixels equal zero, making pixelcount return [0].
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_pixelcount("tile").alias("pixel_count")).first()
    return result["pixel_count"]


rst_pixelcount_python_light_example_output = """
+------------+
|pixel_count |
+------------+
|[64, 64, 64]|
+------------+
"""


# ---------------------------------------------------------------------------
# rst_pixelheight — pixel height in ground units
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 10.0 m pixels in EPSG:32618)
# ---------------------------------------------------------------------------


def rst_pixelheight_python_light_example(spark):
    """Get the pixel height in ground units using the light pyrx tier.

    For nyc_sentinel2_red.tif (EPSG:32618, UTM Zone 18N) the pixel height
    is 10.0 metres (Sentinel-2 10 m bands).
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_pixelheight("tile").alias("pixel_height")).first()
    return result["pixel_height"]


rst_pixelheight_python_light_example_output = """
+------------+
|pixel_height|
+------------+
|10.0        |
+------------+
"""


# ---------------------------------------------------------------------------
# rst_pixelwidth — pixel width in ground units
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 10.0 m pixels in EPSG:32618)
# ---------------------------------------------------------------------------


def rst_pixelwidth_python_light_example(spark):
    """Get the pixel width in ground units using the light pyrx tier.

    For nyc_sentinel2_red.tif (EPSG:32618, UTM Zone 18N) the pixel width
    is 10.0 metres (Sentinel-2 10 m bands).
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_pixelwidth("tile").alias("pixel_width")).first()
    return result["pixel_width"]


rst_pixelwidth_python_light_example_output = """
+-----------+
|pixel_width|
+-----------+
|10.0       |
+-----------+
"""


# ---------------------------------------------------------------------------
# rst_rotation — rotation in radians
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — axis-aligned, rotation=0.0)
# ---------------------------------------------------------------------------


def rst_rotation_python_light_example(spark):
    """Get the rotation angle of a raster tile in radians using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_rotation("tile").alias("rotation")).first()
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
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — scalex=10.0)
# ---------------------------------------------------------------------------


def rst_scalex_python_light_example(spark):
    """Get the pixel scale in the X direction using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_scalex("tile").alias("scale_x")).first()
    return result["scale_x"]


rst_scalex_python_light_example_output = """
+-------+
|scale_x|
+-------+
|10.0   |
+-------+
"""


# ---------------------------------------------------------------------------
# rst_scaley — scale (pixel size) in Y
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — scaley=-10.0 for north-up)
# ---------------------------------------------------------------------------


def rst_scaley_python_light_example(spark):
    """Get the pixel scale in the Y direction using the light pyrx tier.

    For north-up rasters the Y scale is negative (top row = maximum Y).
    The absolute value equals the pixel height.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_scaley("tile").alias("scale_y")).first()
    return result["scale_y"]


rst_scaley_python_light_example_output = """
+-------+
|scale_y|
+-------+
|-10.0  |
+-------+
"""


# ---------------------------------------------------------------------------
# rst_skewx — skew in X
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — axis-aligned, skewx=0.0)
# ---------------------------------------------------------------------------


def rst_skewx_python_light_example(spark):
    """Get the skew coefficient in the X direction using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_skewx("tile").alias("skew_x")).first()
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
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — axis-aligned, skewy=0.0)
# ---------------------------------------------------------------------------


def rst_skewy_python_light_example(spark):
    """Get the skew coefficient in the Y direction using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_skewy("tile").alias("skew_y")).first()
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
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — EPSG:32618)
# ---------------------------------------------------------------------------


def rst_srid_python_light_example(spark):
    """Get the EPSG SRID integer for a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_srid("tile").alias("srid")).first()
    return result["srid"]


rst_srid_python_light_example_output = """
+-----+
|srid |
+-----+
|32618|
+-----+
"""


# ---------------------------------------------------------------------------
# rst_subdatasets — list of subdataset names
# Fixture: NETCDF (prAdjust_day_HadGEM2-CC_*.nc — has time_bnds and prAdjust)
# ---------------------------------------------------------------------------


def rst_subdatasets_python_light_example(spark):
    """Get the subdatasets map for a NetCDF raster tile using the light pyrx tier.

    Uses the committed CMIP5 NetCDF fixture (prAdjust_day_HadGEM2-CC_*.nc) which
    has two subdatasets: time_bnds and prAdjust. Subdatasets require a multi-layer
    format such as NetCDF; plain GeoTIFFs return an empty map.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("netcdf_rasters")
    result = df.select(rx.rst_subdatasets("tile").alias("subdatasets")).first()
    return result["subdatasets"]


rst_subdatasets_python_light_example_output = """
+------------------------------------------------------+
|subdatasets                                           |
+------------------------------------------------------+
|{SUBDATASET_1_NAME -> ..., SUBDATASET_1_DESC -> [31...|
+------------------------------------------------------+
(map with SUBDATASET_1_NAME/DESC for time_bnds and SUBDATASET_2_NAME/DESC for prAdjust)
"""


# ---------------------------------------------------------------------------
# rst_summary — statistical summary as JSON string
# Fixture: MULTIBAND (rgb_nir_small.tif — 8x8, 3 bands; single-band is all-NoData)
# ---------------------------------------------------------------------------


def rst_summary_python_light_example(spark):
    """Get a statistical summary of a raster tile as a JSON string using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) which has real pixel data.
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_summary("tile").alias("summary")).first()
    return result["summary"]


rst_summary_python_light_example_output = """
+------------------------------------------------------------+
|summary                                                     |
+------------------------------------------------------------+
|{"driverShortName": "GTiff", "size": [8, 8], "coordinateS...|
+------------------------------------------------------------+
"""


# ---------------------------------------------------------------------------
# rst_tryopen — validate raster can be opened
# Fixture: MULTIBAND (rgb_nir_small.tif — committed, always openable)
# ---------------------------------------------------------------------------


def rst_tryopen_python_light_example(spark):
    """Validate that a raster tile can be opened successfully using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_tryopen("tile").alias("try_open")).first()
    return result["try_open"]


rst_tryopen_python_light_example_output = """
+--------+
|try_open|
+--------+
|true    |
+--------+
"""


# ---------------------------------------------------------------------------
# rst_type — data type per band
# Fixture: MULTIBAND (rgb_nir_small.tif — UInt16 per band)
# ---------------------------------------------------------------------------


def rst_type_python_light_example(spark):
    """Get the data type string for each band using the light pyrx tier.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands, UInt16).
    """
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("multiband_rasters")
    result = df.select(rx.rst_type("tile").alias("band_types")).first()
    return result["band_types"]


rst_type_python_light_example_output = """
+-----------------------------+
|band_types                   |
+-----------------------------+
|[UInt16, UInt16, UInt16]     |
+-----------------------------+
"""


# ---------------------------------------------------------------------------
# rst_upperleftx — upper-left corner X coordinate
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — upperleftx=2121950.0 in EPSG:32618)
# ---------------------------------------------------------------------------


def rst_upperleftx_python_light_example(spark):
    """Get the X coordinate of the upper-left corner of a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_upperleftx("tile").alias("upper_left_x")).first()
    return result["upper_left_x"]


rst_upperleftx_python_light_example_output = """
+------------+
|upper_left_x|
+------------+
|2121950.0   |
+------------+
"""


# ---------------------------------------------------------------------------
# rst_upperlefty — upper-left corner Y coordinate
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — upperlefty=-10790470.0 in EPSG:32618)
# ---------------------------------------------------------------------------


def rst_upperlefty_python_light_example(spark):
    """Get the Y coordinate of the upper-left corner of a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_upperlefty("tile").alias("upper_left_y")).first()
    return result["upper_left_y"]


rst_upperlefty_python_light_example_output = """
+---------------+
|upper_left_y   |
+---------------+
|-10790470.0    |
+---------------+
"""


# ---------------------------------------------------------------------------
# rst_width — raster width in pixels
# Fixture: SINGLE-BAND (nyc_sentinel2_red.tif — 236 columns)
# ---------------------------------------------------------------------------


def rst_width_python_light_example(spark):
    """Get the width in pixels of a raster tile using the light pyrx tier."""
    from databricks.labs.gbx.pyrx import functions as rx  # noqa: PLC0415

    df = spark.table("rasters")
    result = df.select(rx.rst_width("tile").alias("width")).first()
    return result["width"]


rst_width_python_light_example_output = """
+-----+
|width|
+-----+
|236  |
+-----+
"""
