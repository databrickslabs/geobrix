"""
SQL examples for RasterX functions documentation.

All SQL examples are executable and tested. These are imported into the
documentation via CodeFromTest components to ensure single-copy pattern.

Run Common setup first (Python/Scala) to register RasterX; then create the
views below so SQL examples can use FROM rasters or FROM multiband_rasters.
"""

# Committed fixtures for band-math and terrain examples. Import under an underscore alias so
# the imported functions are not picked up by inspect.getmembers in the
# test_all_sql_* introspection guards (which require public names to be
# *_sql_example functions).
from _fixtures import (
    single_band_path as _single_band_path,
    multiband_path as _multiband_path,
    dem_path as _dem_path,
    netcdf_path as _netcdf_path,
    color_table_path as _color_table_path,
)

SAMPLE_RASTER_PATH = str(_single_band_path())
MULTIBAND_RASTER_PATH = str(_multiband_path())
DEM_RASTER_PATH = str(_dem_path())
NETCDF_RASTER_PATH = str(_netcdf_path())
COLOR_TABLE_PATH = str(_color_table_path())

# Common setup: create the temp views the SQL examples read from. Point the
# paths at your own rasters — GTIFF_SAMPLE_DIR (single-band) is the default;
# GTIFF_MULTI_DIR, DTM_DIR, and NETCDF_DIR back the band-math/terrain/subdataset
# examples. GeoTIFFs use the `gdal` reader; NetCDF uses `netcdf_gdal`.
RASTERX_SQL_SETUP = f"""-- After registering RasterX (Python: rx.register(spark)), create the views:
CREATE OR REPLACE TEMP VIEW rasters AS
SELECT * FROM gdal.`{SAMPLE_RASTER_PATH}`;              -- GTIFF_SAMPLE_DIR (single-band)

CREATE OR REPLACE TEMP VIEW multiband_rasters AS
SELECT * FROM gdal.`{MULTIBAND_RASTER_PATH}`;           -- GTIFF_MULTI_DIR

CREATE OR REPLACE TEMP VIEW dem_rasters AS
SELECT * FROM gdal.`{DEM_RASTER_PATH}`;                 -- DTM_DIR

CREATE OR REPLACE TEMP VIEW netcdf_rasters AS
SELECT * FROM netcdf_gdal.`{NETCDF_RASTER_PATH}`;       -- NETCDF_DIR"""

RASTERX_SQL_SETUP_output = """
Views `rasters`, `multiband_rasters`, `dem_rasters`, and `netcdf_rasters` created.
Every example on this page reads from one of these views.
"""


def _sql_variant(sql: str, *, lateral: bool) -> str:
    """Pick one tier's statement from a dual-variation SQL example.

    Some functions invoke differently per tier — the heavyweight tier registers
    them as scalar (ARRAY-returning) functions called with a plain ``SELECT``,
    while the lightweight (pyrx) tier registers them as streaming table functions
    that must be called with ``LATERAL``. Those examples therefore hold two
    ``;``-separated statements: the heavy scalar form first (so heavy-only
    ``DESCRIBE FUNCTION`` extraction picks it up) and the light ``LATERAL`` form
    second. Execution tests run one statement at a time, so they use this helper
    to select the statement for the tier under test.

    ``lateral=True`` returns the ``LATERAL`` statement (light tier); ``lateral=False``
    returns the non-``LATERAL`` scalar statement (heavy tier). Single-variation
    examples (no divergence) return their sole statement either way.
    """
    # Drop full-line ``--`` comments FIRST, then split on ``;``. A ``;`` can appear
    # inside a comment (e.g. "call directly; then ..."), which would otherwise
    # shatter the statement — so comment removal must precede the split.
    code = "\n".join(ln for ln in sql.splitlines() if not ln.strip().startswith("--"))
    bodies = [s.strip() for s in code.split(";") if s.strip()]
    for body in bodies:
        has_lateral = "LATERAL" in body.upper()
        if lateral == has_lateral:
            return body
    # No divergence (only one form present): return it regardless of `lateral`.
    return bodies[0] if bodies else sql.strip()


# ============================================================================
# Accessor Functions - Get Raster Properties
# ============================================================================


def rst_boundingbox_sql_example():
    """Get bounding box of rasters using SQL"""
    return """
SELECT path, gbx_rst_boundingbox(tile) as bbox FROM rasters;
"""


rst_boundingbox_sql_example_output = """
+--------------------+-----------------+
|path                |bbox             |
+--------------------+-----------------+
|.../nyc_sentinel2...|POLYGON ((-74....|
+--------------------+-----------------+
"""


def rst_width_sql_example():
    """Get pixel width of a raster tile."""
    return """
SELECT gbx_rst_width(tile) AS width FROM rasters;
"""


rst_width_sql_example_output = """
+-----+
|width|
+-----+
|236  |
+-----+
"""


def rst_height_sql_example():
    """Get pixel height of a raster tile."""
    return """
SELECT gbx_rst_height(tile) AS height FROM rasters;
"""


rst_height_sql_example_output = """
+------+
|height|
+------+
|161   |
+------+
"""


def rst_numbands_sql_example():
    """Get number of bands from a multiband raster.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands).
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_numbands(tile) AS num_bands FROM multiband_rasters;
"""


rst_numbands_sql_example_output = """
+---------+
|num_bands|
+---------+
|3        |
+---------+
"""


def rst_metadata_sql_example():
    """Get metadata from rasters"""
    return """
SELECT gbx_rst_metadata(tile) as metadata FROM rasters;
"""


rst_metadata_sql_example_output = """
+--------------------------------------------------+
|metadata                                          |
+--------------------------------------------------+
|{driver -> GTiff, crs -> EPSG:32618, count -> 1,..|
+--------------------------------------------------+
"""


def rst_srid_sql_example():
    """Get spatial reference identifier (integer EPSG code) for a raster tile."""
    return """
SELECT gbx_rst_srid(tile) AS srid FROM rasters;
"""


rst_srid_sql_example_output = """
+-----+
|srid |
+-----+
|32618|
+-----+
"""


def rst_crs_sql_example():
    """Get the CRS as a string (authority code like EPSG:32618 / ESRI:54008, else WKT)."""
    return """
SELECT gbx_rst_crs(tile) AS crs FROM rasters;
"""


rst_crs_sql_example_output = """
+----------+
|crs       |
+----------+
|EPSG:32618|
+----------+
"""


def rst_georeference_sql_example():
    """Get georeference (geotransform) parameters."""
    return """
SELECT gbx_rst_georeference(tile) AS georeference FROM rasters;
"""


rst_georeference_sql_example_output = """
+-------------------------------------------------------------+
|georeference                                                 |
+-------------------------------------------------------------+
|{scaleX -> 10.0, scaleY -> -10.0, upperLeftX -> 2121950.0,...|
+-------------------------------------------------------------+
"""


def rst_bandmetadata_sql_example():
    """Get band 1 metadata for a multiband raster.

    Uses the multiband fixture (rgb_nir_small.tif) which carries per-band
    GDAL metadata tags.
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_bandmetadata(tile, 1) AS band_meta FROM multiband_rasters;
"""


rst_bandmetadata_sql_example_output = """
+----------------------------------------------+
|band_meta                                     |
+----------------------------------------------+
|{name -> red, wavelength_nm -> 665, band_in...|
+----------------------------------------------+
"""


def rst_pixelcount_sql_example():
    """Get count of valid (non-NoData) pixels per band from a multiband raster.

    Uses the multiband fixture (rgb_nir_small.tif, 8x8=64 valid pixels per band).
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_pixelcount(tile) AS pixel_count FROM multiband_rasters;
"""


rst_pixelcount_sql_example_output = """
+------------+
|pixel_count |
+------------+
|[64, 64, 64]|
+------------+
"""


def rst_avg_sql_example():
    """Get per-band average pixel values from a multiband raster.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) because the
    canonical single-band sentinel2 tile is all-NoData.
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_avg(tile) AS band_averages FROM multiband_rasters;
"""


rst_avg_sql_example_output = """
+-----------------------------+
|band_averages                |
+-----------------------------+
|[83.59375, 153.125, 114.3125]|
+-----------------------------+
"""


def rst_min_sql_example():
    """Get minimum pixel values per band from a multiband raster.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands).
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_min(tile) AS band_min FROM multiband_rasters;
"""


rst_min_sql_example_output = """
+-------------------+
|band_min           |
+-------------------+
|[50.0, 102.0, 82.0]|
+-------------------+
"""


def rst_max_sql_example():
    """Get maximum pixel values per band from a multiband raster.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands).
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_max(tile) AS band_max FROM multiband_rasters;
"""


rst_max_sql_example_output = """
+---------------------+
|band_max             |
+---------------------+
|[119.0, 197.0, 148.0]|
+---------------------+
"""


def rst_min_max_sql_example():
    """Get min/max values and calculate range"""
    return """
SELECT
    path,
    gbx_rst_min(tile)[0] as min_value,
    gbx_rst_max(tile)[0] as max_value,
    gbx_rst_max(tile)[0] - gbx_rst_min(tile)[0] as value_range
FROM elevation_rasters;
"""


def rst_max_aggregation_sql_example():
    """Aggregate maximum values by date"""
    return """
SELECT
    date,
    MAX(gbx_rst_max(tile)[0]) as peak_temperature
FROM daily_temps
GROUP BY date
ORDER BY date;
"""


def rst_median_sql_example():
    """Get median pixel values per band from a multiband raster.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands).
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_median(tile) AS band_median FROM multiband_rasters;
"""


rst_median_sql_example_output = """
+--------------------+
|band_median         |
+--------------------+
|[85.0, 157.5, 111.5]|
+--------------------+
"""


def rst_format_sql_example():
    """Get the GDAL format name of a raster tile."""
    return """
SELECT gbx_rst_format(tile) AS format FROM rasters;
"""


rst_format_sql_example_output = """
+------+
|format|
+------+
|GTiff |
+------+
"""


def rst_type_sql_example():
    """Get data type string per band from a multiband raster.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands, UInt16).
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_type(tile) AS band_types FROM multiband_rasters;
"""


rst_type_sql_example_output = """
+------------------------+
|band_types              |
+------------------------+
|[UInt16, UInt16, UInt16]|
+------------------------+
"""


def rst_pixelwidth_sql_example():
    """Get pixel width in coordinate system units (metres for EPSG:32618 rasters)."""
    return """
SELECT gbx_rst_pixelwidth(tile) AS pixel_width FROM rasters;
"""


rst_pixelwidth_sql_example_output = """
+-----------+
|pixel_width|
+-----------+
|10.0       |
+-----------+
"""


def rst_pixelheight_sql_example():
    """Get pixel height in coordinate system units (metres for EPSG:32618 rasters)."""
    return """
SELECT gbx_rst_pixelheight(tile) AS pixel_height FROM rasters;
"""


rst_pixelheight_sql_example_output = """
+------------+
|pixel_height|
+------------+
|10.0        |
+------------+
"""


def rst_pixelsize_sql_example():
    """Get pixel dimensions in coordinate system units"""
    return """
SELECT
    path,
    gbx_rst_pixelwidth(tile) as pixel_width,
    gbx_rst_pixelheight(tile) as pixel_height,
    gbx_rst_width(tile) * gbx_rst_pixelwidth(tile) as total_width_m
FROM rasters;
"""


rst_pixelsize_sql_example_output = """
+----+-----------+------------+-------------+
|path|pixel_width|pixel_height|total_width_m|
+----+-----------+------------+-------------+
|... |0.5        |-0.5        |2.0          |
+----+-----------+------------+-------------+
"""


def rst_getnodata_sql_example():
    """Get NoData values for raster bands.

    The canonical single-band sentinel2 fixture has nodata=0.0.
    """
    return """
SELECT gbx_rst_getnodata(tile) AS nodata FROM rasters;
"""


rst_getnodata_sql_example_output = """
+------+
|nodata|
+------+
|[0.0] |
+------+
"""


def rst_getsubdataset_sql_example():
    """Extract a named subdataset from a NetCDF raster as a tile.

    Uses the committed CMIP5 NetCDF fixture which has two subdatasets:
    time_bnds and prAdjust. Subdatasets require a multi-layer format such as NetCDF.
    Returns the extracted subdataset as a raster tile (wrap with rst_width /
    rst_metadata to inspect it — the prAdjust subdataset is 720x360, 31 bands).
    """
    return """
-- netcdf_rasters view is from the CMIP5 NetCDF fixture (has time_bnds and prAdjust)
SELECT gbx_rst_getsubdataset(tile, 'prAdjust') AS subdataset FROM netcdf_rasters;
"""


rst_getsubdataset_sql_example_output = """
+-----------------------------------------------------------+
|subdataset                                                 |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(the extracted prAdjust subdataset as a tile — 720x360, 31 bands)
"""


def rst_memsize_sql_example():
    """Get in-memory size of a raster tile in bytes."""
    return """
SELECT gbx_rst_memsize(tile) AS memsize FROM rasters;
"""


rst_memsize_sql_example_output = """
+-------+
|memsize|
+-------+
|71749  |
+-------+
"""


def rst_rotation_sql_example():
    """Get rotation angle of a raster tile in radians."""
    return """
SELECT gbx_rst_rotation(tile) AS rotation FROM rasters;
"""


rst_rotation_sql_example_output = """
+--------+
|rotation|
+--------+
|0.0     |
+--------+
"""


def rst_scalex_sql_example():
    """Get the pixel scale in the X direction."""
    return """
SELECT gbx_rst_scalex(tile) AS scale_x FROM rasters;
"""


rst_scalex_sql_example_output = """
+-------+
|scale_x|
+-------+
|10.0   |
+-------+
"""


def rst_scaley_sql_example():
    """Get the pixel scale in the Y direction (negative for north-up rasters)."""
    return """
SELECT gbx_rst_scaley(tile) AS scale_y FROM rasters;
"""


rst_scaley_sql_example_output = """
+-------+
|scale_y|
+-------+
|-10.0  |
+-------+
"""


def rst_scalex_scaley_sql_example():
    """Get scale (pixel size) in X and Y"""
    return """
SELECT
    path,
    gbx_rst_scalex(tile) as scale_x,
    gbx_rst_scaley(tile) as scale_y
FROM rasters;
"""


rst_scalex_scaley_sql_example_output = """
+----+-------+-------+
|path|scale_x|scale_y|
+----+-------+-------+
|... |0.5    |-0.5   |
+----+-------+-------+
"""


def rst_skewx_sql_example():
    """Get the skew coefficient in the X direction."""
    return """
SELECT gbx_rst_skewx(tile) AS skew_x FROM rasters;
"""


rst_skewx_sql_example_output = """
+------+
|skew_x|
+------+
|0.0   |
+------+
"""


def rst_skewy_sql_example():
    """Get the skew coefficient in the Y direction."""
    return """
SELECT gbx_rst_skewy(tile) AS skew_y FROM rasters;
"""


rst_skewy_sql_example_output = """
+------+
|skew_y|
+------+
|0.0   |
+------+
"""


def rst_skewx_skewy_sql_example():
    """Get skew in X and Y"""
    return """
SELECT
    path,
    gbx_rst_skewx(tile) as skew_x,
    gbx_rst_skewy(tile) as skew_y
FROM rasters;
"""


rst_skewx_skewy_sql_example_output = """
+----+------+------+
|path|skew_x|skew_y|
+----+------+------+
|... |0.0   |0.0   |
+----+------+------+
"""


def rst_subdatasets_sql_example():
    """List subdatasets from a NetCDF raster.

    Uses the committed CMIP5 NetCDF fixture which has two subdatasets:
    time_bnds and prAdjust. Plain GeoTIFFs return an empty map.
    """
    return """
-- netcdf_rasters view is from the CMIP5 NetCDF fixture (has time_bnds and prAdjust)
SELECT gbx_rst_subdatasets(tile) AS subdatasets FROM netcdf_rasters;
"""


rst_subdatasets_sql_example_output = """
+------------------------------------------------------+
|subdatasets                                           |
+------------------------------------------------------+
|{SUBDATASET_1_NAME -> ..., SUBDATASET_1_DESC -> [31...|
+------------------------------------------------------+
(map with SUBDATASET_1_NAME/DESC for time_bnds and SUBDATASET_2_NAME/DESC for prAdjust)
"""


def rst_summary_sql_example():
    """Get statistical summary of a multiband raster tile.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) which has real pixel data.
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_summary(tile) AS summary FROM multiband_rasters;
"""


rst_summary_sql_example_output = """
+------------------------------------------------------------+
|summary                                                     |
+------------------------------------------------------------+
|{"driverShortName": "GTiff", "size": [8, 8], "coordinateS...|
+------------------------------------------------------------+
"""


def rst_upperleftx_sql_example():
    """Get the X coordinate of the upper-left corner of a raster tile."""
    return """
SELECT gbx_rst_upperleftx(tile) AS upper_left_x FROM rasters;
"""


rst_upperleftx_sql_example_output = """
+------------+
|upper_left_x|
+------------+
|2121950.0   |
+------------+
"""


def rst_upperlefty_sql_example():
    """Get the Y coordinate of the upper-left corner of a raster tile."""
    return """
SELECT gbx_rst_upperlefty(tile) AS upper_left_y FROM rasters;
"""


rst_upperlefty_sql_example_output = """
+------------+
|upper_left_y|
+------------+
|-10790470.0 |
+------------+
"""


def rst_upperleft_sql_example():
    """Get upper-left corner coordinates"""
    return """
SELECT
    path,
    gbx_rst_upperleftx(tile) as upper_left_x,
    gbx_rst_upperlefty(tile) as upper_left_y
FROM rasters;
"""


rst_upperleft_sql_example_output = """
+----+------------+------------+
|path|upper_left_x|upper_left_y|
+----+------------+------------+
|... |10.0        |50.0        |
+----+------------+------------+
"""


# ============================================================================
# Constructor Functions - Create/Load Rasters
# ============================================================================


def rst_fromfile_sql_example():
    """Load raster from file path"""
    return """
-- gbx_rst_fromfile is a Python UDF (no JVM form; requires geobrix[light]). The
-- SQL call is the same 2-argument form in both tiers — the tier you register
-- decides the result (whichever register() ran last wins):
--   Lightweight (rx.register / pyrx):     returns a VIRTUAL tile (bytes-free,
--                                          path + whole-file window; lazy).
--   Heavyweight (rasterx register):       returns a MATERIALIZED tile (raster
--                                          bytes present) — JVM/heavy callers
--                                          cannot use a virtual path-only tile.
SELECT
    gbx_rst_fromfile('/Volumes/main/geobrix_samples/nyc/sentinel2.tif', 'GTiff') AS tile;

-- Either way, accessors read what they need — width/height come from the header
-- (no pixel read even for the virtual tile):
SELECT
    path,
    gbx_rst_width(gbx_rst_fromfile(path, 'GTiff')) as width,
    gbx_rst_height(gbx_rst_fromfile(path, 'GTiff')) as height
FROM raster_paths;
"""


rst_fromfile_sql_example_output = """
# Both tiers return the SAME v2 tile struct (cellid, raster, path, window, ...);
# only the field values differ — virtual carries the path, materialized the bytes.

# Lightweight registration — a VIRTUAL v2 tile (raster null; path + window set):
+---------------------------------------------------------------+
|tile                                                           |
+---------------------------------------------------------------+
|{0, null, /Volumes/..., {0, 0, 10980, 10980}, ..., null, {...}}|
+---------------------------------------------------------------+

# Heavyweight registration — a MATERIALIZED v2 tile (raster bytes; path null):
+------------------------------------------------------------+
|tile                                                        |
+------------------------------------------------------------+
|{0, <raster bytes>, null, null, ..., {driver -> GTiff, ...}}|
+------------------------------------------------------------+

# width/height (either tier) read from the header:
+----+-----+------+
|path|width|height|
+----+-----+------+
|... |10980|10980 |
+----+-----+------+
"""


def rst_fromcontent_sql_example():
    """Load raster from binary content"""
    return """
-- Load from binary table
SELECT
    path,
    gbx_rst_fromcontent(content, 'GTiff') as tile
FROM binary_raster_table;
"""


rst_fromcontent_sql_example_output = """
+----+-----------------------------------------------------------+
|path|tile                                                       |
+----+-----------------------------------------------------------+
|... |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+----+-----------------------------------------------------------+
"""


def rst_frombands_sql_example():
    """Combine multiple bands into single raster"""
    return """
SELECT
    gbx_rst_frombands(array(band1, band2, band3)) as multi_band
FROM separated_bands;
"""


rst_frombands_sql_example_output = """
+-----------------------------------------------------------+
|multi_band                                                 |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_frombands_agg_sql_example():
    """Aggregator: collect ordered bands per group into a single multi-band tile."""
    return """
-- Collect per-band tiles in acquisition order into one multi-band raster per scene.
SELECT scene_id,
    gbx_rst_frombands_agg(tile, band_index) AS multi_band
FROM band_tiles
GROUP BY scene_id;
"""


rst_frombands_agg_sql_example_output = """
# Heavyweight SQL — one v2 tile struct per group:
+--------+-----------------------------------------------------------+
|scene_id|multi_band                                                 |
+--------+-----------------------------------------------------------+
|...     |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+--------+-----------------------------------------------------------+

# Lightweight SQL — raster bytes as BINARY (see the note above); wrap with
# gbx_rst_fromcontent(multi_band, 'GTiff') to rebuild a tile struct:
+--------+---------------+
|scene_id|multi_band     |
+--------+---------------+
|...     |[B@... (BINARY)|
+--------+---------------+
"""


# ============================================================================
# Transformation Functions - Modify Rasters
# ============================================================================


def rst_clip_sql_example():
    """Clip raster with geometry"""
    return """
-- Clip with WKT geometry
SELECT
    path,
    gbx_rst_clip(
        tile,
        'POLYGON((-122 37, -122 38, -121 38, -121 37, -122 37))',
        true
    ) as clipped
FROM rasters;
"""


rst_clip_sql_example_output = """
+----+-----------------------------------------------------------+
|path|clipped                                                    |
+----+-----------------------------------------------------------+
|... |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+----+-----------------------------------------------------------+
"""


def rst_transform_sql_example():
    """Reproject raster to different CRS"""
    return """
-- Reproject to WGS84
SELECT
    path,
    gbx_rst_transform(tile, 4326) as wgs84_tile,
    gbx_rst_srid(gbx_rst_transform(tile, 4326)) as new_srid
FROM rasters;

-- Reproject and clip
SELECT
    path,
    gbx_rst_clip(gbx_rst_transform(tile, 4326), boundary, true) as result
FROM rasters;
"""


rst_transform_sql_example_output = """
+----+-----------------------------------------------------------+--------+
|path|wgs84_tile                                                 |new_srid|
+----+-----------------------------------------------------------+--------+
|... |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|4326    |
+----+-----------------------------------------------------------+--------+
"""


def rst_transformcrs_sql_example():
    """Reproject a raster to a target CRS given as a string (accepts non-EPSG)."""
    return """
-- Reproject to Web Mercator using a CRS string.
-- Unlike rst_transform (int EPSG only), rst_transformcrs also accepts
-- ESRI codes, WKT, or PROJ4 targets. An int-castable string ('3857')
-- is treated as an EPSG SRID.
SELECT
    path,
    gbx_rst_transformcrs(tile, 'EPSG:3857') as webmercator_tile,
    gbx_rst_crs(gbx_rst_transformcrs(tile, 'EPSG:3857')) as new_crs
FROM rasters;
"""


rst_transformcrs_sql_example_output = """
+----+-----------------------------------------------------------+---------+
|path|webmercator_tile                                           |new_crs  |
+----+-----------------------------------------------------------+---------+
|... |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|EPSG:3857|
+----+-----------------------------------------------------------+---------+
"""


def rst_asformat_sql_example():
    """Convert raster to different format"""
    return """
-- Convert NetCDF to GeoTIFF
SELECT
    path,
    gbx_rst_asformat(tile, 'GTiff') as geotiff_tile
FROM netcdf_rasters;

-- Convert to PNG
SELECT
    path,
    gbx_rst_asformat(tile, 'PNG') as png_tile
FROM visualization_tiles;
"""


rst_asformat_sql_example_output = """
+----+-----------------------------------------------------------+
|path|geotiff_tile                                               |
+----+-----------------------------------------------------------+
|... |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+----+-----------------------------------------------------------+
"""


def rst_ndvi_sql_example():
    """Compute Normalized Difference Vegetation Index (NDVI).

    NDVI = (NIR - Red) / (NIR + Red). Input: multiband tile with red (band 1) and NIR (band 2).
    Output: single-band Float32 tile.
    """
    return """
SELECT gbx_rst_ndvi(tile, 1, 2) AS ndvi FROM multiband_rasters;
"""


rst_ndvi_sql_example_output = """
+-----------------------------------------------------------+
|ndvi                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NDVI raster: (NIR-Red)/(NIR+Red))
"""


def rst_filter_sql_example():
    """Apply spatial filter to raster"""
    return """
-- Median filter (3x3 window)
SELECT
    path,
    gbx_rst_filter(tile, 3, 'median') as denoised
FROM noisy_rasters;

-- Average smoothing (5x5 window)
SELECT
    path,
    gbx_rst_filter(tile, 5, 'avg') as smoothed
FROM rasters;
"""


rst_filter_sql_example_output = """
+----+-----------------------------------------------------------+
|path|denoised                                                   |
+----+-----------------------------------------------------------+
|... |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+----+-----------------------------------------------------------+
"""


def rst_convolve_sql_example():
    """Apply convolution kernel to raster"""
    return """
-- Apply 3x3 kernel (e.g. blur); kernel format is driver-specific
SELECT path, gbx_rst_convolve(tile, kernel) as filtered FROM rasters_with_kernels;
"""


rst_convolve_sql_example_output = """
+----+-----------------------------------------------------------+
|path|filtered                                                   |
+----+-----------------------------------------------------------+
|... |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+----+-----------------------------------------------------------+
"""


# ============================================================================
# Coordinate Transformation Functions
# ============================================================================


def rst_rastertoworldcoord_sql_example():
    """Convert pixel coordinates to world coordinates"""
    return """
SELECT
    gbx_rst_rastertoworldcoord(tile, 100, 80) as world_coord,
    gbx_rst_rastertoworldcoord(tile, 100, 80).x as easting,
    gbx_rst_rastertoworldcoord(tile, 100, 80).y as northing
FROM rasters;
"""


rst_rastertoworldcoord_sql_example_output = """
+---------------+-------+--------+
|world_coord    |easting|northing|
+---------------+-------+--------+
|{500980.0, ...}|500980 |4599220 |
+---------------+-------+--------+
"""


def rst_rastertoworldcoordx_sql_example():
    """Convert pixel X to world X coordinate"""
    return """
SELECT
    gbx_rst_rastertoworldcoordx(tile, 100, 80) as easting
FROM rasters;
"""


rst_rastertoworldcoordx_sql_example_output = """
+-------+
|easting|
+-------+
|500980 |
+-------+
"""


def rst_rastertoworldcoordy_sql_example():
    """Convert pixel Y to world Y coordinate"""
    return """
SELECT
    gbx_rst_rastertoworldcoordy(tile, 100, 80) as northing
FROM rasters;
"""


rst_rastertoworldcoordy_sql_example_output = """
+--------+
|northing|
+--------+
|4599220 |
+--------+
"""


def rst_worldtorastercoord_sql_example():
    """Convert world coordinates to pixel coordinates (single location)"""
    return """
-- Find pixel coordinates for a specific location (center of raster)
SELECT
    gbx_rst_worldtorastercoord(tile, 2122955.0, -10791275.0) as pixel_coord,
    gbx_rst_worldtorastercoord(tile, 2122955.0, -10791275.0).x as pixel_col,
    gbx_rst_worldtorastercoord(tile, 2122955.0, -10791275.0).y as pixel_row
FROM rasters;
"""


def rst_worldtorastercoordx_sql_example():
    """Convert world X to pixel X coordinate"""
    return """
SELECT
    gbx_rst_worldtorastercoordx(tile, 2122955.0, -10791275.0) as pixel_col
FROM rasters;
"""


def rst_worldtorastercoordy_sql_example():
    """Convert world Y to pixel Y coordinate"""
    return """
SELECT
    gbx_rst_worldtorastercoordy(tile, 2122955.0, -10791275.0) as pixel_row
FROM rasters;
"""


rst_worldtorastercoord_sql_example_output = """
+-----------+---------+---------+
|pixel_coord|pixel_col|pixel_row|
+-----------+---------+---------+
|{5490, ...}|5490     |5490     |
+-----------+---------+---------+
"""


rst_worldtorastercoordx_sql_example_output = """
+---------+
|pixel_col|
+---------+
|5490     |
+---------+
"""


rst_worldtorastercoordy_sql_example_output = """
+---------+
|pixel_row|
+---------+
|5490     |
+---------+
"""


# ============================================================================
# Validation Functions
# ============================================================================


def rst_isempty_sql_example():
    """Check if a raster tile is empty (all pixels are NoData).

    Uses the multiband fixture (rgb_nir_small.tif) which carries real pixel data.
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_isempty(tile) AS is_empty FROM multiband_rasters;
"""


rst_isempty_sql_example_output = """
+--------+
|is_empty|
+--------+
|false   |
+--------+
"""


def rst_tryopen_sql_example():
    """Validate that a raster tile can be opened successfully.

    Uses the multiband fixture (rgb_nir_small.tif) which is always openable.
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_tryopen(tile) AS try_open FROM multiband_rasters;
"""


rst_tryopen_sql_example_output = """
+--------+
|try_open|
+--------+
|true    |
+--------+
"""


# ============================================================================
# Advanced Operations
# ============================================================================


def rst_derivedband_agg_sql_example():
    """Aggregator: apply Python UDF to tiles in group by"""
    return """
SELECT region, gbx_rst_derivedband_agg(tile, 'def f(a): return a', 'f') as result FROM rasters GROUP BY region;
"""


rst_derivedband_agg_sql_example_output = """
# Heavyweight SQL — one v2 tile struct per group:
+------+-----------------------------------------------------------+
|region|result                                                     |
+------+-----------------------------------------------------------+
|...   |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+

# Lightweight SQL — raster bytes as BINARY (see the note above); wrap with
# gbx_rst_fromcontent(result, 'GTiff') to rebuild a tile struct:
+------+---------------+
|region|result         |
+------+---------------+
|...   |[B@... (BINARY)|
+------+---------------+
"""


def rst_initnodata_sql_example():
    """Initialize NoData values"""
    return """
SELECT gbx_rst_initnodata(tile) as tile FROM rasters;
"""


rst_initnodata_sql_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(tile with NoData initialized)
"""


def rst_updatetype_sql_example():
    """Convert raster data type"""
    return """
SELECT gbx_rst_updatetype(tile, 'Float32') as float_tile FROM rasters;
"""


rst_updatetype_sql_example_output = """
+-----------------------------------------------------------+
|float_tile                                                 |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


# ============================================================================
# H3 Grid Functions
# ============================================================================


def rst_h3_tessellate_sql_example():
    """Tessellate raster to H3 grid (covering or centroid mode)"""
    return """
-- Heavyweight: the generator in SELECT explodes to one row per overlapping H3
-- cell, clipped to its hexagon (covering mode, default; pass 'centroid' as the
-- 3rd arg for pixel-centroid single-assignment).
SELECT gbx_rst_h3_tessellate(tile, 7, 'covering') FROM rasters;

-- Lightweight (pyrx): registered as a streaming table function — call with LATERAL.
SELECT t.* FROM rasters, LATERAL gbx_rst_h3_tessellate(tile, 7, 'covering') t;
"""


rst_h3_tessellate_sql_example_output = """
+------+------------------+--------------+
|source|cellid            |raster        |
+------+------------------+--------------+
|...   |599686042433355775|<raster bytes>|
+------+------------------+--------------+"""


def rst_quadbin_tessellate_sql_example():
    """Tessellate a raster into CARTO quadbin v0 cells (covering or centroid mode)"""
    return """
-- Heavyweight: the generator in SELECT explodes to one row per overlapping
-- quadbin cell, each chip clipped to its cell (covering mode, default; pass
-- 'centroid' as the 3rd arg for pixel-centroid single-assignment). Zoom 12 for
-- a city-scale raster.
SELECT gbx_rst_quadbin_tessellate(tile, 12, 'covering') FROM rasters;

-- Lightweight (pyrx): registered as a streaming table function — call with LATERAL.
SELECT t.* FROM rasters, LATERAL gbx_rst_quadbin_tessellate(tile, 12, 'covering') t;
"""


rst_quadbin_tessellate_sql_example_output = """
+------+-------------------+--------------+
|source|cellid             |raster        |
+------+-------------------+--------------+
|...   |5250127588525215743|<raster bytes>|
+------+-------------------+--------------+
(SELECT t.* expands the v2-tile struct; cellid is the quadbin index)
"""


def rst_bng_tessellate_sql_example():
    """Tessellate a raster into British National Grid cells (covering or centroid mode)"""
    return """
-- Heavyweight: the generator in SELECT explodes to one row per overlapping BNG
-- cell (covering mode, default; pass 'centroid' as the 3rd arg for
-- pixel-centroid single-assignment). '1km' == integer resolution 3; a raster in
-- any CRS is warped to EPSG:27700 first.
SELECT gbx_rst_bng_tessellate(tile, '1km', 'covering') FROM rasters;

-- Lightweight (pyrx): registered as a streaming table function — call with LATERAL.
SELECT t.* FROM rasters, LATERAL gbx_rst_bng_tessellate(tile, '1km', 'covering') t;
"""


rst_bng_tessellate_sql_example_output = """
+------+------+--------------+
|source|cellid|raster        |
+------+------+--------------+
|...   |TQ2979|<raster bytes>|
+------+------+--------------+
(SELECT t.* expands the v2-tile struct; cellid is the BNG grid-square STRING)
"""


def rst_h3_rastertogridavg_sql_example():
    """Aggregate raster values to H3 grid using average"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_h3_rastertogridavg(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridavg(tile, 4) t;
"""


rst_h3_rastertogridavg_sql_example_output = """
+----+------------------+-------+
|band|cellID            |measure|
+----+------------------+-------+
|1   |599686042433355775|123.45 |
|1   |599686043374559743|98.12  |
|2   |599686042433355775|210.67 |
+----+------------------+-------+
(one row per band×cell. The lightweight LATERAL form yields these [band, cellID,
 measure] rows directly; the heavyweight scalar form returns a nested ARRAY that
 explode(...) flattens to the same rows.)
"""


def rst_h3_rastertogridcount_sql_example():
    """Count pixels per H3 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_h3_rastertogridcount(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridcount(tile, 4) t;
"""


rst_h3_rastertogridcount_sql_example_output = """
+----+------------------+-------+
|band|cellID            |measure|
+----+------------------+-------+
|1   |599686042433355775|256    |
|1   |599686043374559743|240    |
|2   |599686042433355775|256    |
+----+------------------+-------+
(pixel count per band×cell)
"""


def rst_h3_rastertogridmax_sql_example():
    """Get maximum values per H3 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_h3_rastertogridmax(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridmax(tile, 4) t;
"""


rst_h3_rastertogridmax_sql_example_output = """
+----+------------------+-------+
|band|cellID            |measure|
+----+------------------+-------+
|1   |599686042433355775|255.0  |
|1   |599686043374559743|254.0  |
|2   |599686042433355775|240.0  |
+----+------------------+-------+
(max value per band×cell)
"""


def rst_h3_rastertogridmin_sql_example():
    """Get minimum values per H3 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_h3_rastertogridmin(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridmin(tile, 4) t;
"""


rst_h3_rastertogridmin_sql_example_output = """
+----+------------------+-------+
|band|cellID            |measure|
+----+------------------+-------+
|1   |599686042433355775|0.0    |
|1   |599686043374559743|10.0   |
|2   |599686042433355775|5.0    |
+----+------------------+-------+
(min value per band×cell)
"""


def rst_h3_rastertogridmedian_sql_example():
    """Get median values per H3 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_h3_rastertogridmedian(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridmedian(tile, 4) t;
"""


rst_h3_rastertogridmedian_sql_example_output = """
+----+------------------+-------+
|band|cellID            |measure|
+----+------------------+-------+
|1   |599686042433355775|120.5  |
|1   |599686043374559743|122.0  |
|2   |599686042433355775|115.0  |
+----+------------------+-------+
(median value per band×cell)
"""


def rst_h3_rastertogridsum_sql_example():
    """Get the sum of pixel values per H3 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_h3_rastertogridsum(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridsum(tile, 4) t;
"""


rst_h3_rastertogridsum_sql_example_output = """
+----+------------------+-------+
|band|cellID            |measure|
+----+------------------+-------+
|1   |599686042433355775|31563.0|
|1   |599686043374559743|29488.0|
|2   |599686042433355775|28672.0|
+----+------------------+-------+
(sum of pixel values per band×cell)
"""


def rst_h3_rastertogridvariance_sql_example():
    """Get the population variance of pixel values per H3 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_h3_rastertogridvariance(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridvariance(tile, 4) t;
"""


rst_h3_rastertogridvariance_sql_example_output = """
+----+------------------+-------+
|band|cellID            |measure|
+----+------------------+-------+
|1   |599686042433355775|1245.5 |
|1   |599686043374559743|1389.2 |
|2   |599686042433355775|1156.0 |
+----+------------------+-------+
(variance per band×cell)
"""


def rst_h3_rastertogridstddev_sql_example():
    """Get the population standard deviation of pixel values per H3 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_h3_rastertogridstddev(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_h3_rastertogridstddev(tile, 4) t;
"""


rst_h3_rastertogridstddev_sql_example_output = """
+----+------------------+-------+
|band|cellID            |measure|
+----+------------------+-------+
|1   |599686042433355775|35.29  |
|1   |599686043374559743|37.27  |
|2   |599686042433355775|34.01  |
+----+------------------+-------+
(standard deviation per band×cell)
"""


def rst_quadbin_rastertogridavg_sql_example():
    """Aggregate raster values to CARTO quadbin v0 cells using average"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_quadbin_rastertogridavg(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridavg(tile, 4) t;
"""


rst_quadbin_rastertogridavg_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |123.45 |
|1   |12346 |124.20 |
|2   |12345 |210.67 |
+----+------+-------+
(one row per band×Quadbin cell)
"""


def rst_quadbin_rastertogridcount_sql_example():
    """Count pixels per CARTO quadbin v0 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_quadbin_rastertogridcount(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridcount(tile, 4) t;
"""


rst_quadbin_rastertogridcount_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |256    |
|1   |12346 |240    |
|2   |12345 |256    |
+----+------+-------+
(pixel count per band×Quadbin cell)
"""


def rst_quadbin_rastertogridmax_sql_example():
    """Get maximum values per CARTO quadbin v0 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_quadbin_rastertogridmax(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridmax(tile, 4) t;
"""


rst_quadbin_rastertogridmax_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |255.0  |
|1   |12346 |254.0  |
|2   |12345 |240.0  |
+----+------+-------+
(max value per band×Quadbin cell)
"""


def rst_quadbin_rastertogridmin_sql_example():
    """Get minimum values per CARTO quadbin v0 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_quadbin_rastertogridmin(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridmin(tile, 4) t;
"""


rst_quadbin_rastertogridmin_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |0.0    |
|1   |12346 |10.0   |
|2   |12345 |5.0    |
+----+------+-------+
(min value per band×Quadbin cell)
"""


def rst_quadbin_rastertogridmedian_sql_example():
    """Get median values per CARTO quadbin v0 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_quadbin_rastertogridmedian(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridmedian(tile, 4) t;
"""


rst_quadbin_rastertogridmedian_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |120.5  |
|1   |12346 |122.0  |
|2   |12345 |115.0  |
+----+------+-------+
(median value per band×Quadbin cell)
"""


def rst_quadbin_rastertogridsum_sql_example():
    """Get the sum of pixel values per CARTO quadbin v0 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_quadbin_rastertogridsum(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridsum(tile, 4) t;
"""


rst_quadbin_rastertogridsum_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |31563.0|
|1   |12346 |29488.0|
|2   |12345 |28672.0|
+----+------+-------+
(sum of pixel values per band×Quadbin cell)
"""


def rst_quadbin_rastertogridvariance_sql_example():
    """Get the population variance of pixel values per CARTO quadbin v0 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_quadbin_rastertogridvariance(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridvariance(tile, 4) t;
"""


rst_quadbin_rastertogridvariance_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |1245.5 |
|1   |12346 |1389.2 |
|2   |12345 |1156.0 |
+----+------+-------+
(variance per band×Quadbin cell)
"""


def rst_quadbin_rastertogridstddev_sql_example():
    """Get the population standard deviation of pixel values per CARTO quadbin v0 cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_quadbin_rastertogridstddev(tile, 4) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_quadbin_rastertogridstddev(tile, 4) t;
"""


rst_quadbin_rastertogridstddev_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |12345 |35.29  |
|1   |12346 |37.27  |
|2   |12345 |34.01  |
+----+------+-------+
(standard deviation per band×Quadbin cell)
"""


# ============================================================================
# BNG (British National Grid) rastertogrid reducers
#
# BNG works natively in EPSG:27700; a raster in any CRS is warped to 27700
# before pixels are binned. Cell ids are STRINGS (e.g. "TQ38SW"), not Longs.
# Resolution is an integer index (1=100km, 2=10km, 3=1km, 4=100m, 5=10m, 6=1m)
# or a resolution string ("1km", "100m", ...). Most examples use the resolution
# string "1km"; rastertogridcount uses the integer form (2 == 10km) to show the
# integer binding path. Examples use the London SRTM elevation raster
# (srtm_n51w001.tif, EPSG:4326), which overlaps the TQ grid square over central London.
# ============================================================================


def rst_bng_rastertogridavg_sql_example():
    """Aggregate raster values to British National Grid cells using average"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_bng_rastertogridavg(tile, 3) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridavg(tile, 3) t;
"""


rst_bng_rastertogridavg_sql_example_output = """
+----+------+------------------+
|band|cellID|measure           |
+----+------+------------------+
|1   |OW5574|77.22222222222223 |
|1   |OW5575|80.66666666666667 |
|2   |OW5574|144.33333333333334|
+----+------+------------------+
(one row per band × BNG cell; cellID is a STRING grid-square label)
"""


def rst_bng_rastertogridcount_sql_example():
    """Count pixels per British National Grid cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_bng_rastertogridcount(tile, 3) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridcount(tile, 3) t;
"""


rst_bng_rastertogridcount_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |OW5574|9      |
|1   |OW5575|21     |
|2   |OW5574|9      |
+----+------+-------+
(pixel count per band × BNG cell)
"""


def rst_bng_rastertogridmax_sql_example():
    """Get maximum values per British National Grid cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_bng_rastertogridmax(tile, 3) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridmax(tile, 3) t;
"""


rst_bng_rastertogridmax_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |OW5574|106.0  |
|1   |OW5575|118.0  |
|1   |OW5674|107.0  |
+----+------+-------+
(max value per band × BNG cell)
"""


def rst_bng_rastertogridmin_sql_example():
    """Get minimum values per British National Grid cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_bng_rastertogridmin(tile, 3) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridmin(tile, 3) t;
"""


rst_bng_rastertogridmin_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |OW5574|0.0    |
|1   |OW5575|54.0   |
|1   |OW5674|65.0   |
+----+------+-------+
(min value per band × BNG cell)
"""


def rst_bng_rastertogridmedian_sql_example():
    """Get median values per British National Grid cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_bng_rastertogridmedian(tile, 3) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridmedian(tile, 3) t;
"""


rst_bng_rastertogridmedian_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |OW5574|88.0   |
|1   |OW5575|80.0   |
|1   |OW5674|81.0   |
+----+------+-------+
(median value per band × BNG cell)
"""


def rst_bng_rastertogridsum_sql_example():
    """Get the sum of pixel values per British National Grid cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_bng_rastertogridsum(tile, 3) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridsum(tile, 3) t;
"""


rst_bng_rastertogridsum_sql_example_output = """
+----+------+-------+
|band|cellID|measure|
+----+------+-------+
|1   |OW5574|695.0  |
|1   |OW5575|1694.0 |
|1   |OW5674|774.0  |
+----+------+-------+
(sum of pixel values per band × BNG cell)
"""


def rst_bng_rastertogridvariance_sql_example():
    """Get the population variance of pixel values per British National Grid cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_bng_rastertogridvariance(tile, 3) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridvariance(tile, 3) t;
"""


rst_bng_rastertogridvariance_sql_example_output = """
+----+------+------------------+
|band|cellID|measure           |
+----+------+------------------+
|1   |OW5574|963.7283950617285 |
|1   |OW5575|464.126984126984  |
|1   |OW5674|196.66666666666666|
+----+------+------------------+
(population variance per band × BNG cell)
"""


def rst_bng_rastertogridstddev_sql_example():
    """Get the population standard deviation of pixel values per British National Grid cell"""
    return """
-- Heavyweight: scalar ARRAY return (one element per band) — call directly;
-- explode to flatten the per-band arrays to rows.
SELECT gbx_rst_bng_rastertogridstddev(tile, 3) AS grid FROM multiband_rasters;

-- Lightweight (pyrx): streaming table function — must use LATERAL.
SELECT t.* FROM multiband_rasters, LATERAL gbx_rst_bng_rastertogridstddev(tile, 3) t;
"""


rst_bng_rastertogridstddev_sql_example_output = """
+----+------+------------------+
|band|cellID|measure           |
+----+------+------------------+
|1   |OW5574|31.043975181373415|
|1   |OW5575|21.543606571950388|
|1   |OW5674|14.023789311975086|
+----+------+------------------+
(population standard deviation per band × BNG cell)
"""


# ============================================================================
# Generator Functions - Produce Multiple Rows
# ============================================================================


def rst_maketiles_sql_example():
    """Subdivide rasters into tiles by approximate size in MB"""
    return """
-- Subdivide into MB-sized tiles using LATERAL. The second argument is a target
-- size in MB, not pixel dimensions; the tile grid is derived from the MB budget.
SELECT t.*
FROM rasters,
LATERAL gbx_rst_maketiles(tile, 4) t;
"""


rst_maketiles_sql_example_output = """
+------+--------------+----+----------------------+
|cellid|raster        |path|...                   |
+------+--------------+----+----------------------+
|0     |<raster bytes>|... |{driver -> GTiff, ...}|
+------+--------------+----+----------------------+
(one row per sub-tile; t.* expands the v2-Tile struct fields)
"""


def rst_retile_sql_example():
    """Retile rasters to uniform dimensions"""
    return """
SELECT t.*
FROM rasters,
LATERAL gbx_rst_retile(tile, 256, 256) t;
"""


rst_retile_sql_example_output = """
+------+--------------+----+----------------------+
|cellid|raster        |path|...                   |
+------+--------------+----+----------------------+
|0     |<raster bytes>|... |{driver -> GTiff, ...}|
+------+--------------+----+----------------------+
(one row per sub-tile; t.* expands the v2-Tile struct fields)
"""


def rst_tooverlappingtiles_sql_example():
    """Create overlapping tiles for edge-aware processing"""
    return """
SELECT t.*
FROM rasters,
LATERAL gbx_rst_tooverlappingtiles(tile, 256, 256, 10) t;
"""


rst_tooverlappingtiles_sql_example_output = """
+------+--------------+----+----------------------+
|cellid|raster        |path|...                   |
+------+--------------+----+----------------------+
|0     |<raster bytes>|... |{driver -> GTiff, ...}|
+------+--------------+----+----------------------+
(one row per overlapping tile; t.* expands the v2-Tile struct fields)
"""


def rst_separatebands_sql_example():
    """Separate multi-band raster into individual bands"""
    return """
SELECT t.*
FROM multiband_rasters,
LATERAL gbx_rst_separatebands(tile) t;
"""


rst_separatebands_sql_example_output = """
+----+-----------------------------------------------------------+-----------------------------------------------------------+-----------------------------------------------------------+
|path|red_band                                                   |green_band                                                 |blue_band                                                  |
+----+-----------------------------------------------------------+-----------------------------------------------------------+-----------------------------------------------------------+
|... |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+----+-----------------------------------------------------------+-----------------------------------------------------------+-----------------------------------------------------------+
"""


# ============================================================================
# Aggregation Functions
# ============================================================================


def rst_combineavg_agg_sql_example():
    """Aggregator for averaging rasters in group by"""
    return """
-- Group by region and average
SELECT
    region,
    gbx_rst_combineavg_agg(tile) as regional_average
FROM rasters
GROUP BY region;
"""


rst_combineavg_agg_sql_example_output = """
# Heavyweight SQL — one tile struct per group:
+------+-----------------------------------------------------------+
|region|regional_average                                           |
+------+-----------------------------------------------------------+
|...   |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+------+-----------------------------------------------------------+

# Lightweight SQL — raster bytes as BINARY (see the note above); wrap with
# gbx_rst_fromcontent(<agg>, 'GTiff') to rebuild a tile:
+------+----------------+
|region|regional_average|
+------+----------------+
|...   |[B@... (BINARY) |
+------+----------------+
"""


def rst_merge_agg_sql_example():
    """Aggregator for merging rasters in group by"""
    return """
SELECT
    scene_id,
    gbx_rst_merge_agg(tile) as merged_scene
FROM satellite_tiles
GROUP BY scene_id;
"""


rst_merge_agg_sql_example_output = """
# Heavyweight SQL — one v2 tile struct per group:
+--------+-----------------------------------------------------------+
|scene_id|merged_scene                                               |
+--------+-----------------------------------------------------------+
|...     |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+--------+-----------------------------------------------------------+

# Lightweight SQL — raster bytes as BINARY (see the note above); wrap with
# gbx_rst_fromcontent(merged_scene, 'GTiff') to rebuild a tile struct:
+--------+---------------+
|scene_id|merged_scene   |
+--------+---------------+
|...     |[B@... (BINARY)|
+--------+---------------+
"""


# ============================================================================
# Web-Mercator Tile Output Functions
# ============================================================================


def rst_to_webmercator_sql_example():
    """Reproject a raster to EPSG:3857 (web mercator)"""
    return """
-- Reproject to web mercator before slippy-map tiling (default bilinear resampling).
SELECT
    path,
    gbx_rst_to_webmercator(tile) as web_tile,
    gbx_rst_srid(gbx_rst_to_webmercator(tile)) as new_srid
FROM rasters;
"""


rst_to_webmercator_sql_example_output = """
+----+-----------------------------------------------------------+--------+
|path|web_tile                                                   |new_srid|
+----+-----------------------------------------------------------+--------+
|... |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|3857    |
+----+-----------------------------------------------------------+--------+
"""


def rst_tilexyz_sql_example():
    """Render a single web-mercator XYZ tile to PNG bytes"""
    return """
-- Render tile (z=10, x=512, y=512) as 256x256 PNG bytes.
-- rescale='auto' (default) rescales non-8-bit imagery by whole-dataset min/max
-- for display contrast; 'none' keeps the raw full-dtype-range mapping; a
-- 'min,max' string sets explicit bounds.
SELECT
    path,
    gbx_rst_tilexyz(tile, 10, 512, 512, 'PNG', 256, 'bilinear', 'auto') as tile_png
FROM rasters;
"""


rst_tilexyz_sql_example_output = """
+----+--------+
|path|tile_png|
+----+--------+
|... |[BINARY]|
+----+--------+
"""


def rst_xyzpyramid_sql_example():
    """Generate one row per (z, x, y) tile across a zoom range"""
    return """
-- Explode a raster into per-tile rows across zoom levels 4..6 (PNG, 256px).
-- Optional trailing rescale arg (default 'auto') controls 8-bit display contrast:
--   gbx_rst_xyzpyramid(tile, 4, 6, 'PNG', 256, 'bilinear', 'auto')
SELECT
    path,
    t.tile.z as z,
    t.tile.x as x,
    t.tile.y as y,
    t.tile.bytes as png_bytes
FROM rasters
LATERAL VIEW gbx_rst_xyzpyramid(tile, 4, 6) AS t;
"""


rst_xyzpyramid_sql_example_output = """
+----+-+-+-+---------+
|path|z|x|y|png_bytes|
+----+-+-+-+---------+
|... |4|5|6|[BINARY] |
+----+-+-+-+---------+
"""


# ============================================================================
# Vector<->Raster Bridge Functions
# ============================================================================


def rst_rasterize_sql_example():
    """Burn a square polygon (WKB) into a 100x100 raster tile."""
    return """
-- WKB hex below is POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)). The output `tile`
-- is a GTiff-backed raster at the given extent and resolution; pixels inside
-- the polygon carry the burn value (42.0), pixels outside are NoData.
SELECT gbx_rst_rasterize(
    unhex('010300000001000000050000000000000000000000000000000000000000000000000024400000000000000000000000000000244000000000000024400000000000000000000000000000244000000000000000000000000000000000'),
    42.0, 0.0, 0.0, 10.0, 10.0, 100, 100, 4326
) AS tile;
"""


rst_rasterize_sql_example_output = """
+-----------------------------------------------------------+
|tile                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(rasterized tile: pixels inside the polygon carry the burn value; outside = NoData)
"""


def rst_rasterize_agg_sql_example():
    """Aggregator: stream geometry/value pairs and produce one tile per group."""
    return """
-- Aggregate per-feature burn values into one rasterized tile per region.
SELECT region_id,
    gbx_rst_rasterize_agg(
        geom_wkb, burn_value,
        bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
        256, 256, 4326
    ) AS tile
FROM features
GROUP BY region_id;
"""


rst_rasterize_agg_sql_example_output = """
# Heavyweight SQL — one v2 tile struct per group:
+---------+-----------------------------------------------------------+
|region_id|tile                                                       |
+---------+-----------------------------------------------------------+
|...      |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+---------+-----------------------------------------------------------+

# Lightweight SQL — raster bytes as BINARY (see the note above); wrap with
# gbx_rst_fromcontent(tile, 'GTiff') to rebuild a tile struct:
+---------+---------------+
|region_id|tile           |
+---------+---------------+
|...      |[B@... (BINARY)|
+---------+---------------+
"""


def rst_polygonize_sql_example():
    """Extract polygons from contiguous-value regions of a freshly-rasterized tile."""
    return """
-- Heavyweight: scalar ARRAY return — call directly; each feature carries the
-- source pixel value as the `value` field. Round-trip: rasterize a polygon then
-- immediately polygonize it.
SELECT gbx_rst_polygonize(
    gbx_rst_rasterize(
        unhex('010300000001000000050000000000000000000000000000000000000000000000000024400000000000000000000000000000244000000000000024400000000000000000000000000000244000000000000000000000000000000000'),
        42.0, 0.0, 0.0, 10.0, 10.0, 100, 100, 4326
    )
) AS features;

-- Lightweight (pyrx): streaming table function — must use LATERAL; one row per
-- contiguous-value region as (geom_wkb, value).
SELECT t.geom_wkb, t.value FROM rasters, LATERAL gbx_rst_polygonize(tile, 1, 4) t;
"""


rst_polygonize_sql_example_output = """
# Heavyweight SQL — one ARRAY of features per row:
+------------------+
|features          |
+------------------+
|[{[BINARY], 42.0}]|
+------------------+

# Lightweight SQL — LATERAL streams one row per region (geom_wkb, value):
+--------+-----+
|geom_wkb|value|
+--------+-----+
|...     |365.0|
+--------+-----+
"""


# ============================================================================
# Terrain Analysis (DEM Processing) - Wave 8a
#
# Seven thin wrappers around gdal.DEMProcessing. Each one takes a single
# input tile and produces a derived tile. Examples below use the `rasters`
# view (load any single-band DEM tile to taste).
# ============================================================================


def rst_slope_sql_example():
    """Compute slope (degrees) from a DEM tile."""
    return """
-- Slope in degrees per pixel (auto-scale from CRS). Use unit='percent' for rise/run.
-- Pass xscale and yscale together to override the horizontal scale per axis.
SELECT gbx_rst_slope(tile, 'degrees', 1.0, 1.0) AS slope FROM dem_rasters;
"""


rst_slope_sql_example_output = """
+-----------------------------------------------------------+
|slope                                                      |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(slope in degrees; auto-scaled from raster CRS units)
"""


def rst_aspect_sql_example():
    """Compute aspect (compass direction of slope) from a DEM tile."""
    return """
-- Aspect in compass degrees (0=N, 90=E, 180=S, 270=W). Flat areas get -9999
-- unless zero_for_flat=true.
SELECT gbx_rst_aspect(tile, false, false) AS aspect FROM dem_rasters;
"""


rst_aspect_sql_example_output = """
+-----------------------------------------------------------+
|aspect                                                     |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(aspect in compass degrees: 0=N, 90=E, 180=S, 270=W)
"""


def rst_hillshade_sql_example():
    """Compute a shaded relief image from a DEM tile."""
    return """
-- 8-bit (0..255) hillshade: NW sun, 45-deg altitude, default z-factor.
SELECT gbx_rst_hillshade(tile, 315.0, 45.0, 1.0) AS hillshade FROM dem_rasters;
"""


rst_hillshade_sql_example_output = """
+-----------------------------------------------------------+
|hillshade                                                  |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(8-bit hillshade: 0..255, NW azimuth 45-degree altitude)
"""


def rst_tri_sql_example():
    """Compute Terrain Ruggedness Index (TRI) from a DEM tile."""
    return """
-- TRI: mean absolute neighbour difference; useful for landscape ecology.
SELECT gbx_rst_tri(tile) AS tri FROM dem_rasters;
"""


rst_tri_sql_example_output = """
+-----------------------------------------------------------+
|tri                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(TRI: mean absolute neighbour difference)
"""


def rst_tpi_sql_example():
    """Compute Topographic Position Index (TPI) from a DEM tile."""
    return """
-- TPI: difference from neighbour-mean; +ve = ridge, -ve = valley.
SELECT gbx_rst_tpi(tile) AS tpi FROM dem_rasters;
"""


rst_tpi_sql_example_output = """
+-----------------------------------------------------------+
|tpi                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(TPI: positive=ridge, negative=valley)
"""


def rst_roughness_sql_example():
    """Compute Roughness (largest neighbour delta) from a DEM tile."""
    return """
-- Roughness: max absolute neighbour difference in a 3x3 window.
SELECT gbx_rst_roughness(tile) AS roughness FROM dem_rasters;
"""


rst_roughness_sql_example_output = """
+-----------------------------------------------------------+
|roughness                                                  |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(roughness: max absolute difference in 3x3 window)
"""


def rst_color_relief_sql_example():
    """Apply a color relief mapping to a DEM tile.

    The color table file is a plain-text gdaldem color file: each line
    ``elevation R G B [A]``. Special values ``nv``, ``default``, ``0%`` and
    ``100%`` are accepted.
    """
    return f"""
-- Map elevation values to RGBA colors via a gdaldem color table.
SELECT gbx_rst_color_relief(tile, '{COLOR_TABLE_PATH}') AS rgba
FROM dem_rasters;
"""


rst_color_relief_sql_example_output = """
+-----------------------------------------------------------+
|rgba                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(4-band RGBA tile mapped via gdaldem color table)
"""


# ============================================================================
# Spectral Indices (Multi-band Satellite Math) - Wave 8b
#
# Five compositions over gbx_rst_mapalgebra that take user-supplied band
# indices, build a per-pixel formula string, and dispatch to gdal_calc for
# evaluation. All return a single-band Float32 GTiff tile.
# ============================================================================


def rst_evi_sql_example():
    """Enhanced Vegetation Index from red / NIR / blue bands.

    EVI = G * (NIR - Red) / (NIR + C1*Red - C2*Blue + L). Defaults follow the
    MODIS canonical coefficients: L=1.0, C1=6.0, C2=7.5, G=2.5.
    Output: single-band Float32 tile.
    """
    return """
SELECT gbx_rst_evi(tile, 1, 2, 3) AS evi FROM multiband_rasters;
"""


rst_evi_sql_example_output = """
+-----------------------------------------------------------+
|evi                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band EVI raster: G*(NIR-Red)/(NIR+C1*Red-C2*Blue+L))
"""


def rst_savi_sql_example():
    """Soil-Adjusted Vegetation Index from red / NIR bands.

    SAVI = (NIR - Red) / (NIR + Red + L) * (1 + L). L=0.5 (default) is a
    balanced soil-vegetation tradeoff; L=0 reduces to NDVI.
    Output: single-band Float32 tile.
    """
    return """
SELECT gbx_rst_savi(tile, 1, 2, 0.5) AS savi FROM multiband_rasters;
"""


rst_savi_sql_example_output = """
+-----------------------------------------------------------+
|savi                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band SAVI raster: (NIR-Red)/(NIR+Red+L)*(1+L))
"""


def rst_ndwi_sql_example():
    """Normalized Difference Water Index from green / NIR bands.

    NDWI (McFeeters 1996) = (Green - NIR) / (Green + NIR). Positive values
    typically indicate open water. Uses band 3 (green) and band 2 (NIR).
    Output: single-band Float32 tile.
    """
    return """
SELECT gbx_rst_ndwi(tile, 3, 2) AS ndwi FROM multiband_rasters;
"""


rst_ndwi_sql_example_output = """
+-----------------------------------------------------------+
|ndwi                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NDWI raster: (Green-NIR)/(Green+NIR))
"""


def rst_nbr_sql_example():
    """Normalized Burn Ratio from NIR / SWIR bands.

    NBR = (NIR - SWIR) / (NIR + SWIR). Difference of pre-fire and post-fire
    NBR (dNBR) is the canonical burn-severity index. Uses band 2 (NIR) and band 3 (SWIR proxy).
    Output: single-band Float32 tile.
    """
    return """
SELECT gbx_rst_nbr(tile, 2, 3) AS nbr FROM multiband_rasters;
"""


rst_nbr_sql_example_output = """
+-----------------------------------------------------------+
|nbr                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band NBR raster: (NIR-SWIR)/(NIR+SWIR))
"""


def rst_index_sql_example():
    """Generic dispatcher for named spectral indices (NDVI shown).

    Pick a built-in formula by name and wire bands via a MAP<STRING, INT>.
    Built-ins: ndvi, gndvi, msavi, ndvi_re, ndmi, ndsi.
    Output: single-band Float32 tile.
    """
    return """
SELECT gbx_rst_index(tile, 'ndvi', map('red', 1, 'nir', 2)) AS ndvi FROM multiband_rasters;
"""


rst_index_sql_example_output = """
+-----------------------------------------------------------+
|ndvi                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(single-band index raster computed from named formula)
"""


def rst_combineavg_sql_example():
    """Combine multiple aligned tiles via per-pixel NoData-aware mean.

    Input: ARRAY of aligned tiles. Output: single-band Float32 tile.
    """
    return """
SELECT gbx_rst_combineavg(array(tile)) AS combined FROM multiband_rasters;
"""


rst_combineavg_sql_example_output = """
+-----------------------------------------------------------+
|combined                                                   |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_derivedband_sql_example():
    """Apply a user-provided Python pixel-function to the tile's bands.

    The function source (a string literal following GDAL's VRT pixel-function
    signature) and its callable name are passed inline. Output: single-band tile.
    """
    return """
SELECT gbx_rst_derivedband(tile, 'def double(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):\\n    out_ar[:] = in_ar[0] * 2\\n', 'double') AS result FROM multiband_rasters;
"""


rst_derivedband_sql_example_output = """
+-----------------------------------------------------------+
|result                                                     |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_mapalgebra_sql_example():
    """NDVI from two bands of a SINGLE multiband raster — no need to decompose it.

    The spec is a gdal_calc JSON envelope (the same shape on both tiers). The
    per-variable keys select a raster (`*_index`, 0-based into the array) and a
    1-based band (`*_band`): A and B both read raster 0 (the one tile), A = band
    2 (NIR), B = band 1 (Red), giving NDVI = (NIR-Red)/(NIR+Red). Direct
    equivalent of `gdal_calc -A in --A_band=2 -B in --B_band=1
    --calc="(A-B)/(A+B)"`.
    """
    return """
SELECT gbx_rst_mapalgebra(
           array(tile),
           '{"calc": "(A - B) / (A + B)", "A_index": 0, "B_index": 0, "A_band": 2, "B_band": 1}'
       ) AS ndvi
FROM multiband_rasters;
"""


rst_mapalgebra_sql_example_output = """
+-----------------------------------------------------------+
|ndvi                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_merge_sql_example():
    """Mosaic (merge) multiple aligned tiles into one spanning their union.

    Input: ARRAY of aligned tiles (same grid / CRS).
    Output: single merged tile covering the union extent.
    """
    return """
SELECT gbx_rst_merge(array(tile)) AS merged FROM multiband_rasters;
"""


rst_merge_sql_example_output = """
+-----------------------------------------------------------+
|merged                                                     |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_resample_sql_example():
    """Resample a tile by a multiplicative factor."""
    return """
-- Upsample 2x with bilinear interpolation. Output dims = source dims * 2.
SELECT gbx_rst_resample(tile, 2.0, 'bilinear') AS upsampled FROM rasters;
"""


rst_resample_sql_example_output = """
+-----------------------------------------------------------+
|upsampled                                                  |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_resample_to_size_sql_example():
    """Resample a tile to an explicit width x height in pixels."""
    return """
-- Force a 512 x 512 tile, near-neighbour for categorical rasters.
SELECT gbx_rst_resample_to_size(tile, 512, 512, 'near') AS sized FROM rasters;
"""


rst_resample_to_size_sql_example_output = """
+-----------------------------------------------------------+
|sized                                                      |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_resample_to_res_sql_example():
    """Resample a tile to an explicit ground resolution in CRS units."""
    return """
-- Downsample to a 100 m grid (metric CRS). 'average' weights cells by area.
SELECT gbx_rst_resample_to_res(tile, 100.0, 100.0, 'average') AS coarse
FROM rasters;
"""


rst_resample_to_res_sql_example_output = """
+-----------------------------------------------------------+
|coarse                                                     |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_gridfrompoints_sql_example():
    """IDW interpolation - arrays of points / values in a single row."""
    return """
-- IDW (power=2, max_points=12) from arrays of point WKB and values.
-- Output is a 256 x 256 Float64 GTiff covering the requested extent.
SELECT gbx_rst_gridfrompoints(
    points_wkb_array, values_array,
    0.0, 0.0, 1000.0, 1000.0,
    256, 256, 32633
) AS idw
FROM point_clouds;
"""


rst_gridfrompoints_sql_example_output = """
+-----------------------------------------------------------+
|idw                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(IDW-interpolated tile over specified extent)
"""


def rst_gridfrompoints_agg_sql_example():
    """IDW interpolation aggregator - one point/value per row, grouped by extent key."""
    return """
-- Aggregate per-station observations into one IDW tile per region.
SELECT region_id,
    gbx_rst_gridfrompoints_agg(
        station_wkb, observation,
        bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
        256, 256, 32633
    ) AS idw
FROM observations
GROUP BY region_id;
"""


rst_gridfrompoints_agg_sql_example_output = """
# Heavyweight SQL — one v2 tile struct per group:
+---------+-----------------------------------------------------------+
|region_id|idw                                                        |
+---------+-----------------------------------------------------------+
|...      |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+---------+-----------------------------------------------------------+

# Lightweight SQL — raster bytes as BINARY (see the note above); wrap with
# gbx_rst_fromcontent(idw, 'GTiff') to rebuild a tile struct:
+---------+---------------+
|region_id|idw            |
+---------+---------------+
|...      |[B@... (BINARY)|
+---------+---------------+
"""


def rst_fillnodata_sql_example():
    """Interpolate NoData pixels from valid neighbours via gdal.FillNodata."""
    return """
-- Fill NoData holes searching up to 100 pixels in each direction.
SELECT gbx_rst_fillnodata(tile, 100.0, 0) AS filled FROM rasters;
"""


rst_fillnodata_sql_example_output = """
+-----------------------------------------------------------+
|filled                                                     |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_sample_sql_example():
    """Sample raster pixel values at a POINT geometry (one Double per band)."""
    return """
-- Sample the DEM at a point in its native CRS (EPSG:32618). Tagging the point
-- with an SRID (EWKT `SRID=32618;...`) lets gbx_rst_sample land it correctly.
SELECT gbx_rst_sample(tile, 'SRID=32618;POINT(500320 4500320)') AS values FROM dem_rasters;
"""


rst_sample_sql_example_output = """
+-------+
|values |
+-------+
|[302.0]|
+-------+
(array of sampled values, one per band)
"""


def rst_setsrid_sql_example():
    """Re-stamp the raster's spatial-reference header to the given EPSG code."""
    return """
-- Tag the tile as EPSG:4326 without warping pixels.
-- Use rst_transform if you actually need a reprojection.
SELECT gbx_rst_setsrid(tile, 4326) AS tagged FROM rasters;
"""


rst_setsrid_sql_example_output = """
+-----------------------------------------------------------+
|tagged                                                     |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_setcrs_sql_example():
    """Re-stamp the raster's CRS header from a CRS string (no reprojection)."""
    return """
-- Relabel the tile's CRS to Web Mercator without warping pixels.
-- Accepts authority strings (EPSG:/ESRI:), WKT, or PROJ4; an int-castable
-- string ('4326') behaves like rst_setsrid(tile, 4326).
-- Use rst_transformcrs if you actually need a reprojection.
SELECT gbx_rst_setcrs(tile, 'EPSG:3857') AS tagged FROM rasters;
"""


rst_setcrs_sql_example_output = """
+-----------------------------------------------------------+
|tagged                                                     |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_histogram_sql_example():
    """Per-band pixel histogram from a multiband raster.

    Uses the multiband fixture (rgb_nir_small.tif, 3 bands) so the histogram
    has entries for each band.
    """
    return """
-- multiband_rasters view is from rgb_nir_small.tif (3 bands: red, NIR, green)
SELECT gbx_rst_histogram(tile) AS histogram FROM multiband_rasters;
"""


rst_histogram_sql_example_output = """
+-------------------------------------------------+
|histogram                                        |
+-------------------------------------------------+
|{band_1 -> [1, 0, 0, ...], band_2 -> [1, 0, 1,...|
+-------------------------------------------------+
"""


def rst_threshold_sql_example():
    """Binarise a raster: (pixel > value) -> 1, else 0."""
    return """
-- Mark all pixels above 100 m as 1, others as 0.
SELECT gbx_rst_threshold(tile, '>', 100.0) AS mask FROM rasters;
"""


rst_threshold_sql_example_output = """
+-----------------------------------------------------------+
|mask                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_buildoverviews_sql_example():
    """Build internal overviews (image pyramid) on a raster tile."""
    return """
-- Add 2x / 4x overviews to the tile via the 'average' resampling.
SELECT gbx_rst_buildoverviews(tile, array(2, 4), 'average') AS withovr
FROM rasters;
"""


rst_buildoverviews_sql_example_output = """
+-----------------------------------------------------------+
|withovr                                                    |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_band_sql_example():
    """Extract a single band as a new single-band tile."""
    return """
-- Pull band 1 (1-based) as a fresh single-band tile.
SELECT gbx_rst_band(tile, 1) AS b1 FROM rasters;
"""


rst_band_sql_example_output = """
+-----------------------------------------------------------+
|b1                                                         |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_cog_convert_sql_example():
    """Re-layout a tile as a Cloud Optimized GeoTIFF for HTTP range serving."""
    return """
-- Convert to COG with ZSTD compression, 512-pixel blocks, AVERAGE overviews.
-- The lightweight tier also accepts 'AUTO' (size-adaptive ZSTD + dtype
-- predictor, the default); pass 'DEFLATE' for a portable hand-off file.
-- See the Materialized Compression page.
SELECT gbx_rst_cog_convert(tile, 'ZSTD', 512, 'AVERAGE') AS cog
FROM rasters;
"""


rst_cog_convert_sql_example_output = """
+-----------------------------------------------------------+
|cog                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_proximity_sql_example():
    """Compute per-pixel distance to the nearest non-NoData (or target-value) source pixel."""
    return """
-- Distance in pixels to any non-NoData pixel; cap distances at 100 pixels.
SELECT gbx_rst_proximity(tile, '', 'PIXEL', cast(100.0 as double)) AS dist
FROM rasters;
"""


rst_proximity_sql_example_output = """
+-----------------------------------------------------------+
|dist                                                       |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(distance in pixels to nearest non-NoData pixel, capped at 100)
"""


def rst_contour_sql_example():
    """Generate contour LineStrings at an equal interval from an elevation tile."""
    return """
-- Equal-interval contours every 50 m. Pass array() of fixed levels to override.
SELECT gbx_rst_contour(tile, array(), 50.0, 0.0, 'elev') AS contours
FROM dem_rasters;
"""


rst_contour_sql_example_output = """
+-----------------------------------------------------------------------------------------------------------------+
|contours                                                                                                         |
+-----------------------------------------------------------------------------------------------------------------+
|[{[BINARY], 50.0}, {[BINARY], 100.0}, {[BINARY], 150.0}, {[BINARY], 200.0}, {[BINARY], 250.0}, {[BINARY], 300.0}]|
+-----------------------------------------------------------------------------------------------------------------+
(array of contour features: LineString geometry + elevation)
"""


def rst_viewshed_sql_example():
    """Binary viewshed mask from a DEM and an observer POINT (coords in raster CRS)."""
    return """
-- Visibility from observer at (-73.5 40.5), eye 100 m, target 1.6 m, cap 5000 m.
SELECT gbx_rst_viewshed(tile, 'POINT(-73.5 40.5)', 100.0, 1.6, 5000.0) AS vs
FROM dem_rasters;
"""


rst_viewshed_sql_example_output = """
+-----------------------------------------------------------+
|vs                                                         |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
(binary viewshed: 1=visible, 0=not visible)
"""


def rst_dtmfromgeoms_sql_example():
    """DTM via Delaunay-TIN interpolation from Z-valued points (+ optional breaklines)."""
    return """
-- TIN interpolation from arrays of Z-valued point WKB and breakline WKB.
-- Output is a 100 x 100 Float64 GTiff over the extent. For N-metre cells set
-- width_px = round((xmax-xmin)/N): here a 1000 m extent at 10 m cells -> 100 px.
SELECT gbx_rst_dtmfromgeoms(
    points_wkb_array, breaklines_wkb_array,
    0.0, 0.01,
    0.0, 0.0, 1000.0, 1000.0,
    100, 100, 32633
) AS dtm
FROM survey_points;
"""


rst_dtmfromgeoms_sql_example_output = """
+-----------------------------------------------------------+
|dtm                                                        |
+-----------------------------------------------------------+
|{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+-----------------------------------------------------------+
"""


def rst_dtmfromgeoms_agg_sql_example():
    """DTM aggregator - one Z-valued point per row, grouped by extent key."""
    return """
-- Stream survey points per region into one TIN DTM tile. Breaklines are a
-- per-group constant array; for 10 m cells over a 1000 m extent use 100 px.
SELECT region_id,
    gbx_rst_dtmfromgeoms_agg(
        point_wkb, breaklines_wkb_array,
        0.0, 0.01,
        bbox_xmin, bbox_ymin, bbox_xmax, bbox_ymax,
        100, 100, 32633
    ) AS dtm
FROM survey_points
GROUP BY region_id;
"""


rst_dtmfromgeoms_agg_sql_example_output = """
# Heavyweight SQL — one v2 tile struct per group:
+---------+-----------------------------------------------------------+
|region_id|dtm                                                        |
+---------+-----------------------------------------------------------+
|...      |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+---------+-----------------------------------------------------------+

# Lightweight SQL — raster bytes as BINARY (see the note above); wrap with
# gbx_rst_fromcontent(dtm, 'GTiff') to rebuild a tile struct:
+---------+---------------+
|region_id|dtm            |
+---------+---------------+
|...      |[B@... (BINARY)|
+---------+---------------+
"""


# ============================================================================
# H3 Cell Rasterizer Functions
# ============================================================================


def rst_h3_rasterize_agg_sql_example():
    """Aggregator: rasterize a group of H3 cells into one tile (pixel-centroid burn)."""
    return """
-- Rasterize H3 cells into one raster tile per region. Each cell's value is
-- burned at the cell centroid pixel.
SELECT region_id,
    gbx_rst_h3_rasterize_agg(
        cellid, burn_value,
        4326, cast(null as double),
        cast(null as double), cast(null as double),
        cast(null as double), cast(null as double),
        cast(null as int), cast(null as int),
        'centroids', cast(1 as int)
    ) AS tile
FROM h3_cell_values
GROUP BY region_id;
"""


rst_h3_rasterize_agg_sql_example_output = """
# Heavyweight SQL — one v2 tile struct per group:
+---------+-----------------------------------------------------------+
|region_id|tile                                                       |
+---------+-----------------------------------------------------------+
|...      |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+---------+-----------------------------------------------------------+

# Lightweight SQL — raster bytes as BINARY (see the note above); wrap with
# gbx_rst_fromcontent(tile, 'GTiff') to rebuild a tile struct:
+---------+---------------+
|region_id|tile           |
+---------+---------------+
|...      |[B@... (BINARY)|
+---------+---------------+
"""


def rst_quadbin_rasterize_agg_sql_example():
    """Aggregator: rasterize a group of CARTO quadbin v0 cells into one tile (pixel-centroid burn)."""
    return """
-- Rasterize quadbin cells into one raster tile per region. Each cell's value
-- is burned at the cell centroid pixel; the extent auto-derives from the cell
-- set (null canvas args). cellid is BIGINT.
SELECT region_id,
    gbx_rst_quadbin_rasterize_agg(
        cellid, burn_value,
        4326, cast(null as double),
        cast(null as double), cast(null as double),
        cast(null as double), cast(null as double),
        cast(null as int), cast(null as int),
        'centroids', cast(0 as int)
    ) AS tile
FROM quadbin_cell_values
GROUP BY region_id;
"""


rst_quadbin_rasterize_agg_sql_example_output = """
# Heavyweight SQL — one v2 tile struct per group:
+---------+-----------------------------------------------------------+
|region_id|tile                                                       |
+---------+-----------------------------------------------------------+
|...      |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+---------+-----------------------------------------------------------+

# Lightweight SQL — raster bytes as BINARY (see the note above); wrap with
# gbx_rst_fromcontent(tile, 'GTiff') to rebuild a tile struct:
+---------+---------------+
|region_id|tile           |
+---------+---------------+
|...      |[B@... (BINARY)|
+---------+---------------+
"""


def rst_bng_rasterize_agg_sql_example():
    """Aggregator: rasterize a group of British National Grid cells into one tile (pixel-centroid burn)."""
    return """
-- Rasterize BNG cells into one raster tile per region. cellid is a STRING
-- (e.g. 'TQ38SW'); the srid argument is a no-op (BNG always forces EPSG:27700,
-- pass 27700 for clarity). The extent auto-derives from the cell set (null
-- canvas args).
SELECT region_id,
    gbx_rst_bng_rasterize_agg(
        cellid, burn_value,
        27700, cast(null as double),
        cast(null as double), cast(null as double),
        cast(null as double), cast(null as double),
        cast(null as int), cast(null as int),
        'centroids', cast(0 as int)
    ) AS tile
FROM bng_cell_values
GROUP BY region_id;
"""


rst_bng_rasterize_agg_sql_example_output = """
# Heavyweight SQL — one v2 tile struct per group:
+---------+-----------------------------------------------------------+
|region_id|tile                                                       |
+---------+-----------------------------------------------------------+
|...      |{0, <raster bytes>, <virtual path>, {driver -> GTiff, ...}}|
+---------+-----------------------------------------------------------+

# Lightweight SQL — raster bytes as BINARY (see the note above); wrap with
# gbx_rst_fromcontent(tile, 'GTiff') to rebuild a tile struct:
+---------+---------------+
|region_id|tile           |
+---------+---------------+
|...      |[B@... (BINARY)|
+---------+---------------+
"""


def h3_cell_bbox_sql_example():
    """Get bounding box of H3 cells in a given CRS."""
    return """
-- Bounding box (STRUCT<xmin, ymin, xmax, ymax>) for each H3 cell in EPSG:4326.
-- Uses 'centroids' mode with no k-ring padding (kring_pad=0).
SELECT
    cellid,
    gbx_h3_cell_bbox(cellid, 4326, 'centroids', 0) AS bbox
FROM (
    VALUES
        (617733151020810239),
        (617733151085035519),
        (617733151021334527)
) AS t(cellid);
"""


h3_cell_bbox_sql_example_output = """
+------------------+------------------------------+
|cellid            |bbox                          |
+------------------+------------------------------+
|617733151020810239|{-74.02, 40.70, -74.01, 40.71}|
+------------------+------------------------------+
(STRUCT<xmin, ymin, xmax, ymax> per H3 cell, in EPSG:4326)
"""
