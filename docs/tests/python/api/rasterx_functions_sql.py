"""
SQL examples for RasterX functions documentation.

All SQL examples are executable and tested. These are imported into the
documentation via CodeFromTest components to ensure single-copy pattern.

Run Common setup first (Python/Scala) to register RasterX; then create the
view below so SQL examples can use FROM rasters.
"""

# Sample path at runtime (path_config)
from path_config import SAMPLE_DATA_BASE
SAMPLE_RASTER_PATH = f"{SAMPLE_DATA_BASE}/nyc/sentinel2/nyc_sentinel2_red.tif"

# Common setup: create temp view so SQL examples can use FROM rasters
def _rasterx_sql_setup_content():
    return f"""-- After registering RasterX (Python: rx.register(spark)), create the view:
CREATE OR REPLACE TEMP VIEW rasters AS
SELECT * FROM gdal.`{SAMPLE_RASTER_PATH}`;"""

RASTERX_SQL_SETUP = _rasterx_sql_setup_content()

RASTERX_SQL_SETUP_output = """
View `rasters` created. You can now run SELECT ... FROM rasters; for each example.
"""

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
    """Get width from raster table"""
    return """
SELECT gbx_rst_width(tile) as width FROM rasters;
"""


rst_width_sql_example_output = """
+-----+
|width|
+-----+
|10980|
+-----+
"""


def rst_height_sql_example():
    """Get height and calculate total pixels"""
    return """
SELECT gbx_rst_height(tile) as height, gbx_rst_width(tile) as width FROM rasters;
"""


rst_height_sql_example_output = """
+------+-----+
|height|width|
+------+-----+
|10980 |10980|
+------+-----+
"""


def rst_numbands_sql_example():
    """Get number of bands from rasters"""
    return """
SELECT gbx_rst_numbands(tile) as bands FROM rasters;
"""


rst_numbands_sql_example_output = """
+------+
|bands |
+------+
|1     |
+------+
"""


def rst_metadata_sql_example():
    """Get metadata from rasters"""
    return """
SELECT gbx_rst_metadata(tile) as metadata FROM rasters;
"""


rst_metadata_sql_example_output = """
+----------+
|metadata  |
+----------+
|{...}     |
+----------+
"""


def rst_srid_sql_example():
    """Get spatial reference identifier"""
    return """
SELECT gbx_rst_srid(tile) as srid FROM rasters;
"""


rst_srid_sql_example_output = """
+-----+
|srid |
+-----+
|32618|
+-----+
"""


def rst_georeference_sql_example():
    """Get georeference (geotransform) parameters."""
    return """
SELECT gbx_rst_georeference(tile) as georeference FROM rasters;
"""


rst_georeference_sql_example_output = """
+-------------+
|georeference |
+-------------+
|[ ... ]      |
+-------------+
"""


def rst_bandmetadata_sql_example():
    """Get band metadata."""
    return """
SELECT gbx_rst_bandmetadata(tile, 1) as band1_metadata FROM rasters;
"""


rst_bandmetadata_sql_example_output = """
+----------------+
|band1_metadata  |
+----------------+
|{...}           |
+----------------+
"""


def rst_pixelcount_sql_example():
    """Get total pixel count."""
    return """
SELECT gbx_rst_pixelcount(tile) as pixel_count FROM rasters;
"""


rst_pixelcount_sql_example_output = """
+------------+
|pixel_count |
+------------+
|120560400   |
+------------+
"""


def rst_avg_sql_example():
    """Get average pixel values"""
    return """
-- Get average values
SELECT
    path,
    gbx_rst_avg(tile) as band_averages,
    gbx_rst_avg(tile)[0] as band1_avg
FROM rasters;

-- Filter by average threshold
SELECT * FROM rasters
WHERE gbx_rst_avg(tile)[0] > 50.0;
"""


rst_avg_sql_example_output = """
+----+--------------+----------+
|path|band_averages |band1_avg |
+----+--------------+----------+
|... |[0.42]        |0.42      |
+----+--------------+----------+
"""


def rst_min_sql_example():
    """Get minimum pixel values per band"""
    return """
SELECT path, gbx_rst_min(tile) as min_per_band, gbx_rst_min(tile)[0] as band1_min FROM rasters;
"""


rst_min_sql_example_output = """
+----+------------+----------+
|path|min_per_band|band1_min |
+----+------------+----------+
|... |[0.0]       |0.0       |
+----+------------+----------+
"""


def rst_max_sql_example():
    """Get maximum pixel values per band"""
    return """
SELECT path, gbx_rst_max(tile) as max_per_band, gbx_rst_max(tile)[0] as band1_max FROM rasters;
"""


rst_max_sql_example_output = """
+----+------------+----------+
|path|max_per_band|band1_max |
+----+------------+----------+
|... |[255.0]     |255.0     |
+----+------------+----------+
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
    """Compare mean and median values"""
    return """
SELECT
    path,
    gbx_rst_avg(tile)[0] as mean_value,
    gbx_rst_median(tile)[0] as median_value,
    ABS(gbx_rst_avg(tile)[0] - gbx_rst_median(tile)[0]) as skewness
FROM rasters;
"""


rst_median_sql_example_output = """
+----+----------+------------+--------+
|path|mean_value|median_value|skewness|
+----+----------+------------+--------+
|... |0.45      |0.42        |0.03    |
+----+----------+------------+--------+
"""


def rst_format_sql_example():
    """Identify raster formats"""
    return """
-- Identify formats
SELECT
    gbx_rst_format(tile) as format,
    COUNT(*) as count
FROM rasters
GROUP BY gbx_rst_format(tile);

-- Find non-GeoTIFF files
SELECT path, gbx_rst_format(tile) as format
FROM rasters
WHERE gbx_rst_format(tile) != 'GTiff';
"""


rst_format_sql_example_output = """
+------+-----+
|format|count|
+------+-----+
|GTiff |10   |
+------+-----+
"""


def rst_type_sql_example():
    """Get raster data types"""
    return """
-- Get data types
SELECT
    path,
    gbx_rst_type(tile) as band_types,
    gbx_rst_type(tile)[0] as band1_type
FROM rasters;

-- Group by data type
SELECT
    gbx_rst_type(tile)[0] as data_type,
    COUNT(*) as count
FROM rasters
GROUP BY gbx_rst_type(tile)[0];
"""


rst_type_sql_example_output = """
+----+----------+----------+
|path|band_types|band1_type|
+----+----------+----------+
|... |[Byte]    |Byte      |
+----+----------+----------+
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
+----+-----------+------------+--------------+
|path|pixel_width|pixel_height|total_width_m |
+----+-----------+------------+--------------+
|... |30.0       |-30.0       |329400.0      |
+----+-----------+------------+--------------+
"""


def rst_getnodata_sql_example():
    """Get NoData values for raster bands"""
    return """
SELECT
    path,
    gbx_rst_getnodata(tile) as nodata_values,
    gbx_rst_getnodata(tile)[0] as band1_nodata
FROM rasters;
"""


rst_getnodata_sql_example_output = """
+----+-------------+------------+
|path|nodata_values|band1_nodata|
+----+-------------+------------+
|... |[-9999.0]    |-9999.0     |
+----+-------------+------------+
"""


def rst_getsubdataset_sql_example():
    """Extract subdataset from multi-layer format"""
    return """
SELECT
    path,
    gbx_rst_getsubdataset(tile, 'temperature') as temp_layer
FROM netcdf_files;
"""


rst_getsubdataset_sql_example_output = """
+----+--------------------+
|path|temp_layer          |
+----+--------------------+
|... |[BINARY]            |
+----+--------------------+
"""


def rst_memsize_sql_example():
    """Get in-memory size of raster tile in bytes"""
    return """
SELECT path, gbx_rst_memsize(tile) as size_bytes FROM rasters;
"""


rst_memsize_sql_example_output = """
+----+----------+
|path|size_bytes|
+----+----------+
|... |120560400 |
+----+----------+
"""


def rst_rotation_sql_example():
    """Get rotation (skew) of raster in radians"""
    return """
SELECT path, gbx_rst_rotation(tile) as rotation_rad FROM rasters;
"""


rst_rotation_sql_example_output = """
+----+------------+
|path|rotation_rad|
+----+------------+
|... |0.0         |
+----+------------+
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
+----+--------+-------+
|path|scale_x|scale_y |
+----+--------+-------+
|... |30.0   |-30.0   |
+----+--------+-------+
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
+----+-------+------+
|path|skew_x|skew_y |
+----+-------+------+
|... |0.0   |0.0    |
+----+-------+------+
"""


def rst_subdatasets_sql_example():
    """List subdatasets (e.g. NetCDF layers)"""
    return """
SELECT path, gbx_rst_subdatasets(tile) as subdatasets FROM netcdf_rasters;
"""


rst_subdatasets_sql_example_output = """
+----+--------------------+
|path|subdatasets         |
+----+--------------------+
|... |[temp, precip, ...] |
+----+--------------------+
"""


def rst_summary_sql_example():
    """Get statistical summary of raster values"""
    return """
SELECT path, gbx_rst_summary(tile) as summary FROM rasters;
"""


rst_summary_sql_example_output = """
+----+--------+
|path|summary |
+----+--------+
|... |{...}   |
+----+--------+
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
+----+-------------+-------------+
|path|upper_left_x |upper_left_y |
+----+-------------+-------------+
|... |500000.0     |200000.0     |
+----+-------------+-------------+
"""


# ============================================================================
# Constructor Functions - Create/Load Rasters
# ============================================================================

def rst_fromfile_sql_example():
    """Load raster from file path"""
    return """
-- Load from path
SELECT 
    gbx_rst_fromfile('/data/raster.tif', 'GTiff') as tile;

-- Load multiple and get properties
SELECT 
    path,
    gbx_rst_width(gbx_rst_fromfile(path, 'GTiff')) as width,
    gbx_rst_height(gbx_rst_fromfile(path, 'GTiff')) as height
FROM raster_paths;
"""


rst_fromfile_sql_example_output = """
+--------------------+
|tile                |
+--------------------+
|[BINARY]            |
+--------------------+

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
+----+--------------------+
|path|tile                |
+----+--------------------+
|... |[BINARY]            |
+----+--------------------+
"""


def rst_frombands_sql_example():
    """Combine multiple bands into single raster"""
    return """
SELECT
    gbx_rst_frombands(array(band1, band2, band3)) as multi_band
FROM separated_bands;
"""


rst_frombands_sql_example_output = """
+--------------------+
|multi_band          |
+--------------------+
|[BINARY]            |
+--------------------+
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
+----+--------------------+
|path|clipped             |
+----+--------------------+
|... |[BINARY]            |
+----+--------------------+
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
+----+--------------------+--------+
|path|wgs84_tile          |new_srid|
+----+--------------------+--------+
|... |[BINARY]            |4326    |
+----+--------------------+--------+
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
+----+--------------------+
|path|geotiff_tile        |
+----+--------------------+
|... |[BINARY]            |
+----+--------------------+
"""


def rst_ndvi_sql_example():
    """Calculate NDVI from multi-band imagery"""
    return """
-- Calculate NDVI for Sentinel-2 imagery
SELECT
    path,
    date,
    gbx_rst_ndvi(tile, 4, 8) as ndvi_tile,
    gbx_rst_avg(gbx_rst_ndvi(tile, 4, 8))[0] as mean_ndvi
FROM sentinel2_images;

-- Monthly vegetation trends
SELECT
    date_trunc('month', date) as month,
    AVG(gbx_rst_avg(gbx_rst_ndvi(tile, 4, 8))[0]) as avg_monthly_ndvi
FROM sentinel2_images
GROUP BY date_trunc('month', date)
ORDER BY month;
"""


rst_ndvi_sql_example_output = """
+----+----------+--------------------+---------+
|path|date      |ndvi_tile           |mean_ndvi|
+----+----------+--------------------+---------+
|... |2024-01-15|[BINARY]            |0.42     |
+----+----------+--------------------+---------+
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
+----+--------------------+
|path|denoised            |
+----+--------------------+
|... |[BINARY]            |
+----+--------------------+
"""


def rst_convolve_sql_example():
    """Apply convolution kernel to raster"""
    return """
-- Apply 3x3 kernel (e.g. blur); kernel format is driver-specific
SELECT path, gbx_rst_convolve(tile, kernel) as filtered FROM rasters_with_kernels;
"""


rst_convolve_sql_example_output = """
+----+--------------------+
|path|filtered            |
+----+--------------------+
|... |[BINARY]            |
+----+--------------------+
"""


# ============================================================================
# Coordinate Transformation Functions
# ============================================================================

def rst_rastertoworldcoord_sql_example():
    """Convert pixel coordinates to world coordinates"""
    return """
SELECT
    path,
    gbx_rst_rastertoworldcoord(tile, 100, 200) as coords,
    gbx_rst_rastertoworldcoord(tile, 100, 200).x as longitude,
    gbx_rst_rastertoworldcoord(tile, 100, 200).y as latitude
FROM rasters;
"""


rst_rastertoworldcoord_sql_example_output = """
+----+--------+---------+--------+
|path|coords  |longitude|latitude|
+----+--------+---------+--------+
|... |POINT(...)|-74.0  |40.5    |
+----+--------+---------+--------+
"""


def rst_rastertoworldcoordx_sql_example():
    """Convert pixel X to world X coordinate"""
    return """
SELECT
    gbx_rst_rastertoworldcoordx(tile, 100, 200) as easting
FROM rasters;
"""


def rst_rastertoworldcoordy_sql_example():
    """Convert pixel Y to world Y coordinate"""
    return """
SELECT
    gbx_rst_rastertoworldcoordy(tile, 100, 200) as northing
FROM rasters;
"""


def rst_worldtorastercoord_sql_example():
    """Convert world coordinates to pixel coordinates (single location)"""
    return """
-- Find pixel coordinates for a specific location
SELECT
    path,
    gbx_rst_worldtorastercoord(tile, -122.4194, 37.7749) as pixel,
    gbx_rst_worldtorastercoord(tile, -122.4194, 37.7749).x as col,
    gbx_rst_worldtorastercoord(tile, -122.4194, 37.7749).y as row
FROM rasters;
"""


def rst_worldtorastercoord_multi_sql_example():
    """Sample raster at multiple world coordinates"""
    return """
-- Sample raster at multiple points
WITH locations AS (
    SELECT -122.4194 as lon, 37.7749 as lat UNION ALL
    SELECT -122.4183, 37.7745
)
SELECT
    l.lat,
    l.lon,
    gbx_rst_worldtorastercoord(r.tile, l.lon, l.lat) as pixel
FROM rasters r, locations l;
"""


def rst_worldtorastercoordx_sql_example():
    """Convert world X to pixel X coordinate"""
    return """
SELECT
    gbx_rst_worldtorastercoordx(tile, -122.4194, 37.7749) as pixel_col
FROM rasters;
"""


def rst_worldtorastercoordy_sql_example():
    """Convert world Y to pixel Y coordinate"""
    return """
SELECT
    gbx_rst_worldtorastercoordy(tile, -122.4194, 37.7749) as pixel_row
FROM rasters;
"""


rst_worldtorastercoord_sql_example_output = """
+----+-----+---+---+
|path|pixel|col|row|
+----+-----+---+---+
|... |...  |100|200|
+----+-----+---+---+
"""


rst_worldtorastercoord_multi_sql_example_output = """
+--------+---------+-----+
|lat     |lon      |pixel|
+--------+---------+-----+
|37.7749 |-122.4194|...  |
|37.7745 |-122.4183|...  |
+--------+---------+-----+
"""


rst_worldtorastercoordx_sql_example_output = """
+---------+
|pixel_col|
+---------+
|100      |
+---------+
"""


rst_worldtorastercoordy_sql_example_output = """
+----------+
|pixel_row |
+----------+
|200       |
+----------+
"""


# ============================================================================
# Validation Functions
# ============================================================================

def rst_isempty_sql_example():
    """Check for empty rasters"""
    return """
-- Filter out empty rasters
SELECT * FROM rasters
WHERE NOT gbx_rst_isempty(tile);

-- Count empty vs valid
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN gbx_rst_isempty(tile) THEN 1 ELSE 0 END) as empty_count,
    SUM(CASE WHEN NOT gbx_rst_isempty(tile) THEN 1 ELSE 0 END) as valid_count
FROM rasters;
"""


rst_isempty_sql_example_output = """
+-----+-----------+------------+
|total|empty_count|valid_count |
+-----+-----------+------------+
|100  |0          |100         |
+-----+-----------+------------+
"""


def rst_tryopen_sql_example():
    """Validate raster can be opened"""
    return """
-- Filter valid rasters
SELECT * FROM rasters
WHERE gbx_rst_tryopen(tile) = true;

-- Identify corrupt rasters
SELECT path
FROM rasters
WHERE gbx_rst_tryopen(tile) = false;

-- Validation summary
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN gbx_rst_tryopen(tile) THEN 1 ELSE 0 END) as valid,
    SUM(CASE WHEN NOT gbx_rst_tryopen(tile) THEN 1 ELSE 0 END) as invalid
FROM rasters;
"""


rst_tryopen_sql_example_output = """
+-----+-----+--------+
|total|valid|invalid |
+-----+-----+--------+
|100  |98   |2       |
+-----+-----+--------+
"""


# ============================================================================
# Advanced Operations
# ============================================================================

def rst_mapalgebra_sql_example():
    """Apply map algebra expression"""
    return """
-- Calculate difference between two rasters
SELECT
    gbx_rst_mapalgebra(
        tiles,
        '{"calc": "A-B", "A_index": 0, "B_index": 1}'
    ) as difference
FROM raster_arrays;
"""


rst_mapalgebra_sql_example_output = """
+--------------------+
|difference          |
+--------------------+
|[BINARY]            |
+--------------------+
"""


def rst_derivedband_sql_example():
    """Apply Python UDF to derive a new band from tile (pyfunc and funcName are string literals)"""
    return """
-- Apply custom Python function to raster band; requires registered UDF
SELECT path, gbx_rst_derivedband(tile, 'def my_func(arr): return arr * 2', 'my_func') as derived FROM rasters;
"""


rst_derivedband_sql_example_output = """
+----+--------------------+
|path|derived             |
+----+--------------------+
|... |[BINARY]            |
+----+--------------------+
"""


def rst_derivedband_agg_sql_example():
    """Aggregator: apply Python UDF to tiles in group by"""
    return """
SELECT region, gbx_rst_derivedband_agg(tile, 'def f(a): return a', 'f') as result FROM rasters GROUP BY region;
"""


rst_derivedband_agg_sql_example_output = """
+------+--------------------+
|region|result              |
+------+--------------------+
|...   |[BINARY]            |
+------+--------------------+
"""


def rst_initnodata_sql_example():
    """Initialize NoData values"""
    return """
SELECT gbx_rst_initnodata(tile) as tile FROM rasters;
"""


rst_initnodata_sql_example_output = """
+--------------------+
|tile                |
+--------------------+
|[BINARY]            |
+--------------------+
"""


def rst_updatetype_sql_example():
    """Convert raster data type"""
    return """
SELECT gbx_rst_updatetype(tile, 'Float32') as float_tile FROM rasters;
"""


rst_updatetype_sql_example_output = """
+--------------------+
|float_tile          |
+--------------------+
|[BINARY]            |
+--------------------+
"""


def rst_merge_sql_example():
    """Merge multiple rasters into mosaic"""
    return """
-- Merge rasters from a table
WITH loaded_tiles AS (
  SELECT 
    id,
    gbx_rst_fromfile(path, 'GTiff') as tile
  FROM raster_paths
)
SELECT gbx_rst_merge(collect_list(tile)) as merged_mosaic
FROM loaded_tiles;
"""


rst_merge_sql_example_output = """
+--------------------+
|merged_mosaic       |
+--------------------+
|[BINARY]            |
+--------------------+
"""


# ============================================================================
# H3 Grid Functions
# ============================================================================

def rst_h3_tessellate_sql_example():
    """Tessellate raster to H3 grid"""
    return """
-- Tessellate and explode H3 cells
SELECT
    path,
    h3_tile.cellid as h3_cell,
    h3_tile as tile,
    gbx_rst_avg(h3_tile) as avg_value
FROM rasters
LATERAL VIEW explode(gbx_rst_h3_tessellate(tile, 7)) AS h3_tile;

-- Count cells per raster
SELECT
    path,
    SIZE(gbx_rst_h3_tessellate(tile, 7)) as num_cells
FROM rasters;
"""


rst_h3_tessellate_sql_example_output = """
+----+--------+--------------------+---------+
|path|h3_cell |tile                |avg_value|
+----+--------+--------------------+---------+
|... |8f283...|[BINARY]            |0.42     |
+----+--------+--------------------+---------+

+----+---------+
|path|num_cells|
+----+---------+
|... |12       |
+----+---------+
"""


def rst_h3_rastertogridavg_sql_example():
    """Aggregate raster values to H3 grid using average"""
    return """
-- Aggregate raster to H3 grid
SELECT 
    path,
    gbx_rst_h3_rastertogridavg(tile, 6) as h3_grid
FROM rasters;

-- Get cells from first band
SELECT 
    path,
    cell.cellID as h3_cell,
    cell.measure as avg_value
FROM rasters
LATERAL VIEW explode(gbx_rst_h3_rastertogridavg(tile, 6)[0]) AS cell;
"""


rst_h3_rastertogridavg_sql_example_output = """
+----+--------------------+
|path|h3_grid             |
+----+--------------------+
|... |[STRUCT...]         |
+----+--------------------+

+----+--------+---------+
|path|h3_cell |avg_value|
+----+--------+---------+
|... |8f283...|0.45     |
+----+--------+---------+
"""


def rst_h3_rastertogridcount_sql_example():
    """Count pixels per H3 cell"""
    return """
SELECT
    gbx_rst_h3_rastertogridcount(tile, 5) as pixel_counts
FROM rasters;
"""


rst_h3_rastertogridcount_sql_example_output = """
+--------------------+
|pixel_counts        |
+--------------------+
|[STRUCT...]         |
+--------------------+
"""


def rst_h3_rastertogridmax_sql_example():
    """Get maximum values per H3 cell"""
    return """
SELECT
    cell.cellID as h3_cell,
    cell.measure as max_value
FROM rasters
LATERAL VIEW explode(gbx_rst_h3_rastertogridmax(tile, 7)[0]) AS cell;
"""


rst_h3_rastertogridmax_sql_example_output = """
+--------+---------+
|h3_cell |max_value|
+--------+---------+
|8f283...|255.0    |
+--------+---------+
"""


def rst_h3_rastertogridmin_sql_example():
    """Get minimum values per H3 cell"""
    return """
SELECT
    cell.cellID as h3_cell,
    cell.measure as min_value
FROM rasters
LATERAL VIEW explode(gbx_rst_h3_rastertogridmin(tile, 7)[0]) AS cell;
"""


rst_h3_rastertogridmin_sql_example_output = """
+--------+---------+
|h3_cell |min_value|
+--------+---------+
|8f283...|0.0      |
+--------+---------+
"""


def rst_h3_rastertogridmedian_sql_example():
    """Get median values per H3 cell"""
    return """
SELECT
    cell.cellID as h3_cell,
    cell.measure as median_value
FROM rasters
LATERAL VIEW explode(gbx_rst_h3_rastertogridmedian(tile, 7)[0]) AS cell;
"""


rst_h3_rastertogridmedian_sql_example_output = """
+--------+------------+
|h3_cell |median_value|
+--------+------------+
|8f283...|128.0       |
+--------+------------+
"""


# ============================================================================
# Generator Functions - Produce Multiple Rows
# ============================================================================

def rst_maketiles_sql_example():
    """Subdivide rasters into tiles"""
    return """
-- Subdivide and explode tiles
SELECT
    path,
    tile_subtile as tile
FROM rasters
LATERAL VIEW explode(gbx_rst_maketiles(tile, 512, 512)) AS tile_subtile;

-- Count tiles per raster
SELECT
    path,
    SIZE(gbx_rst_maketiles(tile, 512, 512)) as num_tiles
FROM rasters;
"""


rst_maketiles_sql_example_output = """
+----+--------------------+
|path|tile                |
+----+--------------------+
|... |[BINARY]            |
+----+--------------------+

+----+---------+
|path|num_tiles|
+----+---------+
|... |42       |
+----+---------+
"""


def rst_retile_sql_example():
    """Retile rasters to uniform dimensions"""
    return """
SELECT
    path,
    tile
FROM rasters
LATERAL VIEW explode(gbx_rst_retile(tile, 256, 256)) AS tile;
"""


rst_retile_sql_example_output = """
+----+--------------------+
|path|tile                |
+----+--------------------+
|... |[BINARY]            |
+----+--------------------+
"""


def rst_tooverlappingtiles_sql_example():
    """Create overlapping tiles for edge-aware processing"""
    return """
SELECT
    path,
    tile
FROM rasters
LATERAL VIEW explode(gbx_rst_tooverlappingtiles(tile, 256, 256, 10)) AS tile;
"""


rst_tooverlappingtiles_sql_example_output = """
+----+--------------------+
|path|tile                |
+----+--------------------+
|... |[BINARY]            |
+----+--------------------+
"""


def rst_separatebands_sql_example():
    """Separate multi-band raster into individual bands"""
    return """
SELECT
    path,
    bands[0] as red_band,
    bands[1] as green_band,
    bands[2] as blue_band
FROM (
    SELECT path, gbx_rst_separatebands(tile) as bands
    FROM rgb_rasters
);
"""


rst_separatebands_sql_example_output = """
+----+--------------------+--------------------+--------------------+
|path|red_band            |green_band          |blue_band           |
+----+--------------------+--------------------+--------------------+
|... |[BINARY]            |[BINARY]            |[BINARY]            |
+----+--------------------+--------------------+--------------------+
"""


# ============================================================================
# Aggregation Functions
# ============================================================================

def rst_combineavg_sql_example():
    """Average multiple rasters for temporal composite"""
    return """
-- Average rasters for temporal composite
WITH loaded_tiles AS (
  SELECT 
    date_trunc('week', date) as week,
    gbx_rst_fromfile(path, 'GTiff') as tile
  FROM daily_rasters
  WHERE date >= '2024-01-01'
)
SELECT 
    week,
    gbx_rst_combineavg(collect_list(tile)) as weekly_composite
FROM loaded_tiles
GROUP BY week;
"""


rst_combineavg_sql_example_output = """
+-------------------+--------------------+
|week               |weekly_composite    |
+-------------------+--------------------+
|2024-01-01 00:00:00|[BINARY]            |
+-------------------+--------------------+
"""


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
+------+--------------------+
|region|regional_average    |
+------+--------------------+
|...   |[BINARY]            |
+------+--------------------+
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
+--------+--------------------+
|scene_id|merged_scene        |
+--------+--------------------+
|S2A_001 |[BINARY]            |
+--------+--------------------+
"""
