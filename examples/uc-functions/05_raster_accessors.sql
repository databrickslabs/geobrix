-- GeoBrix Unity Catalog functions: raster accessors and statistics.
-- Needs 00_setup.sql. Change the catalog and schema below if you installed elsewhere.
-- A raster is a tile column. ex_rasters holds a DEM and a 5-band scene (red, green, blue, nir, swir).
-- Each statement below that calls GeoBrix Python functions pays about 10 seconds on a SQL warehouse.

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: raster_size_and_type
-- Size, bands, format, data type, NoData and bytes in memory of each tile.
SELECT name,
       gbx_rst_width(tile) AS width, gbx_rst_height(tile) AS height, gbx_rst_numbands(tile) AS bands,
       gbx_rst_format(tile) AS format, gbx_rst_type(tile) AS band_types,
       gbx_rst_getnodata(tile) AS nodata, gbx_rst_memsize(tile) AS pixel_bytes
FROM ex_rasters
ORDER BY name;

-- cid: raster_crs_and_bounds
-- The CRS as an integer SRID and as text, the bounding box as WKT and the georeference map.
SELECT name, gbx_rst_srid(tile) AS srid, gbx_rst_crs(tile) AS crs,
       st_astext(st_geomfromwkb(gbx_rst_boundingbox(tile))) AS bbox,
       gbx_rst_georeference(tile) AS georeference
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_geotransform
-- The six geotransform values one at a time: pixel size, upper-left corner, scale, skew and rotation.
SELECT gbx_rst_pixelwidth(tile) AS pixel_width, gbx_rst_pixelheight(tile) AS pixel_height,
       gbx_rst_upperleftx(tile) AS upper_left_x, gbx_rst_upperlefty(tile) AS upper_left_y,
       gbx_rst_scalex(tile) AS scale_x, gbx_rst_scaley(tile) AS scale_y,
       gbx_rst_skewx(tile) AS skew_x, gbx_rst_skewy(tile) AS skew_y, gbx_rst_rotation(tile) AS rotation
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_metadata
-- File metadata, the metadata of band 1 and any subdatasets (a plain GeoTIFF has none).
SELECT gbx_rst_metadata(tile) AS metadata,
       gbx_rst_bandmetadata(tile, 1) AS band_1_metadata,
       gbx_rst_subdatasets(tile) AS subdatasets
FROM ex_rasters
WHERE name = 'scene';

-- cid: raster_pixel_and_world_coordinates
-- Convert between pixel positions and world coordinates, both as a struct and as single values.
SELECT gbx_rst_rastertoworldcoord(tile, 10, 5) AS world,
       gbx_rst_rastertoworldcoordx(tile, 10, 5) AS world_x,
       gbx_rst_rastertoworldcoordy(tile, 10, 5) AS world_y,
       gbx_rst_worldtorastercoord(tile, 530125.0, 180400.0) AS pixel,
       gbx_rst_worldtorastercoordx(tile, 530125.0, 180400.0) AS pixel_col,
       gbx_rst_worldtorastercoordy(tile, 530125.0, 180400.0) AS pixel_row
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_band_statistics
-- Per-band statistics of the scene: one value for each of its five bands.
SELECT gbx_rst_avg(tile) AS mean, gbx_rst_min(tile) AS min, gbx_rst_max(tile) AS max,
       gbx_rst_median(tile) AS median, gbx_rst_pixelcount(tile) AS valid_pixels
FROM ex_rasters
WHERE name = 'scene';

-- cid: raster_summary_and_histogram
-- A text summary of every band, and a 5-bucket histogram of the DEM.
SELECT gbx_rst_summary(tile) AS summary, gbx_rst_histogram(tile, 5) AS histogram
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_health_checks
-- Is the tile empty, and does GDAL open it? A tile that cannot be read gives false.
SELECT name, gbx_rst_isempty(tile) AS is_empty, gbx_rst_tryopen(tile) AS can_open
FROM ex_rasters
ORDER BY name;

-- cid: raster_sample_at_a_point
-- The value of every band at a point. The point has an SRID, so it is read in that CRS.
SELECT gbx_rst_sample(tile, CAST(st_geomfromwkt('POINT(530125 180350)', 27700) AS GEOMETRY(ANY))) AS band_values
FROM ex_rasters
WHERE name = 'scene';

-- cid: raster_statistics_partial
-- The count, sum, spread (m2), minimum and maximum of each band. These add up across tiles.
WITH parts AS (SELECT gbx_rst_stats_partial(tile) AS stats FROM ex_rasters WHERE name = 'scene')
SELECT s.band, s.count, s.sum, s.m2, s.min, s.max
FROM parts LATERAL VIEW inline(stats) s
ORDER BY s.band;

-- cid: raster_statistics_across_tiles
-- Merge the partial of each tile: add the counts and sums, and add the squared distance of each tile mean to m2.
-- The result is the statistics of the whole DEM, which was cut into tiles in the setup file.
WITH parts AS (SELECT gbx_rst_stats_partial(tile) AS stats FROM ex_dem_tiles),
p AS (SELECT s.* FROM parts LATERAL VIEW inline(stats) s),
w AS (
  SELECT *, try_divide(sum(sum) OVER (PARTITION BY band), sum(count) OVER (PARTITION BY band)) AS mu
  FROM p
)
SELECT band,
       sum(count) AS count,
       try_divide(sum(sum), sum(count)) AS mean,
       min(min) AS min,
       max(max) AS max,
       try_divide(sum(m2) + sum(count * pow(try_divide(sum, count) - mu, 2)), sum(count)) AS variance
FROM w
GROUP BY band
ORDER BY band;
