-- GeoBrix Unity Catalog functions: raster to grid (summarize pixels into grid cells).
-- Needs 00_setup.sql. Change the catalog and schema below if you installed elsewhere.
-- Each grid has one table function for each statistic (avg, count, max, median, min, stddev, sum, variance)
-- and a partial that gives several statistics in one pass. Rows hold the band, the cell and the measure.
-- Each statement that calls GeoBrix Python functions pays about 10 seconds on a SQL warehouse.

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: raster_to_h3_average
-- The mean of band 1 of the scene (red) in each H3 cell at resolution 11.
SELECT g.band, g.cellID, g.measure
FROM ex_rasters AS r, LATERAL gbx_rst_h3_rastertogridavg(r.tile, 11) AS g
WHERE r.name = 'scene_wgs84' AND g.band = 1
ORDER BY g.cellID
LIMIT 10;

-- cid: raster_to_h3_all_statistics
-- Every H3 statistic function on the DEM: how many cells each gives.
SELECT 'avg' AS stat, count(*) AS cells FROM ex_rasters AS r, LATERAL gbx_rst_h3_rastertogridavg(r.tile, 11) WHERE r.name = 'dem'
UNION ALL SELECT 'count', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_h3_rastertogridcount(r.tile, 11) WHERE r.name = 'dem'
UNION ALL SELECT 'max', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_h3_rastertogridmax(r.tile, 11) WHERE r.name = 'dem'
UNION ALL SELECT 'median', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_h3_rastertogridmedian(r.tile, 11) WHERE r.name = 'dem'
UNION ALL SELECT 'min', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_h3_rastertogridmin(r.tile, 11) WHERE r.name = 'dem'
UNION ALL SELECT 'stddev', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_h3_rastertogridstddev(r.tile, 11) WHERE r.name = 'dem'
UNION ALL SELECT 'sum', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_h3_rastertogridsum(r.tile, 11) WHERE r.name = 'dem'
UNION ALL SELECT 'variance', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_h3_rastertogridvariance(r.tile, 11) WHERE r.name = 'dem';

-- cid: raster_to_quadbin
-- The same summary in quadbin cells at zoom 20. The quadbin functions take the zoom as an integer.
SELECT 'avg' AS stat, count(*) AS cells FROM ex_rasters AS r, LATERAL gbx_rst_quadbin_rastertogridavg(r.tile, 20) WHERE r.name = 'dem'
UNION ALL SELECT 'count', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_quadbin_rastertogridcount(r.tile, 20) WHERE r.name = 'dem'
UNION ALL SELECT 'max', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_quadbin_rastertogridmax(r.tile, 20) WHERE r.name = 'dem'
UNION ALL SELECT 'median', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_quadbin_rastertogridmedian(r.tile, 20) WHERE r.name = 'dem'
UNION ALL SELECT 'min', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_quadbin_rastertogridmin(r.tile, 20) WHERE r.name = 'dem'
UNION ALL SELECT 'stddev', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_quadbin_rastertogridstddev(r.tile, 20) WHERE r.name = 'dem'
UNION ALL SELECT 'sum', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_quadbin_rastertogridsum(r.tile, 20) WHERE r.name = 'dem'
UNION ALL SELECT 'variance', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_quadbin_rastertogridvariance(r.tile, 20) WHERE r.name = 'dem';

-- cid: raster_to_bng
-- BNG cells at 50 m. The BNG functions give the cell as text.
SELECT 'avg' AS stat, count(*) AS cells FROM ex_rasters AS r, LATERAL gbx_rst_bng_rastertogridavg(r.tile, '50m') WHERE r.name = 'dem'
UNION ALL SELECT 'count', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_bng_rastertogridcount(r.tile, '50m') WHERE r.name = 'dem'
UNION ALL SELECT 'max', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_bng_rastertogridmax(r.tile, '50m') WHERE r.name = 'dem'
UNION ALL SELECT 'median', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_bng_rastertogridmedian(r.tile, '50m') WHERE r.name = 'dem'
UNION ALL SELECT 'min', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_bng_rastertogridmin(r.tile, '50m') WHERE r.name = 'dem'
UNION ALL SELECT 'stddev', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_bng_rastertogridstddev(r.tile, '50m') WHERE r.name = 'dem'
UNION ALL SELECT 'sum', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_bng_rastertogridsum(r.tile, '50m') WHERE r.name = 'dem'
UNION ALL SELECT 'variance', count(*) FROM ex_rasters AS r, LATERAL gbx_rst_bng_rastertogridvariance(r.tile, '50m') WHERE r.name = 'dem';

-- cid: raster_to_custom_grid
-- Custom grid cells at resolution 5. A custom grid function takes the grid after the tile.
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid)
SELECT 'avg' AS stat, count(*) AS cells FROM ex_rasters AS r, g, LATERAL gbx_rst_custom_rastertogridavg(r.tile, g.grid, 5) WHERE r.name = 'dem'
UNION ALL SELECT 'count', count(*) FROM ex_rasters AS r, g, LATERAL gbx_rst_custom_rastertogridcount(r.tile, g.grid, 5) WHERE r.name = 'dem'
UNION ALL SELECT 'max', count(*) FROM ex_rasters AS r, g, LATERAL gbx_rst_custom_rastertogridmax(r.tile, g.grid, 5) WHERE r.name = 'dem'
UNION ALL SELECT 'median', count(*) FROM ex_rasters AS r, g, LATERAL gbx_rst_custom_rastertogridmedian(r.tile, g.grid, 5) WHERE r.name = 'dem'
UNION ALL SELECT 'min', count(*) FROM ex_rasters AS r, g, LATERAL gbx_rst_custom_rastertogridmin(r.tile, g.grid, 5) WHERE r.name = 'dem'
UNION ALL SELECT 'stddev', count(*) FROM ex_rasters AS r, g, LATERAL gbx_rst_custom_rastertogridstddev(r.tile, g.grid, 5) WHERE r.name = 'dem'
UNION ALL SELECT 'sum', count(*) FROM ex_rasters AS r, g, LATERAL gbx_rst_custom_rastertogridsum(r.tile, g.grid, 5) WHERE r.name = 'dem'
UNION ALL SELECT 'variance', count(*) FROM ex_rasters AS r, g, LATERAL gbx_rst_custom_rastertogridvariance(r.tile, g.grid, 5) WHERE r.name = 'dem';

-- cid: raster_to_h3_many_statistics_one_pass
-- The partial gives the building blocks (count, sum, min, max, m2) of every statistic in one pass.
-- It has default arguments, so call it in a subquery and explode the column: UC refuses a SQL function in a generator.
WITH parts AS (
  SELECT gbx_rst_h3_rastertogrid_partial(tile, 11) AS cells FROM ex_rasters WHERE name = 'dem'
)
SELECT c.band, c.cellid, c.count, try_divide(c.sum, c.count) AS avg, c.min, c.max
FROM parts LATERAL VIEW inline(cells) c
ORDER BY c.cellid
LIMIT 10;

-- cid: raster_to_grid_across_tiles
-- A statistic across many tiles: cut the DEM into tiles, take the partial of each, then merge with GROUP BY.
-- Sum count and sum rather than average the per-tile averages.
WITH pieces AS (
  SELECT struct(t.cellid, t.raster, t.path, t.path_mode, t.window, t.clip_polygon, t.clip_crs, t.crs, t.metadata) AS tile
  FROM ex_rasters AS r, LATERAL gbx_rst_retile(r.tile, 12, 12) AS t
  WHERE r.name = 'dem'
),
parts AS (
  SELECT gbx_rst_bng_rastertogrid_partial(tile, '50m') AS cells FROM pieces
),
p AS (SELECT c.* FROM parts LATERAL VIEW inline(cells) c)
SELECT band, cellid, sum(count) AS count, try_divide(sum(sum), sum(count)) AS avg, min(min) AS min, max(max) AS max
FROM p
GROUP BY band, cellid
ORDER BY cellid;

-- cid: raster_to_quadbin_and_custom_partials
-- The quadbin and custom partials work the same way.
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid),
q AS (SELECT gbx_rst_quadbin_rastertogrid_partial(tile, 20) AS cells FROM ex_rasters WHERE name = 'dem'),
c AS (SELECT gbx_rst_custom_rastertogrid_partial(r.tile, g.grid, 5) AS cells FROM ex_rasters AS r, g WHERE r.name = 'dem')
SELECT (SELECT count(*) FROM q LATERAL VIEW inline(cells) x) AS quadbin_cells,
       (SELECT count(*) FROM c LATERAL VIEW inline(cells) y) AS custom_cells;
