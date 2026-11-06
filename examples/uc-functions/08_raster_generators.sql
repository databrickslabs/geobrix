-- GeoBrix Unity Catalog functions: raster generators (cut a tile into tiles, and build tiles from points).
-- Needs 00_setup.sql. Change the catalog and schema below if you installed elsewhere.
-- A table function returns one row for each tile it makes: call it with LATERAL.
-- Each statement that calls GeoBrix Python functions pays about 10 seconds on a SQL warehouse.

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: generator_retile
-- Cut the DEM into 12 x 12 pixel tiles. Each row records the pixel window it came from.
SELECT t.window.col_off AS col_off, t.window.row_off AS row_off, t.window.width AS width, t.window.height AS height
FROM ex_rasters AS r, LATERAL gbx_rst_retile(r.tile, 12, 12) AS t
WHERE r.name = 'dem'
ORDER BY col_off, row_off;

-- cid: generator_maketiles
-- Cut a tile into tiles of at most a size in megabytes (here 1 MB, so the small DEM stays whole).
SELECT count(*) AS tiles
FROM ex_rasters AS r, LATERAL gbx_rst_maketiles(r.tile, 1) AS t
WHERE r.name = 'dem';

-- cid: generator_overlapping_tiles
-- Cut with an overlap of 25 percent between neighbours.
SELECT count(*) AS tiles
FROM ex_rasters AS r, LATERAL gbx_rst_tooverlappingtiles(r.tile, 12, 12, 25) AS t
WHERE r.name = 'dem';

-- cid: generator_separate_bands
-- One tile for each band of the scene. The band number is in the metadata.
SELECT t.metadata['band'] AS band, length(t.raster) AS bytes
FROM ex_rasters AS r, LATERAL gbx_rst_separatebands(r.tile) AS t
WHERE r.name = 'scene'
ORDER BY band;

-- cid: generator_merge_tiles_back
-- Merge tiles back into one raster. Collect tiles into an array and pass it to gbx_rst_merge.
WITH pieces AS (
  SELECT struct(t.cellid, t.raster, t.path, t.path_mode, t.window, t.clip_polygon, t.clip_crs, t.crs, t.metadata) AS tile
  FROM ex_rasters AS r, LATERAL gbx_rst_retile(r.tile, 12, 12) AS t
  WHERE r.name = 'dem'
)
SELECT gbx_rst_width(gbx_rst_merge(collect_list(tile))) AS merged_width,
       gbx_rst_height(gbx_rst_merge(collect_list(tile))) AS merged_height
FROM pieces;

-- cid: generator_rasterize_polygons
-- Burn a polygon into a raster: pixels it covers take the value, the others are NoData.
SELECT gbx_rst_pixelcount(gbx_rst_rasterize(CAST(geom AS GEOMETRY(ANY)), 7.0, 530000.0, 180000.0, 530250.0, 180200.0, 25, 20, 27700)) AS covered_pixels
FROM ex_parcels
WHERE id = 1;

-- cid: generator_points_to_raster_bins
-- Bin points into pixels with a statistic. Arrays of coordinates and values make one raster.
SELECT gbx_rst_pixelcount(
         gbx_rst_binpoints(collect_list(longitude), collect_list(latitude), collect_list(value),
                           -0.16, 51.48, -0.10, 51.53, 30, 25, 4326, 'mean')) AS occupied_pixels
FROM ex_points;

-- cid: generator_points_to_raster_surface
-- Interpolate points into a surface by inverse-distance weighting.
SELECT gbx_rst_avg(
         gbx_rst_gridfrompoints(collect_list(CAST(geom AS GEOMETRY(ANY))), collect_list(value),
                                -0.16, 51.48, -0.10, 51.53, 30, 25, 4326)) AS surface_mean
FROM ex_points;

-- cid: generator_terrain_from_spot_heights
-- Triangulate spot heights (points with a Z value) into a terrain raster. A point with no Z is refused.
SELECT gbx_rst_avg(
         gbx_rst_dtmfromgeoms(
           array(CAST(st_geomfromwkt('POINT Z(0 0 10)') AS GEOMETRY(ANY)), CAST(st_geomfromwkt('POINT Z(10 0 20)') AS GEOMETRY(ANY)),
                 CAST(st_geomfromwkt('POINT Z(0 10 30)') AS GEOMETRY(ANY)), CAST(st_geomfromwkt('POINT Z(10 10 40)') AS GEOMETRY(ANY))),
           CAST(NULL AS ARRAY<GEOMETRY(ANY)>), 0.0, 0.0, 0.0, 0.0, 10.0, 10.0, 20, 20, 3857)) AS terrain_mean;

-- cid: generator_h3_cells_to_raster
-- Burn H3 cell values into a raster. The extent comes from the cells when you pass NULL.
-- The result is the bytes of a GeoTIFF; gbx_rst_fromcontent turns them back into a tile.
SELECT length(gbx_rst_h3_rasterize(collect_list(struct(cellid, value)), 4326, CAST(NULL AS DOUBLE),
                                   CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
                                   CAST(NULL AS INT), CAST(NULL AS INT), 'centroids', 1)) AS geotiff_bytes
FROM (SELECT h3_longlatash3(longitude, latitude, 8) AS cellid, avg(value) AS value FROM ex_points GROUP BY 1);

-- cid: generator_bng_cells_to_raster
-- Burn BNG cell values into a raster. The cell ids are text. The result is the bytes of a GeoTIFF.
WITH t AS (
  SELECT gbx_bng_polyfill(CAST(geom AS GEOMETRY(ANY)), '100m') AS cells FROM ex_parcels WHERE id = 3
)
SELECT length(gbx_rst_bng_rasterize(collect_list(struct(cellid, value)), 27700,
       CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
       CAST(NULL AS INT), CAST(NULL AS INT), 'centroids', 1)) AS geotiff_bytes
FROM (SELECT cellid, CAST(row_number() OVER (ORDER BY cellid) AS DOUBLE) AS value
      FROM t LATERAL VIEW explode(cells) AS cellid);

-- cid: generator_quadbin_cells_to_raster
-- The same for quadbin cells. Here each cell holds the mean reading of the sensors in it.
SELECT length(gbx_rst_quadbin_rasterize(collect_list(struct(cellid, value)), 4326,
       CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
       CAST(NULL AS INT), CAST(NULL AS INT), 'centroids', 1)) AS geotiff_bytes
FROM (SELECT gbx_quadbin_pointascell(longitude, latitude, 12) AS cellid, avg(value) AS value FROM ex_points GROUP BY 1);

-- cid: generator_custom_grid_cells_to_raster
-- The same for a custom grid. The grid goes in after the cells.
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid),
c AS (
  SELECT g.grid, gbx_custom_pointascell(CAST(st_centroid(p.geom) AS GEOMETRY(ANY)), g.grid, 4) AS cellid, CAST(p.id AS DOUBLE) AS value
  FROM ex_parcels AS p, g
)
SELECT length(gbx_rst_custom_rasterize(collect_list(struct(cellid, value)), first(grid), 27700,
       CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
       CAST(NULL AS INT), CAST(NULL AS INT), 'centroids', 1)) AS geotiff_bytes
FROM c;

-- cid: generator_h3_tessellate
-- Cut the WGS84 scene into the tile of each H3 cell it covers. Each row is one cell and its tile bytes.
SELECT count(*) AS cells, sum(length(t.raster)) AS tile_bytes
FROM ex_rasters AS r, LATERAL gbx_rst_h3_tessellate(r.tile, 10) AS t
WHERE r.name = 'scene_wgs84';

-- cid: generator_bng_tessellate
-- The same for BNG cells of 100 m. The resolution is text.
SELECT count(*) AS cells, sum(length(t.raster)) AS tile_bytes
FROM ex_rasters AS r, LATERAL gbx_rst_bng_tessellate(r.tile, '100m') AS t
WHERE r.name = 'scene';

-- cid: generator_quadbin_tessellate
-- The same for quadbin cells at zoom 18.
SELECT count(*) AS cells, sum(length(t.raster)) AS tile_bytes
FROM ex_rasters AS r, LATERAL gbx_rst_quadbin_tessellate(r.tile, 18) AS t
WHERE r.name = 'scene_wgs84';

-- cid: generator_custom_grid_tessellate
-- The same for a custom grid. The grid goes in after the tile.
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid)
SELECT count(*) AS cells, sum(length(t.raster)) AS tile_bytes
FROM ex_rasters AS r, g, LATERAL gbx_rst_custom_tessellate(r.tile, g.grid, 5) AS t
WHERE r.name = 'scene';
