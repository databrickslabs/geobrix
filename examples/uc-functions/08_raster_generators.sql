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
