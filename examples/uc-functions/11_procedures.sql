-- GeoBrix Unity Catalog functions: procedures.
-- Needs 00_setup.sql and an install made with procedures = True. Change the catalog and schema below,
-- and the table names in each CALL, if you installed elsewhere.
-- A procedure runs a whole ETL step in Unity Catalog: it reads a table, calls GeoBrix functions and merges the result
-- into another table. Table names are parameters, written as catalog.schema.table. Each CALL creates its target table.

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: procedure_clean_geometries
-- Repair the parcels. Valid and repaired geometries go to the target. What cannot be repaired goes to a rejects table with the reason.
CALL gbx_clean_geometries(
  source_table => 'geospatial_docs.geobrix_uc.ex_parcels',
  target_table => 'geospatial_docs.geobrix_uc.ex_parcels_clean',
  rejects_table => 'geospatial_docs.geobrix_uc.ex_parcels_rejects',
  repair_level => 'linework');

-- cid: procedure_clean_geometries_result
SELECT 'clean' AS tbl, count(*) AS n FROM ex_parcels_clean
UNION ALL SELECT 'rejects', count(*) FROM ex_parcels_rejects;

-- cid: procedure_polygons_to_bng
-- Tessellate the cleaned parcels into 50 m British National Grid cells: one row per parcel and cell.
CALL gbx_polygons_to_bng(
  source_table => 'geospatial_docs.geobrix_uc.ex_parcels_clean',
  target_table => 'geospatial_docs.geobrix_uc.ex_parcel_cells',
  bng_resolution => '50m',
  keep_core_geom => false);

-- cid: procedure_polygons_to_bng_result
SELECT id, count(*) AS cells, sum(CASE WHEN core THEN 1 ELSE 0 END) AS core_cells
FROM ex_parcel_cells
GROUP BY id
ORDER BY id;

-- cid: procedure_migrate_mosaic_geometry
-- Convert a table with legacy Mosaic geometry structs into a table with a GEOMETRY column.
CALL gbx_migrate_mosaic_geometry(
  source_table => 'geospatial_docs.geobrix_uc.ex_legacy_geoms',
  target_table => 'geospatial_docs.geobrix_uc.ex_migrated_geoms');

-- cid: procedure_migrate_mosaic_geometry_result
SELECT id, st_geometrytype(geom) AS geometry_type, st_srid(geom) AS srid FROM ex_migrated_geoms ORDER BY id;

-- cid: procedure_features_to_mvt
-- Cut road features into vector tiles for zooms 6 to 8, one row for each tile.
CALL gbx_features_to_mvt(
  source_table => 'geospatial_docs.geobrix_uc.ex_roads',
  target_table => 'geospatial_docs.geobrix_uc.ex_road_tiles',
  min_zoom => 6,
  max_zoom => 8,
  mvt_layer => 'roads');

-- cid: procedure_features_to_mvt_result
SELECT z, count(*) AS tiles FROM ex_road_tiles GROUP BY z ORDER BY z;

-- cid: procedure_points_to_h3_surface
-- Average the sensor readings in H3 cells (resolution 9), fill the gaps from neighbours and write one GeoTIFF surface.
CALL gbx_points_to_h3_surface(
  source_table => 'geospatial_docs.geobrix_uc.ex_points',
  target_table => 'geospatial_docs.geobrix_uc.ex_h3_surface',
  surface_name => 'sensors',
  h3_resolution => 9,
  fill_rings => 1);

-- cid: procedure_points_to_h3_surface_result
-- The surface is stored as GeoTIFF bytes. gbx_rst_fromcontent turns them into a tile.
SELECT surface, gbx_rst_width(t) AS width, gbx_rst_height(t) AS height
FROM (SELECT surface, gbx_rst_fromcontent(tile, 'GTiff') AS t FROM ex_h3_surface);

-- cid: procedure_mosaic_by_key
-- Merge the tiles of each key into one tile per key. Here each 12 pixel DEM tile is its own key, so the keys stay apart.
CALL gbx_mosaic_by_key(
  source_table => 'geospatial_docs.geobrix_uc.ex_dem_tiles',
  target_table => 'geospatial_docs.geobrix_uc.ex_dem_mosaics');

-- cid: procedure_mosaic_by_key_result
SELECT count(*) AS merged_tiles FROM ex_dem_mosaics;

-- cid: procedure_combine_by_key
-- Combine aligned tiles pixel by pixel for each key. The source holds each DEM tile twice, so the average equals the tile.
CREATE OR REPLACE TABLE ex_aligned_tiles AS
SELECT tile_key, tile FROM ex_dem_tiles
UNION ALL
SELECT tile_key, tile FROM ex_dem_tiles;

-- cid: procedure_combine_by_key_call
CALL gbx_combine_by_key(
  source_table => 'geospatial_docs.geobrix_uc.ex_aligned_tiles',
  target_table => 'geospatial_docs.geobrix_uc.ex_combined_tiles',
  op => 'avg');

-- cid: procedure_combine_by_key_result
SELECT count(*) AS combined_tiles FROM ex_combined_tiles;

-- cid: procedure_zonal_stats
-- Summarize the DEM inside zones. The zones need zone_id and geom; the tiles need tile_id, tile and bbox.
CREATE OR REPLACE TABLE ex_zones AS
SELECT zone_id, st_geomfromwkt(wkt, 27700) AS geom
FROM VALUES
  (1, 'POLYGON((530010 180300, 530110 180300, 530110 180400, 530010 180400, 530010 180300))'),
  (2, 'POLYGON((530120 180260, 530220 180260, 530220 180360, 530120 180360, 530120 180260))')
  AS t(zone_id, wkt);

-- cid: procedure_zonal_stats_tiles
CREATE OR REPLACE TABLE ex_zonal_tiles AS
SELECT name AS tile_id, tile, st_geomfromwkt('POLYGON((530000 180240, 530240 180240, 530240 180480, 530000 180480, 530000 180240))', 27700) AS bbox
FROM ex_rasters WHERE name = 'dem';

-- cid: procedure_zonal_stats_call
CALL gbx_zonal_stats(
  source_table => 'geospatial_docs.geobrix_uc.ex_zonal_tiles',
  zones_table => 'geospatial_docs.geobrix_uc.ex_zones',
  target_table => 'geospatial_docs.geobrix_uc.ex_zone_stats');

-- cid: procedure_zonal_stats_result
SELECT zone_id, tile_id, left(summary, 80) AS summary FROM ex_zone_stats ORDER BY zone_id;
