-- GeoBrix Unity Catalog functions: aggregation recipes.
-- Needs 00_setup.sql. Change the catalog and schema below if you installed elsewhere.
-- Unity Catalog has no user-defined aggregate. A function takes the whole group as an array instead:
-- GROUP BY the key, collect the members with collect_list, then call the array function once for each group.
-- Some recipes use only built-in SQL and need no GeoBrix function.

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: recipe_binpoints_partial_and_finalize
-- Points to a raster in two steps. The partial gives each point its pixel (col, row). A GROUP BY reduces the points
-- of each pixel with any statistic. The finalize turns the pixels into a tile. No point leaves SQL.
WITH pts AS (
  SELECT gbx_rst_binpoints_partial(longitude, latitude, -0.16, 51.48, -0.10, 51.53, 30, 25) AS px, value
  FROM ex_points
),
bins AS (
  SELECT px.col AS col, px.row AS row, avg(value) AS value FROM pts WHERE px IS NOT NULL GROUP BY px.col, px.row
)
SELECT gbx_rst_pixelcount(gbx_rst_binpoints_finalize(collect_list(struct(col, row, value)), -0.16, 51.48, -0.10, 51.53, 30, 25, 4326)) AS occupied_pixels
FROM bins;

-- cid: recipe_native_h3_fill
-- Fill H3 cells that hold no reading with the mean of their neighbours, using only built-in SQL.
-- Use this when you do not need the array function: it is plain Spark SQL.
WITH readings AS (
  SELECT h3_longlatash3(longitude, latitude, 9) AS cellid, avg(value) AS value FROM ex_points GROUP BY 1
),
cells AS (
  SELECT nb.cellid, rd.value
  FROM (SELECT DISTINCT n.nb AS cellid FROM readings LATERAL VIEW explode(h3_kring(cellid, 1)) n AS nb) AS nb
  LEFT JOIN readings AS rd ON rd.cellid = nb.cellid
),
holes AS (SELECT cellid FROM cells WHERE value IS NULL),
neighbours AS (
  SELECT h.cellid, c.value
  FROM (SELECT cellid, explode(h3_kring(cellid, 1)) AS nb FROM holes) AS h
  JOIN cells AS c ON c.cellid = h.nb
  WHERE c.value IS NOT NULL
)
SELECT count(*) AS empty_cells_filled, round(avg(mean_value), 2) AS mean_of_filled
FROM (SELECT cellid, avg(value) AS mean_value FROM neighbours GROUP BY cellid);

-- cid: recipe_quadbin_cell_fill
-- The quadbin cell fill array function: group by a key, pass an array of (cellid, value).
-- A NULL value is a hole and takes the mean of the cells around it.
SELECT gbx_quadbin_cellfill_array(
         array(named_struct('cellid', 5233961839712272383, 'value', CAST(NULL AS DOUBLE)),
               named_struct('cellid', 5233961826827370495, 'value', 10.0),
               named_struct('cellid', 5233961831122337791, 'value', 20.0)),
         1, 'mean') AS filled;

-- cid: recipe_bng_cell_fill_by_group
-- A BNG fill for each group. The groups come from GROUP BY.
WITH src AS (
  SELECT * FROM VALUES
    ('a', 'TQ3080', CAST(NULL AS DOUBLE)), ('a', 'TQ3180', 10.0), ('a', 'TQ3081', 20.0),
    ('b', 'TQ3080', CAST(NULL AS DOUBLE)), ('b', 'TQ3180', 30.0)
    AS t(grp, cellid, value)
)
SELECT grp, gbx_bng_cellfill_array(collect_list(struct(cellid, value)), 1, 'mean') AS filled
FROM src
GROUP BY grp
ORDER BY grp;

-- cid: recipe_custom_grid_cell_fill
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid)
SELECT gbx_custom_cellfill_array(
         array(named_struct('cellid', 180530L, 'value', CAST(NULL AS DOUBLE)),
               named_struct('cellid', 180531L, 'value', 10.0),
               named_struct('cellid', 181530L, 'value', 20.0)),
         grid, 1, 'mean') AS filled
FROM g;

-- cid: recipe_union_of_bng_chips_with_native_sql
-- Merge the chips of one BNG cell with built-in ST functions. A core chip stands for the whole cell.
WITH t AS (
  SELECT gbx_bng_tessellate(CAST(geom AS GEOMETRY(ANY)), '100m') AS cells FROM ex_parcels WHERE id IN (1, 2, 3)
),
chips AS (SELECT cell.cellid, cell.core, cell.chip FROM t LATERAL VIEW explode(cells) AS cell)
SELECT cellid,
       CASE WHEN bool_or(core) THEN st_area(st_geomfromewkb(gbx_bng_aswkb(cellid)))
            ELSE st_area(st_union_agg(st_geomfromewkb(chip))) END AS area_m2
FROM chips
GROUP BY cellid
ORDER BY cellid;

-- cid: recipe_combine_tiles_by_key
-- Combine the tiles of each key pixel by pixel. The tiles of a key must be aligned (same size, extent and CRS).
-- Here every key holds the same DEM tile twice.
WITH keyed AS (
  SELECT name AS tile_key, tile FROM ex_rasters WHERE name = 'dem'
  UNION ALL
  SELECT name, tile FROM ex_rasters WHERE name = 'dem'
)
SELECT tile_key, gbx_rst_avg(gbx_rst_combineavg(collect_list(tile))) AS mean_of_combined
FROM keyed
GROUP BY tile_key;

-- cid: recipe_mosaic_tiles_by_key
-- Merge the tiles of each key into one raster. Here the 12 pixel DEM tiles go back into one mosaic.
SELECT 'dem' AS name, gbx_rst_width(gbx_rst_merge(collect_list(tile))) AS width, gbx_rst_height(gbx_rst_merge(collect_list(tile))) AS height
FROM ex_dem_tiles;

-- cid: recipe_vector_tile_layer_by_tile
-- One vector tile layer for each tile key. The geometry is WKB in tile coordinates, the attributes JSON text.
WITH features AS (
  SELECT * FROM VALUES
    (0, 0, 0, st_asbinary(st_geomfromwkt('POLYGON((10 10, 100 10, 100 100, 10 100, 10 10))')), '{"name": "a"}'),
    (0, 0, 0, st_asbinary(st_geomfromwkt('POINT(50 50)')), '{"name": "b"}'),
    (1, 0, 0, st_asbinary(st_geomfromwkt('POINT(20 20)')), '{"name": "c"}')
    AS t(z, x, y, geom, attrs)
)
SELECT z, x, y, length(gbx_st_asmvt_array(collect_list(struct(geom, attrs)), 'places')) AS mvt_bytes
FROM features
GROUP BY z, x, y
ORDER BY z, x, y;
