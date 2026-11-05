-- GeoBrix Unity Catalog functions: quadbin cells and custom grids.
-- Needs 00_setup.sql. Change the catalog and schema below if you installed elsewhere.
-- A quadbin cell is a map tile as a BIGINT; the zoom is the resolution. A custom grid is a struct you build once.

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: quadbin_point_to_cell
-- The zoom 12 quadbin cell of each sensor, its zoom and its centroid.
SELECT id,
       gbx_quadbin_pointascell(longitude, latitude, 12) AS cell,
       gbx_quadbin_resolution(gbx_quadbin_pointascell(longitude, latitude, 12)) AS zoom,
       st_astext(st_geomfromewkb(gbx_quadbin_centroid(gbx_quadbin_pointascell(longitude, latitude, 12)))) AS centroid
FROM ex_points
WHERE id <= 3;

-- cid: quadbin_cell_geometry
-- A cell as an EWKB polygon in longitude and latitude, and its distance to a neighbour.
SELECT st_area(st_geomfromewkb(gbx_quadbin_aswkb(5233961839712272383))) AS area_deg2,
       gbx_quadbin_distance(5233961839712272383, 5233961886956912639) AS cells_apart;

-- cid: quadbin_neighbours
-- The cells within one step (kring) and exactly one step away (kloop).
SELECT size(gbx_quadbin_kring(5233961839712272383, 1)) AS ring_cells,
       size(gbx_quadbin_kloop(5233961839712272383, 1)) AS loop_cells;

-- cid: quadbin_neighbours_of_geometry
SELECT size(gbx_quadbin_geomkring(CAST(st_geomfromwkt('POINT(-0.1278 51.5074)', 4326) AS GEOMETRY(ANY)), 12, 1)) AS ring_cells,
       size(gbx_quadbin_geomkloop(CAST(st_geomfromwkt('POINT(-0.1278 51.5074)', 4326) AS GEOMETRY(ANY)), 12, 1)) AS loop_cells;

-- cid: quadbin_polyfill
-- The zoom 13 cells that cover a small polygon of central London.
SELECT size(gbx_quadbin_polyfill(CAST(st_geomfromwkt('POLYGON((-0.15 51.49, -0.10 51.49, -0.10 51.52, -0.15 51.52, -0.15 51.49))', 4326) AS GEOMETRY(ANY)), 13)) AS cells;

-- cid: quadbin_tessellate
-- Tessellation gives each cell and the piece of the polygon inside it.
SELECT cell.cell, st_area(st_geomfromewkb(cell.geom)) AS piece_area_deg2
FROM (
  SELECT gbx_quadbin_tessellate(CAST(st_geomfromwkt('POLYGON((-0.15 51.49, -0.10 51.49, -0.12 51.52, -0.15 51.49))', 4326) AS GEOMETRY(ANY)), 12) AS cells
) LATERAL VIEW explode(cells) AS cell;

-- cid: quadbin_union_of_cells
-- Several cells as one geometry.
SELECT st_area(st_geomfromewkb(gbx_quadbin_cellunion(array(5233961839712272383, 5233961886956912639)))) AS area_deg2;

-- cid: quadbin_ring_table_functions
-- The geometry ring and loop table functions return one row per cell.
SELECT cellid FROM gbx_quadbin_geomkringexplode(CAST(st_geomfromwkt('POINT(-0.1278 51.5074)', 4326) AS GEOMETRY(ANY)), 12, 1) ORDER BY cellid;

-- cid: quadbin_loop_table_function
SELECT cellid FROM gbx_quadbin_geomkloopexplode(CAST(st_geomfromwkt('POINT(-0.1278 51.5074)', 4326) AS GEOMETRY(ANY)), 12, 1) ORDER BY cellid;

-- cid: custom_grid_definition
-- A custom grid: bounds, how many splits per level, the root cell size and the SRID. Build it once.
SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid;

-- cid: custom_point_to_cell
-- The custom grid cell of a point at resolution 4, with its centroid and its polygon as WKT.
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid)
SELECT cell,
       st_astext(st_geomfromewkb(gbx_custom_centroid(cell, grid))) AS centroid,
       gbx_custom_cellaswkt(cell, grid) AS cell_wkt
FROM (
  SELECT grid, gbx_custom_pointascell(CAST(st_geomfromwkt('POINT(530750 180750)', 27700) AS GEOMETRY(ANY)), grid, 4) AS cell
  FROM g
);

-- cid: custom_cell_geometry_and_distance
-- A cell as a polygon, and the distance between two cells in the grid (the grid comes second).
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid)
SELECT st_area(st_geomfromewkb(gbx_custom_cellaswkb(72057594038650997, grid))) AS area_m2,
       gbx_custom_distance(72057594038650997, grid, 72057594038655000) AS cells_apart
FROM g;

-- cid: custom_neighbours
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid)
SELECT size(gbx_custom_kring(72057594038650997, grid, 1)) AS ring_cells,
       size(gbx_custom_kloop(72057594038650997, grid, 1)) AS loop_cells
FROM g;

-- cid: custom_polyfill_and_geometry_rings
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid)
SELECT size(gbx_custom_polyfill(CAST(st_geomfromwkt('POLYGON((530000 180000, 532000 180000, 532000 182000, 530000 182000, 530000 180000))', 27700) AS GEOMETRY(ANY)), grid, 4)) AS cells,
       size(gbx_custom_geomkring(CAST(st_geomfromwkt('POINT(530500 180500)', 27700) AS GEOMETRY(ANY)), grid, 4, 1)) AS ring_cells,
       size(gbx_custom_geomkloop(CAST(st_geomfromwkt('POINT(530500 180500)', 27700) AS GEOMETRY(ANY)), grid, 4, 1)) AS loop_cells
FROM g;

-- cid: custom_ring_table_functions
-- The custom ring and loop table functions take the grid after the geometry.
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid)
SELECT r.cellid
FROM g, LATERAL gbx_custom_geomkringexplode(CAST(st_geomfromwkt('POINT(530500 180500)', 27700) AS GEOMETRY(ANY)), g.grid, 4, 1) AS r
ORDER BY r.cellid;

-- cid: custom_loop_table_function
WITH g AS (SELECT gbx_custom_grid(0, 1000000, 0, 1000000, 2, 1000, 1000, 27700) AS grid)
SELECT r.cellid
FROM g, LATERAL gbx_custom_geomkloopexplode(CAST(st_geomfromwkt('POINT(530500 180500)', 27700) AS GEOMETRY(ANY)), g.grid, 4, 1) AS r
ORDER BY r.cellid;
