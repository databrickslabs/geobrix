-- GeoBrix Unity Catalog functions: H3 cells.
-- Needs 00_setup.sql. Change the catalog and schema below if you installed elsewhere.
-- Databricks has built-in h3_ functions for most H3 work. GeoBrix adds the rings of a geometry and cell bounding boxes.

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: h3_cells_of_points
-- The built-in h3_longlatash3 gives the H3 cell of a point; GeoBrix functions then work on that cell.
SELECT id, h3_longlatash3(longitude, latitude, 9) AS cell
FROM ex_points
WHERE id <= 3;

-- cid: h3_ring_of_geometry
-- The H3 cells within two steps of a point (kring) and exactly two steps away (kloop).
SELECT size(gbx_h3_geomkring(CAST(st_geomfromwkt('POINT(-0.1278 51.5074)', 4326) AS GEOMETRY(ANY)), 9, 2)) AS ring_cells,
       size(gbx_h3_geomkloop(CAST(st_geomfromwkt('POINT(-0.1278 51.5074)', 4326) AS GEOMETRY(ANY)), 9, 2)) AS loop_cells;

-- cid: h3_ring_table_functions
-- The ring and loop as table functions: one row for each cell.
SELECT cellid FROM gbx_h3_geomkringexplode(CAST(st_geomfromwkt('POINT(-0.1278 51.5074)', 4326) AS GEOMETRY(ANY)), 9, 1) ORDER BY cellid;

-- cid: h3_loop_table_function
SELECT cellid FROM gbx_h3_geomkloopexplode(CAST(st_geomfromwkt('POINT(-0.1278 51.5074)', 4326) AS GEOMETRY(ANY)), 9, 1) ORDER BY cellid;

-- cid: h3_cell_bounding_box
-- The bounding box of an H3 cell in longitude and latitude (EPSG:4326 is the default). The default mode is the box of the
-- cell centroid, and 'spatial_envelope' gives the box of the whole cell.
SELECT gbx_h3_cell_bbox(h3_longlatash3(-0.1278, 51.5074, 9), 4326, 'spatial_envelope', 0) AS cell_box,
       gbx_h3_cell_bbox(h3_longlatash3(-0.1278, 51.5074, 9)) AS centroid_box;

-- cid: h3_fill_empty_cells
-- Cell aggregates fill cells that hold no reading. Group the readings, add the empty neighbours with a NULL value,
-- then pass an array of (cellid, value). Each NULL cell takes the mean of its neighbours within k = 1 steps.
WITH readings AS (
  SELECT h3_longlatash3(longitude, latitude, 9) AS cellid, avg(value) AS value FROM ex_points GROUP BY 1
),
neighbours AS (
  SELECT DISTINCT n.nb AS cellid FROM readings LATERAL VIEW explode(h3_kring(cellid, 1)) n AS nb
),
cells AS (
  SELECT nb.cellid, rd.value FROM neighbours AS nb LEFT JOIN readings AS rd ON rd.cellid = nb.cellid
)
SELECT c.cellid, c.value
FROM (SELECT gbx_h3_cellfill_array(collect_list(struct(cellid, value)), 1, 'mean') AS filled FROM cells)
LATERAL VIEW inline(filled) c
ORDER BY c.cellid
LIMIT 20;
