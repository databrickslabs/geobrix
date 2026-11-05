-- GeoBrix Unity Catalog functions: British National Grid (BNG) cells.
-- Needs 00_setup.sql. Change the catalog and schema below if you installed elsewhere.
-- A BNG cell id is text such as 'TQ3080' (1 km). The resolution is a size ('1km', '100m') or an index.

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: bng_point_to_cell
-- The 100 m BNG cell of each parcel centroid, and the same cell as a polygon in WKT.
SELECT id, name,
       gbx_bng_pointascell(CAST(st_centroid(geom) AS GEOMETRY(ANY)), '100m') AS cell,
       gbx_bng_aswkt(gbx_bng_pointascell(CAST(st_centroid(geom) AS GEOMETRY(ANY)), '100m')) AS cell_wkt
FROM ex_parcels
WHERE district = 'A';

-- cid: bng_easting_northing_to_cell
-- A cell from plain easting and northing numbers, with its area in square metres and its centroid.
SELECT gbx_bng_eastnorthasbng(530500, 180500, '1km') AS cell,
       gbx_bng_cellarea('TQ3080') AS area_m2,
       st_astext(st_geomfromewkb(gbx_bng_centroid('TQ3080'))) AS centroid;

-- cid: bng_cell_geometry
-- A cell as WKT, and as binary EWKB that SQL spatial functions read.
SELECT gbx_bng_aswkt('TQ3080') AS cell_wkt,
       st_area(st_geomfromewkb(gbx_bng_aswkb('TQ3080'))) AS area_m2;

-- cid: bng_distance
-- The distance between two cells in whole cells, and the Euclidean distance in metres.
SELECT gbx_bng_distance('TQ3080', 'TQ3282') AS cells_apart,
       gbx_bng_euclideandistance('TQ3080', 'TQ3282') AS metres_apart;

-- cid: bng_neighbours
-- The cells within one step of a cell (kring) and the cells exactly one step away (kloop).
SELECT size(gbx_bng_kring('TQ3080', 1)) AS ring_cells,
       size(gbx_bng_kloop('TQ3080', 1)) AS loop_cells,
       gbx_bng_kloop('TQ3080', 1) AS loop;

-- cid: bng_neighbours_of_geometry
-- The ring of cells around a geometry, so a point or a polygon grows by k cells on each side.
SELECT size(gbx_bng_geomkring(CAST(st_geomfromwkt('POINT(530500 180500)', 27700) AS GEOMETRY(ANY)), '1km', 1)) AS ring_cells,
       size(gbx_bng_geomkloop(CAST(st_geomfromwkt('POINT(530500 180500)', 27700) AS GEOMETRY(ANY)), '1km', 1)) AS loop_cells;

-- cid: bng_polyfill
-- The 100 m cells that cover each district A parcel.
SELECT id, name, size(gbx_bng_polyfill(CAST(geom AS GEOMETRY(ANY)), '100m')) AS cells
FROM ex_parcels
WHERE district = 'A';

-- cid: bng_tessellate
-- Tessellation cuts a polygon along cell edges. Each element is a cell, a core flag and the cut piece (chip).
-- The function runs in a subquery: UC refuses a SQL function inside a generator.
WITH t AS (
  SELECT id, gbx_bng_tessellate(CAST(geom AS GEOMETRY(ANY)), '100m') AS cells FROM ex_parcels WHERE id = 3
)
SELECT id, cell.cellid, cell.core, st_area(st_geomfromewkb(cell.chip)) AS chip_area_m2
FROM t LATERAL VIEW explode(cells) AS cell
ORDER BY cell.cellid;

-- cid: bng_union_and_intersection_of_chips
-- Chips of one cell combine into one geometry. This takes the chips of two parcels that share a cell.
WITH t AS (
  SELECT id, gbx_bng_tessellate(CAST(geom AS GEOMETRY(ANY)), '100m') AS cells FROM ex_parcels WHERE id IN (1, 2)
),
chips AS (
  SELECT cell.cellid, cell.core, cell.chip FROM t LATERAL VIEW explode(cells) AS cell
)
SELECT cellid,
       st_area(st_geomfromewkb(gbx_bng_cellunion_array(collect_list(struct(cellid, core, chip))))) AS union_area_m2,
       gbx_bng_cellintersection_array(collect_list(struct(cellid, core, chip))) IS NOT NULL AS has_intersection
FROM chips
GROUP BY cellid
ORDER BY cellid;

-- cid: bng_union_of_two_chips
-- The pair forms of the union and the intersection take two chip structs.
WITH t AS (
  SELECT gbx_bng_tessellate(CAST(geom AS GEOMETRY(ANY)), '100m') AS cells FROM ex_parcels WHERE id = 1
),
c AS (
  SELECT cell.cellid, cell.core, cell.chip FROM t LATERAL VIEW explode(cells) AS cell
  WHERE NOT cell.core LIMIT 1
)
SELECT gbx_bng_cellunion(struct(cellid, core, chip), struct(cellid, core, chip)) IS NOT NULL AS union_ok,
       gbx_bng_cellintersection(struct(cellid, core, chip), struct(cellid, core, chip)) IS NOT NULL AS intersection_ok
FROM c;

-- cid: bng_ring_table_function
-- A table function returns one row per cell: call it in FROM.
SELECT cellid FROM gbx_bng_kringexplode('TQ3080', 1) ORDER BY cellid;

-- cid: bng_loop_table_function
SELECT cellid FROM gbx_bng_kloopexplode('TQ3080', 1) ORDER BY cellid;

-- cid: bng_geometry_ring_table_functions
-- The geometry forms of the ring and loop table functions, called once for each row with LATERAL.
SELECT p.id, count(*) AS ring_cells
FROM ex_parcels AS p, LATERAL gbx_bng_geomkringexplode(CAST(p.geom AS GEOMETRY(ANY)), '100m', 1) AS r
WHERE p.id = 1
GROUP BY p.id;

-- cid: bng_geometry_loop_table_function
SELECT cellid
FROM gbx_bng_geomkloopexplode(CAST(st_geomfromwkt('POINT(530500 180500)', 27700) AS GEOMETRY(ANY)), '1km', 1)
ORDER BY cellid;

-- cid: bng_tessellate_table_function
-- Tessellation as a table function: one row per cell with its chip.
SELECT cellid, core
FROM gbx_bng_tessellateexplode(CAST(st_geomfromwkt('POLYGON((530000 180000, 530250 180000, 530250 180200, 530000 180200, 530000 180000))', 27700) AS GEOMETRY(ANY)), '100m')
ORDER BY cellid;
