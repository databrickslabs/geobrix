-- GeoBrix Unity Catalog functions: vector geometry, vector tiles and legacy geometry migration.
-- Needs 00_setup.sql. Change the catalog and schema below if you installed elsewhere.
-- These functions add to the built-in ST functions: cleaners the product lacks, MVT tiles and Mosaic migration.
-- A geometry argument is GEOMETRY(ANY): cast a GEOMETRY(srid) column with CAST(geom AS GEOMETRY(ANY)).

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: vector_explain_validity
-- Why a polygon is invalid. Parcel 6 is a bow-tie.
SELECT id, name, gbx_st_explainvalidity(CAST(geom AS GEOMETRY(ANY))) AS reason
FROM ex_parcels
WHERE id IN (1, 6)
ORDER BY id;

-- cid: vector_make_valid
-- Repair the bow-tie into a valid multi-polygon.
SELECT id, st_isvalid(geom) AS was_valid,
       st_isvalid(gbx_st_makevalid(CAST(geom AS GEOMETRY(ANY)))) AS now_valid,
       st_astext(gbx_st_makevalid(CAST(geom AS GEOMETRY(ANY)))) AS repaired
FROM ex_parcels
WHERE id = 6;

-- cid: vector_node_and_remove_repeated_points
-- Node a self-crossing line at its crossings, and drop repeated points from another line.
SELECT st_astext(gbx_st_node(CAST(st_geomfromwkt('LINESTRING(0 0, 10 10, 10 0, 0 10)', 4326) AS GEOMETRY(ANY)))) AS noded,
       st_astext(gbx_st_removerepeatedpoints(CAST(st_geomfromwkt('LINESTRING(0 0, 0 0, 1 1, 1 1, 2 2)', 4326) AS GEOMETRY(ANY)), 0.0)) AS cleaned;

-- cid: vector_simplify_and_reduce_precision
-- Simplify a polygon without breaking its topology, and round coordinates to a grid.
SELECT st_astext(gbx_st_simplifypreservetopology(CAST(st_geomfromwkt('POLYGON((0 0, 0 5, 0.001 8, 0 10, 10 10, 10 0, 0 0))', 4326) AS GEOMETRY(ANY)), 0.1)) AS simplified,
       st_astext(gbx_st_reduceprecision(CAST(st_geomfromwkt('POINT(1.234 5.678)', 4326) AS GEOMETRY(ANY)), 0.1)) AS rounded;

-- cid: vector_snap_and_split
-- Snap a line onto a reference line within 0.5, and split a polygon with a blade line. Both inputs share an SRID.
SELECT st_astext(gbx_st_snap(CAST(st_geomfromwkt('LINESTRING(0 0.4, 10 0.4)', 4326) AS GEOMETRY(ANY)),
                             CAST(st_geomfromwkt('LINESTRING(0 0, 10 0)', 4326) AS GEOMETRY(ANY)), 0.5)) AS snapped,
       st_astext(gbx_st_split(CAST(st_geomfromwkt('POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))', 4326) AS GEOMETRY(ANY)),
                              CAST(st_geomfromwkt('LINESTRING(5 -1, 5 11)', 4326) AS GEOMETRY(ANY)))) AS pieces;

-- cid: vector_longitude_wrapping
-- Wrap a longitude of 190 back into range, and shift the longitudes of a polygon over the date line.
SELECT st_astext(gbx_st_wrapx(CAST(st_geomfromwkt('POINT(190 10)', 4326) AS GEOMETRY(ANY)), -180.0, 360.0)) AS wrapped,
       st_astext(gbx_st_shiftlongitude(CAST(st_geomfromwkt('POLYGON((170 -10, 190 -10, 190 10, 170 10, 170 -10))', 4326) AS GEOMETRY(ANY)))) AS shifted;

-- cid: vector_coverage_validity_by_district
-- Parcels of one district should tile without gaps or overlaps. Group the parcels, then pass an array of EWKB.
SELECT district,
       gbx_st_coverageisvalid_array(collect_list(st_asewkb(geom)), 0.0) AS is_valid_coverage
FROM ex_parcels
WHERE district IN ('A', 'B')
GROUP BY district
ORDER BY district;

-- cid: vector_coverage_invalid_edges
-- The edges that break the coverage in district B, where two parcels overlap.
SELECT district, st_astext(st_geomfromewkb(gbx_st_coverageinvalidedges_array(collect_list(st_asewkb(geom)), 0.0))) AS bad_edges
FROM ex_parcels
WHERE district = 'B'
GROUP BY district;

-- cid: vector_tiles_for_a_layer
-- One vector tile layer from the features of one tile. The geometry is WKB in tile coordinates, attributes are JSON text.
SELECT gbx_st_asmvt_array(
         array(named_struct('geom', st_asbinary(st_geomfromwkt('POLYGON((10 10, 100 10, 100 100, 10 100, 10 10))')),
                            'attrs', '{"name": "a", "n": 3}'),
               named_struct('geom', st_asbinary(st_geomfromwkt('POINT(50 50)')),
                            'attrs', '{"name": "b", "n": 4}')),
         'places') AS mvt;

-- cid: vector_tile_pyramid
-- A table function encodes one feature into a vector tile for every web-mercator tile it touches, for zooms 6 to 8.
-- The attributes go in as JSON text, because UC has no struct of any fields.
SELECT t.z, t.x, t.y, length(t.mvt_bytes) AS mvt_bytes
FROM ex_roads AS r, LATERAL gbx_st_asmvt_pyramid(r.geom, to_json(r.attrs), 6, 8, 'roads') AS t
WHERE r.id = 1
ORDER BY t.z, t.x, t.y;

-- cid: vector_legacy_geometry_migration
-- Convert Mosaic legacy geometry structs to GEOMETRY values, and read their area and type with built-in ST functions.
SELECT id, st_geometrytype(gbx_st_legacyaswkb(geom)) AS geometry_type,
       st_area(gbx_st_legacyaswkb(geom)) AS area, st_srid(gbx_st_legacyaswkb(geom)) AS srid
FROM ex_legacy_geoms
ORDER BY id;

-- cid: vector_triangulate_spot_heights
-- Triangulate points with a Z value. Each row is one triangle as 2D WKB. The points are an array of GEOMETRY.
SELECT count(*) AS triangles, round(sum(st_area(st_geomfromwkb(t.triangle))), 1) AS total_area
FROM (SELECT array(CAST(st_geomfromwkt('POINT Z(0 0 10)') AS GEOMETRY(ANY)), CAST(st_geomfromwkt('POINT Z(10 0 20)') AS GEOMETRY(ANY)),
         CAST(st_geomfromwkt('POINT Z(0 10 30)') AS GEOMETRY(ANY)), CAST(st_geomfromwkt('POINT Z(10 10 40)') AS GEOMETRY(ANY))) AS pts) AS s,
     LATERAL gbx_st_triangulate(s.pts, CAST(NULL AS ARRAY<GEOMETRY(ANY)>), 0.0, 0.0, 'NONENCROACHING') AS t;

-- cid: vector_interpolate_elevation_on_a_bounding_box
-- Interpolate the surface at the cells of a grid that fills a bounding box. Each row is one point with its Z.
SELECT count(*) AS grid_points
FROM (SELECT array(CAST(st_geomfromwkt('POINT Z(0 0 10)') AS GEOMETRY(ANY)), CAST(st_geomfromwkt('POINT Z(10 0 20)') AS GEOMETRY(ANY)),
         CAST(st_geomfromwkt('POINT Z(0 10 30)') AS GEOMETRY(ANY)), CAST(st_geomfromwkt('POINT Z(10 10 40)') AS GEOMETRY(ANY))) AS pts) AS s,
     LATERAL gbx_st_interpolateelevationbbox(s.pts, CAST(NULL AS ARRAY<GEOMETRY(ANY)>), 0.0, 0.0, 'NONENCROACHING',
                                             0.0, 0.0, 10.0, 10.0, 5, 5, 3857) AS t;

-- cid: vector_interpolate_elevation_on_a_grid
-- The same on a grid you describe by an origin point, a column and row count and a cell size.
SELECT count(*) AS grid_points
FROM (SELECT array(CAST(st_geomfromwkt('POINT Z(0 0 10)') AS GEOMETRY(ANY)), CAST(st_geomfromwkt('POINT Z(10 0 20)') AS GEOMETRY(ANY)),
         CAST(st_geomfromwkt('POINT Z(0 10 30)') AS GEOMETRY(ANY)), CAST(st_geomfromwkt('POINT Z(10 10 40)') AS GEOMETRY(ANY))) AS pts) AS s,
     LATERAL gbx_st_interpolateelevationgeom(s.pts, CAST(NULL AS ARRAY<GEOMETRY(ANY)>), 0.0, 0.0, 'NONENCROACHING',
                                             CAST(st_geomfromwkt('POINT(0 0)') AS GEOMETRY(ANY)), 5, 5, 2.0, 2.0) AS t;
