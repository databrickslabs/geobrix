-- GeoBrix Unity Catalog functions: spectral indices, terrain and classification.
-- Needs 00_setup.sql. Change the catalog and schema below if you installed elsewhere.
-- The scene has five bands in this order: 1 red, 2 green, 3 blue, 4 near infrared, 5 shortwave infrared.
-- Each statement that calls GeoBrix Python functions pays about 10 seconds on a SQL warehouse.

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: raster_vegetation_indices
-- Mean index values over the scene. Each function takes the 1-based band numbers it needs.
SELECT gbx_rst_avg(gbx_rst_ndvi(tile, 1, 4)) AS ndvi,
       gbx_rst_avg(gbx_rst_ndwi(tile, 2, 4)) AS ndwi,
       gbx_rst_avg(gbx_rst_nbr(tile, 4, 5)) AS nbr,
       gbx_rst_avg(gbx_rst_savi(tile, 1, 4, 0.5)) AS savi,
       gbx_rst_avg(gbx_rst_evi(tile, 1, 4, 3)) AS evi
FROM ex_rasters
WHERE name = 'scene';

-- cid: raster_named_index
-- gbx_rst_index picks an index by name and takes a map from band name to band number.
SELECT gbx_rst_avg(gbx_rst_index(tile, 'ndvi', map('red', 1, 'nir', 4))) AS ndvi
FROM ex_rasters
WHERE name = 'scene';

-- cid: raster_terrain
-- Terrain from the DEM: slope in percent, aspect, hillshade, and three ruggedness measures.
SELECT gbx_rst_avg(gbx_rst_slope(tile, 'percent')) AS slope_percent,
       gbx_rst_avg(gbx_rst_aspect(tile)) AS aspect,
       gbx_rst_avg(gbx_rst_hillshade(tile, 315.0, 45.0, 1.0)) AS hillshade,
       gbx_rst_avg(gbx_rst_tpi(tile)) AS tpi,
       gbx_rst_avg(gbx_rst_tri(tile)) AS tri,
       gbx_rst_avg(gbx_rst_roughness(tile)) AS roughness
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_color_relief_and_viewshed
-- Colour the DEM from a color table's text (blue at 60 m, red at 120 m), and the share of the DEM a 2 m tall
-- observer in its middle sees (a visible cell is 255). The observer is a GEOMETRY in the DEM's SRID.
SELECT gbx_rst_numbands(gbx_rst_color_relief(tile, '60 0 0 255\n120 255 0 0')) AS relief_bands,
       gbx_rst_avg(gbx_rst_viewshed(tile, CAST(st_point(530120, 180360, 27700) AS GEOMETRY(ANY)), 2.0))[0] / 255 AS visible_share
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_canopy_height
-- A canopy height model is the surface model minus the terrain model. Here the surface is the DEM plus 8 m.
SELECT gbx_rst_avg(gbx_rst_chm(gbx_rst_mapalgebra(array(tile), 'A + 8'), tile)) AS mean_height_m
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_land_cover
-- Classify the scene into vegetation (0), bare (1), impervious (2) and dark (3) from its colours.
SELECT gbx_rst_histogram(gbx_rst_land_cover(tile), 4, 0.0, 4.0) AS class_histogram
FROM ex_rasters
WHERE name = 'scene';
