-- GeoBrix Unity Catalog functions: raster operations (warp, edit, combine, files and vectors).
-- Needs 00_setup.sql. Change the catalog and schema below if you installed elsewhere.
-- A tile function returns a tile. These statements read the result with an accessor so that you see numbers.
-- Each statement that calls GeoBrix Python functions pays about 10 seconds on a SQL warehouse.

-- cid: use_catalog
USE CATALOG geospatial_docs;

-- cid: use_schema
USE SCHEMA geobrix_uc;

-- cid: raster_reproject
-- Warp the DEM to longitude and latitude with an SRID, to a CRS text, and to web mercator.
SELECT gbx_rst_srid(gbx_rst_transform(tile, 4326)) AS srid_from_int,
       gbx_rst_crs(gbx_rst_transformcrs(tile, 'EPSG:4326')) AS crs_from_text,
       gbx_rst_srid(gbx_rst_to_webmercator(tile)) AS web_mercator_srid
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_set_crs_label
-- Set the CRS label without moving pixels (for a file that lost its CRS).
SELECT gbx_rst_srid(gbx_rst_setsrid(tile, 3857)) AS srid, gbx_rst_crs(gbx_rst_setcrs(tile, 'EPSG:3857')) AS crs
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_resample
-- Change resolution by a factor, to a pixel size, or to a pixel grid size.
SELECT gbx_rst_width(gbx_rst_resample(tile, 0.5)) AS half_width,
       gbx_rst_pixelwidth(gbx_rst_resample_to_res(tile, 20.0, 20.0)) AS pixel_20m,
       gbx_rst_height(gbx_rst_resample_to_size(tile, 48, 48)) AS height_48
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_clip_to_polygon
-- Clip the DEM to a polygon. cutline_all_touched = true keeps every pixel the polygon touches.
SELECT gbx_rst_width(clipped) AS width, gbx_rst_height(clipped) AS height, gbx_rst_avg(clipped) AS mean
FROM (
  SELECT gbx_rst_clip(tile, CAST(st_geomfromwkt('POLYGON((530050 180300, 530150 180300, 530150 180420, 530050 180420, 530050 180300))', 27700) AS GEOMETRY(ANY)), true) AS clipped
  FROM ex_rasters WHERE name = 'dem'
);

-- cid: raster_align_to_a_reference
-- Align the scene to the grid of the DEM.
SELECT gbx_rst_pixelwidth(gbx_rst_align_to(s.tile, d.tile)) AS pixel_width
FROM ex_rasters AS s, ex_rasters AS d
WHERE s.name = 'scene' AND d.name = 'dem';

-- cid: raster_pixel_edits
-- On the 5-band scene: cast the type, mark NoData, keep one band, threshold, and re-encode to another format.
SELECT gbx_rst_type(gbx_rst_updatetype(tile, 'Float32')) AS new_type,
       gbx_rst_getnodata(gbx_rst_initnodata(tile)) AS nodata,
       gbx_rst_numbands(gbx_rst_band(tile, 4)) AS bands_after_band,
       gbx_rst_max(gbx_rst_threshold(tile, '>', 1000.0)) AS thresholded_max,
       gbx_rst_format(gbx_rst_asformat(gbx_rst_band(tile, 1), 'PNG')) AS band_1_as_png
FROM ex_rasters
WHERE name = 'scene';

-- cid: raster_filters
-- A 3 x 3 window filter, a custom convolution kernel, a NoData fill, and a percentile stretch to 8 bits.
SELECT gbx_rst_avg(gbx_rst_filter(tile, 3, 'mean')) AS smoothed,
       gbx_rst_avg(gbx_rst_convolve(tile, array(array(0.0, 0.0, 0.0), array(0.0, 1.0, 0.0), array(0.0, 0.0, 0.0)))) AS identity_kernel,
       gbx_rst_avg(gbx_rst_fillnodata(tile)) AS filled,
       gbx_rst_max(gbx_rst_percentile_stretch(tile, 2.0, 98.0)) AS stretched_max
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_distance_to_cells
-- Distance in metres to the nearest pixel above 90 m of height, up to 50 m. A threshold keeps the pixels that pass
-- and sets the rest to NoData; NULL for the target values makes every remaining pixel a source.
SELECT gbx_rst_max(gbx_rst_proximity(gbx_rst_threshold(tile, '>', 90.0), CAST(NULL AS STRING), 'GEO', 50.0)) AS farthest_m
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_combine_tiles
-- Array functions take an array of tiles, so they need no aggregate. This combines two copies of the DEM.
SELECT gbx_rst_avg(gbx_rst_combineavg(array(tile, tile))) AS avg_mean,
       gbx_rst_avg(gbx_rst_combinesum(array(tile, tile))) AS sum_mean,
       gbx_rst_avg(gbx_rst_combinemax(array(tile, tile))) AS max_mean,
       gbx_rst_avg(gbx_rst_combinemin(array(tile, tile))) AS min_mean
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_combine_statistics
SELECT gbx_rst_avg(gbx_rst_combinecount(array(tile, tile))) AS count_mean,
       gbx_rst_avg(gbx_rst_combinemedian(array(tile, tile, tile))) AS median_mean,
       gbx_rst_avg(gbx_rst_combinestddev(array(tile, tile))) AS stddev_mean
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_bands_and_map_algebra
-- Stack tiles as bands, and compute with a map algebra expression (band 1 of each tile is A, B, ...).
SELECT gbx_rst_numbands(gbx_rst_frombands(array(tile, tile, tile))) AS stacked_bands,
       gbx_rst_avg(gbx_rst_mapalgebra(array(tile, tile), '(A + B) / 2')) AS algebra_mean
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_merge_tiles
-- Cut the DEM into 12 pixel tiles, then merge them back into one raster.
WITH parts AS (
  SELECT t.raster AS raster FROM ex_rasters AS r, LATERAL gbx_rst_retile(r.tile, 12, 12) AS t WHERE r.name = 'dem'
)
SELECT count(*) AS tiles FROM parts;

-- cid: raster_overviews_and_cog
-- Add overview levels, and lay the tile out as a Cloud Optimized GeoTIFF.
SELECT gbx_rst_format(gbx_rst_buildoverviews(tile, array(2, 4))) AS with_overviews,
       gbx_rst_format(gbx_rst_cog_convert(tile, 'DEFLATE', 256, 'AVERAGE')) AS cog
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_contours_and_isobands
-- Vectorize the DEM: contour lines at chosen heights, and polygon bands between breaks.
SELECT size(gbx_rst_contour(tile, array(70.0, 80.0, 90.0))) AS contour_lines,
       size(gbx_rst_isoband(tile, array(60.0, 80.0, 100.0, 120.0))) AS band_patches
FROM ex_rasters
WHERE name = 'dem';

-- cid: raster_web_map_tile
-- Render the scene as a 256 pixel PNG web tile (zoom 18, a tile that covers it).
SELECT length(gbx_rst_tilexyz(tile, 18, 130988, 87118, 'PNG', 256, 'bilinear', 'auto')) AS png_bytes
FROM ex_rasters
WHERE name = 'scene_wgs84';

-- cid: raster_web_tile_pyramid
-- Render the web tiles the scene touches across a range of zoom levels. Each row is one tile.
SELECT t.z, count(*) AS tiles, sum(length(t.bytes)) AS png_bytes
FROM ex_rasters AS r, LATERAL gbx_rst_xyzpyramid(r.tile, 17, 18) AS t
WHERE r.name = 'scene_wgs84'
GROUP BY t.z
ORDER BY t.z;
