-- Genie Map app support: wells_enriched_latest gold MV.
-- Current well inventory as map-ready points, tagged with shale play + county/state.

-- Row shape: one row per api (multi-play containment deduped to a single play),
-- map-facing geometry re-tagged SRID 4326, county/state populated from polygons.
SELECT count(*)                              AS n_rows,
       count(DISTINCT api)                   AS n_distinct_api,
       count(play_name)                      AS n_with_play,
       count(county_name)                    AS n_with_county,
       count(longitude)                      AS n_with_lon,
       min(st_srid(well_geom_native))        AS min_srid,
       max(st_srid(well_geom_native))        AS max_srid
FROM geospatial_docs.vapor_eyes_lf.wells_enriched_latest;
-- Expect: n_rows == n_distinct_api (no fan-out); min_srid == max_srid == 4326;
--         n_with_lon == n_rows; n_with_county close to n_rows (AOI counties present).
