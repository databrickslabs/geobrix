-- Silver read coverage: every dated bronze inventory has silver rows.
-- Silver reads each inventory with read_paths(..., on_error="skip"), so an unreadable
-- file is skipped with only a warning in the executor logs. The silver tables aggregate
-- the source file away, so coverage is checked per observation_date: a date whose files
-- were all skipped has no silver row.

-- S5P: granule dates with no hotspot row.
WITH inventory AS (
  SELECT observation_date, count(*) AS n_granules
  FROM geospatial_docs.vapor_eyes_lf.s5p_granules
  GROUP BY observation_date
),
silver AS (
  SELECT DISTINCT observation_date
  FROM geospatial_docs.vapor_eyes_lf.s5p_hotspots
)
SELECT count(*)                                  AS n_inventory_dates,
       count(silver.observation_date)            AS n_silver_dates,
       count(*) - count(silver.observation_date) AS n_dates_without_silver,
       sum(CASE WHEN silver.observation_date IS NULL THEN n_granules ELSE 0 END)
                                                 AS n_granules_without_silver
FROM inventory LEFT JOIN silver ON inventory.observation_date = silver.observation_date;
-- Expect: n_dates_without_silver near 0. A date can also be absent legitimately (every
--         observation below qa_min or outside the AOI bbox), so check the executor-log
--         warnings of the update for the dates it reports. A single skipped granule on a
--         date that has other granules is not visible here.

-- S2: scene dates with no plume-cell row (a skipped B11 or B12 file drops its scene).
WITH inventory AS (
  SELECT observation_date, count(*) AS n_assets
  FROM geospatial_docs.vapor_eyes_lf.s2_swir_assets
  GROUP BY observation_date
),
silver AS (
  SELECT DISTINCT observation_date
  FROM geospatial_docs.vapor_eyes_lf.s2_plume_cells
)
SELECT count(*)                                  AS n_inventory_dates,
       count(silver.observation_date)            AS n_silver_dates,
       count(*) - count(silver.observation_date) AS n_dates_without_silver,
       sum(CASE WHEN silver.observation_date IS NULL THEN n_assets ELSE 0 END)
                                                 AS n_assets_without_silver
FROM inventory LEFT JOIN silver ON inventory.observation_date = silver.observation_date;
-- Expect: n_dates_without_silver == 0 once the SWIR scenes have landed; both bands of a
--         scene must read for it to appear.
