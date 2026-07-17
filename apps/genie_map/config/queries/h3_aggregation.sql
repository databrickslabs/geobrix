-- H3 aggregation query for viewport-based analytics.
--
-- Canonical column names — produced by notebooks/data_engineering.ipynb.
-- CELL_RES_1..5 hold the pre-computed H3 cell IDs at the 5 user-chosen resolutions
-- (e.g. 4,5,6,7,8 for USA-scale data or 8,9,10,11,12 for city-scale data).
--
-- Parameters:
--   x_min, x_max, y_min, y_max  DOUBLE  WGS84 degrees
--   zoom_level                  INT     current map zoom (rounded)
--   zoom_break_1..4             INT     breakpoints separating the 5 H3 resolution tiers
--   res_1..5                    INT     actual H3 resolution numbers for each tier
--   agg_op                      STRING  COUNT | SUM | AVG | MAX | MIN
--   table_name                  STRING  IDENTIFIER() — fully-qualified table name
--   category_filter             STRING  '' to skip
--   group_filter                STRING  '' to skip

WITH map_extent AS (
  SELECT CONCAT(
    'POLYGON((',
    :x_min, ' ', :y_min, ', ',
    :x_min, ' ', :y_max, ', ',
    :x_max, ' ', :y_max, ', ',
    :x_max, ' ', :y_min, ', ',
    :x_min, ' ', :y_min,
    '))'
  ) AS extent
),
h3_resolution AS (
  SELECT CASE
    WHEN :zoom_level <= :zoom_break_1 THEN :res_1
    WHEN :zoom_level <= :zoom_break_2 THEN :res_2
    WHEN :zoom_level <= :zoom_break_3 THEN :res_3
    WHEN :zoom_level <= :zoom_break_4 THEN :res_4
    ELSE :res_5
  END AS resolution
),
cells_in_view AS (
  SELECT EXPLODE(h3_coverash3(m.extent, r.resolution)) AS h3_cell
  FROM map_extent m, h3_resolution r
),
aggregated_data AS (
  SELECT
    CASE
      WHEN :zoom_level <= :zoom_break_1 THEN CELL_RES_1
      WHEN :zoom_level <= :zoom_break_2 THEN CELL_RES_2
      WHEN :zoom_level <= :zoom_break_3 THEN CELL_RES_3
      WHEN :zoom_level <= :zoom_break_4 THEN CELL_RES_4
      ELSE CELL_RES_5
    END AS h3_cell,
    CASE :agg_op
      WHEN 'COUNT' THEN COUNT(*)
      WHEN 'SUM'   THEN SUM(METRIC_1)
      WHEN 'AVG'   THEN AVG(METRIC_2)
      WHEN 'MAX'   THEN MAX(METRIC_1)
      WHEN 'MIN'   THEN MIN(METRIC_1)
      ELSE COUNT(*)
    END AS count
  FROM IDENTIFIER(:table_name)
  WHERE 1=1
    AND (:category_filter = '' OR CATEGORY_FILTER = :category_filter)
    AND (:group_filter    = '' OR UPPER(GROUP_FILTER) = UPPER(:group_filter))
  GROUP BY
    CASE
      WHEN :zoom_level <= :zoom_break_1 THEN CELL_RES_1
      WHEN :zoom_level <= :zoom_break_2 THEN CELL_RES_2
      WHEN :zoom_level <= :zoom_break_3 THEN CELL_RES_3
      WHEN :zoom_level <= :zoom_break_4 THEN CELL_RES_4
      ELSE CELL_RES_5
    END
)
SELECT
  h3_h3tostring(a.h3_cell) AS hex,
  CAST(a.count AS DOUBLE)  AS count
FROM aggregated_data a
JOIN cells_in_view v ON a.h3_cell = v.h3_cell
WHERE a.count IS NOT NULL

