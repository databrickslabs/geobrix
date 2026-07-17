-- Params: x_min,x_max,y_min,y_max DOUBLE ; zoom_level INT ; zoom_break_1..4 INT ;
--         res_1..5 INT ; min_res INT ; max_res INT ; target_cells INT ;
--         table_name STRING via IDENTIFIER()
WITH in_view AS (
  SELECT longitude, latitude, operator
  FROM IDENTIFIER(:table_name)
  WHERE longitude BETWEEN :x_min AND :x_max
    AND latitude  BETWEEN :y_min AND :y_max
    AND longitude IS NOT NULL AND latitude IS NOT NULL
),
zoom_ceiling AS (
  SELECT LEAST(:max_res, CASE
    WHEN :zoom_level <= :zoom_break_1 THEN :res_1
    WHEN :zoom_level <= :zoom_break_2 THEN :res_2
    WHEN :zoom_level <= :zoom_break_3 THEN :res_3
    WHEN :zoom_level <= :zoom_break_4 THEN :res_4
    ELSE :res_5 END) AS zc
),
-- Estimate density at the ceiling resolution (distinct cells occupied in view).
ceiling_cells AS (
  SELECT COUNT(DISTINCT h3_longlatash3(longitude, latitude, (SELECT zc FROM zoom_ceiling))) AS n
  FROM in_view
),
target_res AS (
  SELECT GREATEST(:min_res,
           (SELECT zc FROM zoom_ceiling)
           - GREATEST(0, CAST(FLOOR(LOG(7.0, GREATEST(c.n, 1) / CAST(:target_cells AS DOUBLE))) AS INT))
         ) AS res
  FROM ceiling_cells c
)
SELECT
  h3_h3tostring(h3_longlatash3(v.longitude, v.latitude, t.res)) AS hex,
  CAST(COUNT(*)                    AS DOUBLE) AS well_count,
  CAST(COUNT(DISTINCT v.operator)  AS DOUBLE) AS operator_count
FROM in_view v CROSS JOIN target_res t
GROUP BY h3_longlatash3(v.longitude, v.latitude, t.res)
