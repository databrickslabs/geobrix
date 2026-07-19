-- Params: x_min,x_max,y_min,y_max DOUBLE ; zoom_level INT ;
--         zoom_break_1..4 INT ; res_1..5 INT ; min_res INT ; native_res INT ;
--         target_cells INT ; table_name STRING via IDENTIFIER()
WITH in_view AS (
  SELECT h3_cellid, ch4_max, ch4_mean, n_obs
  FROM IDENTIFIER(:table_name)
  WHERE center_lon BETWEEN :x_min AND :x_max
    AND center_lat BETWEEN :y_min AND :y_max
    AND ch4_max IS NOT NULL
),
zoom_ceiling AS (
  SELECT CASE
    WHEN :zoom_level <= :zoom_break_1 THEN :res_1
    WHEN :zoom_level <= :zoom_break_2 THEN :res_2
    WHEN :zoom_level <= :zoom_break_3 THEN :res_3
    WHEN :zoom_level <= :zoom_break_4 THEN :res_4
    ELSE :res_5 END AS zc
),
counted AS (SELECT COUNT(*) AS n FROM in_view),
target_res AS (
  -- ceiling capped at native_res; density subtracts coarsening levels (each ≈ ÷7);
  -- floored at min_res. Sparse (n <= target) → levels = 0 → stays at ceiling.
  SELECT GREATEST(:min_res,
           LEAST(zc.zc, :native_res)
           - GREATEST(0, CAST(FLOOR(LOG(7.0, GREATEST(c.n, 1) / CAST(:target_cells AS DOUBLE))) AS INT))
         ) AS res
  FROM zoom_ceiling zc CROSS JOIN counted c
)
SELECT
  h3_h3tostring(h3_toparent(v.h3_cellid, t.res)) AS hex,
  CAST(MAX(v.ch4_max)  AS DOUBLE) AS ch4_max,
  CAST(AVG(v.ch4_mean) AS DOUBLE) AS ch4_mean,
  CAST(SUM(v.n_obs)    AS DOUBLE) AS n_obs
FROM in_view v CROSS JOIN target_res t
GROUP BY h3_toparent(v.h3_cellid, t.res)
