-- Top groups by record count within viewport bounds.
--
-- Canonical column names — produced by notebooks/data_engineering.ipynb.
-- GROUP_FILTER holds the grouping column (e.g. operator name, payment type).
-- Parameters:
--   x_min, x_max, y_min, y_max  DOUBLE  WGS84 degrees
--   table_name                  STRING  IDENTIFIER() — fully-qualified table name

SELECT
  GROUP_FILTER  AS group_name,
  COUNT(*)      AS record_count
FROM IDENTIFIER(:table_name)
WHERE st_intersects(
    GEOM_POINT,
    ST_GeomFromText(
      CONCAT(
        'POLYGON((',
        :x_min, ' ', :y_min, ', ',
        :x_min, ' ', :y_max, ', ',
        :x_max, ' ', :y_max, ', ',
        :x_max, ' ', :y_min, ', ',
        :x_min, ' ', :y_min,
        '))'
      ),
      4326
    )
  )
  AND GROUP_FILTER IS NOT NULL
  AND GROUP_FILTER != ''
GROUP BY GROUP_FILTER
ORDER BY record_count DESC
LIMIT 5
