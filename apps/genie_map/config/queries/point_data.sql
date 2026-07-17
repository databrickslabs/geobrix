-- Individual point data query for high-zoom viewports.
--
-- Canonical column names — produced by notebooks/data_engineering.ipynb.
-- Parameters:
--   x_min, x_max, y_min, y_max  DOUBLE  WGS84 degrees
--   table_name                  STRING  IDENTIFIER() — fully-qualified table name
--   category_filter             STRING  '' to skip
--   group_filter                STRING  '' to skip

SELECT
  POINT_X             AS longitude,
  POINT_Y             AS latitude,
  RECORD_ID           AS record_id,
  GROUP_FILTER        AS group_filter,
  CATEGORY_FILTER     AS category_filter,
  METRIC_1            AS metric_1,
  METRIC_2            AS metric_2
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
  AND (:category_filter = '' OR CATEGORY_FILTER = :category_filter)
  AND (:group_filter    = '' OR UPPER(GROUP_FILTER) = UPPER(:group_filter))
LIMIT 10000
