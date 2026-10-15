-- Params: x_min,x_max,y_min,y_max DOUBLE ; table_name STRING via IDENTIFIER()
SELECT
  longitude,
  latitude,
  CAST(api AS STRING) AS record_id,
  operator, field, county_name AS county, play_name
FROM IDENTIFIER(:table_name)
WHERE longitude BETWEEN :x_min AND :x_max
  AND latitude  BETWEEN :y_min AND :y_max
LIMIT 10000
