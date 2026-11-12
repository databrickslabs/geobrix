-- Params: x_min,x_max,y_min,y_max DOUBLE ; table_name STRING via IDENTIFIER()
SELECT
  longitude,
  latitude,
  CAST(api AS STRING) AS record_id,
  operator, field, county_name AS county, play_name
FROM IDENTIFIER(:table_name)
WHERE longitude BETWEEN :x_min AND :x_max
  AND latitude  BETWEEN :y_min AND :y_max
-- LIMIT 50000: a safety cap above the whole gold table (22,588 wells), so every view is
-- complete. Fetched as ARROW_STREAM (raw Arrow in the response body), which is not bound by
-- AppKit's 1 MiB SSE event cap (guard: client/src/config/__tests__/query-limits.test.ts).
LIMIT 50000
