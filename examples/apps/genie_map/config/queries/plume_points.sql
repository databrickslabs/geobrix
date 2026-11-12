-- Params: x_min,x_max,y_min,y_max DOUBLE ; table_name STRING via IDENTIFIER()
SELECT
  lon_max AS longitude,
  lat_max AS latitude,
  CAST(plume_id AS STRING) AS record_id,
  CAST(max_conc_ppmm AS DOUBLE) AS max_conc_ppmm,
  lead_operator, lead_county
FROM IDENTIFIER(:table_name)
WHERE lon_max BETWEEN :x_min AND :x_max
  AND lat_max BETWEEN :y_min AND :y_max
-- LIMIT 4000: AppKit streams the whole JSON_ARRAY result as one SSE event and rejects
-- events over 1 MiB; 4000 rows x ~240 B fits (guard: client/src/config/__tests__/query-limits.test.ts).
LIMIT 4000
