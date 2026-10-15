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
LIMIT 10000
