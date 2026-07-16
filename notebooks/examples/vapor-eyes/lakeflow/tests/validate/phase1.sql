-- Bronze populated, exactly-once, dated.
SELECT count(*) AS n_granules,
       count(DISTINCT source_file) AS n_files,
       count(DISTINCT observation_date) AS n_dates,
       max(observation_date) AS latest_obs
FROM geospatial_docs.vapor_eyes_lf.s5p_granules;
-- Expect: n_granules > 0, n_granules = n_files (no dup ingest), observation_date NOT NULL.

-- Silver hotspots present and per-observation_date.
SELECT observation_date, count(*) AS n_cells,
       round(max(ch4_max), 1) AS peak_ch4
FROM geospatial_docs.vapor_eyes_lf.s5p_hotspots
GROUP BY observation_date ORDER BY observation_date;
