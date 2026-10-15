-- Phase 2: full bronze inventory. Each Auto Loader table populated, dated,
-- bi-temporal (observation_date + _ingested_at present).

-- EMIT scenes: rows > 0, observation_date NOT NULL, exactly-once per file.
SELECT count(*) AS n_scenes,
       count(DISTINCT source_file) AS n_files,
       count(observation_date) AS n_dated,
       count(_ingested_at) AS n_ingested,
       min(observation_date) AS min_obs,
       max(observation_date) AS max_obs
FROM geospatial_docs.vapor_eyes_lf.emit_scenes;

-- Wells snapshot: rows > 0, observation_date = ingest date (NOT NULL).
SELECT count(*) AS n_wells_files,
       count(observation_date) AS n_dated,
       count(_ingested_at) AS n_ingested,
       max(observation_date) AS obs_date
FROM geospatial_docs.vapor_eyes_lf.wells_raw;

-- S2 SWIR assets: deploys clean; expected empty until Phase 3 lands S2.
SELECT count(*) AS n_s2_assets
FROM geospatial_docs.vapor_eyes_lf.s2_swir_assets;

-- S5P bronze must remain unbroken (regression guard).
SELECT count(*) AS n_granules, max(observation_date) AS latest_obs
FROM geospatial_docs.vapor_eyes_lf.s5p_granules;
