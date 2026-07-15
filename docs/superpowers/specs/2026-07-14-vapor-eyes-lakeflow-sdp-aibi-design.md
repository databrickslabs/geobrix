# Vapor-Eyes Lakeflow SDP + AI/BI Dashboard — Design

**Date:** 2026-07-14
**Status:** Approved (brainstorming)
**Location of deliverable:** `notebooks/examples/vapor-eyes/lakeflow/`
**Branch:** `examples/vapor-eyes`

## Goal

Add a Lakeflow Declarative Pipeline (Spark Declarative Pipelines, Python) that
reproduces the vapor-eyes methane-detection cascade as a self-contained,
**incremental, as-of-aware** medallion pipeline: it ingests new satellite/well
data on a daily cadence while preserving full history, supports historic
backfill, materializes both *latest* (operational) and *trend* (time-series)
analytics as gold materialized views, prepares an app-ready PMTiles product, and
drives an AI/BI (Lakeview) dashboard with maps and trends. Packaged as a
Databricks Asset Bundle (DAB), deployed + run live, and documented with real
screenshots.

Mirrors the notebook series at `notebooks/examples/vapor-eyes/` (NB01–05 +
`config_nb.ipynb`) but as a declarative pipeline, using the **lightweight
GeoBrix tier** (`geobrix[light,stac,vizx]`, pure Python/PySpark — pyrx/pyvx,
`databricks.labs.gbx.ds` readers) so it runs on Serverless with no JAR.

## Decisions (from brainstorming)

1. **Ingestion:** **bundle job with two tasks** — Task 1 a date-parameterized
   Python **land** task (runs the downloaders idempotently for the requested
   `--window`/`--asof`, stages new files to the Volume), Task 2 the **Lakeflow
   pipeline** (Auto Loader streaming tables incrementally consume). One
   self-contained bundle; scheduled daily. *(Refines an earlier "downloads
   inside the pipeline body" idea — daily-incremental requires separating "land
   new files" from "declaratively process files".)*
2. **Schema:** dedicated **`geospatial_docs.vapor_eyes_lf`**, decoupled from the
   notebook series' `geospatial_docs.vapor_eyes`. Own Volume subtree.
3. **AOI:** the **full AOI** (`FULL_BBOX = (-103.60, 31.05, -102.60, 31.85)`).
   Downloaded once, idempotent repair-only re-runs. **Caveat, documented: favors
   portability** — a user cloning the example gets a pipeline that pulls its own
   data rather than depending on the notebook series having pre-staged files.
4. **Temporal / as-of:** bi-temporal (observation time + ingestion time),
   incremental daily refresh with full history retained, historic backfill via
   the same code path with a wide window. See "Temporal model" below.
5. **Gold MVs:** *latest* (operational) + *trend* (time-series) families —
   plume leaderboard, operator emissions, field/county emissions, hotspot
   ranking + AOI KPIs, plus daily-trend variants.
6. **Packaging:** Databricks Asset Bundle (job + pipeline + dashboard + target
   schema as code).
7. **Delivery scope:** build → deploy → run live into `vapor_eyes_lf` → deploy
   dashboard → capture real screenshots.
8. **PMTiles output:** **fanout (sharded)** archives + `pmtiles_shards` catalog
   table (app-ready) **and** a single **light overview** archive capped at
   overview→z12.
9. **Phasing:** built and validated in 7 incremental phases (see "Phasing").

## Grounding facts

- **Lakeflow Python API:** `from pyspark import pipelines as dp`.
  - `@dp.materialized_view` — batch MVs (gold, and full-recompute silver).
  - `@dp.table` — **streaming table** (bronze ingest + append-only silver).
  - `@dp.temporary_view` — transient views.
  - `@dp.expect(...)` / `@dp.expect_or_drop(...)` — data-quality gates.
  - `dp.create_auto_cdc_flow(...)` — SCD (APPLY CHANGES) for the wells reference.
  - Dataset functions must return a Spark DataFrame; upstream batch read via
    `spark.read.table(...)`, streaming read via `spark.readStream.table(...)`.
  - **Constraint:** module code runs multiple times during planning → all
    imperative/side-effecting work lives inside function bodies only (and the
    downloads live in the separate land task, not in the pipeline at all).
  - Parameters read via `spark.conf.get("<key>", "<default>")`, set from the
    pipeline `configuration` block in the bundle.
- **Auto Loader** (`cloudFiles`) over each Volume source dir gives incremental,
  exactly-once file ingestion — the daily run processes only newly-landed files.
- **AI/BI maps** (`dashboards/manage/visualizations/maps`): **Point map** (needs
  lat/lon columns OR native `GEOMETRY`/`GEOGRAPHY` POINT) and **Choropleth**
  (needs admin boundaries OR native `GEOMETRY`/`GEOGRAPHY` POLYGON), both with a
  numeric measure. **WKB blobs and H3 cell IDs are NOT directly renderable** →
  gold exposes native `GEOMETRY` (via `st_geomfromwkb(wkb[, srid])`) and lat/lon.

## Temporal model (as-of / daily / backfill)

Two time axes on every fact row:

- **`observation_date`** — event time, parsed from the granule/scene (S5P/S2/EMIT
  acquisition date). The axis for trend analysis and the real-world "as-of".
- **`_ingested_at`** — system time (`current_timestamp()`) when the pipeline
  processed the row. Enables "what did we know at time T" / audit.

Behavior:

- **Daily refresh:** the land task runs with a narrow window (e.g. yesterday),
  stages only new granules; Auto Loader bronze appends only new files; silver
  appends new `observation_date` partitions. Nothing is overwritten — history
  accumulates.
- **Backfill:** the land task runs with a wide historic window; the identical
  downstream code processes the extra granules as additional `observation_date`
  partitions. No special-casing. Daily vs backfill differ only by the `--window`
  parameter (and a full-refresh on affected streaming tables if reprocessing).
- **Wells (slowly-changing reference):** modeled **SCD Type 2** via
  `dp.create_auto_cdc_flow` keyed on `api` with `__START_AT`/`__END_AT`.
  Attribution joins a plume (at its `observation_date`) to the well row **valid
  as of that date** — genuinely as-of operator attribution. *Caveat: TX RRC
  serves current state only, so SCD2 history starts accumulating from first
  pipeline run; pre-history ownership is not recoverable.*
- **Latest vs historic in gold:** *latest* MVs pick the most-recent observation
  per spatial unit (window `row_number() over (partition by unit order by
  observation_date desc) = 1`); *trend* MVs group by `observation_date`.

## Architecture

Databricks Asset Bundle → a **job** (`vapor_eyes_lf_job`, scheduled daily) with
Task 1 `land` (Python wheel/script task, `--window`/`--asof` params, runs the
downloaders idempotently) → Task 2 the **Lakeflow pipeline** (Serverless,
environment version 5, `geobrix[light,stac,vizx]` library; Auto Loader streaming
bronze). Plus an **AI/BI dashboard** resource and target schema `vapor_eyes_lf`.
`databricks bundle deploy` + `bundle run` reproduces the whole example.

Layering: **bronze** (Auto Loader ingest + catalog, append-only) → **silver**
(cascade, temporal append + SCD2 wells) → **gold** (latest + trend MVs) →
**tiles** (MVT pyramid + PMTiles fanout + overview).

### File layout

```
notebooks/examples/vapor-eyes/lakeflow/
  databricks.yml               # bundle: job (land->pipeline) + pipeline + dashboard + vars
  README.md                    # deploy/run/schedule/backfill; screenshots; caveats
  land/
    land.py                    # date-parameterized downloader driver (Task 1)
  transformations/
    _config.py                 # AOI, params, Volume paths, geobrix registration helpers
    bronze_ingest.py           # s5p_granules, s2_swir_assets, emit_scenes, wells_raw (Auto Loader STs)
    silver_cascade.py          # s5p_hotspots, s2_plume_cells, emit_plumes, plume_quant, wells_shl(SCD2), plume_candidate_wells
    gold_analytics.py          # *_latest + *_daily MVs, aoi_kpis
    portfolio_tiles.py         # portfolio_mvt_tiles, pmtiles_shards (+ fanout), vapor_eyes_overview.pmtiles
  dashboards/
    vapor_eyes_lf.lvdash.json  # AI/BI dashboard (date filter + trends)
```

### Parameters (pipeline `configuration` / job params, read via `spark.conf.get`)

| Key | Default | Meaning |
|---|---|---|
| `catalog` | `geospatial_docs` | UC catalog |
| `schema` | `vapor_eyes_lf` | pipeline target schema |
| `volume` | `data` | UC Volume (must pre-exist) |
| `full_aoi` | `true` | use FULL_BBOX |
| `date_window` | `2023-07-15/2023-08-20` | EMIT/S2/wells window (land task) |
| `s5p_temporal` | `2024-08-23/2024-08-24` | S5P granule window (land task) |
| `h3_res` | `6` | S5P hotspot H3 resolution |
| `qa_min` | `0.5` | S5P qa_value cut |
| `cloud_max` | `20` | S2 max cloud % |
| `s2_h3_res` | `10` | S2 plume-cell H3 resolution |
| `k_candidates` | `5` | nearest wells per plume |
| `min_z` / `max_z` | `6` / `13` | MVT pyramid zoom range |
| `overview_max_z` | `12` | light overview archive zoom cap |
| `earthdata_secret` | `geospatial_docs.vapor_eyes.earthdata_token` | UC secret scope.key for EMIT |

Volume subtree (own): `ETL_DIR = /Volumes/{catalog}/{schema}/{volume}/vapor-eyes-lf`
with subdirs `s5p/ sentinel2/ emit/ wells/ tiles/`.

## Data flow (DAG)

```
land task (daily/backfill window) → stages granules to Volume subtree
S5P  → s5p_granules(ST) ─→ s5p_hotspots(obs_date) ─┬──────────→ portfolio_mvt_tiles → pmtiles_shards
S2   → s2_swir_assets(ST) → s2_plume_cells(obs_date)  (windowed to latest top hotspot)  └→ vapor_eyes_overview.pmtiles
EMIT → emit_scenes(ST)  ─→ emit_plumes(obs_date) ──┬─→ plume_quant(obs_date) ─┐
                                                    ├─→ plume_candidate_wells(as-of) ← wells_shl(SCD2) ← wells_raw(ST)
wells_raw(ST)/wells_shl ─────────────────────────────────────────────────────────────→ portfolio_mvt_tiles

gold latest: plume_leaderboard_latest, operator_emissions_latest, field_county_emissions_latest,
             hotspot_latest, aoi_kpis_latest
gold trend:  emissions_trend_daily, operator_emissions_daily, hotspot_trend
```

`s2_plume_cells` is a detection side-branch (materialized per requirement 5, not
consumed downstream). The S2 windowing target is the **latest** top
`s5p_hotspots` cell, computed inside the land/silver body over the current
hotspot state.

## Tables (all in `vapor_eyes_lf`)

### Bronze — Auto Loader streaming tables (`@dp.table`, append-only)

Each reads its Volume source dir via `cloudFiles`; one row per newly-landed file;
every row tagged `observation_date`, `_ingested_at`, `_source_file`.

- **`s5p_granules`** — staged S5P `.nc` granules. `item_id, asset_name, ch4_path,
  out_file_sz, is_out_file_valid, observation_date, _ingested_at`.
- **`s2_swir_assets`** — Sentinel-2 B11/B12 SWIR COGs. `item_id, asset_name,
  band_path, observation_date, _ingested_at` (+ contract cols).
- **`emit_scenes`** — EMIT L2B enhancement COG + plume-complex products catalog.
  Volume paths + `is_out_file_valid, observation_date, _ingested_at`.
- **`wells_raw`** — TX RRC WellSHL merged GeoJSON snapshots. staged path +
  validity + `_ingested_at` (snapshot date).

### Silver — cascade (temporal)

- **`s5p_hotspots`** (`@dp.table`, append; partitioned by `observation_date`) —
  `tropomi.read` swath points → `qa_value ≥ qa_min` → AOI clip →
  `h3_longlatash3(res=h3_res)` → group by `(h3_cellid, observation_date)`. Cols:
  `h3_cellid, observation_date, ch4_mean, ch4_max, n_obs, geom_wkb`. `@dp.expect`:
  `ch4_mean` non-null, `n_obs > 0`.
- **`s2_plume_cells`** (append; by `observation_date`) —
  `(B11−B12)/(B11+B12)` via `rx.rst_mapalgebra` → `gbx_rst_h3_tessellate`
  (res `s2_h3_res`) → `gbx_rst_summary`. Cols: `h3_cellid, observation_date, stats`.
- **`emit_plumes`** (append; by `observation_date`) — `emit.read_plumes`. Cols:
  `plume_id, observation_date, max_conc_ppmm, emission_rate_kg_hr,
  emission_rate_uncert_kg_hr, wind_speed_ms, fetch_length_m, lon_max, lat_max,
  plume_geom`. `@dp.expect`: `emission_rate_kg_hr >= 0`, `plume_geom` non-null.
- **`plume_quant`** (append; by `observation_date`) — `emit_plumes` × `emit.read_enh`
  → `rx.rst_clip` → `rx.rst_summary` → per-plume max-segment. Adds `gbx_mean_ppmm,
  gbx_max_ppmm`.
- **`wells_shl`** (**SCD2** via `dp.create_auto_cdc_flow`, key `api`) — `wells.read`
  (`geojson_gbx`). Cols: `api, operator, lease, well_no, field, county, well_url,
  well_geom, __START_AT, __END_AT`.
- **`plume_candidate_wells`** (append; by `observation_date`) — `emit_plumes` ×
  `wells_shl` **as-of** (well valid at plume `observation_date`) →
  `st_distancesphere` → per-plume `row_number() ≤ k_candidates`. Cols: plume attrs
  + well attrs + `well_lon, well_lat, dist_m, rank`.

### Gold — latest (operational) + trend (time-series) MVs

Each *latest*/ranking MV exposes native `GEOMETRY` + lat/lon (no WKB/H3 blobs).

- **`plume_leaderboard_latest`** — latest observation per plume: emission rate
  (+uncert), max conc, GeoBrix cross-check, lead operator/lease/field + dist,
  `origin_geom` (GEOMETRY POINT), `plume_geom_native` (GEOMETRY POLYGON),
  `lon_max/lat_max`, `observation_date`.
- **`operator_emissions_latest`** — rank-1 operator rollup (latest): total/max
  emission, plume & well counts.
- **`field_county_emissions_latest`** — by `field, county` (latest).
- **`hotspot_latest`** — latest S5P cells: `h3_cellid, ch4_mean, ch4_max, n_obs,
  center_lon, center_lat, hex_geom` (GEOMETRY POLYGON from `h3_boundaryaswkb`),
  ranked by `ch4_max`.
- **`aoi_kpis_latest`** — 1 row: `total_plumes, total_emission_kg_hr,
  wells_scanned, hotspot_cells, aoi_area_km2, latest_observation_date`.
- **`emissions_trend_daily`** — per `observation_date`: total & max emission,
  plume count.
- **`operator_emissions_daily`** — per `(observation_date, operator)` emission
  totals (multi-series trend).
- **`hotspot_trend`** — per `(observation_date, h3_cellid)` CH4 measures.

### Tiles

Built from **latest** state (each MVT feature carries `observation_date` as an
attribute so an app can filter; per-date tiling is a future extension).

- **`portfolio_mvt_tiles`** (`@dp.materialized_view`) — three layers (hotspot
  hexagons, plume outlines, wells) → `gbx_st_asmvt_pyramid` (`min_z`..`max_z`),
  unioned. `layer, z, x, y, mvt_bytes`.
- **`pmtiles_shards`** — catalog of fanout archives: `shard_id, min_x/min_y/max_x/
  max_y, archive_path, layer_feature_counts, min_z, max_z`. Fanout: shard
  `portfolio_mvt_tiles` spatially (tile prefix) → one bounded `.pmtiles` per shard
  under `tiles/shards/`. Binary-free (no `tile-join` — segfaults on Serverless).
  App-ready.
- **`vapor_eyes_overview.pmtiles`** — single light archive: all layers, whole AOI,
  overview→`overview_max_z` (12). Written to `tiles/vapor_eyes_overview.pmtiles`
  (single-file FUSE write in the last table body).

## Phasing (incremental build + validation)

1. **Vertical slice (S5P only).** DAB scaffold (`databricks.yml`, Serverless env
   v5, geobrix light library), `land.py` for S5P only, bronze `s5p_granules`
   (Auto Loader ST), minimal silver `s5p_hotspots` (append by `observation_date`).
   Deploy + run live; validate incremental behavior (run twice → no re-download,
   history appends). Proves the whole pattern end-to-end on one source.
2. **All downloads + full bronze.** Extend `land.py` to S2/EMIT/wells (EMIT secret
   wiring); all four bronze Auto Loader STs.
3. **Silver cascade (temporal).** `s2_plume_cells`, `emit_plumes`, `plume_quant`,
   `wells_shl` (SCD2), `plume_candidate_wells` (as-of attribution) + `@dp.expect`.
4. **Gold analytics.** *latest* + *daily/trend* MVs, `aoi_kpis_latest`.
5. **Tiles / synthesis.** `portfolio_mvt_tiles`, `pmtiles_shards` (fanout),
   `vapor_eyes_overview.pmtiles` (app-ready product).
6. **AI/BI dashboard.** `.lvdash.json` with date filter + operational maps
   (latest) + Trends page (daily); deploy + screenshot.
7. **Docs.** `lakeflow/README.md`, update vapor-eyes `README.md`, docs page under
   `docs/docs/`, screenshots + cross-links.

Each phase is independently deployable/validatable and suits subagent-driven
execution.

## AI/BI dashboard (`vapor_eyes_lf.lvdash.json`, deployed via bundle)

A **date-range filter** (default = latest observation) scopes the operational
pages. Pages:

- **Regional screen (latest):** KPI counters (`aoi_kpis_latest`), hotspot
  choropleth (`hotspot_latest.hex_geom` by `ch4_max`), top-cells table.
- **Quantify & attribute (latest):** plume-origin point map + plume-outline
  choropleth (`plume_leaderboard_latest`), emission-rate leaderboard table,
  operator-emissions bar (`operator_emissions_latest`), field/county bar.
- **Wells & candidates (latest):** wells point map + nearest-candidate table
  (`plume_candidate_wells`, `well_lon/well_lat`).
- **Trends:** emissions-over-time line (`emissions_trend_daily`), per-operator
  multi-series line (`operator_emissions_daily`), hotspot CH4 over time.

## Best practices applied

- Serverless + environment version 5; Auto Loader streaming bronze; batch
  materialized views for gold; SCD2 (APPLY CHANGES) for the wells reference.
- Land/process separation; downloads only in the land task; no side effects in
  pipeline module scope.
- All parameters via job/pipeline `configuration` → `spark.conf.get`.
- `@dp.expect` data-quality gates on silver.
- No `spark.conf.set` / `_jvm` / `.rdd` in product paths (lightweight tier);
  `repartition` only where needed for fanout.

## Verification

1. `databricks bundle validate` → `deploy` (auth: `mjohns-databricks`; Serverless,
   env v5).
2. `bundle run` the job (land → pipeline); confirm **every** table populates in
   `vapor_eyes_lf`; confirm `pmtiles_shards` + `vapor_eyes_overview.pmtiles` land
   on the Volume.
3. **Incrementality:** run the job twice → second run re-downloads nothing and
   appends no duplicate `observation_date` rows.
4. **Backfill:** run once with a wide `date_window` → extra `observation_date`
   partitions appear; trend MVs show multiple dates.
5. Deploy the AI/BI dashboard; verify each map/chart/trend renders against the
   populated MVs; capture real screenshots.

## Docs (requirement 8)

- New `notebooks/examples/vapor-eyes/lakeflow/README.md` — deploy/run/schedule,
  daily vs backfill, parameters, the portability caveat, SCD2/as-of note,
  screenshots.
- Update `notebooks/examples/vapor-eyes/README.md` — section introducing the
  Lakeflow + AI/BI example, cross-linking `lakeflow/`.
- New/updated page under `docs/docs/` explaining the example (incremental
  Lakeflow SDP + as-of modeling + AI/BI dashboard with native spatial maps),
  screenshots, cross-links. Respect user-facing voice (no internal/wave vocab).

## Risks / open items

- **Land/pipeline coordination:** the job guarantees land completes before the
  pipeline; the pipeline itself performs no network I/O.
- **EMIT Earthdata secret** must be readable by the land task; `emit_scenes`
  empty without staged files.
- **SCD2 wells history** starts at first run (source is current-state only) —
  documented limitation.
- **Fanout sharding granularity** (tile-prefix vs geo-grid) finalized in the plan;
  stays binary-free (no `tile-join`).
- **AI/BI dashboard-as-code:** `.lvdash.json` widget schema pinned during
  implementation via the lakeview-dashboard skill; date-filter binding verified
  live.
- **Single-file overview archive** memory ceiling on Serverless — capped zoom
  (≤12) keeps it small; monitor.
- **Auto Loader on `/Volumes`** paths: confirm `cloudFiles` schema/checkpoint
  locations resolve under the UC Volume on Serverless during Phase 1.
