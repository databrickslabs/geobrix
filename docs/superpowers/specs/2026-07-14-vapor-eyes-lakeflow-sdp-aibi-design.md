# Vapor-Eyes Lakeflow SDP + AI/BI Dashboard — Design

**Date:** 2026-07-14
**Status:** Approved (brainstorming)
**Location of deliverable:** `notebooks/examples/vapor-eyes/lakeflow/`
**Branch:** `examples/vapor-eyes`

## Goal

Add a Lakeflow Declarative Pipeline (Spark Declarative Pipelines, Python) that
reproduces the entire vapor-eyes methane-detection cascade as a self-contained,
declarative medallion pipeline, materializes the key analytics as gold
materialized views, and drives an AI/BI (Lakeview) dashboard with maps. The
whole thing is packaged as a Databricks Asset Bundle (DAB) and deployed + run
live on the workspace. The vapor-eyes docs and README are updated to explain,
screenshot, and link the Lakeflow + AI/BI example.

Mirrors the notebook series at `notebooks/examples/vapor-eyes/` (NB01–05 +
`config_nb.ipynb`) but as a declarative pipeline, using the **lightweight
GeoBrix tier** (`geobrix[light,stac,vizx]`, pure Python/PySpark — pyrx/pyvx,
`databricks.labs.gbx.ds` readers) so it runs on Serverless with no JAR.

## Decisions (from brainstorming)

1. **Ingestion:** downloads run **inside the pipeline** (self-contained), confined
   to decorated function bodies (never module scope — Lakeflow evaluates module
   code repeatedly during planning).
2. **Schema:** dedicated **`geospatial_docs.vapor_eyes_lf`**, fully decoupled from
   the notebook series' `geospatial_docs.vapor_eyes`. Own Volume subtree.
3. **AOI:** the **full AOI** (`FULL_BBOX = (-103.60, 31.05, -102.60, 31.85)`,
   `FULL_AOI = True`). Downloaded **once**; re-runs are idempotent (repair-only,
   skip valid staged files). **Caveat, documented: this favors portability** — a
   user cloning the example gets a pipeline that pulls its own data rather than
   depending on the notebook series having pre-staged files.
4. **Gold MVs:** plume leaderboard, operator emissions rollup, field/county
   emissions rollup, hotspot ranking + AOI KPIs.
5. **Packaging:** Databricks Asset Bundle (pipeline + dashboard + target schema
   as code).
6. **Delivery scope:** build → deploy → run live into `vapor_eyes_lf` → deploy
   dashboard → capture real screenshots for docs.
7. **PMTiles output:** **fanout (sharded)** archives + a `pmtiles_shards` catalog
   table (app-ready) **and** a single **light overview** archive capped at
   overview→z12. Replaces the single bloated archive.

## Grounding facts

- **Lakeflow Python API:** `from pyspark import pipelines as dp`.
  - `@dp.materialized_view` — batch MVs (this pipeline is all batch snapshots).
  - `@dp.table` — streaming table (not used; no streams here).
  - `@dp.temporary_view` — transient views.
  - `@dp.expect(...)` / `@dp.expect_or_drop(...)` — data-quality gates.
  - Dataset functions must return a Spark DataFrame; upstream read via
    `spark.read.table("<name>")`.
  - **Constraint:** module code runs multiple times during planning → all
    imperative/network/side-effecting work lives inside function bodies only.
  - Parameters read via `spark.conf.get("<key>", "<default>")`, set from the
    pipeline `configuration` block in the bundle.
- **AI/BI maps** (`dashboards/manage/visualizations/maps`): two map types —
  **Point map** (needs lat/lon columns OR native `GEOMETRY`/`GEOGRAPHY` POINT)
  and **Choropleth** (needs admin boundaries OR native `GEOMETRY`/`GEOGRAPHY`
  POLYGON), both with a numeric measure for color/size. **WKB blobs and H3 cell
  IDs are NOT directly renderable** → gold layer must expose native `GEOMETRY`
  (via `st_geomfromwkb(wkb[, srid])`) and/or lat/lon columns.

## Architecture

Databricks Asset Bundle → one Lakeflow declarative pipeline (Serverless,
environment version 5, `geobrix[light,stac,vizx]` as a pipeline library) + one
AI/BI dashboard resource + target schema `vapor_eyes_lf`. `databricks bundle
deploy` then `databricks bundle run` reproduces the whole example.

Medallion layering: **bronze** (ingest + catalog raw) → **silver** (cascade
tables) → **gold** (analytics MVs) → **tiles** (MVT pyramid + PMTiles fanout +
overview).

### File layout

```
notebooks/examples/vapor-eyes/lakeflow/
  databricks.yml               # bundle: pipeline + dashboard + vars (catalog/schema/AOI/dates/zoom)
  README.md                    # deploy/run this example; screenshots; caveats
  transformations/
    _config.py                 # AOI (full), params, Volume paths, geobrix registration helpers
    bronze_ingest.py           # s5p_granules, s2_swir_assets, emit_scenes, wells_raw
    silver_cascade.py          # s5p_hotspots, s2_plume_cells, emit_plumes, plume_quant, wells_shl, plume_candidate_wells
    gold_analytics.py          # plume_leaderboard, operator_emissions, field_county_emissions, hotspot_ranking, aoi_kpis
    portfolio_tiles.py         # portfolio_mvt_tiles, pmtiles_shards (+ fanout archives), vapor_eyes_overview.pmtiles
  dashboards/
    vapor_eyes_lf.lvdash.json  # AI/BI dashboard definition
```

### Parameters (pipeline `configuration`, read via `spark.conf.get`)

| Key | Default | Meaning |
|---|---|---|
| `catalog` | `geospatial_docs` | UC catalog |
| `schema` | `vapor_eyes_lf` | pipeline target schema |
| `volume` | `data` | UC Volume (must pre-exist) |
| `full_aoi` | `true` | use FULL_BBOX |
| `date_window` | `2023-07-15/2023-08-20` | EMIT/S2/wells window |
| `s5p_temporal` | `2024-08-23/2024-08-24` | S5P granule window (per NB01) |
| `h3_res` | `6` | S5P hotspot H3 resolution |
| `qa_min` | `0.5` | S5P qa_value cut |
| `cloud_max` | `20` | S2 max cloud % |
| `s2_h3_res` | `10` | S2 plume-cell H3 resolution |
| `k_candidates` | `5` | nearest wells per plume |
| `min_z` / `max_z` | `6` / `13` | MVT pyramid zoom range |
| `overview_max_z` | `12` | light overview archive zoom cap |
| `earthdata_secret` | `geospatial_docs.vapor_eyes.earthdata_token` | UC secret scope.key for EMIT |

Volume subtree (own, not shared with notebooks):
`ETL_DIR = /Volumes/{catalog}/{schema}/{volume}/vapor-eyes-lf` with subdirs
`s5p/ sentinel2/ emit/ wells/ tiles/`.

## Data flow (DAG)

```
S5P (Planetary Computer)  → s5p_granules ─→ s5p_hotspots ─┬─────────────→ portfolio_mvt_tiles → pmtiles_shards
S2  (Planetary Computer)  → s2_swir_assets → s2_plume_cells  (windowed to top s5p_hotspot)   └→ vapor_eyes_overview.pmtiles
EMIT (NASA LP DAAC)       → emit_scenes    → emit_plumes ──┬─→ plume_quant ──┐
                                                           ├─→ plume_candidate_wells ←── wells_shl ←── wells_raw ←── TX RRC
wells_raw / wells_shl ─────────────────────────────────────────────────────────────────────────────→ portfolio_mvt_tiles

gold: plume_leaderboard ← plume_quant + plume_candidate_wells(rank=1)
      operator_emissions ← plume_candidate_wells(rank=1) + plume_quant
      field_county_emissions ← plume_candidate_wells(rank=1) + plume_quant
      hotspot_ranking ← s5p_hotspots ;  aoi_kpis ← plume_quant + wells_shl + s5p_hotspots + AOI
```

`s2_plume_cells` is a detection side-branch (not consumed downstream) but is
materialized per requirement (5). The top-hotspot selection for S2 windowing is
computed **inside** the `s2_swir_assets` / `s2_plume_cells` bodies (driver-side
argmax over `s5p_hotspots`), not as a cross-module Python value.

## Tables (all in `vapor_eyes_lf`)

### Bronze (downloads in `@dp.materialized_view` bodies; idempotent skip-guards)

- **`s5p_granules`** — one row per staged S5P `.nc` granule via
  `TropomiDownloader.download(FULL_BBOX, s5p/, temporal=s5p_temporal)`. Cols:
  `item_id, asset_name, ch4_path, out_file_sz, is_out_file_valid, last_update`.
- **`s2_swir_assets`** — Sentinel-2 B11/B12 SWIR COGs via `StacClient`, windowed
  to the top `s5p_hotspots` cell footprint. Cols: `item_id, asset_name, band_path`
  (+ download-contract cols).
- **`emit_scenes`** — EMIT L2B enhancement COG + plume-complex products catalog
  via `EmitDownloader.download(FULL_BBOX, emit/, temporal=date_window)`. Requires
  `EARTHDATA_TOKEN` env from UC secret. Cols include Volume paths + `is_out_file_valid`.
- **`wells_raw`** — TX RRC WellSHL merged GeoJSON catalog via
  `WellsDownloader.download(FULL_BBOX, wells/)`. Cols: staged path + validity.

### Silver (cascade; `@dp.materialized_view`, upstream via `spark.read.table`)

- **`s5p_hotspots`** — `tropomi.read(s5p/)` netCDF swath points → `qa_value ≥
  qa_min` filter → AOI clip → `h3_longlatash3(res=h3_res)` bin → per-cell agg.
  Cols: `h3_cellid` (bigint), `ch4_mean`, `ch4_max`, `n_obs`, `geom_wkb` (H3
  center point WKB). `@dp.expect`: `ch4_mean` non-null, `n_obs > 0`.
- **`s2_plume_cells`** — `(B11−B12)/(B11+B12)` via `rx.rst_mapalgebra` →
  `gbx_rst_h3_tessellate` (res `s2_h3_res`) → `gbx_rst_summary`. Cols:
  `h3_cellid`, `stats` (struct).
- **`emit_plumes`** — `emit.read_plumes(emit/)` (PLM GeoJSON + JPL estimates).
  Cols: `plume_id, max_conc_ppmm, emission_rate_kg_hr, emission_rate_uncert_kg_hr,
  wind_speed_ms, fetch_length_m, lon_max, lat_max, plume_geom` (outline WKB).
  `@dp.expect`: `emission_rate_kg_hr >= 0`, `plume_geom` non-null.
- **`plume_quant`** — `emit_plumes` × `emit.read_enh` ENH tiles → `rx.rst_clip` →
  `rx.rst_summary` → per-plume max-segment select. Cols: `emit_plumes` cols +
  `gbx_mean_ppmm, gbx_max_ppmm`.
- **`wells_shl`** — `wells.read(wells/)` via `geojson_gbx`. Cols: `api, operator,
  lease, well_no, field, county, well_url, well_geom` (WKB point).
- **`plume_candidate_wells`** — `emit_plumes` × `wells_shl` → `st_distancesphere`
  → per-plume `row_number() ≤ k_candidates`. Cols: plume attrs + well attrs +
  `well_lon, well_lat, dist_m, rank`.

### Gold MVs (map-ready: native `GEOMETRY` + lat/lon; no WKB/H3 blobs exposed)

- **`plume_leaderboard`** — one row per plume: `plume_id, emission_rate_kg_hr,
  emission_rate_uncert_kg_hr, max_conc_ppmm, gbx_max_ppmm, gbx_mean_ppmm,
  wind_speed_ms, fetch_length_m, lead_operator, lead_lease, lead_field,
  lead_dist_m, lon_max, lat_max, origin_geom` (GEOMETRY POINT via
  `st_point`/`st_geomfromwkb`), `plume_geom_native` (GEOMETRY POLYGON via
  `st_geomfromwkb(plume_geom)`). Source: `plume_quant` + `plume_candidate_wells`
  filtered `rank = 1`.
- **`operator_emissions`** — group by rank-1 `operator`: `total_emission_kg_hr`,
  `max_emission_kg_hr`, `plume_count`, `well_count`.
- **`field_county_emissions`** — group by `field, county`: `total_emission_kg_hr`,
  `plume_count`.
- **`hotspot_ranking`** — top S5P cells: `h3_cellid, ch4_mean, ch4_max, n_obs,
  center_lon, center_lat, hex_geom` (GEOMETRY POLYGON from
  `h3_boundaryaswkb(h3_cellid)` → `st_geomfromwkb`), ranked by `ch4_max`.
- **`aoi_kpis`** — single row: `total_plumes, total_emission_kg_hr, wells_scanned,
  hotspot_cells, aoi_area_km2`.

### Tiles

- **`portfolio_mvt_tiles`** — three MVT layers (hotspots hexagons via
  `h3_boundaryaswkb`, plume outlines, wells points) → `gbx_st_asmvt_pyramid`
  (`min_z`..`max_z`), unioned. Cols: `layer, z, x, y, mvt_bytes`.
- **`pmtiles_shards`** — catalog of the fanout archives: `shard_id, min_x, min_y,
  max_x, max_y` (tile/geo bounds), `archive_path` (Volume), `layer_feature_counts`,
  `min_z, max_z`. Fanout write: shard `portfolio_mvt_tiles` spatially (by tile
  prefix) → one bounded `.pmtiles` per shard under `tiles/shards/`. Binary-free
  (no `tile-join` — segfaults on Serverless). App-ready product.
- **`vapor_eyes_overview.pmtiles`** — single light archive: all layers, whole AOI,
  overview→`overview_max_z` (12). Written to `tiles/vapor_eyes_overview.pmtiles`
  (single-file FUSE write in the last table body). The "show everything" view.

## AI/BI dashboard (`vapor_eyes_lf.lvdash.json`, deployed via bundle)

Datasets = the gold MVs (+ `plume_candidate_wells` for the wells page). Three pages:

- **Page 1 — Regional screen:** KPI counter tiles (`aoi_kpis`), hotspot
  **choropleth** (`hotspot_ranking.hex_geom` colored by `ch4_max`), top-cells table.
- **Page 2 — Quantify & attribute:** plume-origin **point map** +
  plume-outline **choropleth** (`plume_leaderboard`), emission-rate leaderboard
  table, operator-emissions **bar** (`operator_emissions`), field/county **bar**
  (`field_county_emissions`).
- **Page 3 — Wells & candidates:** wells **point map** + nearest-candidate table
  (`plume_candidate_wells`, using `well_lon/well_lat`).

## Best practices applied

- Serverless + environment version 5; batch **materialized views** (not streaming
  tables — satellite snapshots).
- All parameters via pipeline `configuration` → `spark.conf.get`.
- All imperative/network work confined to decorated function bodies.
- `@dp.expect` data-quality gates on silver.
- No `spark.conf.set` / `_jvm` / `.rdd` in product paths (lightweight tier
  constraint); `repartition` only where needed for fanout.

## Verification

1. `databricks bundle validate` then `deploy` to the workspace (auth:
   `mjohns-databricks` account context; Serverless, env v5).
2. `databricks bundle run` the pipeline; confirm **every** table populates in
   `vapor_eyes_lf` (bronze → silver → gold → tiles), and `pmtiles_shards` +
   `vapor_eyes_overview.pmtiles` land on the Volume.
3. Open/deploy the AI/BI dashboard; verify each map/chart renders against the
   populated MVs; capture real screenshots.

## Docs (requirement 8)

- New `notebooks/examples/vapor-eyes/lakeflow/README.md` — deploy/run steps,
  parameters, the portability caveat, screenshots.
- Update `notebooks/examples/vapor-eyes/README.md` — a section introducing the
  Lakeflow + AI/BI example, cross-linking `lakeflow/`.
- New/updated page under `docs/docs/` explaining the example (Lakeflow SDP +
  AI/BI dashboard, maps with native spatial types), screenshots, cross-links.
  Respect user-facing voice (no internal/wave vocabulary).

## Risks / open items

- **Downloads in a declarative pipeline are unconventional.** Mitigated by
  function-body confinement + idempotent skip-guards. First full run downloads
  the full AOI (accepted for portability).
- **EMIT Earthdata secret** must be readable by the pipeline; bronze `emit_scenes`
  fails without it.
- **Fanout sharding granularity** (tile-prefix vs geo-grid) to be finalized in the
  plan; must stay binary-free (no `tile-join`).
- **AI/BI dashboard-as-code**: `.lvdash.json` authored + deployed via bundle;
  exact widget schema pinned during implementation using the lakeview-dashboard
  skill.
- **Single-file writer memory ceiling** on Serverless for the overview archive —
  capped zoom (≤12) keeps it small; monitor.
