# Vapor-Eyes Lakeflow — Permian Methane Monitoring (SDP + AI/BI)

A standalone, production-grade **Permian Basin methane-monitoring pipeline**: a [Lakeflow Declarative Pipeline](https://docs.databricks.com/aws/en/dlt/) (SDP) plus an [AI/BI dashboard](https://docs.databricks.com/aws/en/dashboards/), built as a [Databricks Asset Bundle](https://docs.databricks.com/aws/en/dev-tools/bundles/) and running entirely on the GeoBrix **lightweight tier** over **Serverless** compute.

This is not the notebook cascade documented in [`../README.md`](../README.md) — it's a different artifact with a different purpose. The notebook series is a five-step teaching walkthrough of one overpass. **This pipeline is the production shape**: it runs on a schedule, ingests incrementally across a multi-year date range, cascades bronze → silver → gold through a declared dependency graph with data-quality expectations, and serves a live dashboard off Delta tables instead of one-off notebook outputs. It also adds a fifth data source — Carbon Mapper Tanager — that the notebook cascade does not use, making it the *current* (through 2026) view of Permian methane activity rather than a single historical case study.

---

## Architecture

```
databricks bundle  →  vapor_eyes_lf_job
                         ├─ Task 1 "land"      (Serverless spark_python_task)
                         │    downloads/stages all 5 sources for the configured
                         │    date window(s) to a Unity Catalog Volume
                         └─ Task 2 "pipeline"  (depends on land)
                              runs vapor_eyes_lf_pipeline (the Lakeflow SDP)
```

- **Task 1 — `land`** (`land/land.py`): a date-parameterized downloader driver. Takes `--sources`, `--window` / `--s5p-windows` / `--emit-windows` / `--cm-window`, and secret references as CLI parameters (all wired from bundle variables), and stages raw files/metadata to `/Volumes/<catalog>/<schema>/data/vapor-eyes-lf/{s5p,sentinel2,emit,wells,cm}`. Runs on its own Serverless environment (`land_env`, environment version 5) with the same GeoBrix wheel installed.
- **Task 2 — `pipeline`**: the Lakeflow Declarative Pipeline itself (`vapor_eyes_lf_pipeline`, `serverless: true`), rooted at `./transformations`. Its environment installs `${var.gbx_wheel}[light,stac,vizx]` — the GeoBrix wheel with the `light`, `stac`, and `vizx` extras — as a **single pip dependency entry**. Lakeflow resolves one `%pip install` per dependency entry, so splitting the extras into separate entries breaks pip's resolver (a rebuild is forced once `rasterio` is already pinned); the bracketed single-entry form keeps it one pass.

### Medallion layers

- **Bronze** (`transformations/bronze_ingest.py`) — Auto Loader **metadata tables**: one row per staged file/scene with the raw path payload plus hoisted fields (`item_id`, `product_type`, `band`, `granule_id`, …). Five tables: `s5p_granules`, `emit_scenes`, `wells_raw`, `cm_scenes`, `s2_swir_assets`.
- **Silver** (`transformations/silver_cascade.py`) — the cascade, wired bronze → silver by reading the bronze table (not the Volume directly), so lineage is real. Every table carries a bi-temporal pair: `observation_date` (when the underlying satellite/detection event happened) and `_ingested_at` (when GeoBrix/Lakeflow processed it) — the two diverge by design here since seasons are backfilled well after the fact. Key tables: `s5p_hotspots` (H3 CH4 screen), `s2_plume_cells`, `emit_plumes` + `plume_quant` (EMIT cross-check, with a configurable CH4-enhancement QC floor), `wells_shl` (SCD2 via `create_auto_cdc_flow`, so well operator/lease history is versioned) + `plume_candidate_wells` (as-of nearest-well attribution against the SCD2 wells at the plume's `observation_date`), and `cm_detections` + `cm_candidate_wells` (Carbon Mapper).
- **Gold** (`transformations/gold_analytics.py`) — analytics MVs described below.
- **Tiles** (`transformations/portfolio_tiles.py`) — `portfolio_mvt_tiles` (MVT pyramid per layer via `gbx_st_asmvt_pyramid`), `pmtiles_shards`, and `overview_manifest` (a shareable PMTiles portfolio, sharded for web-scale delivery — see [PMTiles spatial sharding](https://protomaps.com/docs/pmtiles) in the parent series for the single-archive version).

---

## Data sources

| Source | Provider / license | Role |
|---|---|---|
| **Sentinel-5P TROPOMI** | Copernicus (ESA), open | Regional CH4 screen — wide-area H3 hotspot surface (`s5p_hotspots`), the first-pass "where is elevated methane" signal. |
| **Sentinel-2 SWIR** | Copernicus (ESA), open | Targeted SWIR band-ratio detection (`(B11−B12)/(B11+B12)`) at the strongest hotspot — an illustrative proxy, not an operational retrieval. |
| **EMIT** | NASA LP DAAC, open — requires an Earthdata Login token (BYOT) | Spectral **validation**: JPL's plume-complex product plus a GeoBrix `rst_clip` / `rst_summary` cross-check of the enhancement raster against JPL's reported max concentration. EMIT is a science instrument on the ISS with sparse revisit and roughly a 10-month lag before plume products are published, so it validates historical detections rather than driving current status. |
| **Carbon Mapper Tanager** | Carbon Mapper, public-good — requires a free registered token (BYOT) | The **authoritative current rated-plume layer**: quantified, wind-corrected `emission_rate_kg_hr` per detection, current through 2026. This is what the dashboard's leaderboard and monitoring-status panels are built on. See [Good-citizen use](#good-citizen-use--carbon-mapper) below. |
| **TX RRC WellSHL** | Texas Railroad Commission, public (ArcGIS REST, no auth) | Attribution — nearest-well / operator lookup for plume origins. |

---

## Key analytics (gold)

- **`operator_emissions_leaderboard`** — the headline table. Operators are ranked by **number of high-confidence Carbon Mapper plume detections** (`detection_rank`, `plume_count`), with `mean_emission_kg_hr` / `max_emission_kg_hr` as the per-detection intensity columns. A detection's `emission_rate_kg_hr` is an instantaneous rate at one overpass — summing it across 2024–2026 would double-count repeat detections of the same source and is **not** a continuous flow rate, so that sum is kept only as a clearly-labeled, non-ranking secondary column (`cumulative_detected_rate_kg_hr`).
- **`cm_monitoring_status`** — single-row current-status panel: `last_detection_date`, `days_since_last_detection`, `plumes_last_90d`, `active_operators_last_90d`, `total_plumes_all_time` — the "is anything active right now" read.
- **`cm_activity_monthly`** — per-month plume count / emission-rate / active-operator counts — the activity timeline.
- **`hotspot_latest`** / **`hotspot_persistence`** — S5P H3 hexagon choropleths: the current regional CH4 screen, and a persistence view distinguishing chronic emitters (elevated on most overpasses) from transient ones.
- **`plume_leaderboard_latest`**, **`operator_intensity_latest`**, **`field_county_intensity_latest`**, **`aoi_kpis_latest`**, **`regional_ch4_trend_daily`**, **`plume_detection_timeline`**, **`hotspot_trend`** — supporting concentration-framed views feeding the S5P/EMIT side of the dashboard.

All map-facing columns are native `GEOMETRY` (tagged `SRID 4326`) or plain lon/lat doubles — AI/BI cannot render raw WKB or H3 cell IDs directly.

---

## Dashboard

`dashboards/vapor_eyes_lf.lvdash.json` — three pages, each backed by the gold MVs above, using AI/BI's native-geometry map widgets (H3 hexagon choropleths for the regional screen, a point map for Carbon Mapper detections).

### Current Status & Leakiest Operators

KPI tiles from `cm_monitoring_status` (plumes in the last 90 days, active operators, days since last detection, all-time plume count), the `operator_emissions_leaderboard` table and bar chart, and a Carbon Mapper point map (`cm_plume_attributed`, colored by emission rate) with the required attribution note.

![Current Status & Leakiest Operators](../../../resources/images/diagrams/vapor-eyes/lakeflow-dashboard-current-status.png)
<!-- TODO-screenshot: capture from the deployed dashboard, page 1 -->

### Activity Over Time

`cm_activity_monthly` bar/line combo (plume count + emission rate by month) and the `regional_ch4_trend_daily` regional CH4 trend line, scoped by a shared date-range filter.

![Activity Over Time](../../../resources/images/diagrams/vapor-eyes/lakeflow-dashboard-activity.png)
<!-- TODO-screenshot: capture from the deployed dashboard, page 2 -->

### Regional Screen (Sentinel-5P)

`aoi_kpis_latest` KPI tiles plus the `hotspot_latest` and `hotspot_persistence` H3 hexagon choropleths — the wide-area CH4 screen and chronic-vs-transient emitter view. No Carbon Mapper data on this page, so no attribution widget here (attribution is shown only where Carbon Mapper data appears).

![Regional Screen (Sentinel-5P)](../../../resources/images/diagrams/vapor-eyes/lakeflow-dashboard-regional-screen.png)
<!-- TODO-screenshot: capture from the deployed dashboard, page 3 -->

---

## Deploy and run

```bash
databricks bundle deploy
databricks bundle run vapor_eyes_lf_job
```

- The bundle validates and deploys with the committed defaults in `databricks.yml` out of the box. Set your own workspace CLI profile and SQL warehouse by copying `databricks.override.yml.example` to `databricks.override.yml` (git-ignored) and filling in your profile and `warehouse_id`; it's layered in automatically via `include: ["*.override.yml"]`. The warehouse is used both for the AI/BI dashboard and for validation queries — a fresh clone with no override file still validates/deploys, it just has no dashboard until a `warehouse_id` is set.
- **Schedule**: the job carries a daily schedule (`0 0 7 * * ?`, America/Chicago) but ships **paused** (`pause_status: PAUSED`) — unpause it in the workspace (or in `databricks.yml`) once you've confirmed a manual run.
- **Backfill**: to widen or shift the historical window, edit the `date_window`, `s5p_temporal`, `s5p_windows`, or `emit_windows` variables (in `databricks.yml` or your override file) and re-run the job. Widening the window re-downloads a correspondingly larger volume of granules — see [Caveats](#caveats).
- **The GeoBrix wheel**: `gbx_wheel` points at a wheel staged on a Volume (`geobrix-0.4.0-py3-none-any.whl`); both the pipeline and the `land` task install `${var.gbx_wheel}[light,stac,vizx]` from that path. Stage your own build there, or point the variable at wherever your wheel lives.

---

## Prerequisites

- **Unity Catalog**: a catalog/schema (default `geospatial_docs.vapor_eyes_lf`) and a Volume named `data` under it — the pipeline creates sub-directories inside the Volume but not the Volume itself.
- **Two BYOT (bring-your-own-token) UC secrets** — both referenced by bundle variables (`earthdata_secret`, `cm_secret`), defaulting to the `geospatial_docs.vapor_eyes` scope shared with the notebook cascade so you don't need a second token if you've already set one up there:
  - **`earthdata_token`** — an Earthdata Login token for EMIT. Create one at [urs.earthdata.nasa.gov](https://urs.earthdata.nasa.gov/) and store it as a UC secret.
  - **`carbon_mapper_token`** — a free Carbon Mapper API token. Register at [data.carbonmapper.org](https://data.carbonmapper.org/) and store the issued token as a UC secret. See [Good-citizen use](#good-citizen-use--carbon-mapper) below before using it.
- **Network access**: all downloaders fetch over HTTPS (Copernicus, NASA LP DAAC, Carbon Mapper, TX RRC ArcGIS) — Serverless has outbound internet by default.
- **A SQL warehouse** for the AI/BI dashboard and post-deploy validation queries (set via `warehouse_id` in your override file).

---

## Good-citizen use — Carbon Mapper

Carbon Mapper's Tanager plume catalog is a public-good dataset, and this example is built to use it responsibly:

- **Bring your own token (BYOT).** The pipeline is a client, not a redistributor: every user supplies their own free Carbon Mapper token (the `carbon_mapper_token` UC secret above), and `land.py` fetches directly from the Carbon Mapper API at run time using that token.
- **Terms of use.** Using the Carbon Mapper API means agreeing to Carbon Mapper's [Terms of Use](https://carbonmapper.org/terms-of-use) — read them before registering for a token.
- **Attribution.** The dashboard displays **"Data © Carbon Mapper"** on every page that shows Carbon Mapper-derived data (the Current Status & Leakiest Operators page).
- **Public-good / non-commercial scope.** This example is built for public-good analytics and demonstration, not commercial or low-latency operational monitoring. If you need commercial licensing or low-latency operational access to Carbon Mapper / Tanager data, contact [Planet Labs](https://www.planet.com/).
- **No data is committed to this repository.** Carbon Mapper detections are fetched at runtime and land only on your own Volume (`cm` subdirectory) — nothing from the Carbon Mapper API is ever checked into git.

---

## Caveats

- **EMIT is validation, not the current driver.** EMIT is a science mission flown on the ISS with sparse revisit and roughly a 10-month lag before JPL publishes plume-complex products — there are no EMIT plume products for 2026. Carbon Mapper is what makes the "current status" and leaderboard panels current; EMIT's role is to independently validate the CH4 enhancement measurement on the historical plumes it does cover (the `plume_quant` cross-check).
- **Concentration vs. emission rate.** Sentinel-5P and EMIT surface CH4 *concentration/enhancement* (`ch4_mean`, `max_conc_ppmm`, `gbx_max_ppmm`) — a proxy for where and how strong a signal is, not a rate. Carbon Mapper is the only source in this pipeline with a wind-corrected, quantified `emission_rate_kg_hr`; don't compare concentration columns across sources as if they were the same unit.
- **Download cost of widening the window.** `s5p_windows` / `emit_windows` / `cm_window` collectively drive a non-trivial download volume once widened — a full historical backfill (the committed default spans 2023–2026 across all three) re-fetches a meaningfully larger set of granules/scenes than a single-overpass run. Narrow the windows for a quick smoke-test deploy.

---

## Files

| Path | Purpose |
|---|---|
| `databricks.yml` | The bundle definition: variables, the `vapor_eyes_lf_pipeline` resource, the `vapor_eyes_lf_dashboard` resource, and the `vapor_eyes_lf_job` (land + pipeline tasks, schedule). |
| `databricks.override.yml.example` | Template for the git-ignored `databricks.override.yml` (workspace CLI profile, SQL warehouse, variable overrides). |
| `land/land.py`, `land/_dates.py` | The Task 1 downloader driver and date-window parsing helpers. |
| `transformations/_config.py` | Shared pipeline configuration (`cfg`, `paths`, `register_gbx`) read from `spark.conf`. |
| `transformations/bronze_ingest.py` | Auto Loader bronze metadata tables (one per source). |
| `transformations/silver_cascade.py` | The bi-temporal silver cascade (S5P, S2, EMIT, wells SCD2, Carbon Mapper). |
| `transformations/gold_analytics.py` | Gold analytics MVs (leaderboard, monitoring status, activity, hotspots, trends). |
| `transformations/portfolio_tiles.py`, `transformations/_shard.py` | MVT pyramid + sharded PMTiles portfolio output. |
| `dashboards/vapor_eyes_lf.lvdash.json` | The AI/BI dashboard definition (three pages). |
| `tests/` | Pytest unit tests for `land`/`_dates`/`_shard` pure-Python logic, plus `tests/validate/*.sql` post-deploy validation queries. |
