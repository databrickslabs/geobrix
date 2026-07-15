# Design — vapor-eyes methane-detection notebook series

- **Date:** 2026-07-10
- **Branch:** `examples/vapor-eyes`
- **Status:** Approved (design); implementation plan pending
- **Sub-project:** Spec B of two. Consumes the `netcdf_gbx` reader from Spec A
  (`docs/superpowers/specs/2026-07-10-netcdf-gbx-reader-design.md`, shipped).
- **Origin:** refined from `input/dais_rewind/methane_detection_example.md` (a Gemini
  brainstorm), re-centered on GeoBrix.

## 1. Overview

A **five-notebook** GeoBrix example series that monitors the **Permian Basin
(Delaware sub-basin)** for methane super-emitters, following an **additive
detection cascade** — each notebook adds one data source and ends in a concrete
**payoff visualization/insight**, closing on a portfolio-level synthesis.

Lightweight tier (Serverless) by default, mirroring `eo-series`/`helios`: 5
notebooks + `config_nb`, managed Delta tables + a Unity Catalog Volume ETL tree,
a `README.md` with per-notebook diagrams, and doc structure consistent with the
other series. "GeoBrix-maxxing": RasterX + VectorX + Databricks-native ST do the
work; no Apache Sedona / RasterFrames.

## 2. The cascade (5 notebooks, each additive with a payoff)

| NB | Adds | GeoBrix hero | Payoff |
|---|---|---|---|
| **01 — S5P screening** | Sentinel-5P L2 CH4 | `netcdf_gbx` **vector** mode → per-pixel points → H3 aggregation | Regional hotspot map; candidate super-emitter cells over the AOI |
| **02 — Sentinel-2 detection** | Sentinel-2 L2A B11/B12 SWIR | RasterX band-ratio map-algebra (MBMP) + tessellation | A detected plume zoomed to a flagged hotspot |
| **03 — EMIT quantification** | EMIT CH4 (ENH COG + PLM GeoJSON) | COG ingestion + zonal **IME** (integrated mass enhancement) via RasterX; PLM emission rate | High-res quantified plume (mass + rate) |
| **04 — facility attribution** | TX RRC well pads | native ST / GeoBrix spatial nearest-neighbor | "This plume → this facility (operator)" |
| **05 — portfolio synthesis** | (no new source) | `gbx_st_asmvt` + `gbx_pmtiles_agg` tiling | Super-emitter leaderboard + unified multi-layer PMTiles map + ESG summary |

**Note on scope decisions (from brainstorming):**
- **No ERA5 wind.** Consequences, both accepted: (a) `netcdf_gbx` **raster mode
  is not exercised by this series** — only vector mode (NB01); raster mode ships
  documented + tested in Spec A. (b) NB04 attribution is a **spatial
  nearest-well join** off the EMIT PLM plume-origin point, not a wind
  back-trajectory.
- **No Carbon Mapper** (account token + non-commercial license; EMIT PLM already
  provides an emission-rate ground truth).

## 3. Area of interest + `FULL_AOI` toggle

Delaware Basin (Reeves/Loving County TX + Eddy County NM). A `FULL_AOI` toggle in
`config_nb` drives the bbox **and** datetime window across every downloader.
Both bboxes are coverage-verified (2026-07-10) — every source returns
demo-worthy data:

| bbox (WGS84 `[minx,miny,maxx,maxy]`) | S5P | S2 (cloud<20) | EMIT plumes | TX wells |
|---|---|---|---|---|
| **`FULL_AOI=False`** (default) `[-103.90, 31.65, -103.40, 32.15]` (0.5°) | 175 | 38 | 15 | 244 |
| **`FULL_AOI=True`** `[-104.4, 31.3, -103.0, 32.7]` (1.4°) | 184 | 168 | 54 | 2,989 |

SMALL is a strict subset of FULL, anchored on a **15-plume EMIT super-emitter
cluster** (~-103.65, 31.90; EMIT overpasses 2023-06-08, 2023-10-08, 2023-12-24,
2024-08-23). Both are real multi-plume clusters, not toys.

## 4. Data sources & access

| Source | Host | Auth | Product / assets |
|---|---|---|---|
| Sentinel-5P L2 CH4 | Planetary Computer `sentinel-5p-l2-netcdf` | anonymous | `ch4` NetCDF (swath) |
| Sentinel-2 L2A | Planetary Computer `sentinel-2-l2a` | anonymous | `B11`, `B12` SWIR COG |
| EMIT CH4 | **NASA LP DAAC via `earthaccess`** (not on PC) | **Earthdata Login** | `EMITL2BCH4ENH.002` (ENH 60m COG) + `EMITL2BCH4PLM.002` (COG + GeoJSON plume outline + emission rate) |
| Well pads | TX RRC `WellSHL` ArcGIS FeatureServer | anonymous | surface-hole points (API, operator, lease, county) |

**The one credential:** NASA Earthdata Login for EMIT, via a Databricks **secret
scope** read in `config_nb` (feeds `earthaccess`). Everything else is anonymous.
EMIT discovery via NASA CMR (`earthaccess`) or the US GHG Center STAC
(`emit-ch4plume-v1`, anonymous search); bytes require the login.

## 5. New library code (`databricks.labs.gbx.sample`)

Following the `NaipDownloader`/`DemDownloader`/`OvertureClient` pattern
(`discover`/`download`/`read`, Serverless-safe: no `spark.conf.set`/`_jvm`/`.rdd`/
cache/persist; injectable client for offline tests). `TropomiDownloader` already
shipped in Spec A. New:

- **`EmitDownloader`** — `earthaccess`/CMR discovery of `EMITL2BCH4ENH.002` +
  `EMITL2BCH4PLM.002` over the AOI; distributed download to the Volume; `read`
  loads ENH COGs via `raster_gbx` and PLM GeoJSON via `geojson_gbx`.
- **`WellsDownloader`** — paginated TX RRC `WellSHL` ArcGIS-REST fetch (handles
  `exceededTransferLimit` via `resultOffset` paging), clip to bbox, write GeoJSON
  to the Volume + a `wells` table.
- Sentinel-2 uses the existing `StacClient` (as in `eo-series`); no new class.

**Smart missing-asset recovery (required, parity with existing downloaders).**
Every downloader must:
1. **Read-validate** each staged asset (rasterio decode for COG/NetCDF; JSON
   parse + feature count for GeoJSON; a size floor to catch truncated/auth-error
   payloads), recording `is_out_file_valid` + `out_file_sz` per asset.
2. Be **idempotent**: skip assets already present and valid.
3. Expose a **`repair()` / download-missing** path that re-fetches only the
   invalid/missing assets and MERGEs them back (mirrors
   `StacClient.download` + `.repair(...)` and the `DemDownloader` retry loop).
`FORCE_REBUILD` forces a full re-download. Each downloader ships offline unit
tests (injected client/mock REST) + a marked integration test.

## 6. Unity Catalog & Volume layout

Mirrors `config_nb` conventions: `USE CATALOG`/`USE DATABASE` (unqualified table
names), a pre-existing Volume named `data`, an idempotent `finalize_delta(df,
tbl)` helper (`FORCE_REBUILD`-aware, self-heals schema drift), and
metadata/catalog tables carrying `*_path` columns that point at Volume assets.

- **Catalog / schema (USER SETTINGS, configurable):** `catalog_name =
  "geospatial_docs"`, `schema_name = "vapor_eyes"`.
- **Volume:** `data` (must pre-exist). `VAPOR_EYES_DIR =
  /Volumes/geospatial_docs/vapor_eyes/data/vapor-eyes`, subdirs `s5p/
  sentinel2/ emit/ wells/ tiles/`.

**Table + asset inventory** (managed Delta in `geospatial_docs.vapor_eyes`;
**→** marks path-bearing metadata columns → Volume assets):

| NB | Volume assets | Delta tables (key columns) |
|---|---|---|
| 01 | `s5p/*.nc` | `s5p_granules` (item_id, date, **ch4_path**, out_file_sz, is_out_file_valid); `s5p_hotspots` (h3_cellid, ch4_mean, ch4_max, geom_wkb) |
| 02 | `sentinel2/*.tif` | `s2_swir_assets` (item_id, date, **b11_path**, **b12_path**, is_out_file_valid); `s2_plume_cells` (h3_cellid, mbmp_frac, geom_wkb) |
| 03 | `emit/*.tif`, `emit/*.json` | `emit_scenes` (plume_id, date, **enh_cog_path**, **plm_geojson_path**, is_out_file_valid); `plume_quant` (plume_id, ime, emission_rate, rate_uncertainty, outline_wkb, origin_lon, origin_lat) |
| 04 | — | `wells` (api, operator, lease, county, geom_wkb); `plume_attribution` (plume_id, origin_lon, origin_lat, nearest_well_api, operator, dist_m) |
| 05 | `tiles/*.pmtiles`, `tiles/mosaic.json` | `super_emitters` (operator, total_rate, plume_count, well_count); `pmtiles_catalog` (layer, **pmtiles_path**, bounds) |

## 7. Config, toggles, tier

`config_nb` (run via `%run ./config_nb` from each NB): 2-step wheel install
(`--no-deps` then `[light,stac,vizx]`), tier selection (option-1 `pyrx` default /
option-2 heavyweight), `register(spark)` + `rx.register(spark)`, catalog/schema
USE, Volume ETL tree `mkdirs`, `set_conf_safe()`, `finalize_delta()`, downloader
instantiation, the Earthdata secret read, and the toggles:

- `FULL_AOI` (SMALL default / FULL) — bbox + datetime across downloaders.
- `FORCE_REBUILD` (re-download / re-create; skip-guards off).
- `INTERACTIVE_PLOTS` (static GitHub-friendly maps vs MapLibre `plot_interactive`).

Lightweight-tier Serverless discipline (per `eo-series`): no runtime
`spark.conf.set` (use `repartition(N, col)`), no `.cache()` (write managed tables),
~1 GB per-UDF Arrow cap (one item/tile per task), sequential Volume I/O
(stage-to-local for windowed reads).

## 8. Deliverables & docs (mirror the other series)

- `notebooks/examples/vapor-eyes/`: `config_nb.ipynb` + `01`…`05` notebooks.
- **`README.md`** in that directory, structured like `eo-series`/`helios`:
  intro + lightweight-tier note + data-source note; "Notebooks at a glance" with
  **an embedded diagram per NB** and 2-3 bullets each; a Files table; Prerequisites;
  Run order; a Data-flow ASCII block; a Serverless execution-strategy section; a
  Gotchas section; and a "Key GeoBrix / Databricks functions shown" list.
- **Per-notebook diagrams** (5): `resources/images/diagrams/vapor-eyes/vapor-eyes-0X.png`
  (+ `.svg`), generated by extending `resources/images/generators/example-diagrams.py`
  in the established style; embedded in the README via
  `![alt](../../../resources/images/diagrams/vapor-eyes/vapor-eyes-0X.png)`. Must
  pass `docs/scripts/check-diagram-coverage.py`.
- **`.ipynb` hygiene:** last source line of each cell must not end in `\n`
  (`rstrip("\n").splitlines(keepends=True)`); notebook narrative markdown tracks
  the code in its section.

## 9. GeoBrix / Databricks surface exercised

- **`netcdf_gbx` vector mode** (NB01) — S5P swath → points (the Spec-A reader's
  hero use in-series).
- **RasterX** (NB02-03): band-ratio map-algebra / `rst_mapalgebra`, `rst_h3_tessellate`,
  `rst_summary`, zonal reduction for IME, `rst_clip` to plume polygon; `gtiff_gbx`
  / `geojson_gbx` readers.
- **VectorX / tiling** (NB05): `gbx_st_asmvt` (+ `gbx_st_asmvt_pyramid`),
  `gbx_pmtiles_agg`.
- **Databricks-native ST / H3** throughout: `h3_longlatash3`, `h3_*` aggregation,
  `st_*` distance/nearest for attribution, `st_*` geometry construction.
- **vizx**: `plot_raster`/`plot_pmtiles`/`plot_interactive` for the per-NB payoffs.

## 10. Validation

- Downloaders: offline unit tests + marked integration tests (as in Spec A / the
  existing `sample` tests).
- Notebooks: runnable end-to-end on Serverless (SMALL default); validated via
  `gbx:test:notebooks` where applicable and a manual Serverless pass. Each NB's
  payoff cell must render a real figure/table from real staged data.
- Diagram coverage check + README link audit before merge.

## 11. Risks / open items

- **R1 — Earthdata Login provisioning.** EMIT bytes need the secret scope; the
  integration test + a real run require it. Document the one-time setup in the
  README; keep discovery (CMR/GHG-Center STAC) anonymous so the notebook is
  inspectable without creds.
- **R2 — EMIT temporal alignment.** EMIT is opportunistic (4 overpasses over the
  SMALL cluster). The "event" narrative should anchor S5P/S2 windows near an EMIT
  overpass date; confirm a coherent date during implementation.
- **R3 — MBMP realism.** The Sentinel-2 B12/B11 SWIR ratio is a credible but
  *illustrative* proxy, not an operational retrieval; frame it as demonstrative
  (no over-claiming) in the narrative.
- **R4 — TX RRC WellSHL is TX-only.** The NM sliver of FULL is uncovered
  (immaterial — plumes are TX-side). Note it; NM OCD is a documented extension.
- **R5 — netcdf_gbx raster mode undemonstrated in-series** (see §2). Acceptable;
  covered by Spec A docs/tests. Revisit if a wind/ERA5 step is later added.
