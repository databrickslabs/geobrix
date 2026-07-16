# Genie Map — Design Spec

- **Date:** 2026-07-16
- **Branch:** `apps/genie-map` (new; depends on the `examples/vapor-eyes` Lakeflow gold tables existing)
- **Status:** Design approved; pending user review before writing the implementation plan.
- **Related:** [[vapor-eyes-lakeflow-sdp]], [[vapor-eyes-methane-example]], [[aibi-custom-geometry-choropleth]], [[gbx-wkb-to-native-st-bridge]], [[pmtiles-spatial-sharding-model]]

## 1. Summary & goals

**Genie Map** is a Databricks App that renders GeoBrix-processed geospatial data on an
interactive map, driven by two complementary paths:

- **Viewport path** — map pan/zoom triggers parameterized Spark SQL against the
  vapor-eyes gold tables, producing an H3 aggregation hexagon layer (and a point layer at
  high zoom). No LLM involved; fast and deterministic.
- **Genie NLP path** — an AI Assistant panel where natural-language questions hit a
  curated **vapor-eyes Genie Space**; geometry-bearing results render as map layers.

It is positioned as a **halo reference example**: a well-architected Databricks App +
Genie Spaces + GeoBrix, to be demoed at an upcoming conference and an in-person Exxon
event. It adapts an existing prototype (`/Users/mjohns/isaac_work/genie_map`, a
kepler.gl + `@databricks/appkit` app that was wired to a now-retired NYC-taxi
placeholder dataset) onto the vapor-eyes methane-detection data as its data spine.

### Non-goals (explicitly deferred)

- PMTiles/MVT vector-tile layers (Phase 2).
- Raster/EMIT overlays and helios as a selectable Space (Phase 3+).
- Full data-agnostic generalization of the layer system (only the seam is built now; one
  concrete dataset config ships).

### MVP bar for the conference (this week)

H3 + NLP working end-to-end against vapor-eyes, including **wells as a first-class
layer**. Everything else is a later phase.

## 2. Source-prototype assessment (what we are adapting)

The prototype is coherent but non-functional as-is. Key facts driving the design:

- **Stack:** React 18 + TypeScript + **kepler.gl 3.2.5** (deck.gl) client; Node/Express
  via **`@databricks/appkit` 0.41.6** server; Vite 6 build; `@openassistant/*` for the
  AI chat. `pnpm@10`, `node>=20`.
- **Two independent data flows:**
  - *Analytics/viewport:* `useViewportBounds` (600ms debounce) → bounds →
    `useH3AggregationData`/`usePointData` → `useKeplerDataset` → AppKit
    `useAnalyticsQuery(queryName, params)` → server `analytics` plugin runs
    `config/queries/<name>.sql` on the SQL warehouse → rows → kepler `addDataToMap` /
    `replaceDataInMap`. **Never touches Genie.**
  - *Genie:* `@openassistant` `useAssistant` calls a `databricksGenie` tool →
    Genie SSE stream at `/api/genie/default/messages` → `summarizeGenieEvents` →
    `{sql, columns, rows}`. Renders a layer **only if** a column named
    `geojson`/`geometry` exists (`genie-tool.ts` `parseGeoJsonFromRows`).
- **Why it doesn't run:** (a) hardwired to an NYC-taxi *canonical schema*
  (`CELL_RES_1..5`, `METRIC_1/2`, `GEOM_POINT`, `POINT_X/Y`, `CATEGORY_FILTER`, …)
  spread across every `.sql` file and layer config; (b) ships no data (depends on
  notebooks the user must run against a Kaggle CSV); (c) `VITE_DATASET_TABLE` has no
  fallback → empty map out of the box; (d) **no working app-deploy path** — the
  `deploy/` folder only stages notebooks (`databricks workspace import-dir`), there is no
  DAB bundle or `databricks apps deploy`.
- **Config surface today:** dataset shape is `VITE_*`-driven; warehouse ID, Genie space
  ID, serving endpoint are env/app-resources. Serving model is baked at build time via
  Vite `define __LLM_MODEL__`. Genie alias `'default'` is hardcoded client-side.

## 3. Target data spine — vapor-eyes Lakeflow gold (`geospatial_docs.vapor_eyes_lf`)

The Lakeflow SDP already produces map-ready gold (native `GEOMETRY`, SRID 4326;
latest + trend splits; SCD2 wells). Tables this app consumes:

| Gold table | Grain | Map-facing columns | Use |
|---|---|---|---|
| `hotspot_latest` | H3 cell (latest overpass) | `hex_geom` (GEOMETRY), `ch4_mean`, `ch4_max`, `n_obs`, `center_lon/lat`, `hotspot_rank` | Primary CH4 hexagon layer |
| `hotspot_persistence` | H3 cell (full window) | `hex_geom`, `persistence_ratio`, `mean_ch4`, `max_ch4` | Chronic-emitter hexagon layer (optional) |
| `plume_leaderboard_latest` | EMIT plume | `plume_geom_native`, `origin_geom`, `max_conc_ppmm`, `gbx_max_ppmm`, `lead_operator/lease/field/county` | Plume points + NL drill-down |
| `wells_shl` (SCD2; current = `__END_AT IS NULL`) | well (API) | `well_geom` (WKB point), `api`, `operator`, `lease`, `field`, `county`, `well_url` | Base well inventory for the two new MVs below |
| `plume_candidate_wells` / `cm_candidate_wells` | plume↔well (nearest-K) | `operator`, `lease`, `field`, `county`, `dist_m`, `api`, `rank` | Attribution for Genie NL questions |
| `ref_shale_plays` | play/basin | `play_name`, `play_geom` (GEOMETRY), `area_sq_km` | Basin/play spatial join + choropleth |
| `ref_counties` | county | `county_name`, `state_fp`, `geoid`, `county_geom` (GEOMETRY) | County/state spatial join + choropleth |

**Geometry contract (from [[aibi-custom-geometry-choropleth]] / [[gbx-wkb-to-native-st-bridge]]):**
map-facing geometry is native `GEOMETRY` at **SRID 4326** — never raw WKB bytes or H3
cell ids. Round-tripping GEOMETRY through an MV can drop SRID to 0 (silently unrendered);
re-tag with `st_setsrid(..., 4326)` in the new MVs, matching existing gold practice.

## 4. Architecture change — config-driven layer registry

Replace the hardwired canonical schema with a **layer registry**: a single typed config
plus SQL templates that declare, per dataset, everything the viewport and Genie paths
need. Ship **exactly one config (vapor-eyes)** now; the seam makes helios a later drop-in.

**New layout (under `apps/genie_map/`, client side):**

```
config/
  datasets/
    vapor-eyes.ts        # the one shipped dataset registry (see shape below)
    index.ts             # active-dataset selector (env/config driven)
  queries/               # SQL templates keyed by (dataset, layer)
    hotspot_h3.sql       # cell-sourced dynamic H3 (coarsen-only)
    wells_h3.sql         # point-sourced dynamic H3 (refine + coarsen)
    plume_points.sql
    wells_points.sql
```

**Registry entry shape (per selectable layer):**

```ts
interface LayerDef {
  id: string;                 // "ch4_hotspots" | "well_density" | "wells" | "plumes"
  kind: "h3" | "point";       // drives which kepler layer factory + query template
  label: string;
  sourceTable: string;        // fully-qualified vapor_eyes_lf table/MV
  queryTemplate: string;      // key into config/queries
  h3?: {                      // when kind === "h3" — see "Dynamic H3 resolution" below
    source: "cells" | "points";  // cells → coarsen-only via h3_toparent;
                                  // points → refine+coarsen via h3_longlatash3
    cellIdCol?: string;       // source==="cells": e.g. "h3_cellid" (native res)
    nativeRes?: number;       // source==="cells": the stored resolution (hard ceiling)
    lonCol?: string;          // source==="points": e.g. "well_lon"
    latCol?: string;          // source==="points"
    minRes: number;           // coarsest resolution ever rendered
    maxRes: number;           // finest resolution allowed (points can zoom past cells)
    zoomResBreaks: number[];  // 4 zoom thresholds → the per-zoom MAX resolution ceiling
    resByBreak: number[];     // 5 resolutions (one per zoom band), each ≤ maxRes
    aggExpr: string;          // how to aggregate children, e.g. "MAX(ch4_max)"
  };
  point?: { lonCol: string; latCol: string; idCol: string };
  metricCol: string;          // color/elevation driver (e.g. "ch4_max", "well_count")
  filters?: FilterDef[];      // operator/basin/county selectors
  styling: { palette: string; scale: string; elevation?: boolean };
  zoomVisible: { min: number; max: number };
}

interface DatasetConfig {
  id: "vapor-eyes";
  displayName: "Vapor-Eyes — Permian Basin Methane";
  genieSpaceAlias: "vapor-eyes";
  layers: LayerDef[];
  defaultViewport: { longitude; latitude; zoom };  // Delaware Basin
}
```

The viewport path resolves `(activeLayer.queryTemplate, params)` through the existing
AppKit `useAnalyticsQuery` machinery — **the analytics/kepler bridge is reused unchanged**;
only the config it reads from is generalized. Layer visibility/zoom-swap logic
(`useLayerVisibility`, `LAYER_RULES`) is driven from `zoomVisible` instead of taxi
constants.

### Dynamic, density-aware H3 resolution (design decision)

The prototype re-derived H3 resolution purely from zoom (`CASE WHEN zoom <= break …`),
which suited dense taxi data. vapor-eyes gold is mostly **sparse** (dozens–hundreds of
rows per layer), so a pure zoom rule mis-behaves: coarsening a few dozen res-6 cells to
res-3 yields a handful of giant, useless hexes. The resolution decision must be
**density-aware, not just zoom-aware**, and its *direction* depends on the source:

- **Refinement (finer on zoom-in) requires point-level source.** `h3_toparent` only goes
  coarser; you cannot split a stored cell into finer children you don't have data for.
  So only **point-sourced** layers can get finer as you zoom in.
- **Coarsening (`h3_toparent`) works on any cell-sourced layer**, but should fire *only
  when the data is genuinely dense* — otherwise leave the fine resolution alone.

**The one heuristic (target on-screen cell count ≈ 300, band 150–500):** at the working
resolution the query counts in-view cells; if that exceeds the target it coarsens by
`levels = floor(log₇(count / target))` H3 parent steps (each step ≈ ÷7). Sparse data →
`levels = 0` → no pointless coarsening. Zoom sets the *maximum* allowed resolution
(`zoomResBreaks`/`resByBreak`); density only ever coarsens *below* that ceiling.

Per-layer application:

| Layer | `h3.source` | Native/finest | Behavior |
|---|---|---|---|
| `ch4_hotspots` | `cells` (`hotspot_latest.h3_cellid`) | res 6 = S5P's real footprint (hard ceiling; finer would be fake precision) | Coarsen-only, density-gated. Sparse → stays res 6. Honest to the science. |
| `well_density` | `points` (`wells_enriched_latest.well_lon/lat`) | up to res ~9 | Fully dynamic: `h3_longlatash3(lon,lat,target_res)` on the fly — **finer as you zoom in**, coarser only when wells are genuinely dense (TX RRC in-basin can be 1000s). |

Because the wells H3 layer aggregates from the *points* in `wells_enriched_latest`, the
originally-planned fixed `wells_h3_density_latest` MV is **dropped** — one wells source
(`wells_enriched_latest`) feeds both the wells point layer and the wells H3 layer.

### Shipped vapor-eyes layers (MVP)

1. `ch4_hotspots` — H3 (cell-sourced, coarsen-only), from `hotspot_latest`, colored by `ch4_max`.
2. `well_density` — H3 (point-sourced, refine+coarsen), from `wells_enriched_latest`, colored by `well_count`.
3. `wells` — point, from `wells_enriched_latest`, shown at high zoom.
4. `plumes` — point, from `plume_leaderboard_latest`, colored by `max_conc_ppmm`.

(1)+(2) satisfy "wells as H3 for the initial query"; all four share the same registry
machinery. Layers 2 + 3 share one source table.

### Layer-visibility choreography (H3 ↔ points with zoom)

Two different relationships, two different rules — driven by per-layer `zoomVisible`
bands (and, in the overlap, an opacity fade):

- **Within a feature (wells H3 ↔ well points): swap with a ~1-level overlap band.** Hexes
  own low/mid zoom, points own high zoom, and for ~1 zoom level at the crossover *both*
  render (hexes fading out as points fade in) so the transition never blinks. Improves on
  the prototype's single-threshold binary swap (`POINT_ZOOM_THRESHOLD`).
- **Across datasets (CH4 hotspot hexes ↔ EMIT plume points): coexist; plumes appear on
  zoom-in.** These are not two views of one thing — the CH4 hexes are the wide-area S5P
  screen, the plume points are EMIT's pinpointed detections. The CH4 hex layer stays
  visible as context; plume points layer *on top* once zoomed in far enough to resolve
  individual sources. This is the "wide-area screen → pinpoint the source" narrative.

Concrete bands (zoom, tunable in Phase 1), expressed as `zoomVisible: {min, max}` with an
optional `fadeBand` for the overlap:

| Layer | `zoomVisible` | Notes |
|---|---|---|
| `ch4_hotspots` (H3) | `{min: 0, max: 24}` | Always-on wide-area context (density heuristic keeps it readable). |
| `plumes` (point) | `{min: 9, max: 24}` | Layers on top of CH4 hexes once zoomed in. Coexists. |
| `well_density` (H3) | `{min: 0, max: 12}` | Owns low/mid zoom; fades out over `[11, 12]`. |
| `wells` (point) | `{min: 11, max: 24}` | Fades in over `[11, 12]`; ~1-level overlap with well_density. |

`useLayerVisibility`/`LayerRule` already toggles per-layer visibility on zoom; the overlap
fade is a small extension (opacity ramp across the shared band) — layers that don't
declare a `fadeBand` keep the existing hard toggle.

## 5. New gold MV (added to the Lakeflow SDP `gold_analytics.py`)

One first-class pipeline output (materialized once, queried cheaply). Follow the file's
existing conventions: `@dp.materialized_view`, no-driver-collect windowing,
`st_setsrid(..., 4326)` on every map-facing geometry, current-inventory filter
`wells_shl` where `__END_AT IS NULL`.

> The originally-planned `wells_h3_density_latest` MV is **dropped** (see §4 "Dynamic H3"):
> the wells H3 layer aggregates the points of `wells_enriched_latest` on the fly at a
> zoom+density-driven resolution, so a fixed-resolution density MV would defeat the
> dynamic behavior. `wells_enriched_latest` is the single wells source.

**`wells_enriched_latest`** — current well inventory spatially tagged with
basin/play + authoritative county/state.
- Basin/play: `st_contains(play_geom, well_pt)` against `ref_shale_plays` → `play_name`.
- County/state: `st_contains(county_geom, well_pt)` against `ref_counties` →
  `county_name`, `state_fp`, `geoid` (polygon-derived, authoritative — distinct from the
  RRC-supplied `wells_shl.county` string, which is carried through as `county_rrc`).
- Carries `api`, `operator`, `lease`, `field`, `well_url`, `well_lon/lat`, and
  `well_geom_native = st_setsrid(st_geomfromwkb(well_geom), 4326)`.
- **Ambiguity resolved:** a well may fall in multiple/zero plays; keep one row per `api`
  with the *first* containing play (deterministic tie-break by `play_name`) and NULL when
  outside all plays. County is expected to be unique per point.

This MV is additive to the pipeline; Phase-1 execution reruns the SDP to materialize it.
(No GeoBrix SQL needed — pure Databricks-native `st_*`/`h3_*`, matching the rest of
`gold_analytics.py`.)

## 6. Genie Space (curated, new)

No curated space exists yet. As part of Phase 1, create a Genie Space over
`geospatial_docs.vapor_eyes_lf` with:

- **Tables:** `hotspot_latest`, `plume_leaderboard_latest`, `wells_enriched_latest`,
  `plume_candidate_wells`, `ref_shale_plays`, `ref_counties`
  (+ `operator_intensity_latest`, `detections_by_county`, `emissions_by_play` for
  aggregate NL questions).
- **Instructions/metadata:** column descriptions; join hints (wells↔plumes via
  `plume_candidate_wells`; wells/plumes↔basin via play polygons; ↔county via county
  polygons); the concentration-led framing (rank by `max_conc_ppmm`, never sum emission
  rates — mirrors `gold_analytics.py`'s design note).
- **Example NL→SQL pairs** tuned so geometry-bearing answers emit an
  `ST_ASGEOJSON(...)`-shaped column the app can render (widening the app's geometry
  detection beyond the literal `geojson`/`geometry` names). Example prompts: *"well
  density in Loving County, TX"*, *"operators with the most wells in the Delaware
  Basin"*, *"highest-concentration plumes and their nearest operator."*
- Space ID becomes a DAB **app resource** (`genie-space-id`), injected via `app.yaml`.

## 7. Packaging & deployment

- **Location:** new top-level **`apps/genie_map/`** — the app (cleaned copy of the
  prototype's `kepler-demo/`) plus a `bundle/` (DAB) and `README.md`. Retire the taxi
  notebooks, taxi `.sql` files, and the two dead-weight SQL files during the copy.
- **Branch:** `apps/genie-map`.
- **Deploy:** a **DAB bundle** (`databricks.yml`) declaring the Databricks App and its
  resources (SQL warehouse `82e587bd93c6cbcf` = GeoBrix warehouse; `genie-space-id`),
  wired to new `gbx:*` commands per repo convention:
  - `gbx:app:dev` — local run (`pnpm dev`, env from a template).
  - `gbx:app:deploy` — `databricks bundle deploy` + `databricks apps deploy` (or
    `bundle run`), profile `oauth-fe`.
  - Commands follow the `scripts/commands/` `.md`+`.sh` pattern (source `common.sh`,
    `--log`, `--help`).
- **Auth/exec context:** workspace `e2-demo-field-eng`, profile `oauth-fe`, catalog
  `geospatial_docs`, schema `vapor_eyes_lf`. App runtime uses AppKit's workspace client;
  local dev uses `DATABRICKS_HOST`/token via an env template.

## 8. Phasing

- **Phase 0 — Copy & clean.** Stand up `apps/genie_map/` from the prototype; strip taxi
  data/SQL/notebooks; build green locally (`pnpm i && pnpm build`).
- **Phase 1 — MVP (this week).** Layer registry + vapor-eyes config; two new gold MVs in
  the SDP; curated Genie Space; DAB deploy via `gbx:app:*`; H3 viewport (CH4 + well
  density) + wells/plume points + NLP paths working end-to-end. **Storytelling artifacts
  (§9) are produced within this phase** — `BUILD.md` narrative accrues as steps land,
  provenance/screenshots captured at each working path, diagram sources authored (batch
  render when online).
- **Phase 2 — PMTiles.** Consume the vapor-eyes SDP's PMTiles fanout shards + light
  overview as a MapLibre/deck.gl vector layer (GeoBrix tile-perf showcase). No new tile
  generation. See [[pmtiles-spatial-sharding-model]], [[tilejoin-segfaults-on-serverless]].
- **Phase 3+.** Raster/EMIT overlay; helios as a selectable Genie Space + dataset config
  (proves the registry seam); multi-layer selection UI.

## 9. Storytelling artifacts — two audiences (first-class deliverable)

This is a halo/demo example, so "how the sausage was made" is a shipped deliverable, not
an afterthought. Two distinct audiences, two distinct artifact sets, produced alongside
the code (not retrofitted):

### 9a. For technical implementers — the build log / narrative

A reproducible, honest account of the end-to-end build so another SA/engineer can rebuild
it. Lives under `apps/genie_map/docs/` (and surfaces on the docs site page):

- **`BUILD.md` — annotated build narrative.** The path from prototype → vapor-eyes app:
  what was reused vs. rewritten, the layer-registry contract, the two new gold MVs and
  *why* (the wells-as-H3 requirement, basin/county joins), the Genie Space curation
  decisions, and the DAB/`gbx:app:*` deploy wiring. Includes the real gotchas as they
  surface (SRID-0 round-trip re-tagging, geometry-column detection widening, build-time
  `__LLM_MODEL__` bake, `oauth-fe` profile), cross-linked to the relevant memory entries.
- **Architecture reference** — the layer-registry `LayerDef`/`DatasetConfig` contract
  documented as the extension point (this is what makes helios a later drop-in).
- **Reproduce-it runbook** — the exact ordered steps: rerun SDP for the 2 new MVs →
  create/curate Genie Space → set app resources → `gbx:app:deploy`. Each step verifiable.
- **Provenance capture** — during Phase-1 execution, capture the actual commands, SQL
  templates, and screenshots of the working paths (viewport H3, wells layers, an NL
  query rendering) as evidence embedded in `BUILD.md`. This is the "how it was made"
  record.

### 9b. For slide-ware — diagrams & explainers

Presentation-grade visuals for the conference + Exxon talk, following the repo's existing
example-diagram pipeline (`resources/images/generators/`, Chrome-render SVG→PNG→PIL-crop;
the vapor-eyes palette from [[vapor-eyes-methane-example]]). Target set:

- **System architecture diagram** — User → Genie Map (kepler.gl client / appkit server) →
  {SQL warehouse ← vapor_eyes_lf gold, Genie Space}, with the two data paths (viewport vs.
  NLP) visually distinguished. The headline "how it fits together" slide.
- **Two-paths explainer** — side-by-side of the deterministic viewport path vs. the Genie
  NLP path (what each is good at); frames the demo narrative.
- **Data-lineage explainer** — GeoBrix/vapor-eyes upstream → gold MVs (incl. the new
  wells MVs + basin/county joins) → map layers; shows where GeoBrix adds value.
- **Layer-registry / extensibility diagram** — one config → many layers; helios as a
  future plug-in (the reusability story).
- These reuse the vapor-eyes accent progression and generator conventions so they sit
  visually alongside the existing NB01–NB05 diagrams. Diagram sources committed under
  `resources/images/generators/` (or `apps/genie_map/docs/diagrams/`), rendered PNGs
  referenced from `BUILD.md` and the docs-site page.

Both artifact sets are produced *incrementally during Phase 1* (capture as you build),
not as a separate documentation phase — the plan sequences capture points into each
implementation step. **Constraint:** diagram *rendering* uses Chrome/PIL tooling that can
be slow; author diagram sources during design/build, render in a batch when online.

## 10. Testing & docs

- **This session:** design + spec + plan only. **No long-running builds/deploys** (user
  is at an airport with intermittent comms). All `pnpm`/deploy/pipeline runs happen when
  back online, dispatched to subagents per repo convention.
- **Verification (later phases):** local `pnpm build` green; a smoke check that each
  registry layer's SQL template runs against `vapor_eyes_lf` on the warehouse and returns
  a geometry column at SRID 4326; a Genie Space smoke query returns an `ST_ASGEOJSON`
  column that renders.
- **Docs:** `apps/genie_map/README.md` (quickstart), `apps/genie_map/docs/BUILD.md`
  (§9a implementer narrative), and a `docs/docs/examples/` page embedding the §9b
  diagrams and tying Genie Map into the vapor-eyes story.

## 11. Open risks

- **SDP rerun cost/time** to materialize the two new MVs — must run when online; keep the
  MVs cheap (current-inventory only).
- **Genie geometry reliability** — NL answers only render when a geometry column comes
  back; mitigated by curated example SQL + widened client detection, but NL is
  inherently variable. The deterministic viewport path is the demo's reliable backbone;
  Genie is the "wow."
- **H3 resolution for well density** — pick a demo resolution that reads well at the
  Delaware Basin default viewport (tune during Phase 1).
- **Serving model** is build-time baked (`__LLM_MODEL__`); changing it needs a rebuild,
  not just runtime config — document this.
- **Storytelling capture is easy to defer and lose** — screenshots/commands must be
  captured *as the working paths come up* in Phase 1, or the "how it was made" record
  becomes a lossy reconstruction. The plan bakes capture into each step (§9).
