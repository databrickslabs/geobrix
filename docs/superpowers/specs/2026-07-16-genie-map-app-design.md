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
    hotspot_h3.sql
    wells_h3_density.sql
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
  h3?: {                      // when kind === "h3"
    cellIdCol: string;        // e.g. "h3_cellid"
    zoomResBreaks: number[];  // zoom→H3 res tiers (viewport-adaptive)
    geomCol: string;          // "hex_geom" (native GEOMETRY, 4326)
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
(`useLayerVisibility`, `LAYER_RULES`) is driven from `zoomVisible`/`zoomResBreaks`
instead of taxi constants.

### Shipped vapor-eyes layers (MVP)

1. `ch4_hotspots` — H3, from `hotspot_latest`, colored by `ch4_max`.
2. `well_density` — H3, from **new** `wells_h3_density_latest`, colored by `well_count`.
3. `wells` — point, from **new** `wells_enriched_latest`, shown at high zoom.
4. `plumes` — point, from `plume_leaderboard_latest`, colored by `max_conc_ppmm`.

(1)+(2) satisfy "wells as H3 for the initial query"; (2)/(3)/(4) share the same registry
machinery.

## 5. New gold MVs (added to the Lakeflow SDP `gold_analytics.py`)

Both are first-class pipeline outputs (materialized once, queried cheaply). Follow the
file's existing conventions: `@dp.materialized_view`, no-driver-collect windowing,
`st_setsrid(..., 4326)` on every map-facing geometry, current-inventory filter
`wells_shl` where `__END_AT IS NULL`.

**(a) `wells_h3_density_latest`** — current well inventory aggregated to H3 cells.
- Per cell: `h3_cellid`, `well_count`, `operator_count` (distinct operators),
  `center_lon/lat`, `hex_geom = st_geomfromwkb(h3_boundaryaswkb(h3_cellid), 4326)`.
- H3 cell derived from `well_geom` via `h3_coverash3` / native `h3_*` at a demo-fixed
  resolution (registry can request a tier); mirrors how `hotspot_latest` produces
  `hex_geom`.

**(b) `wells_enriched_latest`** — current well inventory spatially tagged with
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

These MVs are additive to the pipeline; Phase-1 execution reruns the SDP to materialize
them. (No GeoBrix SQL needed — pure Databricks-native `st_*`/`h3_*`, matching the rest of
`gold_analytics.py`.)

## 6. Genie Space (curated, new)

No curated space exists yet. As part of Phase 1, create a Genie Space over
`geospatial_docs.vapor_eyes_lf` with:

- **Tables:** `hotspot_latest`, `plume_leaderboard_latest`, `wells_h3_density_latest`,
  `wells_enriched_latest`, `plume_candidate_wells`, `ref_shale_plays`, `ref_counties`
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
  density) + wells/plume points + NLP paths working end-to-end.
- **Phase 2 — PMTiles.** Consume the vapor-eyes SDP's PMTiles fanout shards + light
  overview as a MapLibre/deck.gl vector layer (GeoBrix tile-perf showcase). No new tile
  generation. See [[pmtiles-spatial-sharding-model]], [[tilejoin-segfaults-on-serverless]].
- **Phase 3+.** Raster/EMIT overlay; helios as a selectable Genie Space + dataset config
  (proves the registry seam); multi-layer selection UI.

## 9. Testing & docs

- **This session:** design + spec + plan only. **No long-running builds/deploys** (user
  is at an airport with intermittent comms). All `pnpm`/deploy/pipeline runs happen when
  back online, dispatched to subagents per repo convention.
- **Verification (later phases):** local `pnpm build` green; a smoke check that each
  registry layer's SQL template runs against `vapor_eyes_lf` on the warehouse and returns
  a geometry column at SRID 4326; a Genie Space smoke query returns an `ST_ASGEOJSON`
  column that renders.
- **Docs:** `apps/genie_map/README.md` (architecture, the layer-registry contract, deploy
  steps) + a short `docs/docs/examples/` page tying it into the vapor-eyes story.

## 10. Open risks

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
