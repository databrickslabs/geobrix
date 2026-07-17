# Build notes

## Baseline

This app was seeded from the kepler.gl + Databricks AppKit demo prototype
(`kepler-demo`), copied into `apps/genie_map/` unmodified. Before any changes,
the copied source was verified to build cleanly so that later build failures are
attributable to our changes rather than the starting point.

### Environment built against

- Node.js v25.2.1 (satisfies the `engines.node >= 20` floor)
- pnpm 10.34.4 (lockfile pinned to pnpm 10.30.3; same major, compatible)
- TypeScript 5.9.3, Vite 6.4.1

### Install

```
pnpm install --frozen-lockfile
```

The frozen lockfile install completed successfully with no resolution changes.
pnpm reports "Ignored build scripts" for a handful of dependencies (esbuild,
protobufjs, AppKit, deckgl typings, heroui shared-utils); this is pnpm's default
lifecycle-script sandboxing and does not affect the build.

### Build

```
pnpm build
```

This runs the server type-check/emit (`tsc -p tsconfig.server.json`) followed by
the client bundle (`vite build`). Both stages complete successfully:

- `dist/server/index.js` is produced.
- `dist/client/` is produced (`index.html` plus the `assets/` bundle).

Vite prints a chunk-size advisory for the large client bundle; this is an
informational warning, not a build error.

### Environment variables

The build is green with no environment file present. Data-source settings are
only required at query time, not at build time, so a missing environment file
does not affect this baseline.

---

## How this app was built

This section is the implementer's account of turning the copied prototype into a
Genie Map over the Permian methane data — what was reused, what was rewritten,
and the decisions and gotchas along the way. It is meant for an engineer who wants
to rebuild it or adapt it to a different dataset.

### 1. What we adapted

The prototype was a coherent kepler.gl + AppKit app wired to a placeholder NYC-taxi
dataset through a hardcoded column schema. We kept its strong parts and replaced the
data binding:

- **Reused unchanged:** the AppKit server plugin composition (analytics + Genie + serving
  proxy), the analytics-query → kepler-dataset bridge (`useKeplerDataset`), the AI
  Assistant (Genie) panel, and the viewport-bounds/layer-visibility machinery.
- **Rewritten:** the hardcoded taxi column schema became a **config-driven layer
  registry**; the two taxi-specific data hooks were replaced by one generic,
  registry-driven `useLayerData`; the SQL templates were rewritten against the
  methane gold tables; the app was pointed at the `geospatial_docs.vapor_eyes_lf`
  gold schema.
- **Removed:** the taxi analytics/filter side panel (its metrics assume taxi columns;
  it is out of scope for this map), and the placeholder notebooks.

### 2. The layer-registry contract (the extension point)

The heart of the app is a single typed registry (`client/src/config/datasets/`). One
`DatasetConfig` declares a list of `LayerDef`s, and everything downstream — the SQL to
run, the kepler layer to build, when each layer is visible — is derived from it. To add
a dataset (e.g. a second scenario later), you write one `DatasetConfig` and select it
with `VITE_ACTIVE_DATASET`; no app code changes.

A `LayerDef` declares: `id`, `kind` (`h3` or `point`), the `queryName` (a SQL template in
`config/queries/`), the metric/tooltip columns, styling (`palette`), the `zoomVisible`
band, and — for H3 layers — an `H3ResConfig` (below). `getActiveDataset()` is the seam:
it reads `VITE_ACTIVE_DATASET` and returns the active `DatasetConfig`, defaulting to
`vapor-eyes`.

The four shipped layers:

| Layer | Kind | Source table | Metric |
|---|---|---|---|
| `ch4_hotspots` | H3 (cell-sourced) | `hotspot_latest` | `ch4_max` |
| `well_density` | H3 (point-sourced) | `wells_enriched_latest` | well count |
| `wells` | point | `wells_enriched_latest` | — |
| `plumes` | point | `plume_leaderboard_latest` | `max_conc_ppmm` |

Note `well_density` and `wells` share one source table: the H3 layer aggregates the well
points on the fly, and the point layer plots them directly.

### 3. Density-aware dynamic H3 resolution

A map of aggregated hexagons should show coarse hexes when zoomed out and fine hexes when
zoomed in — but naively keying resolution to zoom alone misbehaves on sparse data (a
handful of hexes coarsened to a continental scale is useless). So resolution here is
chosen from **both** zoom and data density:

- **Zoom sets a ceiling.** Each H3 layer's `zoomResBreaks`/`resByBreak` map the current
  zoom to a maximum resolution.
- **Density lowers it only when crowded.** If the in-view cell count exceeds a target
  (~300), the query coarsens further by `floor(log₇(count / target))` H3 parent steps.
  Sparse data stays at the ceiling — no pointless coarsening.

Two source modes, because refinement needs point-level data:

- **Cell-sourced** (`ch4_hotspots`): the gold MV stores H3 cells at the satellite's native
  resolution. The query coarsens with `h3_toparent` and is **capped at that native
  resolution** — going finer would invent precision the instrument doesn't have.
- **Point-sourced** (`well_density`): the query bins raw well coordinates with
  `h3_longlatash3` at the chosen resolution, so it can **refine finer as you zoom in** as
  well as coarsen when dense.

kepler's `hexagonId` layer draws each hexagon from its H3 string id, so returning a
coarser (or finer) cell id is all that's needed — no geometry column travels on this path.

### 4. Layer visibility choreography

Two relationships, two rules, both expressed via each layer's `zoomVisible` band:

- **Within a feature** (well-density hexes ↔ individual well points): they *swap* with a
  one-zoom-level overlap so the transition doesn't blink.
- **Across features** (the CH₄ hotspot hexes ↔ the plume points): they *coexist* — the
  hexagon screen stays as wide-area context while the plume points appear on zoom-in,
  telling a "screen the region, then pinpoint the source" story.

### 5. The wells gold view

`well_density` and `wells` both need one map-ready well table, so a single gold
materialized view — `wells_enriched_latest` — was added to the methane pipeline. It takes
the current well inventory (the as-of-now version of the slowly-changing well history) and
spatially tags each well with its shale play and its county/state, carrying operator,
lease, field, and a map-ready point geometry. One row per well: a well can sit in several
overlapping plays, so a deterministic tie-break keeps a single play per well.

Two details that matter for rendering and for the natural-language path:

- Map-facing geometry is re-tagged to **SRID 4326**. Geometry that loses its SRID through a
  view is silently not drawn on Databricks maps, so the tag is explicit.
- Point columns are aliased `longitude`/`latitude` so the app's SQL templates read them
  directly with no per-layer column mapping.

### 6. Genie Space curation

The natural-language path only renders a map layer when the query result carries a
geometry column. The app recognizes any column whose name contains `geojson` (plus a
couple of exact fallbacks), so the Genie Space is curated to return geometry as an
`ST_ASGEOJSON(...)`-aliased `*_geojson` column. The space is scoped to the map-ready gold
tables (hotspots, plumes, enriched wells, the attribution and reference tables) with join
hints so a question can cross wells ↔ plumes ↔ basin ↔ county.

### 7. Deploy wiring

The app deploys as a Databricks App via an asset bundle
(`databricks.yml`, at the app root) and two palette commands:

- `gbx:app:dev` — run locally with hot reload.
- `gbx:app:deploy` — build and deploy through the bundle.

Both commands source a local `databricks.env` (see below) into the shell before building,
so the same variables reach both the Vite build and the server.

### 8. Gotchas worth knowing

- **SRID 0 is invisible.** Native geometry that round-trips through a materialized view can
  come back without its SRID and then silently won't render. Re-tag with SRID 4326 at the
  gold layer.
- **The serving model is baked at build time.** The chat model name is compiled into the
  client bundle, so changing it requires a rebuild, not just a runtime setting.
- **Environment file naming.** The committed template `databricks.env.example` holds only
  placeholders. Copy it to `databricks.env` (which is git-ignored) and fill in real values.
  Because the tooling reads variables from the shell, the `gbx:app:*` commands source
  `databricks.env` before building; a bare file of that name is not auto-loaded by the
  bundler otherwise.
- **Tooltip columns must be declared.** kepler only surfaces a column in a tooltip if it is
  part of the fetched dataset, so an H3 layer's tooltip fields are included in the columns
  the query returns.

## Reproduce it

Ordered, each step independently checkable:

1. **Materialize the wells view.** Deploy and run the methane pipeline so
   `wells_enriched_latest` exists and is non-empty (verify:
   `SELECT count(*) FROM geospatial_docs.vapor_eyes_lf.wells_enriched_latest`).
2. **Create and curate the Genie Space** over the gold tables; note its id.
3. **Set local env.** `cp databricks.env.example databricks.env` and fill in host, token,
   warehouse id, the Genie Space id, and a Mapbox token.
4. **Run locally.** `bash scripts/commands/gbx-app-dev.sh` and confirm the map renders the
   CH₄ hexagons, the well-density hexagons, and the well/plume points across zoom, and that
   a natural-language question renders a layer.
5. **Deploy.** `bash scripts/commands/gbx-app-deploy.sh` and confirm the deployed app renders.
