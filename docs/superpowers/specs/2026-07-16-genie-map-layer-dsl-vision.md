# Genie Map — Layer DSL (forward-looking vision)

- **Date:** 2026-07-16
- **Status:** Vision / north-star. NOT scheduled for the MVP. The MVP (`2026-07-16-genie-map-app-design.md`) ships a deliberate, forward-compatible **subset** of this DSL.
- **Related:** [[genie-map-app-design]] (MVP), [[vapor-eyes-lakeflow-sdp]], [[pmtiles-spatial-sharding-model]], [[document-pmtiles-reader]], [[heavy-tier-tiling-parity-assess]]

## Purpose

Genie Map should eventually manage map layers through a small **declarative DSL** rather
than bespoke per-layer code. A layer author declares *what a layer is* and *how it behaves
across zoom*, and the app resolves the rest (which query/tile source to hit, which kepler/
MapLibre layer to build, when to cut over between representations). This keeps new layers
cheap, makes helios/other datasets drop-ins, and turns GeoBrix's spatial formats
(H3, MVT, PMTiles, COG) into first-class, composable map primitives.

## The two orthogonal axes (the whole idea)

The seven layer behaviors the user enumerated collapse into **two independent axes**, which
is what keeps the DSL small:

### Axis 1 — Render kind (what a layer *is* at a given zoom)

```
kind = geom | h3 | raster | cog | mvt | pmtiles
```

| kind | Source | Rendered as | Notes |
|---|---|---|---|
| `geom` | table with native `GEOMETRY` (points/lines/polys) | kepler geojson/point layer | the "vector" base case |
| `h3` | cells (stored H3 col) **or** points (lon/lat) | kepler `hexagonId` | carries an H3 resolution policy (Axis-1a) |
| `raster` | raster bytes / tile struct | image overlay | plain raster read (small/whole) |
| `cog` | Cloud-Optimized GeoTIFF (range-request, tiled/overviews) | tiled image overlay | GeoBrix `rst_cogconvert` output; the efficient way to serve raster to a web map |
| `mvt` | Mapbox Vector Tiles (`gbx_st_asmvt` output) | MapLibre/deck vector-tile layer | tile-local coords |
| `pmtiles` | single-file PMTiles archive (+ overview) | MapLibre/deck PMTiles source | GeoBrix fanout shards + light overview |

### Axis 1a — H3 resolution policy (only for `h3` kind)

Exactly the `H3ResConfig` shipped in the MVP:

```
h3Policy = {
  source: 'cells' | 'points',     // cells → coarsen-only (h3_toparent); points → refine+coarsen (h3_longlatash3)
  minRes, maxRes,                  // finest/coarsest bounds; points may exceed any stored cell's res
  zoomResBreaks[4], resByBreak[5], // zoom → max-resolution ceiling
  targetCells,                     // density target (~300); coarsen below the ceiling when dense
  aggExpr,                         // how children roll up (MAX/SUM/COUNT/AVG …)
}
```

"Coarsening / finering policy up to the cutover" = this block, evaluated for each `h3`
stage below its cutover zoom.

### Axis 2 — Zoom staging (how a layer *changes* with zoom)

A layer is an **ordered list of stages**, each a render-kind active over a zoom band, with
optional overlap fade at the cutover:

```
stages = [ { kind, fromZoom, toZoom, h3Policy?, styling, fade? }, … ]
```

- **Single-stage** layer = the user's types 1–5 (`geom`/`h3`/`raster`/`mvt`/`pmtiles` only).
- **Multi-stage** layer = types 6–7 and beyond: `h3 → geom` at a cutover zoom, `h3 → raster`
  at a cutover, `pmtiles → geom`, `cog → raster`, etc. **Any kind can cut over to any kind.**
- The cutover is just where one stage's `toZoom` meets the next stage's `fromZoom`; a
  `fade` band across the boundary gives the smooth swap (the MVP's `fadeBand`).

## The DSL shape

```ts
type RenderKind = 'geom' | 'h3' | 'raster' | 'cog' | 'mvt' | 'pmtiles';

interface LayerStage {
  kind: RenderKind;
  fromZoom: number;
  toZoom: number;
  h3Policy?: H3ResConfig;      // required iff kind === 'h3'
  source?: SourceRef;          // per-stage override (e.g. h3 stage reads a table, geom stage a tile url)
  fade?: [number, number];     // opacity ramp across a cutover
  styling: StyleSpec;          // palette/scale/elevation/radius…
}

interface LayerDef {
  id: string;
  label: string;
  source: SourceRef;           // default source (table | volume path | tile-url template | pmtiles url)
  stages: LayerStage[];        // 1 = single-kind; N = zoom-staged with cutovers
  genie?: GenieBinding;        // optional: how NL answers map onto this layer
}
```

Worked examples:

- **Type 2 (h3 only):** `stages: [{ kind:'h3', fromZoom:0, toZoom:24, h3Policy:{…} }]`
- **Type 6 (h3 → geom at cutover):** `stages: [{ kind:'h3', fromZoom:0, toZoom:12, h3Policy:{…} }, { kind:'geom', fromZoom:11, toZoom:24, fade:[11,12] }]` — this is exactly the MVP's wells behavior, expressed in the DSL.
- **Type 7 (h3 → raster/cog at cutover):** `stages: [{ kind:'h3', … toZoom:10 }, { kind:'cog', fromZoom:10, toZoom:24 }]` — e.g. CH4 hex screen → EMIT enhancement COG when you zoom into a scene.
- **Type 5 (pmtiles only):** `stages: [{ kind:'pmtiles', fromZoom:0, toZoom:24 }]` — the Phase-2 vapor-eyes fanout shards + overview.

## Relationship to the MVP (what ships now vs. later)

The MVP `LayerDef` is a **strict, forward-compatible subset**:

| DSL concept | MVP today | Becomes |
|---|---|---|
| `kind` | implicit `'h3' | 'point(geom)'` | explicit `RenderKind` enum |
| single-stage | flat `zoomVisible` + optional `fadeBand` | one-element `stages[]` |
| multi-stage cutover | not present (2 layers coordinated via `zoomVisible`) | `stages[]` with N kinds |
| `h3Policy` | `H3ResConfig` (shipped) | unchanged — lifts directly into a stage |
| `raster`/`cog`/`mvt`/`pmtiles` | not present | Phase 2/3 kinds |

**Migration is deferred to Phase 2** (when the second render-kind — PMTiles — actually
arrives), per the MVP's YAGNI decision: reshaping the four MVP layers into `stages[]` earns
nothing until a non-`h3`/`geom` kind exists. When PMTiles lands, the flat shape becomes a
one-element `stages[]` and PMTiles is added as a kind — additive, not a rewrite.

## Why this is a good GeoBrix story

Each render-kind is a GeoBrix capability made directly consumable on a map:
`h3` (native H3 + dynamic resolution), `mvt` (`gbx_st_asmvt`), `pmtiles` (fanout shards +
overview), `cog` (`rst_cogconvert`), `raster` (raster readers). The DSL is where "GeoBrix
processed your data into format X" becomes "declare a layer of kind X" — a compelling,
reusable demo surface and an on-ramp to Databricks-native spatial.

## Open questions (for the DSL design proper, later)

- **Source abstraction (`SourceRef`):** unify table (warehouse SQL), Volume path (COG/PMTiles
  bytes), and tile-URL template behind one type; how does auth/serving differ per kind?
- **Serving COG/PMTiles to the browser:** range-request proxy through the appkit server vs.
  signed Volume/object-store URLs vs. a tiny tile endpoint. Interacts with
  [[pmtiles-spatial-sharding-model]] and the app's auth model.
- **Genie binding across kinds:** today NL answers render as `geom`; can an NL answer target
  an `h3` or `mvt` stage? Probably NL always yields `geom`, layered over the declared stack.
- **Cutover semantics with the density heuristic:** when an `h3` stage cuts over to `cog`,
  does the density policy still gate the `h3` stage, or does the cutover pre-empt it?
- **Validation:** stages must tile the zoom range without gaps (except intentional); a schema
  check should reject overlapping non-fade bands and unknown kinds.
