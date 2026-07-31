# v2 Virtual-Tile Reader — Increment 1: primitive + `open_tile` core

**Date:** 2026-07-31
**Branch:** `feature/large-raster-reader`
**Status:** design approved, ready for planning
**Related:** `2026-07-29-large-raster-reader-design.md`, `2026-07-31-serverless-cog-preparer-design.md`, `2026-06-30-raster-bbox-window-read-design.md`

## Problem & motivation

The light-tier raster tile currently carries a materialized `raster: binary` payload. That
payload exists only because the **heavy** JVM cannot read Volume FUSE (the UC credential lives in
the Python worker). **Light has no such constraint** — Python reads Volumes fine — so a light tile
could carry a **virtual tile = `(path, window, ...)` with no bytes**, deferring the pixel read
until an op needs it.

Why it matters: at ingest you hold N tiny descriptor rows instead of hundreds of MiB of encoded
tiles, so the serverless-OOM problem (per-task tile accumulation) largely evaporates — strictly
better than shrinking per-tile budgets. This is the **cloud-native raster model** (STAC + COG +
range reads; rio-tiler / TiTiler): don't move pixels, read windows on demand. A virtual tile over a
COG is a cheap, low-memory windowed/overview range-read; over a striped GeoTIFF it still works (with
the strip-inflation cost). COG becomes the *performance tier* of one general mechanism, not a
separate read lane.

This is a large, multi-increment direction. **This spec covers increment 1 only.**

## Scope

### In scope (increment 1)

1. A **`VirtualTile` representation** carrying the full v2 struct — bytes-free when virtual.
2. A shared **`open_tile` / `materialize` front-door** — the single chokepoint that returns an open
   dataset / decoded window regardless of tile shape (v1 bytes, v2 materialized, v2 virtual).
3. **Clip application** with SRID resolution, per-tile intersection semantics, and polygon
   reprojection.
4. A **WarpedVRT lazy-warp probe** (measure cost + edge correctness; the `crs` field is defined and
   minimally exercised).
5. **Proof local + Serverless** across three storage layouts: COG, tiled GeoTIFF, striped GeoTIFF.

### Explicitly NOT in scope (later increments)

- Reader `virtualTiles` emit mode (Inc 2).
- Window taxonomy: overlapPercent / user-bounds-array / user x,y tiling (Inc 3).
- Rewriting `rst_*` functions to be virtual-aware (Inc 4).
- `rst_transform` triple-provenance + mosaic/stack virtual-tile combinators (Inc 5).
- Heavy-tier parity.
- Catalog registration / `registered_functions.txt` / `function-info.json` / bindings. This
  increment is **exploratory / non-wired** so binding-parity + QC stay green (same discipline as the
  scalar-UDF spike).

## Success criteria

**Local** (via `gbx:test:python` in Docker):
- `open_tile` materializes a correct window from **all three layouts**; windowed read equals the
  same slice of a full read (`np.array_equal`).
- A window that **spans more than one block** materializes correctly.
- `raster`-present precedence path returns the payload without touching `path`.
- Clip path produces the polygon-clipped subarray; **partial** intersection returns only the tile's
  slice; **disjoint** returns an empty/NoData result (not an error).
- WarpedVRT probe: reading one layout's window into a different CRS is correct, with peak-RSS/cost
  captured.
- **Partial-clip reassembly probe**: one polygon applied across two adjacent virtual tiles yields
  two partial results that tile back together pixel-aligned at the seam.

**Serverless** (via `jobs.submit` + env-v5 harness):
- The same `open_tile` core runs under the harness, reads the 3 layouts from a Volume, materializes
  windows, and self-reports results back to a Volume.

## The v2 tile struct

```
struct<
  cellid:       bigint,
  raster:       binary  (nullable),
  path:         string  (nullable),
  window:       struct<col_off:int, row_off:int, width:int, height:int>  (nullable),
  clip_polygon: binary  (nullable),   -- WKB/EWKB (WKT/EWKT accepted on input)
  clip_crs:     string  (nullable),   -- authoritative CRS of clip_polygon (e.g. "EPSG:4326")
  crs:          string  (nullable),   -- working/target CRS; distinct from source's native CRS
  metadata:     map<string,string>
>
```

Field rules:
- **`path` is hoisted to top-level** — it is structural identity, making the virtual case
  first-class (not a metadata-map key).
- **`window` is singular, nullable** — one window per tile (tile = unit of parallelism; one
  row → one task → one window materialized + released). It is a **pixel window** into the source
  (block-aligned for cheap reads, but may span multiple blocks — see below). Always
  reader-produced provenance: "this tile is this window of `path`." An **optional overview index**
  (`z`) affordance is reserved so a virtual tile may name a window at a reduced-resolution IFD (the
  COG cheap-overview path); defined now, applied in a later increment.
- **`clip_polygon` is nullable geometry** — arbitrary AOI, distinct from `window`. `window` bounds
  what is *read* from `path` (pixel/block-aligned, I/O-efficient); `clip_polygon` is the exact
  geographic clip *within* the read (need not align to blocks). Two-stage lazy pipeline: read
  `window`@`path` (block-efficient) → clip to `clip_polygon` (exact AOI) → tile pixels. Stays
  virtual until a pixel-producing op.
- **`clip_crs` is nullable** — the authoritative CRS of `clip_polygon` as a string (e.g.
  `"EPSG:4326"`, future-proof for WKT2/PROJ strings), decoupling the polygon's CRS from its
  encoding. It lets a plain WKB/WKT clip still declare its CRS, and an EWKB/EWKT clip populate it
  from the embedded SRID. Resolution precedence (see Unit C): explicit `clip_crs` wins; else the
  SRID embedded in an EWKB/EWKT; else assume the source raster's CRS. Never reprojects the raster —
  only the polygon.
- **`crs` is nullable** — the working/target CRS, **distinct** from the backing source's native CRS
  (discoverable from `path`). Null or equal to source CRS → no warp. Differs → misalignment is
  *recorded*; `open_tile` warps `window`→`crs` at read time (lazy warp). `window` and
  `clip_polygon` provenance always stay in the **backing source's** pixel/coordinate space; the
  pending warp is what reconciles them to `crs`.

Back-compat: a v1 tile `(cellid, raster, metadata)` is still expressible with `path`/`window`/
`clip_polygon`/`crs` null. v1 is supported **indefinitely** (hard back-compat), because it flows the
raster-set path through the same `open_tile` chokepoint with zero per-function effort.

## Components

Three focused, independently testable units in the light tier
(`python/geobrix/src/databricks/labs/gbx/pyrx/core/`). This **reuses existing machinery**
(`ds/_encode.encode_tile`, `budget.plan_layout`, `_get_or_stage_file`) rather than reinventing it —
the windowed-read mechanics already exist. What is new is the *bytes-free representation* and the
*single materialize chokepoint*.

### Unit A — `virtual_tile.py`: the representation

- `VirtualTile` dataclass mirroring the struct above; `Window4 = (col_off, row_off, width, height)`
  plain tuple (matches `_encode`/`budget` convention).
- Light validators: at least one of `raster`/`path` present; `window` required when `path`-only.
- `to_spark_struct()` / `from_row()` round-trip. The **full** struct (incl. `clip_polygon`, `crs`)
  is defined here now — even though the reader does not emit it yet — so parity locks once and there
  is no second schema break.
- Pure data + validation. No I/O.

### Unit B — `open_tile.py`: the front-door (sole chokepoint)

- `open_tile(tile: VirtualTile)` — a **contextmanager** yielding an open rasterio dataset positioned
  at the tile's window (or an in-memory dataset of the materialized/clipped array), guaranteeing
  release on exit.
- `materialize(tile) -> (array, transform, profile)` — thin wrapper for callers that want pixels.
- Resolution rule (identical everywhere; no per-caller branching — R1-clean):
  1. `raster` non-null → open the bytes (`MemoryFile`). `window`/`clip_polygon`/`crs` are
     provenance; the bytes already are that result.
  2. `raster` null → `_get_or_stage_file(path)` → `rasterio.open` → read exactly `window` (may span
     >1 block) → if `crs` set and ≠ source CRS, warp via `WarpedVRT` → if `clip_polygon` set, clip
     (Unit C) → return.
- This is the **only** place that knows v1/v2/virtual. Function bodies never branch on tile shape.

### Unit C — `_clip.py`: clip application

- `clip_to_polygon(ds_or_array, transform, source_crs, clip_polygon) -> (array, transform, envelope_window, empty)`.
  Uses `rasterio.mask.mask`.
- **CRS resolution precedence** (consistent with the WKB/EWKB/WKT/EWKT geometry-input convention):
  1. **explicit `clip_crs`** (e.g. `"EPSG:4326"`) → authoritative; overrides any embedded SRID.
  2. else **SRID embedded in EWKB/EWKT** (if > 0).
  3. else **assume the polygon is already in the raster's CRS** (plain WKB/WKT, or SRID 0/unset).
  - Once the polygon CRS is resolved: if == source raster CRS, mask directly; if ≠, **reproject the
    polygon** (polygon CRS → source CRS, via `rasterio.warp.transform_geom`) then mask.
  - We reproject the **polygon**, never the raster pixels — clipping stays virtual; only an explicit
    transform op moves pixels.
- **Per-tile intersection is the normal case.** The polygon is a global/shared AOI; the unit of work
  is a tile. The per-tile result is `window_extent ∩ clip_polygon`:
  - polygon fully inside window → tile holds the entire clip;
  - polygon partially overlaps → tile holds its slice (common for a tiled source);
  - polygon disjoint from window → **empty/NoData** result (valid; dropped or carried as empty by a
    later mosaic step), **not** an error.
- Clip **refines** the window (intersection, always ⊆ window; never widens). A tile can only mask
  *within* the pixels it read, so grouping across many tiles is automatically consistent.
- **Envelope window after clip = envelope of the intersection** (that tile's slice), so a
  materialized tile's `window` is reset to the clipped extent as provenance. When `raster` is
  present and a clip was applied, `raster` IS the clipped result and `window` is its envelope.

## Data flow (lazy materialize path)

```
VirtualTile(cellid, raster=null, path, window, clip_polygon?, crs?, metadata)
        │
   open_tile(tile)                          ← the ONLY chokepoint
        │
   raster non-null? ──yes──▶ open bytes (MemoryFile); window/clip/crs = provenance ──▶ dataset
        │no
   _get_or_stage_file(path)                 ← /Volumes → worker-local (existing helper)
        │
   rasterio.open → read window (may span >1 block)
        │
   crs set & != source CRS? ──yes──▶ WarpedVRT window→crs   ← lazy-warp probe
        │
   clip_polygon set? ──yes──▶ resolve SRID (WKT/WKB=assume source; EWKT/EWKB=read→reproject if≠)
        │                     rasterio.mask → intersection (may be partial/empty) + envelope window
        ▼
   yield open dataset / (array, transform, profile)   ← released on context exit
```

## Provenance coherence invariant (recorded for later increments)

A tile's three spatial descriptors — `raster`/`path` pixels, `window`, `clip_polygon` — must always
agree in CRS/grid. When `rst_transform` (Inc 5) reprojects a tile it must update **all three**: warp
the pixels, recompute the `window` in the output grid, and reproject the `clip_polygon`. Increment 1
writes the struct and `open_tile` semantics so this contract is well-defined: `window` and
`clip_polygon` are provenance tied to the backing source's CRS; `crs` records a *pending* target.

**Default transform model (spec direction; full op is Inc 5):** lazy warp (`WarpedVRT`) keeps the
backing SRID and warps per-window on read, staying virtual and bytes-free (Model B). An explicit
eager materialize / re-COG op is the escape hatch for a concrete reprojected product (Model C).
Eager-on-transform (Model A) is rejected as default: it holds bytes at exactly the moment
reprojection is common, and a good reprojected COG requires an overview rebuild — the expensive
transient that OOM'd Serverless workers. Increment 1's WarpedVRT probe measures the lazy-warp
windowed-read cost and edge/halo correctness so the Inc-5 choice is made with numbers in hand.

## Reader window taxonomy (recorded for later increments)

`window` is not always one block. All modes are reader-produced provenance.

| Mode | `window` | Blocks pulled | Increment |
|---|---|---|---|
| default virtual | one block (blockSize), optional overview level `z` | 1 | Inc 2 |
| blockSize + overlapPercent | block + halo → larger than one block | multiple (neighbors) | Inc 3 |
| user bounds (array) | array of bounds → N virtual tiles, one window each | varies | Inc 3 |
| user x,y tiling size | regular grid → N tiles | varies | Inc 3 |

Because overlap/user-tiling produce `window ≠ blockSize` spanning **multiple** source blocks,
`open_tile` must already handle an arbitrary multi-block window in increment 1 — hence the
multi-block window test is an early seed, not deferred.

## Error handling

- Missing/unstageable `path` → `TileMaterializeError` (not a bare rasterio error); tile-isolated.
- `raster` null and `window` null → validation error at construction (Unit A).
- `clip_polygon` EWKB with SRID ≠ source and no transform available → explicit error naming both
  CRSs.
- Clip intersection empty (disjoint) → empty/NoData result with a flag, **not** a crash.
- Clip envelope exceeding the read `window` → refine (intersect), never widen.
- Release discipline: `open_tile` is a contextmanager; staged temp cleanup follows the existing
  `_get_or_stage_file` cache semantics (once per worker+file).

## Testing (TDD, local first)

- **Unit A** (`test_virtual_tile.py`): struct validators; Spark round-trip
  (`to_spark_struct`/`from_row`).
- **Unit C** (`test_clip.py`): known polygon — same-CRS, EWKB-same-SRID, EWKB-different-SRID
  (reproject), partial intersection, disjoint→empty, envelope recompute. Pure, no staging.
- **Unit B** (`test_open_tile.py`): for **each** of COG / tiled GTIFF / striped GTIFF —
  - windowed materialize == full-read slice (`np.array_equal`);
  - multi-block window correctness;
  - `raster`-present precedence path;
  - clip path end-to-end;
  - WarpedVRT probe (different CRS) — correctness + peak-RSS/cost capture;
  - partial-clip reassembly probe: one polygon across two adjacent tiles → partial results tile back
    together pixel-aligned at the seam.
- **Fixture**: one helper generates the 3 layouts (same pixels, different layout) + a small
  reprojectable variant; used by all tests and copied to a Volume for the Serverless leg.

## Deliverables

**New modules** (`python/geobrix/src/databricks/labs/gbx/pyrx/core/`):
- `virtual_tile.py`, `open_tile.py`, `_clip.py`.

**Tests** (`python/geobrix/test/pyrx/core/`):
- `test_virtual_tile.py`, `test_open_tile.py`, `test_clip.py`, plus the synthetic-corpus fixture
  helper.

**Serverless experiment** (`prompts/features/`, gitignored scratch):
- Notebook + runner driven by the `jobs.submit` + env-v5 harness: two-line `%pip` reinstall from the
  Volume wheel, `dbutils.notebook.exit(json)` + `jobs.get_run_output` self-report to a Volume.
  Corpus staged under `/Volumes/geospatial_docs/geobrix/sample-data/large-raster/` (or a
  virtual-tile subdir).

**Probes captured (feed later increments):**
1. Peak-RSS: COG-overview windowed read vs striped full-strip inflation.
2. Multi-block window read correctness.
3. WarpedVRT lazy-warp cost vs eager warp + edge/halo correctness.
4. Partial-clip reassembly across two adjacent tiles.

## Roadmap

- **Inc 1 (this spec):** `VirtualTile` struct + `open_tile`/`materialize` + clip (SRID rules,
  per-tile intersection, reproject) + WarpedVRT probe + local & Serverless proof. Seeds: multi-block
  window, partial-clip reassembly.
- **Inc 2:** reader `virtualTiles` emit mode (default one-block window @ optional overview `z`).
- **Inc 3:** window taxonomy — overlapPercent / user-bounds-array / user x,y tiling.
- **Inc 4:** all `rst_*` functions virtual-aware via `open_tile`.
- **Inc 5:** `rst_transform` triple-provenance (raster + window + clip_polygon) + mosaic/stack
  virtual-tile combinators.
