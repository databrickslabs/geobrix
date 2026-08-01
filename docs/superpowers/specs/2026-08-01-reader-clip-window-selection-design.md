# v2 Virtual-Tile Reader — Increment 2.5: reader `clipPolygons` / `windows` selection (drop bbox)

**Date:** 2026-08-01
**Branch:** `feature/large-raster-reader`
**Status:** design approved, ready for planning
**Related:** `2026-07-31-virtual-tile-reader-inc2-emit-mode-design.md` (Inc 2), `2026-07-31-v2-virtual-tile-reader-design.md` (Inc 1)

## Problem & motivation

Increment 2 gave the `raster_gbx` reader a v2 output struct and a `virtualTiles` emit mode, but its
only sub-file selection surface is the legacy single-`bbox` option. Increment 2.5 reshapes the reader
selection surface to match the v2 tile model: select by an arbitrary **clip polygon** (single or
list) or by explicit **pixel windows** (single or list), producing one tile per selection per raster.
This generalizes the single rectangle to arbitrary geometry, supports multiple AOIs in one read, and
carries the clip into the tile struct so downstream trimming is exact.

## Scope

### In scope (increment 2.5)

1. **Drop `bbox` and `bboxCrs` options entirely** (breaking; beta, no aliases). The internal
   `window_for_bbox` primitive stays (reused), but is no longer reachable via an option.
2. **Add `clipPolygons`** — single geometry OR list of geometries, documented as "one value or a
   list." Per raster, emit one tile per polygon whose envelope intersects the raster; a polygon whose
   envelope misses the raster → no tile; a polygon that masks out all pixels in its window → no tile.
   - **List encoding over `.option()` (critical — Spark options are `Map<String,String>`):** a Python
     list handed to `.option()`/`.options()` is `str()`-repr'd by Spark and does NOT survive as a
     list. So a **list is supplied as a JSON-array string**, auto-detected: the parser tries
     `json.loads(value)` and, **if it yields a list**, uses that list; **otherwise the bare string is
     treated as one geometry**. So a single polygon stays a plain WKT/EWKT string (never valid JSON →
     falls through to single), and a list is `'["<wkt1>","<wkt2>"]'`. Example:
     `.option("clipPolygons", json.dumps([wkt1, wkt2]))` for a list, or
     `.option("clipPolygons", wkt1)` for one. Programmatic callers may still pass a real Python list.
   - **Geometry encodings:** on the `.option()` surface pass **WKT or EWKT strings** (EWKT carries the
     SRID). Raw **WKB/EWKB bytes** are accepted only from **programmatic callers** passing a real
     Python list via the DataSource options dict (a `bytes` value handed to `.option()` fails to
     parse). No hex/base64 decoding this increment. `parse_geom` handles WKB/EWKB/WKT/EWKT for the
     programmatic/list path.
3. **Add `clipCrs`** — single value, nullable. Per-polygon CRS resolution (see below); governs only
   plain-geometry members.
4. **Add `windows`** — single pixel window `(col,row,w,h)` OR list, documented as "one value or a
   list." Per raster, emit one tile per window; a window partly outside the extent is clipped to the
   extent; a window fully outside → no tile. Over `.option()` a single window is a JSON 4-int array
   `"[0,0,256,256]"`; a list is a JSON array of 4-int arrays
   `"[[0,0,256,256],[256,0,256,256]]"` (auto-detected via `json.loads` → list-of-lists vs a single
   4-int list). Programmatic callers may pass a real list of 4-int tuples.
5. **`clipPolygons` and `windows` are mutually exclusive** — supplying both raises a clear error.
   Supplying neither = whole-file behavior (unchanged from Inc 2).
6. Both selection modes work in **materialized** (default) and **virtual** (`virtualTiles=true`)
   modes.

### Explicitly NOT in scope (deferred)

- `overlapPercent` and regular `x,y` tiling-size selection (Inc 3).
- Routing `rst_*` through `open_tile` catalog-wide (Inc 4).
- Heavy-tier v2 handling.
- Catalog / binding registration — reader options only; no SQL surface, no
  `registered_functions.txt` / `function-info.json` / bindings changes.

## The reference-vs-instruction principle (governs both tiers)

A tile's `window` / `clip_polygon` / `clip_crs` fields mean different things by tier:

- **Materialized tile** (`raster` non-null): the fields are **reference / provenance** — a record of
  what was *already applied*. Choice-2 clip: the reader has already masked the pixels to the polygon;
  `clip_polygon`/`clip_crs` describe that completed action.
- **Virtual tile** (`raster` null): the fields are **instructions** — pending actions `open_tile`
  applies at materialize time.

Corollary (design-consistent, realized in later increments): `rst_transform` on a virtual tile
produces a materialized tile in which all fields become reference. This principle removes any
ambiguity about whether the reader "should have" applied the clip: materialized = applied-and-recorded,
virtual = recorded-to-apply.

## CRS resolution (`clipCrs`)

Per-polygon precedence (variation B — matches Inc-1 `_clip.clip_dataset`):

1. **embedded SRID** of an EWKB/EWKT polygon (if > 0) — authoritative for that polygon.
2. else the reader-level **`clipCrs`** (if set).
3. else **assume the raster's CRS** (no reprojection).

Consequences:
- A list supplied exclusively as EWKB/EWKT is self-describing → `clipCrs` is naturally null.
- If any plain WKB/WKT is present, `clipCrs` is the CRS for those members; if `clipCrs` is also null,
  they are assumed to already be in the raster's CRS (no error — the graceful fallback).
- Mixed-CRS lists are supported: EWKB members use their own SRID, plain members use `clipCrs`/raster.

**Unified precedence across all layers (change to Inc-1 `_clip`).** Historically
`_clip.clip_dataset` treated an explicit `clip_crs` arg as *authoritative over* any embedded SRID
(it unconditionally `set_srid`'d the geometry). That contradicts the reader rule above. We align
`_clip.clip_dataset` to the SAME rule: `clip_crs` applies **only when the geometry carries no
embedded SRID** (i.e. plain WKB/WKT); an EWKB/EWKT with SRID > 0 keeps its own SRID. So the ordering
is identical everywhere — embedded SRID → `clip_crs` → raster CRS — at the reader, in `_clip`, and
for a tile-struct `clip_crs`. No layer contradicts another; a caller sets `clip_crs` only to give a
CRS to geometries that lack one. (Safe: all existing Inc-1 `_clip` tests pass plain WKB, so their
behavior is unchanged — plain WKB + `clip_crs` still reprojects.)

## Architecture

Changes are confined to the light-tier reader (`ds/raster.py`) plus a small window helper
(`ds/_window.py`) and the existing clip engine (`pyrx/core/_clip.py`, reused unchanged).

### Option parsing (`RasterGbxReader.__init__`)

- Parse `clipPolygons` (accept a single value or a list → normalize to a list of geometry inputs) and
  `windows` (single `(c,r,w,h)` or list → normalize to a list of 4-tuples). `clipCrs` = optional
  string.
- If both `clipPolygons` and `windows` are non-empty → `ValueError`.
- Remove all `bbox` / `bboxCrs` parsing.

### `window_for_geom` (`ds/_window.py`, new — generalizes `window_for_bbox`)

- `window_for_geom(src, geom, geom_crs=None) -> Optional[Window]`: reproject the geometry's bounds
  from `geom_crs` (resolved per CRS precedence) into `src.crs`, compute the pixel envelope window
  (`from_bounds` → floor/ceil → clip to `(0,0,W,H)`), return None if disjoint. Mirrors the
  clip-safe pattern of `window_for_bbox` (window and `window_transform` agree). `window_for_bbox`
  stays for internal reuse / can be expressed as `window_for_geom` over a box.

### Planning (`_plan_partitions_for_file`)

Signature gains `clip_polygons: list`, `clip_crs: Optional[str]`, `windows: list` (replacing
`bbox`/`bbox_crs`). Branch order after the `emit_virtual` handling:

- **`clipPolygons`:** for each polygon → `parse_geom` → resolve CRS (embedded → clipCrs → raster) →
  `window_for_geom` (envelope ∩ extent) → disjoint? skip : emit `_TilePartition(window=envelope,
  clip_polygon=<wkb>, clip_crs=<resolved crs string>)`.
- **`windows`:** for each `(c,r,w,h)` → intersect with `(0,0,W,H)` → fully outside? skip : emit
  `_TilePartition(window=clipped, clip_polygon=None)`.
- **neither:** whole-file (Inc 2 passthrough / whole-encode / split — unchanged).

`_TilePartition` already carries `clip_polygon` / `clip_crs` (added in Inc 2); planning now populates
them for the clip path.

### Emission (`read`)

Reuses the Inc-2 v2 emission, honoring the Choice-2 materialized-clip rule:

- **Materialized + clip:** stage → read the envelope window → `_clip.clip_dataset(ds, clip_polygon,
  clip_crs)` (mask to polygon, reproject cutline as needed) → if the result is None/all-nodata, skip
  (no tile) → else v2 tile with `raster`=pre-clipped bytes and `clip_polygon`/`clip_crs` as reference.
- **Materialized + window (no clip):** stage → read window → v2 tile with `raster`=window bytes.
- **Virtual (either selection):** v2 tile with `raster`=null; `window` + (for clip) `clip_polygon` /
  `clip_crs` as instructions. No staging / encode.

The `_read_legacy` path (test-compat) drops its `bbox` branch; if it retains any windowing it uses the
same `_v2_tile_row` assembly.

## Data flow

```
clipPolygons=[g1,g2,...]:
  per gi → parse_geom → resolve CRS (embedded SRID → clipCrs → raster)
         → window_for_geom(envelope) ∩ extent → disjoint? SKIP
         → _TilePartition(window, clip_polygon=gi_wkb, clip_crs=resolved)
  materialized: stage → read window → _clip.clip_dataset → all-nodata? SKIP
                : v2 tile(raster=clipped bytes; clip_* = REFERENCE)
  virtual:      v2 tile(raster=null; window + clip_* = INSTRUCTIONS)

windows=[w1,w2,...]:
  per wi → wi ∩ (0,0,W,H) → fully outside? SKIP
         → _TilePartition(window=clipped wi, clip_polygon=None)
  materialized: stage → read window → v2 tile(raster=window bytes)
  virtual:      v2 tile(raster=null, window=wi)

neither: whole-file (Inc 2)
both clipPolygons & windows: ValueError at option-parse
```

## Error handling

- `clipPolygons` and `windows` both non-empty → `ValueError` at parse (mutually exclusive).
- Unparseable geometry in `clipPolygons` → clear error identifying the offending entry.
- Plain WKB/WKT + null `clipCrs` → assume raster CRS (no error).
- Polygon envelope disjoint from raster → skip (no tile). Clip masking out all window pixels → skip.
- `windows` entry fully outside extent → skip; partly outside → clip to extent.
- All `bbox` / `bboxCrs` parsing removed; their tests migrated to the clip/window equivalents.

## Testing (TDD, local Docker first)

Run via `gbx:test:pyrx --path python/geobrix/test/ds/...`. Reuse the 3-layout fixture.

- **clipPolygons:** single + list → N tiles, each with correct `clip_polygon`/`clip_crs`; materialized
  = pre-clipped (nodata outside the polygon), virtual = `raster` null + clip fields set.
- **CRS resolution:** EWKB (embedded SRID, `clipCrs` null); plain WKB + `clipCrs`; plain WKB + null
  `clipCrs` (assume raster CRS); mixed EWKB/plain list.
- **windows:** single + list → N tiles; partial-overhang clipped to extent; fully-outside skipped.
- **Mutual exclusion:** both set → error. Disjoint polygon → skipped. All-nodata clip → skipped.
- **Round-trip:** a virtual clip tile → `open_tile` → pixels == a manual polygon-mask of the window
  (reuses Inc-1 clip machinery).
- **Migrate:** `test_raster_bbox.py` → `test_raster_clip.py` (bbox is gone); delete bbox-specific
  assertions.
- **Serverless (fire directly, oauth-fe + `.venv-pyrx`):** reader with `clipPolygons` (mixed CRS) and
  `windows` on real `/Volumes`, both tiers, worker-side materialize.

## Deliverables

- `ds/raster.py`: option parsing (`clipPolygons`/`windows`/`clipCrs`, drop `bbox`/`bboxCrs`), planning
  branches, emission honoring Choice-2 clip; `_read_legacy` bbox branch removed.
- `ds/_window.py`: `window_for_geom` (generalized envelope window).
- Reuse `pyrx/core/_clip.clip_dataset`, `pyrx/_geom.parse_geom`, `_v2_tile_row` (Inc 2) unchanged.
- Tests: `test_raster_clip.py` (new, replaces `test_raster_bbox.py`), `test_raster_window_select.py`,
  CRS-resolution cases; migrate any `test_raster_datasource.py` bbox references.
- Serverless experiment notebook under `prompts/features/` (gitignored scratch).

## Known gaps / follow-ons (tracked, not built here)

- `overlapPercent` + regular `x,y` tiling-size selection (Inc 3).
- Functions virtual-aware via `open_tile` (Inc 4); heavy-tier v2 handling.
- Carried: `ds/writer.py` crashes on a virtual `raster=None` row (guard when virtual-write becomes a
  wired flow); COG `format` metadata reads `"gtiff"`; `materialize_to_bytes` clean-profile fix; dedup
  `_epsg_of`/`_epsg_int`; non-EPSG (WKT2/PROJ) CRS handling.
