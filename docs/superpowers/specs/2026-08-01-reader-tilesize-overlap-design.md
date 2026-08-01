# v2 Virtual-Tile Reader — Increment 3: regular `tileSize` grid + `overlapPercent`

**Date:** 2026-08-01
**Branch:** `feature/large-raster-reader`
**Status:** design approved, ready for planning
**Related:** `2026-08-01-reader-clip-window-selection-design.md` (Inc 2.5), `2026-07-31-virtual-tile-reader-inc2-emit-mode-design.md` (Inc 2)

## Problem & motivation

Increments 2/2.5 gave the light raster readers a v2 tile struct and a selection surface:
whole-file (default), budget-driven `splitStrategy`, explicit `windows`, and `clipPolygons`. What is
missing is a **regular fixed-size tiling grid** — "cut this raster into 512×512 tiles" — and the
ability for those tiles to **overlap** so a spatial feature straddling a seam appears whole in at
least one tile. Increment 3 adds `tileSize` (a regular grid selection mode) and `overlapPercent` (a
modifier of `tileSize`), reusing the existing overlap-step math so semantics match the
`rst_tooverlappingtiles` function.

## Scope

### In scope (increment 3)

1. **`tileSize`** reader option — a string `"w,h"` (e.g. `"512,512"`); a bare `"512"` means square
   `512×512`. Tiles the whole raster into a regular fixed-size pixel grid, one tile per cell. A
   **third selection mode**, mutually exclusive with `clipPolygons` and `windows`.
2. **`overlapPercent`** reader option — companion int, default `0`, a modifier of `tileSize` **only**.
   Overlap on both axes, using the existing `_overlap_steps` contract:
   `overlap_px = ceil(tile_dim · pct/100)`, `step = max(1, tile_dim − overlap_px)`. `pct=0` →
   non-overlapping regular grid.
3. Works in both **materialized** (default) and **virtual** (`virtualTiles=true`) modes.
4. The ~2GB decoded-cell guard applies to **materialized tiles only** (bytes in the Spark cell); a
   `tileSize` grid is honored exactly (no auto-shrink) with a clear error if a materialized cell would
   exceed the limit. Virtual-mode `tileSize` is **unguarded** (bytes-free); the guard fires later at
   materialize time if such a tile is materialized.

### Explicitly NOT in scope (deferred)

- Wiring `overlapPercent` into the budget-driven `splitStrategy`/`plan_layout` path (the queued
  "overlap in auto-split" idea) — that path stays hard-cut this increment. `overlapPercent` is a
  `tileSize`-only modifier here.
- Heavy-tier support.
- Catalog / binding registration — reader options only; no SQL surface, no `registered_functions.txt`
  / `function-info.json` / bindings changes.

## Why overlap is `tileSize`-only

`windows` and `clipPolygons` are **explicit user-specified extents** — the user already stated exactly
the region they want, so an overlap modifier has no meaning there. Overlap only makes sense when *the
reader subdivides* into a grid it chose, i.e. `tileSize`. This also settles the deferral: overlap is
not wired into `splitStrategy` (another reader-chosen subdivision) this increment — only `tileSize`.

## The materialized-only guard (tier distinction)

The ~2GB guard protects against raster **bytes** overflowing a Spark cell. A **virtual** tile carries
no bytes (`raster` null; just `path`+`window`), so its `tileSize` can be arbitrarily large with zero
cell risk — no guard. Only **materialized** tiles need it. It composes cleanly: a virtual tile later
materialized (via `open_tile`/`materialize_to_bytes`) hits the guard **there**, at the point bytes are
produced — not at reader plan time. Rule: **guard the per-tile decoded-cell size only when
materializing; virtual-mode `tileSize` is unguarded.**

## Architecture

Changes are confined to the light reader (`ds/raster.py`) plus a pure window-enumerator in
`pyrx/core/tiling.py`. The `open_tile` consumer needs **no** change — a `tileSize` tile is just a
windowed tile with no clip.

Key insight: the reader plans **windows at the driver** (bytes-free, so virtual mode works), but the
existing `tiling.iter_to_overlapping_tiles` yields *encoded bytes* from an open dataset. Inc 3 must not
call that byte path in planning — it needs a pure **window enumerator** producing `(col,row,w,h)`
tuples that both tiers then consume identically.

### Unit A — `plan_grid_windows` (`pyrx/core/tiling.py`, new)

`plan_grid_windows(width, height, tile_w, tile_h, overlap=0) -> list[tuple[int,int,int,int]]` —
enumerate the regular grid over `width×height`, honoring `overlap` via the **existing**
`_overlap_steps` step contract, each window clamped to the raster extent (edge tiles smaller). Pure
computation: no dataset, no bytes. The DRY counterpart to `_iter_window_tiles` (same stepping math,
returns windows instead of reading them). Factor the shared step logic so overlap semantics stay
identical to `rst_tooverlappingtiles`.

### Unit B — reader option parsing (`RasterGbxReader.__init__`)

- `self.tile_size = _as_tile_size(options.get("tileSize"))` → `(w,h)` or `None` (parse `"w,h"`; a bare
  `"n"` → `(n,n)`; validate positive ints).
- `self.overlap_percent = int(options.get("overlapPercent", "0"))` (validate `0 <= pct < 100`).
- Extend mutual-exclusion: at most one of `clip_polygons` / `windows` / `tile_size` non-empty, else
  `ValueError`. `overlap_percent > 0` without `tile_size` → clear `ValueError`.

### Unit C — planning branch (`_plan_partitions_for_file`)

New branch after the `emit_virtual`/clipPolygons/windows branches, before the normal/split branch: if
`tile_size` set → open header for `(W,H)`, call `plan_grid_windows(W, H, tw, th, overlap)`, emit one
`_TilePartition(window=w)` per window (no clip), threading `emit_virtual`. One header open; all windows
planned on the driver.

### Unit D — materialized-only guard

In the materialized emission path (where `encode_tile` produces bytes), apply the existing
~2GB decoded-cell check per tile (`_estimate_tile_bytes` / `_MAX_TILE_BYTES`) with a clear message
naming `tileSize`. Virtual emission skips it. Verify the `tileSize` materialized path routes through
the shared guard (add a targeted test).

### Reuse (no reinvention)

`_overlap_steps` (overlap math), `_TilePartition` / `_v2_tile_row` (Inc 2), the existing cell-guard
constant, `emit_virtual` threading. New surface is `plan_grid_windows` + `_as_tile_size` + one planning
branch.

## Data flow

```
tileSize=(tw,th), overlapPercent=p:
  plan: open header (W,H) -> plan_grid_windows(W,H,tw,th,p)
        -> one _TilePartition(window=(c,r,w,h)) per grid cell (clamped to extent)
  materialized: stage -> read window -> encode -> v2 tile(raster=bytes)  [~2GB cell guard HERE]
  virtual:      v2 tile(raster=null, window)                             [no guard - bytes-free]

selection exclusivity (at most one): clipPolygons XOR windows XOR tileSize  (>1 -> ValueError)
  none set -> whole-file / splitStrategy (Inc 2, unchanged)
  overlapPercent>0 requires tileSize
```

Overlap: `overlap_px = ceil(tile_dim·p/100)`, `step = max(1, tile_dim − overlap_px)`, both axes; edge
windows clamp to extent; `p=0` → `step = tile_dim` (non-overlapping).

## Error handling

- More than one of `clipPolygons` / `windows` / `tileSize` → `ValueError` (extends existing check).
- `overlapPercent > 0` without `tileSize` → clear `ValueError`.
- `tileSize` unparseable / non-positive, or `overlapPercent` outside `0..99` → clear `ValueError` at
  parse.
- Materialized tile whose decoded cell exceeds ~2GB → clear error naming `tileSize`. Virtual → no
  guard.
- `tileSize` larger than the raster → one clamped whole-file window (grid of one), not an error.

## Testing (TDD, local Docker first)

- **Unit A** (`test_core_tiling.py`): `plan_grid_windows` — count + offsets for known W,H,tile,overlap
  (parity with `to_overlapping_tiles` origins); `p=0` exact non-overlapping grid; tile>raster → single
  clamped window; complete coverage (union covers every pixel).
- **Option parsing** (`test_raster_options.py`): `tileSize` `"512,512"`/`"512"`/malformed; `overlapPercent`
  parse + out-of-range; mutual-exclusion (tileSize+windows, tileSize+clipPolygons → raise);
  overlapPercent-without-tileSize → raise.
- **Planning** (`test_raster_plan_select.py`): `tileSize` → N partitions, correct windows; overlap
  changes N + offsets; tile>raster → 1.
- **Emission** (`test_raster_tilesize.py`, new): materialized `tileSize` → per-cell bytes, windowed
  reads == source slices; virtual `tileSize` → raster null + windows, `open_tile` round-trip; oversized
  materialized cell → clear guard error; oversized virtual → no error.
- **Serverless (fire directly, oauth-fe + `.venv-pyrx`):** `tileSize` + `overlapPercent` on the Volume
  corpus, both tiers — grid cell count, overlap offsets, materialized bytes vs virtual worker
  `open_tile`.

## Deliverables

- `pyrx/core/tiling.py`: `plan_grid_windows` (+ factored shared step logic).
- `ds/raster.py`: `_as_tile_size`, `tileSize`/`overlapPercent` parsing + mutual-exclusion, planning
  branch, materialized guard wiring.
- Tests: additions to `test_core_tiling.py` / `test_raster_options.py` / `test_raster_plan_select.py`;
  new `test_raster_tilesize.py`.
- Reader docs (`readers/raster.mdx` + geotiff/cog option tables): add `tileSize` / `overlapPercent`
  with the feature-straddling-a-seam "why" example.
- Serverless experiment notebook under `prompts/features/` (gitignored scratch).

## Known gaps / follow-ons (tracked, not built here)

- `overlapPercent` in the budget-driven `splitStrategy`/`plan_layout` path (per
  `reader-overlap-percent-option` memory) — deferred; overlap is `tileSize`-only here.
- Heavy-tier support.
- Carried: `ds/writer.py` virtual `raster=None` guard; COG `format` metadata reads `"gtiff"`;
  `materialize_to_bytes` clean-profile fix; dedup `_epsg_of`/`_epsg_int`; non-EPSG (WKT2/PROJ) CRS.
