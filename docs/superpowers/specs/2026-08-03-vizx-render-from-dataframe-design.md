# VizX render from a Spark DataFrame — Design

**Status:** Approved design (2026-08-03). Feeds an implementation plan.

**One-liner:** Render raster tiles straight from a (filtered) Spark DataFrame, transparently resolving virtual **or** materialized payloads and v1 **or** v2 tile shapes — removing the "materialize-before-viz" friction the virtual-tile default introduced.

---

## Problem

VizX raster renderers today take raw `raster_bytes` (`plot_raster`, `plot_mask_layers`, `raster_layer`) or a tile struct/bytes (`plot_tile`) and read the `raster` bytes directly. Under the GeoBrix 0.5.0 virtual-tile default, tiles off the readers (and off a light pipeline) are **virtual** — `raster is None`, with `path` + `window`. So to visualize them a user must first materialize (`rst_initnodata(..., materialize=True)` or `.option("virtualTiles","false")`), which is exactly the friction that broke `eo-series/03` cell 25/26 during the light-through-finalize work.

There is also no DataFrame-level entry: a user with a filtered DataFrame of tiles must `.collect()` and index a row's `raster` themselves.

The same "reads `tile['raster']` directly, breaks on virtual" gap exists in the pyrx escape hatches `tile_to_numpy` and `rst_apply` (`pyrx/core/escape.py`), flagged in the light-through-finalize final review.

---

## Design

### New entry: `plot_tiles(df, ...)`

A dedicated VizX function (does **not** overload the existing bytes/struct functions):

```
plot_tiles(
    df,                      # a (filtered) Spark DataFrame of tile rows
    tile_col="tile",         # the tile-struct column
    *,
    mode="facet",            # "facet" | "first" | "mosaic"
    limit=None,              # hard cap on rows pulled to the driver (mode-defaulted)
    band=None, cmap=..., composite="auto", stretch=..., fig_w=..., fig_h=...,
    # (render kwargs mirror plot_raster / plot_tile where sensible)
)
```

**Modes:**
- **`facet` (default):** render up to `limit` tiles as a bounded grid of thumbnail panels (matplotlib subplot grid) — the "what's in this DataFrame" contact sheet. No reprojection/merge.
- **`first`:** resolve + render exactly one tile (first row, or a sampled/index-selected one) — the minimal peek that matches how `plot_raster(to_plot[i]...)` is used today.
- **`mosaic`:** stitch tiles into one georeferenced image by their `window`/transform + CRS (same-CRS assumption — see CRS scope). The "show me the whole thing" view.

**Size guard (hard cap + explicit `limit`):** each mode has a conservative default cap (`first` → 1; `facet` → ~25 panels; `mosaic` → a pixel/tile budget). `plot_tiles` pulls at most `limit` rows via `df.limit(N)` — it **never** collects the whole DataFrame. If the DataFrame has more rows than the cap, it renders the first `N` and emits a **warning** telling the user to filter or raise `limit`. Reuses the embed-cap discipline VizX already applies to `displayHTML` payload size (see the vizx displayHTML size caps). No `df.count()` on every call.

### Payload resolver: `resolve_tile_row`

The single tile→pixels primitive, in VizX, delegating to the tier:

```
resolve_tile_row(row, tile_col="tile") -> (open rasterio dataset OR bytes)
```

- Extract the tile struct/value from `row[tile_col]` (or accept a bare struct/bytes).
- Delegate opening to **`pyrx.core.open_tile.open_tile`** (via `_to_virtual_tile`): materialized (raster set) → open the bytes; virtual (path+window) → stage + read the window, **applying pending `nodata`/`srid`/`bands` instructions** automatically (the light-through-finalize behavior — free correctness).
- **v1 and v2 tile shapes both supported:** `_to_virtual_tile` already normalizes v1 (3-field `cellid,raster,metadata`), v2 (8-field), and raw `bytes`. So `plot_tiles` accepts any tile shape with no extra code.

VizX gains a dependency on `pyrx.core` — both ship in `geobrix[light]`, so this is a clean intra-package import, and it makes the tier the single source of truth for tile→pixels (no reimplementation, no drift, pending instructions honored).

`plot_tiles` renders each resolved dataset through the existing `_decimated_read` + `_render` path (`facet`/`first`) or a merge step (`mosaic`), so the actual pixel rendering, stretch, and composite logic are unchanged.

### Adjacent gap fixed (same primitive)

pyrx `tile_to_numpy` and `rst_apply` (`pyrx/core/escape.py`) become **virtual-aware**: when `tile["raster"]` is null, open via `open_tile` (path+window) instead of failing/returning null. This uses the same `_to_virtual_tile` → `open_tile` path, so "read a tile's pixels" is uniformly virtual-safe across the light tier — not just in VizX. (These live in pyrx, not VizX, but are the same one-line gap and belong with this work.)

---

## Non-Goals (explicit)

- **CRS-across-the-board (Spec B).** Mosaic assumes tiles **share a CRS** (the common case: one scene's windows, or one grid's cells). If rows carry differing CRS, `plot_tiles(mode="mosaic")` **raises** a clear error naming the distinct CRS values and pointing the user to filter to one CRS (a cross-CRS reprojecting mosaic is the CRS effort's job). Rationale: silently rendering a "dominant group" would drop data without the user knowing — a raise is honest and the mismatch is rare for a single-source mosaic. `facet`/`first` modes are CRS-agnostic (each tile stands alone) and never raise on mixed CRS. This spec does not pull CRS/reproject design forward.
- **No overload of existing functions.** `plot_raster` / `plot_tile` / `plot_mask_layers` / `raster_layer` keep their current bytes/struct signatures. `plot_tiles` is the new DataFrame entry. (A future pass could route them through `resolve_tile_row` too, but that's not required here.)
- **No v2 struct schema change.** Nothing about the tile struct changes.
- **Vector DataFrames** are out of scope — this is raster tiles. (VizX already has `as_gdf`/`cells_as_gdf` for vector.)

---

## Testing

VizX render tests run where the `[vizx]` extra + matplotlib (Agg) are available. Use real rasters (bench-corpus / modis) and a real (local) Spark session for the DataFrame path; no mocking of rasterio/serde.

1. **Virtual DataFrame → `plot_tiles`:** a DataFrame of virtual tiles (from `gtiff_gbx` default) renders in each mode without a manual materialize and without error; returns the expected matplotlib Axes/Figure.
2. **`resolve_tile_row`** on: a virtual v2 tile (raster null, path+window) → opens the window; a materialized v2 tile → opens bytes; a **v1** 3-field tile → opens bytes; raw bytes → opens. Pending instructions honored: a virtual tile carrying `pending_nodata` resolves with that nodata applied.
3. **Size guard:** a DataFrame larger than the mode cap renders `limit` tiles and warns (assert the warning), and does not collect all rows (assert via a spy/`limit` call count or a large synthetic DF).
4. **`mode` behaviors:** `first` renders one; `facet` renders min(N, limit) panels; `mosaic` composites same-CRS tiles into one image with correct combined bounds.
5. **Mosaic CRS mismatch:** `plot_tiles(mode="mosaic")` on a DataFrame with mixed CRS **raises** a clear error naming the distinct CRS values — asserted. `facet`/`first` on the same mixed-CRS DataFrame render without raising.
6. **Escape hatches:** `tile_to_numpy(virtual_tile)` returns the window array (not None/raise); `rst_apply` over a virtual-tile column computes per-tile (opens via path+window). v1 and materialized still work.

---

## Files (anticipated; finalized in the plan)

- Create: `python/geobrix/src/databricks/labs/gbx/vizx/_tiles.py` — `plot_tiles` + `resolve_tile_row` (+ mosaic/facet helpers).
- Modify: `python/geobrix/src/databricks/labs/gbx/vizx/__init__.py` — export `plot_tiles` (and `resolve_tile_row` if public).
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/escape.py` — `tile_to_numpy` + `rst_apply` virtual-aware via `open_tile`.
- Reuse (no change): `pyrx/core/open_tile.py` (`open_tile`, `_to_virtual_tile`), `vizx/_raster.py` (`_decimated_read`, `_render`).
- Tests: `python/geobrix/test/vizx/` (or the repo's vizx test location) for `plot_tiles`/resolver; `python/geobrix/test/pyrx/` for the escape-hatch fixes.
- Docs: a VizX page / the visualization docs — a short `plot_tiles` section (DataFrame in, modes, `limit`, virtual-transparent). Doc-test sourced per the repo's docs-are-tests convention if a viz doc-test harness exists; otherwise a usage snippet.
- Dependency/env: confirm `[vizx]` + `[light]` co-install (vizx now imports pyrx.core); update CI lock/tier gating per the new-feature dep checklist if needed.

---

## Open items for the plan

- Exact `limit` defaults per mode and the warning wording.
- `first` selection semantics (first row vs. an index/sample param).
- Mosaic implementation: `rasterio.merge` vs. manual windowed paste (windows are pixel offsets into possibly-different source rasters, so a bounds-based merge on the resolved datasets is likely cleanest).
- Whether `resolve_tile_row` is public API or internal (`_resolve_tile_row`).
