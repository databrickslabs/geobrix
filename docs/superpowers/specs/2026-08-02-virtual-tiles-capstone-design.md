# Virtual Tiles capstone — hero diagram + docs page — design

**Date:** 2026-08-02
**Branch:** `feature/large-raster-reader`
**Status:** design approved, ready for planning
**Related:** the whole v2 virtual-tile arc — Inc 1–5 + heavy-tier v2 ([[heavy-tier-v2-complete]]); [[light-virtual-tiling-by-reference]], [[virtual-tiles-docs-page]], [[virtual-tile-lifecycle-diagram]]. This is the **capstone** — built LAST, now that both prerequisites (Inc 5, heavy-tier v2) have landed. Ties to the 0.5.0 release ([[virtual-tiling-is-0.5.0]]).

## Problem & motivation

The v2 virtual-tile capability is fully built across both tiers but has no single, compelling home in the docs. Virtual-tiling concepts are scattered (Large Rasters page, Execution Tiers, per-reader pages). The capstone delivers: (1) a polished, slide-reusable **hero diagram** of the virtual-tile lifecycle, and (2) a dedicated **Virtual Tiles** docs page under RasterX that is the canonical concept/overview, with the Large Rasters page trimmed to link to it. This is the payoff artifact for the entire arc and the visual anchor for 0.5.0.

## The hero diagram

**Production:** a new hand-crafted SVG generator `resources/images/generators/virtual-tiles-lifecycle.py` that emits an SVG, rasterized to PNG via Chrome-headless — matching the existing repo pipeline (`rasterx-tile-structure.py`, `rasterx-function-categories.py`, `example-diagrams.py`). Output assets under `resources/images/diagrams/rasterx/` (e.g. `virtual-tiles-lifecycle.svg` + `.png`). Must be **dark/light legible** and **stand alone** (legible without geobrix context) for reuse in external slides. **First render is a working draft to iterate on** (user decision) — structure + real numbers first, polish after.

**Spine:** a left→right 4-stage lifecycle. The **virtual-vs-materialized** distinction is a *property drawn on the tile columns* (a badge / fill state), NOT a swimlane — it threads through stages (b)/(c)/(d).

**(a) SOURCE** (left) — data formats + tabular, short label + distinct glyph each:
- **striped GeoTIFF** (horizontal-bands glyph), **tiled GeoTIFF** (grid glyph), **COG** (grid + overview-pyramid + cloud glyph — COG is the format that carries the overviews/pyramid motif; do NOT draw a separate "tiled+overviews" box), **NetCDF** (layered-cube glyph), **tabular** (table-with-tile-column glyph).
- **Both/and (not a gate):** every source is usable **as-is** (straight arrow into Load) AND **optimizable** — a small inner "prepare → COG" loop (`prepare_cogs`) shows striped/arbitrary formats *optionally* standardizing to COG for cheap windowed range-reads. Label it "optional: optimize," never as a required step.

**(b) LOAD** — **readers are the primary glyph**. A source fans out into **N partitions across executors**, each emitting **tile rows** into **v2 tile-structure columns**. This stage must visually convey the **windowed / tiled / parallel** read pattern (partitioned across the cluster; per-window lazy range-reads on the virtual tiles). Tiles shown in two badge states: **virtual** (bytes-free, `path`+`window` badge, light-tier only) vs **materialized** (raster-bytes badge). The **DataFrame-of-tiny-reference-rows** is the visual centerpiece — contrasted with the DataFrame-of-bytes it replaces, annotated with real numbers from the perf runs (≈100 B reference rows vs 148 KB–527 KB materialized bytes; the OOM-dissolving ingest win).

**(c) OPERATE** — a few prominent operations, each label + glyph: **`rst_clip`**, **`rst_transform`**, **`rst_merge`/mosaic**, **`rst_slope`** (representative pixel op). Annotate each with its **default/auto** output shape — BUT the diagram must make clear that in the **lightweight tier there is ALWAYS the option to virtualize** the output (`virtualize_dir`). Do NOT draw any op as materialize-only; every op has an available "→ virtual" branch, with the auto-default indicated. Tiles flow through and change badge state per the chosen output.

**(d) WRITE** — two sinks: **writers → file** (COG/GeoTIFF/NetCDF glyphs) and **Databricks SQL → save to table** (table glyph).

**Explicitly OMITTED from the diagram:** the light↔heavy bridge callout ("virtual→heavy = error"). Kept off per user decision, reinforced by [[heavy-tier-virtual-capable-filetype-future]] — a future file type may let heavy consume virtual tiles, so that boundary is not a permanent truth to enshrine in the hero.

## The Virtual Tiles docs page

**Location:** `docs/docs/api/virtual-tiles.mdx` (RasterX docs live under `docs/docs/api/`). Add to the **RasterX** category in `docs/sidebars.js` immediately after `api/large-rasters` (manual wiring — [[wire-new-doc-pages-into-sidebar]]).

**Structure:**
1. **Hero diagram** at the top (the PNG, with SVG source in-repo).
2. **What a virtual tile is** — the v2 tile struct `(cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata)`; virtual = `raster` null + `path`/`window`; materialized = `raster` bytes present. The reference-vs-instruction principle (materialized fields = provenance of what was applied; virtual = pending instructions applied on read).
3. **Why it matters** — the OOM-dissolving ingest win with real numbers; windowed/tiled/parallel reads across executors (no driver-side collect).
4. **The lifecycle** — prepare (optional COG standardization) → read as virtual → operate → materialize; mirrors the diagram stages.
5. **Reader selection surface** — `virtualTiles`, `clipPolygons` / `windows` / `clipCrs` (JSON-list over `.option()`), `tileSize` / `overlapPercent`. Link the reader pages for full option detail rather than duplicating.
6. **Operating on tiles** — the force-output params (`virtualize_dir` / `virtualize_prefix` / `materialize`); the **always-can-virtualize** point; link the existing three-bucket taxonomy on Execution Tiers rather than duplicating it.
7. **Tiers** — "the lightweight tier is for light (virtual) raster tiles; the heavyweight tier is for heavy (binary) raster tiles"; heavy accepts v1+v2 materialized in, emits v2, and a virtual tile passed to heavy raises a clear materialize-first error. State this **present-tense/factually**, NOT as a permanent architectural truth ([[heavy-tier-virtual-capable-filetype-future]]).
8. **See also** — Large Rasters, Execution Tiers, Readers/Writers overviews.

**Large Rasters trim:** `docs/docs/api/large-rasters.mdx` keeps its format/`prepare_cogs`/reader-options **how-to depth**, but its conceptual "virtual tiles" content collapses to a short **summary + link** to the new page. Large Rasters = the how-to; Virtual Tiles = the concept + hero.

**Doc-test discipline (docs-are-tests):** any executable code snippet must live in `docs/tests/python/...` and be imported via webpack raw-loader — no inline unverified code. This is a concept/overview page: prefer prose + the diagram, and reuse existing doc-test snippets where a runnable example is warranted rather than inventing new test code. If a new runnable snippet is genuinely needed, add it to the doc-test tree with a real assertion.

**Voice:** user-facing, no internal planning vocabulary (no "wave/inc/increment N"); silent-removal honored — NO mention of the removed checkpoint / path-tile machinery.

## Scope

### In scope
- The SVG generator + rendered PNG (working-draft-first, then polish).
- The new `api/virtual-tiles.mdx` page + `sidebars.js` wiring.
- Trim of `api/large-rasters.mdx` virtual-tile concept content to summary+link.
- Any doc-test snippet the page needs (real, executed).

### Explicitly NOT in scope
- The future heavy-tier-virtual-capable file type ([[heavy-tier-virtual-capable-filetype-future]]) — its own future brainstorm.
- The 0.5.0 version bump + PR into `branch/0.5.0` — follows the capstone ([[virtual-tiling-is-0.5.0]]).
- New product capability — this is docs + a diagram only; no Scala/Python behavior change.

## Success criteria
- A polished, dark/light-legible, slide-reusable hero diagram exists (SVG source + PNG in-repo, re-renderable via the documented pipeline), conveying the 4-stage lifecycle with the bytes-free ingest win as the visual centerpiece and the always-can-virtualize property clear.
- A `Virtual Tiles` page under RasterX is the canonical concept/overview, wired into the sidebar, with the hero at top.
- Large Rasters is trimmed to summary+link (no duplicated concept content).
- Voice-clean; silent-removal honored; heavy-tier boundary stated present-tense not permanent.
- Docs build; any runnable snippet is doc-test-sourced.
