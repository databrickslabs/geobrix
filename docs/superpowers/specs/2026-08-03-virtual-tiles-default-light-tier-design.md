# Virtual tiles as the light-tier default — design

**Date:** 2026-08-03
**Branch:** `branch/0.5.0`
**Status:** design approved, ready for planning
**Related:** the v2 virtual-tile arc — [[light-virtual-tiling-by-reference]], [[heavy-tier-v2-complete]], [[virtual-tiles-docs-page]]; ships in 0.5.0 ([[virtual-tiling-is-0.5.0]]).

## Problem & motivation

Virtual tiles are the light tier's answer to large-raster ingest OOM, but they are currently **opt-in** (`.option("virtualTiles", "true")`). The light-tier raster readers should default to virtual tiles — the readers are the "new front door" for loading data into the virtual-tile model, so that model should be what you get by default. This increment flips the default, makes the reader option surface + defaults clearly documented and prominently oriented (Readers Overview, Readers & Writers Overview, Quickstart), validates that virtual-by-default is ship-ready by executing the examples, and captures a virtual-tiles column in the Benchmarking comparison.

## Key finding from recon (shrinks the scope)

The reader option surface is **already unified**. `raster_gbx` / `gtiff_gbx` / `cog_gbx` all inherit one `RasterGbxReader.__init__` (`python/geobrix/src/databricks/labs/gbx/ds/raster.py:684–723`) that already parses **every** windowing option — `virtualTiles`, `tileSize`, `overlapPercent`, `clipPolygons`, `clipCrs`, `windows`, `splitStrategy`, `sizeInMB`, `filterRegex`. There are **no COG-only reader options to pull through** — the "pull COG's options through all readers" goal is already satisfied by the shared base class. The `cog_gbx` reader class adds only `driver="GTiff"`. So the code change is a one-line default flip, not option plumbing.

## Decisions (user-pinned)

1. **Flip the default for ALL light raster readers** (`raster_gbx`, `gtiff_gbx`, `cog_gbx`) — `virtualTiles` defaults to `true`. This is a **breaking behavior change** (existing code that consumed raster bytes now gets virtual tiles; a virtual tile handed to a heavy function raises the materialize-first error). Documented as breaking in the release notes.
2. **NetCDF is scoped out.** `netcdf_gbx`'s raster reader overrides `partitions()` and bypasses the virtual-tile plan (`_FilePartition` path); it stays materialized-by-default this increment, documented as a known exception (virtual-tile support for NetCDF is a follow-up with its own design).
3. **Examples: validate + minimal fixes + surface the concept.** Run each raster example on the new default; fix only what breaks (add `materialize` where a virtual tile now hits a heavy function or a bytes-expecting step); add a brief callout where a notebook is a natural place to show virtual-tile behavior. Do not rewrite working examples.
4. **Benchmarking: add a `light (virtual)` column** beside light(materialized)/heavy, marked NEW DEFAULT; reuse existing materialized + heavy numbers (do NOT re-run those); benchmark all readers + tile-emitting functions in virtual mode.

## Scope — three phases

### Phase A — code + tests + docs + quickstart (no cluster needed)

- **Code:** `raster.py:723` default `"false"` → `"true"`. Inherited by all three readers.
- **Tests:** (a) grep the reader/ds test suite + doc-tests for reads that omit `virtualTiles` and assert raster bytes present / pass to a bytes-expecting step — update those to either pass `virtualTiles=false` (if they specifically test materialized) or add `materialize` (if incidental). (b) Add a test asserting each of raster_gbx / gtiff_gbx / cog_gbx emits a **virtual** tile by default (raster null, path+window set), and that `virtualTiles=false` still materializes. (c) Confirm netcdf_gbx raster still materializes (unchanged).
- **Reader docs:** add a `virtualTiles` row (**default `true`**) to the option tables in `docs/docs/readers/raster.mdx`, `cog.mdx`, `geotiff.mdx`, alongside the full shared windowing option set; add a "default changed" admonition on each (mirroring the existing `splitStrategy` one) stating virtual is now default + how to opt out (`virtualTiles=false`). Note the NetCDF exception on `netcdf.mdx`.
- **Orienting content:** a prominent "Loading into virtual tiles (the default)" callout near the top of `docs/docs/readers/overview.mdx` and `docs/docs/readers-writers.mdx` — light raster readers load bytes-free virtual tiles by default; link to [Virtual Tiles](./api/virtual-tiles). Fix `virtual-tiles.mdx` line ~92 ("virtual by default in the lightweight tier") which currently reads prospectively — now accurate.
- **Quickstart:** `docs/docs/quick-start.mdx` loads via `gtiff_gbx` (doc-test `READ_GEOTIFF_LIGHT` in `docs/tests/python/quickstart/examples.py`). Make the virtual-tile default explicit — a one-line note that the returned tiles are virtual (bytes-free) and how to materialize / read bytes when needed. Any executable snippet stays doc-test-sourced and must run (`gbx:test:python-docs`).
- **Release notes:** add a breaking-change note to the v0.5.0 section that light raster readers now default to virtual tiles.

### Phase B — examples: validate / fix / surface (needs execution)

For each raster example that loads via a light reader — `notebooks/examples/eo-series/03`, `eo-series/04`, `helios/03`, `vapor-eyes/02`, `vapor-eyes/03`, `xview/Clipping` (NOT vapor-eyes/01, which is NetCDF vector, scoped out) — run on the new default; fix only breaks (add `materialize()` at heavy-function or bytes-expecting boundaries); add a brief virtual-tile callout where natural (per [[notebook-narrative-tracks-code]] — narrative tracks code). Execution on Serverless is the ship-readiness gate. Mirror any narrative changes into the corresponding `docs/docs/notebooks/*.mdx`.

### Phase C — benchmarking (needs staged wheel + cluster)

- **Harness:** add a virtual leg to the reader bench (`run_format_read(..., virtualTiles=true)` / a `run_virtual_reader`) in `python/geobrix/src/databricks/labs/gbx/bench/readers.py`, and virtual output for the tile-emitting functions in the functions bench.
- **Runs:** benchmark all readers + all tile-emitting functions in virtual mode on the bench cluster (staged 0.5.0 wheel; see [[bench-wheel-path-divergence]], [[cluster-bench-setup]], [[benchmarking-preflight-discipline]]). Do NOT re-run materialized light or heavy — reuse existing numbers.
- **Doc table:** extend the Benchmarking comparison table (`docs/docs/api/benchmarking.mdx`) with a `light (virtual)` column marked NEW DEFAULT; prose calls out that virtual is the default, the ingest row-size win (~1,400–5,000× smaller rows), and the per-op lazy-read cost. Per [[bench-changes-update-docs]], bench change updates benchmarking.mdx same stroke.

### Explicitly NOT in scope
- NetCDF virtual-tile support (its own follow-up).
- Re-benchmarking materialized-light or heavy (reuse existing numbers).
- Any heavy-tier change (heavy still consumes only materialized; a virtual tile raises the existing guard).
- The future heavy-virtual-capable file type ([[heavy-tier-virtual-capable-filetype-future]]).

## Error handling & edge cases
- A virtual tile reaching a heavy function raises the existing `VirtualTileException` (unchanged). The default flip makes this reachable from previously-materialized pipelines — hence the examples validation pass.
- `virtualTiles=false` remains the explicit opt-out and must still materialize on all three readers.
- Writers already auto-materialize a virtual tile on write, so read→write pipelines stay correct.
- NetCDF raster reads are unaffected (materialized), documented.

## Success criteria
- All three light raster readers emit virtual tiles by default; `virtualTiles=false` still materializes; netcdf_gbx raster still materializes. Tests prove all three.
- Reader docs document `virtualTiles` (default `true`) + the full windowing option set, with a default-changed admonition; Readers Overview + Readers & Writers Overview carry prominent virtual-tile orienting content; Quickstart shows the default; release notes flag the breaking change.
- Every raster example executes on the new default (ship-readiness proven), with minimal fixes + concept callouts.
- Benchmarking table has a `light (virtual)` column (new default) covering all readers + tile-emitting functions, with prose on the row-size win.
- Voice-clean; docs build passes.
