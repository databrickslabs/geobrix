# Design: Light raster reader listing performance

- **Date:** 2026-08-14
- **Status:** Design (approved direction; spec under review)
- **Scope:** GeoBrix **light tier** raster readers (`raster_gbx`/`gtiff_gbx`/`cog_gbx`, shared base). No FILE dependency.
- **Related:** memory `reader-listing-optimization`; the eo-series NB04 workaround (read a materialized Delta table instead of `.load(dir)`).

## 1. Problem
`RasterGbxReader.partitions()` (`ds/raster.py:725`) plans a `.load(<dir>)` by: (a) `_listing.list_files()` = a serial `os.walk` on the driver (`ds/_listing.py:145`), then (b) a serial per-file loop that `os.path.getsize()` **and** `rasterio.open()`s **every** file's header (`ds/raster.py:534,539`) — all on the driver at query planning, before any task runs. Over ~29K tiny TIFs on UC Volume FUSE (each open paying transient-retry backoff), this was a **~28-minute** `limit(1).show()`. All three readers share this path.

Hard constraint: pyrx must stay **Serverless/Connect-safe** — `partitions()` runs on the driver and must not use `sparkContext`/`.rdd`/`_jvm`. So parallelizing planning across executors (launching a nested Spark job from `partitions()`) is out of scope / infeasible here.

## 2. Goals / Non-goals
**Goals**
- A **pre-computed-tile input** so a caller who already knows their tiles skips the `os.walk` + per-file header opens entirely (the eo-series pattern, as a first-class feature). [Approach 1]
- **Lazy planning** for the plain `.load(dir)` case: don't open every file's header on the driver when the split decision doesn't need it. [Approach 3]
- Preserve `.load(dir)` behavior + all AOI/split options; all three readers benefit (shared base).
- A user-facing docs note steering many-small-files toward the manifest/table path or fewer-larger COGs.

**Non-goals**
- Parallel/executor-side planning (Connect-unsafe here).
- FILE support (separate spec).
- Changing the tile struct or the emitted schema.

## 3. Approach 1 — pre-computed tile input (manifest / table)
Add a reader input that supplies **tile rows** directly, so `partitions()` builds partitions from them without `os.walk` or per-file opens:
- **Row shape:** `path` (required) + optional `window` `(col_off,row_off,w,h)` + optional dims (`tile_px`/`width`/`height`, `bands`, `dtype`, `srid`). If dims/window are present → **no header read** at all; if only `path` is present → read the header for *those files only* (still skips `os.walk` and the 28K-file scan).
- **Two supply forms, one internal "tile rows" concept:**
  - `.option("manifest", "<path-to.json|.parquet>")` — read once on the driver (Connect-safe: a single file read / `spark.read.parquet(...).collect()`), not an N-file walk.
  - `.option("tilesTable", "<catalog.schema.table>")` — `spark.read.table(...).select(the row columns).collect()` on the driver (metadata rows are small vs 29K header opens). This promotes the eo-series `band_stack` workaround to a supported path (a table of `path`+window+dims, distinct from a table of materialized tile *bytes*, which you read via `spark.table` directly with no reader).
- Mutually exclusive with a bare `path` dir scan; validated with a clear error if both/neither.
- Emits the same v2 virtual tiles as today (bytes-free `path`+`window`), so downstream is unchanged.

## 4. Approach 3 — lazy planning for `.load(dir)`
Keep the `os.walk` (listing names is far cheaper than opening headers), but **defer the per-file `rasterio.open` header read** from planning to the executor `read()` **when the split decision doesn't need dims at plan time**:
- **Passthrough / whole-file virtual tiles** (the common `virtualTiles=true`, no split, no `tileSize`, no `clipPolygons`/`windows`): emit one partition per file with the window resolved lazily; read the header in `read()` on the executor. Driver work drops to the walk only (no 28K opens).
- **Split / `tileSize` / `sizeInMB` / AOI**: these need dims to compute windows, so the header is still read at plan for those files — OR the caller supplies dims via Approach 1's manifest to avoid it. (Document this: heavy split over many small files still costs; prefer the manifest/table there.)
- `os.path.getsize` at plan is only needed for the byte-budget split; skip it for the passthrough/lazy case.

## 5. Docs (the parked "rake" note, now shipped with the remedy)
Add a concise note to the raster-readers page + the Virtual Tiles page: reading a directory of **many small files** incurs planning overhead (the reader must discover them); for large tile counts prefer (a) the new `manifest`/`tilesTable` input, or (b) fewer, larger COGs. User-facing voice; no internal vocabulary.

## 6. Invariants
- Connect/Serverless-safe: `partitions()` uses only `spark.sql`/`spark.read` (a query) + file reads for the manifest/table; no `sparkContext`/`.rdd`/`_jvm`.
- Back-compat: `.load(dir)` with no `manifest`/`tilesTable` still works; the lazy-planning change is transparent for passthrough and preserves current output; split/AOI paths unchanged where they need the header.
- All three readers (shared `partitions()`) benefit.

## 7. Testing
- **Manifest/table input:** `.load` + `manifest` (with dims) builds partitions with **zero `rasterio.open` at plan** (mock/assert `rasterio.open` not called during `partitions()`); reads the correct tiles; a bogus file NOT in the manifest is never touched. Same for `tilesTable`.
- **Lazy planning:** a temp dir of GeoTIFFs, `virtualTiles=true` passthrough → assert `partitions()` does **not** open every header at plan (open-count == 0 or O(1)), and `read()` still returns correct pixels (header read moved to the executor). `tileSize`/split path → header still read at plan (correctness preserved).
- Regression: existing reader tests stay green; `.load(dir)` output byte-identical for the covered cases.
- (Optional) a scale sanity on a large dir showing plan time collapses vs the header-open baseline.

## 8. Rollout & next step
Additive options + a transparent lazy-planning change; ships in a 0.5.x release. On spec approval → writing-plans (Approach 1 and Approach 3 as separate task groups; the docs note folded into the group that ships the manifest option).
