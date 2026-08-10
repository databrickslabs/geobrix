# RasterX Tabbed Examples (Corrected) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render every RasterX function's example as a 4-tab block whose tabs are unmistakably the SAME example — the tier-idiomatic invocation of that function on a conventionally-defined `tile` column, established once in a page-top Conventions section.

**Architecture:** The tabbing mechanism (`FunctionExamples` wrapper, `generate-function-info.py` bindings glob-scan, `gbx-example-lang` CSS) is SHIPPED and reused UNCHANGED. This plan (a) adds a canonical multiband GeoTIFF fixture, (b) authors a page-top Conventions section + shared setup helpers so every example is a bare invocation on `tile`, (c) re-authors the drifted Batch A accessors to the standard, (d) authors families B–G, (e) alphabetizes headings within each family. Supersedes plan `2026-08-10-complete-rasterx-tabbed-examples.md`.

**Tech Stack:** Docusaurus/MDX, the `FunctionExamples` React wrapper + `CodeFromTest`, Python doc-tests (`docs/tests/python/api/`), Scala doc-tests (`docs/tests/scala/api/ScalaApiExamples.scala`), `generate-function-info.py`, rasterio (fixture construction). Run in the `geobrix-dev` Docker container via `gbx:*`.

## Global Constraints

- **ONE example per function, shared across all 4 tabs.** The example IS the tier-idiomatic invocation of the function on a conventionally-defined `tile` column — nothing more. SQL `SELECT gbx_rst_X(tile) ... FROM rasters`; light/heavy `df.select(rx.rst_X("tile"))`; Scala `rasters.select(rx.rst_X(col("tile")))`. All four MUST use the same fixture + operation + argument values.
- **Conventions section defines the shared setup ONCE** — at the TOP of the Functions reference (after the existing `## Tier availability` section, immediately before `## Accessor Functions`). It states the canonical files, the `tile`-column convention (`rasters`/`df` = the sample loaded as a `tile` column), how to read the tabs, and the output-note convention. No per-function boilerplate load.
- **Output renders in each tier's NATURAL form**; where a form genuinely differs use a consistent shorthand + a one-line note: `...` + "(WKB binary)" for geometry; `SUBDATASET_1_NAME -> ..., SUBDATASET_1_DESC -> ...` for subdataset maps; similar shorthand for band-metadata maps. Scalar values must be REAL and IDENTICAL across tiers (same fixture → same value). No placeholders (`[<float>]`), no `{}` where a real value exists.
- **Doc-tests stay REAL + executable + asserting.** The shared fixture load lives in a shared setup helper the example calls; the SHOWN snippet is the bare invocation; the TEST around it runs end-to-end against the fixture and asserts the real value. Naming matches the generator scan EXACTLY: `def <base>_python_light_example(spark)`, `def <base>_python_heavy_example(spark)`, `val <base>_scala_example`, `def <base>_sql_example()` (+ each `_output`).
- **Code-indicators green checkmark REQUIRED:** every present tab earns the green "🔗 Fully Validated" badge — `FunctionExamples` already passes `source`/`testFile`/`functionName`; every example doc-test file lives under `docs/tests/...`, NEVER `integration/` or `tests-dbr/`.
- **Per-function override note:** a function using a NON-default fixture (multiband / DEM / NetCDF) or a fuller example (constructors) carries a one-line note flagging the exception. Default (single-band GeoTIFF) needs no per-function note — it's covered by Conventions.
- **Canonical fixtures:** single-band `nyc_sentinel2_red.tif` (default); NEW committed multiband GeoTIFF (band-math/`numbands`/`bandmetadata`); DEM `srtm_n40w073.tif` (terrain); NetCDF `nyc_climate.nc` (`rst_subdatasets`/`rst_getsubdataset` only, with note); an aggregation set of a few `bench-corpus/rows/r*.tif` (or several tiles of the canonical GeoTIFF) for `*_agg`.
- **Never fabricate an example for a nonexistent binding** — the only genuine gap is `rst_fromfile`/Scala (renders the note). Any other genuinely-missing binding is a surfaced finding.
- **Port 3000 is the user's.** A `gbx:docs:build` stops it; run ONE build per batch, and the ORCHESTRATOR restarts + curl-verifies `gbx:docs:dev --port 3000` after EACH batch (standing rule). Agent previews use a NON-3000 port (3001), stopped after.
- **Mechanism is out of scope** — do NOT modify `FunctionExamples.js`, `generate-function-info.py`, or the CSS.
- **Commit hygiene:** subjects ≤72 chars, WHY body, end `Co-authored-by: Isaac`; single plain `git commit`; `git add` ONLY the batch's files (unrelated pre-existing working-tree edits must never be swept in); no push; NEVER `-n`/`--no-verify`; do NOT sub-delegate.
- **Review checks, every batch:** (a) all present tabs invoke THIS function; (b) real per-function SQL example present (no raw/empty SQL tab); (c) non-degenerate real output; (d) ALL tabs use the SAME example (fixture+op+args) — the structural check; (e) non-default fixture / fuller example noted per-function; (f) headings alphabetical within family.

## Family assignments (authoritative — 130 RasterX names, from registered_functions.txt)

- **Accessors (~29):** rst_avg, rst_bandmetadata, rst_boundingbox, rst_format, rst_georeference, rst_getnodata, rst_getsubdataset, rst_height, rst_max, rst_median, rst_memsize, rst_metadata, rst_min, rst_numbands, rst_pixelcount, rst_pixelheight, rst_pixelwidth, rst_rotation, rst_scalex, rst_scaley, rst_skewx, rst_skewy, rst_srid, rst_crs, rst_subdatasets, rst_summary, rst_type, rst_upperleftx, rst_upperlefty, rst_width, rst_isempty, rst_tryopen, rst_histogram.
- **Tile ops & constructors (~20):** rst_frombands, rst_fromcontent, rst_fromfile, rst_asformat, rst_clip, rst_filter, rst_initnodata, rst_transform, rst_transformcrs, rst_updatetype, rst_band, rst_buildoverviews, rst_fillnodata, rst_setsrid, rst_setcrs, rst_threshold, rst_resample, rst_resample_to_res, rst_resample_to_size, rst_convolve, rst_cog_convert.
- **Band math & indices (~10) [MULTIBAND fixture]:** rst_combineavg, rst_derivedband, rst_mapalgebra, rst_merge, rst_ndvi, rst_evi, rst_index, rst_nbr, rst_ndwi, rst_savi.
- **Aggregators (~10) [MULTI-TILE]:** rst_combineavg_agg, rst_derivedband_agg, rst_merge_agg, rst_frombands_agg, rst_rasterize_agg, rst_gridfrompoints_agg, rst_dtmfromgeoms_agg, rst_h3_rasterize_agg, rst_quadbin_rasterize_agg, rst_bng_rasterize_agg.
- **Terrain (~14) [DEM fixture]:** rst_aspect, rst_color_relief, rst_hillshade, rst_roughness, rst_slope, rst_tpi, rst_tri, rst_contour, rst_proximity, rst_viewshed, rst_gridfrompoints, rst_dtmfromgeoms, rst_sample.
- **Coordinate transforms & tiling (~12):** rst_rastertoworldcoord{,x,y}, rst_worldtorastercoord{,x,y}, rst_tilexyz, rst_to_webmercator, rst_xyzpyramid, rst_h3_tessellate, rst_bng_tessellate, rst_quadbin_tessellate.
- **Generators / UDTFs & grid-tessellation (~35) [many UDTF-only]:** rst_maketiles, rst_retile, rst_separatebands, rst_tooverlappingtiles, rst_polygonize, rst_rasterize, the rst_{h3,quadbin,bng}_rastertogrid{avg,count,max,min,median,sum,variance,stddev} families, gbx_h3_cell_bbox.

(A function may reasonably move families; the implementer takes the family's list as authoritative. Self-review verifies every name lands in exactly one family.)

---

### Task 1: Canonical multiband GeoTIFF fixture (early dependency)

**Files:**
- Create: a small committed multiband GeoTIFF under `sample-data/Volumes/main/default/geobrix_samples/geobrix-examples/` (mirror the existing sample tree; exact subpath chosen to sit beside the sentinel2 samples, e.g. `.../multiband/rgb_nir_small.tif`).
- Create: `docs/tests/python/api/_fixtures.py` (or extend an existing shared helper module) with a path constant + a tiny generator/verifier.
- Test: `docs/tests/python/api/test_multiband_fixture.py`

**Interfaces:**
- Produces: a real N-band (recommend 3-band: red, NIR, green — enough for NDVI/EVI/NBR/NDWI/SAVI which need red+NIR, and for `rst_numbands`→3, `rst_bandmetadata`→real per-band tags) GeoTIFF, small (e.g. 8×8), with real per-band metadata tags and a real CRS/transform. Consumed by the band-math batch (Task 6) and by `rst_numbands`/`rst_bandmetadata` in the accessors re-author (Task 3).

- [ ] **Step 1: Write the failing test** asserting the fixture exists and has the expected shape:

```python
# docs/tests/python/api/test_multiband_fixture.py
import rasterio
from pathlib import Path

FIXTURE = Path("sample-data/Volumes/main/default/geobrix_samples/geobrix-examples/multiband/rgb_nir_small.tif")

def test_multiband_fixture_exists_and_has_bands():
    assert FIXTURE.exists(), f"missing {FIXTURE}"
    with rasterio.open(FIXTURE) as ds:
        assert ds.count == 3, f"expected 3 bands, got {ds.count}"
        assert ds.crs is not None
        # real per-band metadata so rst_bandmetadata returns a non-empty map
        assert ds.tags(1), "band 1 must carry metadata tags"
```

- [ ] **Step 2: Run it — fails** (`gbx:test:python-docs --path docs/tests/python/api/test_multiband_fixture.py`): fixture absent.

- [ ] **Step 3: Generate + commit the fixture.** Write a one-off generator (run once in the container) that builds the 3-band GeoTIFF with rasterio — real transform/CRS (EPSG:4326), distinct per-band data (so NDVI etc. produce meaningful values), and per-band tags (e.g. band 1 `{name: red}`, band 2 `{name: nir}`, band 3 `{name: green}`). Write it to the fixture path and `git add` the `.tif`. (Keep the generator code in `_fixtures.py` as documentation of how it was made, but the committed `.tif` is the artifact the examples load.)

- [ ] **Step 4: Run the test — passes.**

- [ ] **Step 5: Commit** (`git add` the `.tif` + `_fixtures.py` + the test; subject `test(docs): add canonical multiband GeoTIFF fixture`).

---

### Task 2: Conventions section + shared setup helpers

**Files:**
- Modify: `docs/docs/api/raster-functions.mdx` (insert the Conventions section after `## Tier availability`, before `## Accessor Functions`)
- Modify: `docs/tests/python/api/_fixtures.py` (shared setup helpers: canonical fixture paths + `tile`-DataFrame builders for light/heavy)
- Modify: `docs/tests/scala/api/ScalaApiExamples.scala` (a shared Scala setup val documenting the same convention, if not already present)

**Interfaces:**
- Produces: (1) the rendered Conventions section every function inherits; (2) shared Python helpers the example doc-tests call so the SHOWN snippet is a bare invocation while the TEST loads the canonical fixture. Consumed by ALL example tasks (3–9).

- [ ] **Step 1: Author the Conventions section** in `raster-functions.mdx` (prose, no code-fence tabs needed — it's the setup contract). It states, RasterX/pyrx-specifically:
  - the canonical sample files + what each demonstrates (single-band `nyc_sentinel2_red.tif` = default; multiband `rgb_nir_small.tif` = band-math/numbands/bandmetadata; DEM `srtm_n40w073.tif` = terrain; NetCDF `nyc_climate.nc` = subdatasets only);
  - the `tile`-column convention: in every example below, `rasters` is a table (SQL) / `df` is a DataFrame (light Python, heavy Python, Scala) whose `tile` column holds the sample loaded via the reader; so `FROM rasters` / `df` = "the sample as tiles";
  - how to read the tabs (SQL default; Python-light; Python-heavy blue; Scala blue — same example each);
  - the output-note convention (natural per-tier form; `...`+"(WKB binary)" for geometry; `SUBDATASET_1_NAME -> ...` for subdataset maps; band-metadata map shorthand — with a note whenever a tier's rendering differs);
  - the per-function override rule (non-default fixture / fuller example → per-function note).
  - No internal vocabulary (QC internals-leak).

- [ ] **Step 2: Author shared setup helpers** in `_fixtures.py`: path constants (`SINGLE_BAND`, `MULTIBAND`, `DEM`, `NETCDF`), and builders `single_band_tile_df(spark)`, `multiband_tile_df(spark)`, `dem_tile_df(spark)`, `netcdf_tile_df(spark)`, plus heavy equivalents, each returning a DataFrame with a `tile` column loaded from the canonical file (via the pyrx/rasterx reader). These are what the example doc-tests call so the shown invocation is bare. (Reuse the existing `_tile_df`/`_make_*` where a synthetic tile is genuinely the right choice, but the DEFAULT shared example uses the canonical FILE fixtures.)

- [ ] **Step 3: Build + verify the Conventions section renders.** `bash scripts/commands/gbx-docs-build.sh --log verify-conventions.log` (Docker; stops 3000 — orchestrator restarts after). Confirm the section renders above the families; `grep -rn -iE "wave [0-9]+" raster-functions.mdx` empty.

- [ ] **Step 4: Commit** (`git add` the mdx + `_fixtures.py` + scala setup; subject `docs(rasterx): add Conventions section + shared example fixtures`).

---

### Task 3: Re-author the Accessors family to the standard (TEMPLATE batch)

**Files:**
- Modify: `docs/tests/python/api/rasterx_accessors_python_light.py`, `rasterx_functions_python_light.py` (the 5 originals), `rasterx_functions.py` (heavy), `rasterx_functions_sql.py`, `docs/tests/scala/api/ScalaApiExamples.scala`, their test files, `docs/docs/api/raster-functions.mdx`, `src/main/resources/com/databricks/labs/gbx/function-info.json`.

**Interfaces:**
- Consumes: the Conventions section + shared setup helpers (Task 2), the multiband fixture (Task 1) for `rst_numbands`/`rst_bandmetadata`. Produces the template all later families copy.

**Family list:** the Accessors set from the assignments table. This RE-AUTHORS the drifted Batch A (commit a758b027) — the old work stays in history, is not reverted.

- [ ] **Step 1: Establish the per-tier invocation template** by doing ONE accessor (`rst_height`) all four tiers first, so the shape is locked before the other 32:
  - light: `def rst_height_python_light_example(spark): df = single_band_tile_df(spark); return df.select(rx.rst_height("tile").alias("height"))` — the SHOWN body is the `.select(...)` invocation; the fixture load is the shared helper call.
  - heavy: same via the rasterx shim.
  - SQL: `SELECT gbx_rst_height(tile) AS height FROM rasters`.
  - scala: `rasters.select(rx.rst_height(col("tile")).alias("height"))`.
  - `_output`: the SAME real value across all four (the canonical single-band raster's real height), `.show()`-table form.
  Confirm all four render identically-shaped in a local build spot-check before scaling.

- [ ] **Step 2: Re-author the remaining accessors** to that template, one shared fixture (single-band default), each a bare invocation. Exceptions get their per-function note + fixture: `rst_numbands` + `rst_bandmetadata` → multiband fixture; `rst_getsubdataset` + `rst_subdatasets` → NetCDF (`nyc_climate.nc`) with note; geometry-returning (`rst_boundingbox`) → `...`+WKB note. Every accessor's four tabs use the SAME example. Salvage real values from the existing drifted examples where correct; unify the fixtures/shown-code.

- [ ] **Step 3: Update test assertions** to the real values on the canonical fixtures (not the old synthetic `4×3` values). Real assertions.

- [ ] **Step 4: Run doc-tests green.** `gbx:test:python-docs --suite api` (light+heavy), `gbx:test:scala-docs`.

- [ ] **Step 5: Regenerate bindings.** `gbx:docs:function-info`; confirm every accessor has a real per-function SQL example (fixes the earlier missing-`*_sql_example` class) and 4 bindings (or 3 for `rst_fromfile` — not an accessor, ignore here).

- [ ] **Step 6: Wire the MDX** — each accessor's block = heading → optional per-fn fixture note → `<FunctionExamples>` (bare invocation tabs). Reuse the shipped prop pattern.

- [ ] **Step 7: Build + code-indicators + same-example visual check.** `gbx:docs:build` (orchestrator restarts 3000 after). Then `gbx:docs:dev --port 3001`: toggle code-indicators ON, confirm a converted accessor shows 4 tabs, green 🔗 on each, and — the key check — all four tabs are visibly the SAME example. Stop 3001.

- [ ] **Step 8: Lint + commit.** `gbx:lint:python --check` (Docker) on changed files. Commit the batch's files + function-info.json; subject `docs(rasterx): re-author accessors to one-example-per-function standard`.

---

### Tasks 4–9: remaining families (deltas of Task 3)

Each follows **Task 3's Steps 1–8**, with the per-family fixture + list below. Per-family light file `docs/tests/python/api/rasterx_<family>_python_light.py`; heavy appends to `rasterx_functions.py`; scala to `ScalaApiExamples.scala`; MDX blocks wired the same way. **Ordering: Task 6 (band-math) depends on Task 1 (multiband fixture) — already satisfied since Task 1 is first.**

- [ ] **Task 4 — Tile ops & constructors.** List = the tile-ops set. Fixture: single-band default. EXCEPTION: constructors (`rst_fromfile`, `rst_fromcontent`, `rst_frombands`) PRODUCE a tile → show the fuller load/build example (per-fn note), consistent across tabs; `rst_fromfile` scala tab = "Not available" note (genuine gap). Commit: `docs(rasterx): tab the tile-ops family (one-example standard)`.

- [ ] **Task 5 — Aggregators.** List = the `*_agg` set. Fixture: MULTI-TILE set (a few `bench-corpus/rows/r*.tif` or several tiles of the canonical GeoTIFF); each example a `GROUP BY` over multiple `tile` rows, per-fn note stating the multi-tile fixture. Commit: `docs(rasterx): tab the aggregators family (one-example standard)`.

- [ ] **Task 6 — Band math & indices [MULTIBAND].** List = the band-math set. Fixture: the multiband GeoTIFF (Task 1), per-fn note. NDVI/EVI/NBR/NDWI/SAVI reference specific bands — state which band indices in the example. Commit: `docs(rasterx): tab the band-math family (one-example standard)`.

- [ ] **Task 7 — Terrain [DEM].** List = the terrain set. Fixture: DEM `srtm_n40w073.tif`, per-fn note. `rst_gridfrompoints`/`rst_dtmfromgeoms` build a surface from points/geoms (fuller example, note). Commit: `docs(rasterx): tab the terrain family (one-example standard)`.

- [ ] **Task 8 — Coordinate transforms & tiling.** List = the transforms set. Fixture: single-band default (needs real georeferencing — the canonical raster has it). Commit: `docs(rasterx): tab the transforms family (one-example standard)`.

- [ ] **Task 9 — Generators / UDTFs & grid-tessellation.** List = the generators set (largest). Per-fn: classify `[UDTF]` vs Column against pyrx/functions.py; light example uses the DataFrame-native UDTF invocation (`spark.sql(...)` fallback only where no native form). Grid families use griddable fixtures. Commit: `docs(rasterx): tab the generators/UDTF family (one-example standard)`.

---

### Task 10: Alphabetize headings within each family section

**Files:** Modify `docs/docs/api/raster-functions.mdx`.

**Interfaces:** Docs-display-only; no example/test/binding change. Run LAST (after all families are tabbed) so blocks aren't reordered mid-authoring.

- [ ] **Step 1: Reorder** the `### rst_X` blocks (each heading + its full body incl `<FunctionExamples>`) to be alphabetical WITHIN each `## <family>` section, across the whole page. NOT one global A–Z (keep family grouping). Combined headings (`rst_scalex / rst_scaley`) stay single blocks sorted by first name.
- [ ] **Step 2: Verify** no block truncated/duplicated (heading count unchanged: 130), build clean, sidebar TOC now alphabetical within each section (spot-check the accessor section: `rst_crs` now before `rst_format`, not after `rst_srid`).
- [ ] **Step 3: Build** (`gbx:docs:build`; orchestrator restarts 3000 after) + confirm clean.
- [ ] **Step 4: Commit** (`git add raster-functions.mdx`; subject `docs(rasterx): alphabetize function headings within each family`).

---

## Self-Review

**1. Spec coverage.** One-example-per-function via bare invocation on `tile` → Global Constraints + every family task Step 1. Conventions section at top → Task 2. Canonical fixtures incl. NEW multiband → Task 1 (early, dependency for Task 6). Tier-natural output + shorthand/notes → Global Constraints + per-fn steps. Doc-tests real+executable, shown=invocation → Global Constraints + Task 2 helpers + Task 3 Step 1. Constructor/NetCDF/multiband/DEM/agg exceptions each noted → family tasks. Code-indicators checkmark → Global Constraints + Task 3 Step 7. Review checks (same-example, real SQL, non-degenerate, right-fn, alphabetical) → Global Constraints + Task 10. Re-author drifted Batch A → Task 3. Alphabetical headings → Task 10. Mechanism unchanged → Global Constraints. No gap.

**2. Placeholder scan.** No TBD/TODO. The multiband fixture is a concrete construct-and-commit task (Task 1), not a placeholder. Family lists reference the concrete assignments table.

**3. Type/name consistency.** Example-symbol naming (`<base>_python_light_example` / `_python_heavy_example` / `_scala_example` / `_sql_example` + `_output`) is identical across all tasks and matches the shipped generator scan. Shared-helper names (`single_band_tile_df`, `multiband_tile_df`, `dem_tile_df`, `netcdf_tile_df` + heavy equivalents) defined in Task 2, consumed by name in Tasks 3–9. Fixture path constants (`SINGLE_BAND`/`MULTIBAND`/`DEM`/`NETCDF`) defined Task 2. `FunctionExamples` props reused unchanged.

**Open items for the implementer (verify before coding):** (a) exact committed multiband fixture subpath + that rasterio can write per-band tags the pyrx/rasterx readers surface for `rst_bandmetadata`; (b) whether the canonical single-band raster is reachable in the container at the referenced `/Volumes` path for the shared helper (else use the repo `sample-data/` mount path); (c) Task 9 per-function `[UDTF]` vs Column split against pyrx/functions.py.
