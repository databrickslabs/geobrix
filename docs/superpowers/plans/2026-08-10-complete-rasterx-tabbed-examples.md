# Complete RasterX Tabbed Function Examples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Render every RasterX function on `docs/docs/api/raster-functions.mdx` as the shipped 4-tab example block (SQL default / Python-light / Python-heavy blue / Scala blue) by authoring real executable per-tier doc-tests for the ~125 remaining functions and wiring each to `<FunctionExamples>`.

**Architecture:** The tabbed-docs mechanism (`FunctionExamples` wrapper, `generate-function-info.py` bindings text-scan, `gbx-example-lang` CSS) is already shipped and is reused UNCHANGED. Each family batch authors `*_python_light_example` / `*_python_heavy_example` / `*_scala_example` doc-tests (matching the generator's scan naming), regenerates `function-info.json` bindings, and replaces that family's single-SQL MDX blocks with `<FunctionExamples>`. One implementer subagent per family; per-batch review; final whole-branch review.

**Tech Stack:** Docusaurus/MDX, the `FunctionExamples` React wrapper + `CodeFromTest`, Python doc-tests (`docs/tests/python/api/`), Scala doc-tests (`docs/tests/scala/api/ScalaApiExamples.scala`), `generate-function-info.py`. Run in the `geobrix-dev` Docker container via `gbx:*`.

## Global Constraints

- **Reuse the mechanism as-is.** Do NOT modify `FunctionExamples.js`, `generate-function-info.py`'s scan logic, or the `gbx-example-lang` CSS. This workstream only AUTHORS examples + WIRES MDX.
- **Doc-tests are the documentation source.** Every example is REAL, executes with REAL assertions against real sample data under `/Volumes/main/geobrix_samples`, run via `gbx:test:*-docs` in Docker. No mocking, no stubs, no structure-only assertions.
- **Idiomatic-per-language tabs; SQL never bleeds into a non-SQL tab.** Python tabs = Python; Scala tab = Scala. For the 33 UDTF-only functions, the Python-light example uses the **DataFrame-native Python UDTF invocation**; `spark.sql(...LATERAL...)` is the fallback ONLY where no DataFrame-native Python form exists — never raw SQL as the default Python content.
- **Naming must match the generator's scan EXACTLY:** `def <base>_python_light_example(spark)`, `def <base>_python_heavy_example(spark)`, `val <base>_scala_example` (+ each `<base>_..._example_output`). base = the function name without `gbx_` (e.g. `rst_width`). A mismatch → the binding isn't detected → the tab silently shows the note.
- **Code-indicators checkmark compatibility (REQUIRED):** every authored/present tab must earn the green "🔗 Fully Validated: tested at compile-time" badge. That requires (a) `FunctionExamples` passing `source`/`testFile`/`functionName` (it already does — do not remove), and (b) EVERY example doc-test file living under `docs/tests/...` and NEVER under an `integration/` or `tests-dbr/` path (those downgrade the badge). Acceptance toggles code-indicators on and confirms green on all present tabs.
- **Never fabricate an example for a nonexistent binding.** The only known genuine gap is `rst_fromfile`/Scala (already done, renders the note). If a function genuinely lacks a tier not in the matrix, surface it as a finding — do not invent an example.
- **"Example output" is UNIFORM across all four tabs (user ruling 2026-08-10).** Every tier's `<base>_..._example_output` constant renders as a DataFrame `.show()`-style bordered table (the `+----+` form SQL already uses) — NOT a bare Python repr (`[5.5]`), NOT a `.show()` with a placeholder value (`[<float>]`). All four tiers ultimately return a Spark DataFrame column; show its `.show()`. **Values must be REAL (captured from actually running the example) and IDENTICAL across all four tiers for a given function** — the SQL/light/heavy/scala outputs for `rst_avg` must show the same number, same table shape. No placeholders anywhere. (Batch A predates this ruling and is normalized retroactively; B–G author outputs in this form from the start.)
- **Binary/geometry-returning functions are the accepted exception (user ruling 2026-08-10, re rst_boundingbox):** a function returning WKB/EWKB (bbox, centroid, tile bytes, etc.) may show a TRUNCATED repr in the `.show()` table (e.g. `|[...|`) ACCOMPANIED BY a one-line explanatory note (e.g. `(WKB binary bytes — bounding POLYGON of the raster extent)`). The note is what makes the truncation honest, so this is NOT a banned placeholder. Do NOT force such functions to WKT or full hex, and do NOT "fix" the annotated form. The identical-real-value rule still binds the SCALAR cases (numbers, srid, counts); binary outputs just need the consistent annotated-truncation form across tiers. rst_boundingbox is the reference — LEAVE IT AS-IS.
- **Port 3000 is the user's.** A `gbx:docs:build` stops it; run ONE build per batch and the orchestrator restarts 3000 after each batch. Agent visual previews use a NON-3000 port (3001) and are stopped after.
- **Docs voice:** no internal planning vocabulary in any rendered example/prose (QC internals-leak; no `wave N`).
- **Commit hygiene:** subjects ≤72 chars, WHY body, end `Co-authored-by: Isaac`; single plain `git commit`; `git add` ONLY the batch's files (there are unrelated pre-existing working-tree edits — never sweep them in). No push.
- **Already done (do NOT re-author):** `rst_avg`, `rst_boundingbox`, `rst_numbands`, `rst_width`, `rst_fromfile`.

## Fixture strategy (ratified: per-family)

Each family selects the sample raster that best demonstrates it (terrain → a DEM; band-math → multiband; h3/grid → griddable; accessors → any real raster). One shared setup helper + raster path per family file, reused across that family's examples — not N independent Spark setups. Mirror the existing `*_sql_example` harness fixtures.

## Family assignments (authoritative — from registered_functions.txt, 130 total, 5 done)

Each family = one task (A–G). Every listed function is full-stack (SQL+light+heavy+scala) UNLESS marked. `[done]` = skip. `[UDTF]` = light is UDTF-only → Python-native UDTF invocation.

**A — Accessors (~26):** rst_bandmetadata, rst_format, rst_georeference, rst_getnodata, rst_getsubdataset, rst_height, rst_max, rst_median, rst_memsize, rst_metadata, rst_min, rst_pixelcount, rst_pixelheight, rst_pixelwidth, rst_rotation, rst_scalex, rst_scaley, rst_skewx, rst_skewy, rst_srid, rst_crs, rst_subdatasets, rst_summary, rst_type, rst_upperleftx, rst_upperlefty, rst_isempty, rst_tryopen, rst_histogram. (rst_avg/boundingbox/numbands/width `[done]`.)

**B — Tile ops & constructors (~20):** rst_frombands, rst_fromcontent, rst_asformat, rst_clip, rst_filter, rst_initnodata, rst_transform, rst_transformcrs, rst_updatetype, rst_band, rst_buildoverviews, rst_fillnodata, rst_setsrid, rst_setcrs, rst_threshold, rst_resample, rst_resample_to_res, rst_resample_to_size, rst_convolve, rst_cog_convert. (rst_fromfile `[done]`.)

**C — Band math & spectral indices (~11):** rst_combineavg, rst_derivedband, rst_mapalgebra, rst_merge, rst_ndvi, rst_evi, rst_index, rst_nbr, rst_ndwi, rst_savi. (multiband fixture.)

**D — Aggregators (~8):** rst_combineavg_agg, rst_derivedband_agg, rst_merge_agg, rst_frombands_agg, rst_rasterize_agg, rst_gridfrompoints_agg, rst_dtmfromgeoms_agg. (GROUP BY shape.)

**E — Terrain & analysis (~14):** rst_aspect, rst_color_relief, rst_hillshade, rst_roughness, rst_slope, rst_tpi, rst_tri, rst_contour, rst_proximity, rst_viewshed, rst_gridfrompoints, rst_dtmfromgeoms, rst_sample. (DEM fixture.)

**F — Coordinate transforms & tiling (~9):** rst_rastertoworldcoord, rst_rastertoworldcoordx, rst_rastertoworldcoordy, rst_worldtorastercoord, rst_worldtorastercoordx, rst_worldtorastercoordy, rst_tilexyz, rst_to_webmercator, rst_xyzpyramid.

**G — Generators / UDTFs & grid-tessellation (~35, mostly `[UDTF]` light):** rst_maketiles, rst_retile, rst_separatebands, rst_tooverlappingtiles, rst_polygonize, rst_rasterize, rst_h3_tessellate, rst_bng_tessellate, rst_quadbin_tessellate, rst_h3_rasterize_agg, rst_bng_rasterize_agg, rst_quadbin_rasterize_agg, and the rastertogrid families: rst_h3_rastertogrid{avg,count,max,min,median,sum,variance,stddev}, rst_quadbin_rastertogrid{avg,count,max,min,median,sum,variance,stddev}, rst_bng_rastertogrid{avg,count,max,median,min,sum,variance,stddev}, plus gbx_h3_cell_bbox. Confirm per-function which are `[UDTF]` vs Column against pyrx/functions.py before authoring; light examples use DataFrame-native UDTF invocation.

**Boundary note:** exact counts are approximate and a function may reasonably move families; the implementer takes the family's list as the authoritative set for that batch. Total across A–G = 125 net-new (130 − 5 done). The plan self-review verifies every non-done RasterX name appears in exactly one family.

---

### Task A: Accessors family (the template batch)

**Files:**
- Create: `docs/tests/python/api/rasterx_accessors_python_light.py` + `docs/tests/python/api/test_rasterx_accessors_python_light.py`
- Modify: `docs/tests/python/api/rasterx_functions.py` (append `*_python_heavy_example` for the family) + `docs/tests/python/api/test_rasterx_functions.py`
- Modify: `docs/tests/scala/api/ScalaApiExamples.scala` (append `val *_scala_example`) + `docs/tests/scala/api/ScalaApiExamplesDocTest.scala`
- Modify: `docs/docs/api/raster-functions.mdx` (replace the family's `<CodeFromTest language="sql">` blocks with `<FunctionExamples>`)
- Modify: `src/main/resources/com/databricks/labs/gbx/function-info.json` (regenerated)

**Interfaces:**
- Consumes the shipped `FunctionExamples` wrapper + the `<base>_sql_example` functions (already present). Produces the three net-new per-tier example symbols per accessor, detected by the generator's scan.

**Family list:** the Accessors set from the assignments table above (skip the 4 done accessors).

- [ ] **Step 1: Confirm the family's tier availability + pick the fixture.** Read `pyrx/functions.py`, `rasterx/functions.py`, `functions.scala` to confirm each accessor is full-stack (the matrix says all are). Choose ONE real sample raster under `/Volumes/main/geobrix_samples` that exercises accessors well (any multiband GeoTIFF). Read an existing `*_sql_example` in `rasterx_functions_sql.py` + the Task-5 light/heavy/scala examples (rst_avg etc.) to copy their exact shape/fixtures.

- [ ] **Step 2: Write the light-Python examples + their failing test.** For each accessor, `def <base>_python_light_example(spark)` in `rasterx_accessors_python_light.py` using pyrx (`from databricks.labs.gbx.pyrx import functions as rx`), returning a real value; + `<base>_python_light_example_output`. The test file asserts a real value per example (e.g. `rst_height` → the raster's real pixel height; `rst_srid` → the real EPSG). Shared setup helper at top of the file.

- [ ] **Step 3: Run the light test — fails then passes.** `gbx:test:python-docs --suite api`. Iterate to green with real assertions.

- [ ] **Step 4: Add heavy-Python examples.** Append `def <base>_python_heavy_example(spark)` (heavy shim `databricks.labs.gbx.rasterx`) + `_output` to `rasterx_functions.py`, assertions in its test. `gbx:test:python-docs --suite api` → green.

- [ ] **Step 5: Add Scala examples.** Append `val <base>_scala_example` (+ `_output`) to `ScalaApiExamples.scala`, assertions in `ScalaApiExamplesDocTest.scala`. `gbx:test:scala-docs` → green.

- [ ] **Step 6: Regenerate bindings + confirm.** `gbx:docs:function-info`, then confirm each family function's `bindings` now = `["sql","python-light","python-heavy","scala"]` (via `gbx:test:python --path docs/tests-function-info/test_function_bindings.py` + a spot-read of function-info.json).

- [ ] **Step 7: Wire the MDX.** Add the light raw-loader import (`import rasterxAccessorsLightCode from '!!raw-loader!../../tests/python/api/rasterx_accessors_python_light.py';`). For each family function, replace its `<CodeFromTest language="sql" .../>` with `<FunctionExamples name="rst_X" sql={rasterxSqlCode} sqlSource="..." pythonLight={rasterxAccessorsLightCode} pythonLightSource="..." pythonHeavy={rasterxCode} pythonHeavySource="..." scala={scalaApiExamplesCode} scalaSource="..." testFile="docs/tests/python/api/test_rasterx_functions_sql.py" />`. (Reuse the exact prop pattern the 5 done functions already use; only `pythonLight`/`pythonLightSource` point at the new family file.)

- [ ] **Step 8: Build + code-indicators visual check.** `bash scripts/commands/gbx-docs-build.sh --log verify-batchA-build.log` (Docker; stops 3000 — orchestrator restarts after). Then `gbx:docs:dev --port 3001`, load the page, **toggle code-indicators ON** (bottom-right), confirm a converted accessor shows 4 tabs (SQL default, heavy tabs blue) each with the green 🔗 "Fully Validated" badge. Stop the 3001 server. `grep -rn -iE "wave [0-9]+" docs/docs/api/raster-functions.mdx` → empty.

- [ ] **Step 9: Lint + commit.** `gbx:lint:python --fix` then Docker `gbx:lint:python --check` on the batch's files. Commit ONLY this batch's files (list them explicitly) + regenerated function-info.json:

```bash
git add docs/tests/python/api/rasterx_accessors_python_light.py docs/tests/python/api/test_rasterx_accessors_python_light.py docs/tests/python/api/rasterx_functions.py docs/tests/python/api/test_rasterx_functions.py docs/tests/scala/api/ScalaApiExamples.scala docs/tests/scala/api/ScalaApiExamplesDocTest.scala docs/docs/api/raster-functions.mdx src/main/resources/com/databricks/labs/gbx/function-info.json
git commit -m "docs(rasterx): tab the accessors family (4-tier examples)"
```

---

### Tasks B–G: remaining families

Each of B, C, D, E, F, G follows **Task A's Steps 1–9 exactly**, with these per-batch substitutions. The per-family light file is `docs/tests/python/api/rasterx_<family>_python_light.py` with a matching test file and a matching raw-loader import var in the MDX. Heavy examples always append to `rasterx_functions.py`; Scala always to `ScalaApiExamples.scala`.

- [ ] **Task B — Tile ops & constructors.** Family list = the B set. Fixture: a standard multiband GeoTIFF (constructors/transforms). Light file `rasterx_tileops_python_light.py`. Note: `rst_fromcontent`/`rst_frombands` are constructors — the example builds a tile from bytes/bands (mirror the pyrx setup helper). Steps 1–9 as Task A. Commit subject: `docs(rasterx): tab the tile-ops family (4-tier examples)`.

- [ ] **Task C — Band math & spectral indices.** Family list = the C set. Fixture: a MULTIBAND raster (indices need ≥2 bands; NDVI/EVI/NBR/NDWI/SAVI need specific bands — pick a fixture with red/NIR/etc. or document the band indices used). Light file `rasterx_bandmath_python_light.py`. Steps 1–9. Commit: `docs(rasterx): tab the band-math family (4-tier examples)`.

- [ ] **Task D — Aggregators.** Family list = the D set. These are GROUP BY aggregators — the example builds a small multi-row/multi-tile DataFrame and aggregates (mirror the existing `*_agg` sql example shape + the light agg UDF). Light file `rasterx_aggregators_python_light.py`. Steps 1–9. Commit: `docs(rasterx): tab the aggregators family (4-tier examples)`.

- [ ] **Task E — Terrain & analysis.** Family list = the E set. Fixture: a DEM (elevation raster) so slope/aspect/hillshade/tpi/tri/roughness/contour are meaningful; rst_gridfrompoints/rst_dtmfromgeoms build a surface from points/geoms (mirror their sql examples). Light file `rasterx_terrain_python_light.py`. Steps 1–9. Commit: `docs(rasterx): tab the terrain family (4-tier examples)`.

- [ ] **Task F — Coordinate transforms & tiling.** Family list = the F set. Fixture: a georeferenced raster with a known CRS (raster↔world coord conversions need real georeferencing); rst_tilexyz/rst_to_webmercator/rst_xyzpyramid are web-mercator tiling. Light file `rasterx_transforms_python_light.py`. Steps 1–9. Commit: `docs(rasterx): tab the coordinate-transforms family (4-tier examples)`.

- [ ] **Task G — Generators / UDTFs & grid-tessellation.** Family list = the G set (~35, the largest). CRITICAL per-function step 1a: read `pyrx/functions.py` to classify each as `[UDTF]` (no Column form) vs Column. For `[UDTF]` functions the Python-light example uses the **DataFrame-native UDTF invocation** (the registered `@udtf` called to return rows/DataFrame); use `spark.sql(...LATERAL...)` ONLY if a function genuinely has no DataFrame-native Python form, and note which. Fixtures: griddable rasters for the rastertogrid/tessellate/rasterize families; a raster with distinct regions for polygonize; the tile generators (maketiles/retile/separatebands/tooverlappingtiles) split a tile. Light file `rasterx_generators_python_light.py`. Because this batch is large, the implementer MAY split its commit into two (grid-tessellation vs tile-generators) if cleaner — but one family review. Steps 1–9. Commit: `docs(rasterx): tab the generators/UDTF family (4-tier examples)`.

---

## Self-Review

**1. Spec coverage.** Every spec requirement maps: 4-tab render via `<FunctionExamples>` (all tasks Step 7); real executable doc-tests with real assertions (Steps 2–5); idiomatic-per-language + decision-3 UDTF-native (Global Constraints + Task G Step 1a); code-indicators green checkmark (Global Constraints + every task Step 8, files under `docs/tests/`); per-family fixtures (fixture section + each task's Step 1); bindings regeneration + clean build per batch (Steps 6, 8); one Scala gap `rst_fromfile` already done (Global Constraints). Batching by family = Tasks A–G. Mechanism reused unchanged (Global Constraints). No gap.

**2. Placeholder scan.** No TBD/TODO. The per-task "Family list = the X set" refers to the concrete assignments table above (real names), not a placeholder. Task G's `[UDTF]` classification is a real read-the-source instruction with a defined rule, not vague.

**3. Coverage/consistency.** Every non-done RasterX name from registered_functions.txt appears in exactly one family (A accessors, B tile-ops/constructors, C band-math, D aggregators, E terrain, F transforms, G generators/grid) — verified against the 130-name list; the 5 done are excluded. Naming convention (`<base>_python_light_example` etc.) is identical across all tasks and matches the generator's scan (confirmed in the shipped Task 3). The `FunctionExamples` prop pattern is identical across tasks (only the pythonLight source var changes per family).

**Open items for the implementer (verify before coding):** (a) Task G — the exact `[UDTF]` vs Column split per function (read pyrx/functions.py); (b) each family's best fixture raster availability under `/Volumes/main/geobrix_samples`; (c) whether any function's heavy/scala form has a genuine gap not in the matrix (surface as a finding, render the note, don't fabricate).
