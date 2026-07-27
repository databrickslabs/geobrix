# Raster BNG + Quadbin (H3 parity) — Phase 2 (Light/pyrx) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give all 9 heavy-tier raster BNG/quadbin functions a lightweight (`pyrx`) implementation so every one flips `<Tier heavy/>` → `<Tier both/>`, completing both-tier parity for the feature.

**Architecture:** Add grid-generic branches to the three light engine modules (`gridagg.py`, `tessellate.py`, `cellraster.py`), each keeping its existing H3 path unchanged and delegating new grids' cell math to `pygx` (`_bng`, `_quadbin`) — single source of truth, no cell-math duplication. Register 9 light UDTFs/UDFs. Quadbin first (engine already 4326-native), then BNG (adds EPSG:27700 warp + String cell ids), then docs/diagram/badges.

**Tech Stack:** Python 3.12, `pyrx` (rasterio + shapely + pyproj + numpy), `pygx` (BNG/quadbin cell math). Cross-tier parity tests run in Docker (heavy needs the JAR). No new dependencies.

**Spec:** `docs/superpowers/specs/2026-07-24-raster-bng-quadbin-h3-parity-design.md` §4.1
**Branch:** `issues/49` (off `beta/0.4.0`). Builds toward **v0.4.3**.

## Global Constraints

- **Single source of truth for cell math:** BNG cell math comes ONLY from `pygx._bng` (`point_to_cell_id`, `format`, `parse`, `cell_id_to_geometry`, `is_valid`, `get_edge_size`, `get_resolution_from_digits`, `CRS_ID=27700`); quadbin from `pygx._quadbin`. NEVER reimplement/duplicate a grid's cell math in `pyrx/core/*` (the one existing exception, `gridagg._quadbin_cells`, is a pre-existing bit-exact numpy encoder — do NOT add a second BNG copy; BNG uses a scalar `pygx._bng` loop).
- **Each engine keeps its H3 path unchanged.** New grids are sibling branches/adapters. The `cellraster.py` grid-adapter refactor is refactor-only for H3 (no behavior change); existing H3 light tests are the regression gate.
- **BNG CRS:** warp input raster to EPSG:27700 (`rasterio.warp`, **nearest**) before cell math; read eastings/northings from the warped geotransform (0.5-px centroid); drop out-of-GB pixels/cells via `pygx._bng.is_valid`. Quadbin/H3 stay 4326 (no warp).
- **Cell id types:** BNG cell ids are **String** at the output boundary (`pygx._bng.format`), Long internally. Quadbin/H3 are Long. New `_GRID_FLAT_STRING_SCHEMA` (`cellID StringType`) for BNG reducers.
- **Parity bar (def-of-done):** light output == heavy output — **exact cell-set** (BNG String / quadbin Long ids) + measures within tolerance (1e-9 numeric, 1e-6 geometry). This is per the pygx exact-parity standard.
- **Empty-cell / NoData (spec §2.6):** rastertogrid emits a cell only for ≥1 valid pixel (never a zero-valid-pixel cell). rasterize_agg uses `_NODATA = -9999.0` written into a raster whose nodata is set to -9999 (masked on read-back) — mirror the existing `cellraster` H3 handling exactly.
- **Python wrappers:** `rst_*` reducer/tessellate wrappers stay SQL-LATERAL-invocation pointers raising `NotImplementedError` with the LATERAL example (matching existing H3/quadbin). `rasterize_agg` gets a callable wrapper mirroring `rst_h3_rasterize_agg`.
- **Light-CI checklist:** no new deps (pygx/rasterio/shapely/pyproj/numpy already present); Phase-2 tests land under `python/geobrix/test/pyrx/`, which is already in all three light-CI sync points (`_LIGHT_TEST_DIRS` in conftest, the light-run list in `pyrx_build/action.yml`, and the heavy `LIGHT_IGNORES` in `python_build/action.yml` — synced 2026-07-25 so light tests only run in the light phase). No new test dir is introduced, so no CI sync change is needed for this plan; if a future task adds a NEW light dir, update all three. If any dep is added, pin across all 3 envs + recompile hashed txt.
- **Commands:** light Python tests `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/`; cross-tier/doc tests in Docker via `gbx:test:*-docs`; lint `bash scripts/commands/gbx-lint-python.sh --check` (host black may differ from Docker — verify with Docker `--check` before push). Do NOT push (batch-push; user-gated).

## File Structure

**Modify (engines):**
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/gridagg.py` — add `"bng"` branch: `_bng_cells` scalar encoder (calls `pygx._bng.point_to_cell_id`), 27700 warp for bng, `is_valid` drop, String-id yield.
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/tessellate.py` — add `iter_tessellate_quadbin`, `iter_tessellate_bng` (mirror `iter_tessellate_h3`; reuse shared clip/emit; BNG warp+is_valid).
- `python/geobrix/src/databricks/labs/gbx/pyrx/core/cellraster.py` — refactor `_h3_str`/`_resolution`/`compute_gridspec`/`cell_bbox`/`cells_to_raster` to dispatch on a `grid` param via per-grid adapters backed by `pygx`; H3 path preserved.

**Modify (registration/bindings):**
- `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` — `_GRID_FLAT_STRING_SCHEMA`; 5 `_RstBngRasterToGrid*UDTF` via factory; `_RstQuadbinTessellateUDTF` + `_RstBngTessellateUDTF`; quadbin/BNG `rasterize_agg` UDFs; register all 9; `rasterize_agg` callable wrappers.

**Create (tests):** `python/geobrix/test/pyrx/test_gridagg_bng.py`, `test_tessellate_quadbin.py`, `test_tessellate_bng.py`, `test_cellraster_grids.py`; cross-tier parity in `python/geobrix/test/` doc/integration area (Docker).

**Modify (docs, final task):** `docs/docs/api/raster-functions.mdx` (9 badges heavy→both; fix page-level "every function both-tier" claim + BNG-Grid reason), `docs/docs/api/execution-tiers.mdx`, `docs/docs/api/performance.mdx`, `resources/images/generators/rasterx-function-categories.py` (+ regenerate PNG/SVG), `docs/docs/api/beta-release-notes.mdx`.

---

## Task 1: Light quadbin tessellate

**Files:**
- Modify: `pyrx/core/tessellate.py` (add `iter_tessellate_quadbin`)
- Modify: `pyrx/functions.py` (`_RstQuadbinTessellateUDTF` + register + wrapper)
- Test: `python/geobrix/test/pyrx/test_tessellate_quadbin.py`
- Reference: `iter_tessellate_h3` / `_centroid_chips` in tessellate.py; `_RstH3TessellateUDTF` in functions.py; `pygx._quadbin` (`point_as_cell`, `as_wkb`, `resolution`).

**Interfaces:**
- Consumes: `pygx._quadbin.point_as_cell(lon,lat,res)`, quadbin cell→polygon (via `_quadbin.as_wkb(cell)` → shapely, or the cell bbox); shared clip/emit helpers in tessellate.py.
- Produces: `iter_tessellate_quadbin(ds, resolution, mode="covering") -> Iterator[(int cellid, bytes raster)]`; `_RstQuadbinTessellateUDTF` registered as `gbx_rst_quadbin_tessellate`.

- [ ] **Step 1: Write the failing test** — `test_tessellate_quadbin.py`: covering over a small EPSG:4326 raster yields ≥1 chip each tagged with a Long quadbin cell id; centroid mode single-assigns; unknown mode raises. Use the real sample-raster fixture pattern from the existing H3 tessellate test (read `test/pyrx/` for it).

```python
def test_iter_tessellate_quadbin_covering_yields_tagged_chips():
    from databricks.labs.gbx.pyrx.core import tessellate as t
    import rasterio
    with rasterio.open(SMALL_4326_TIF) as ds:
        chips = list(t.iter_tessellate_quadbin(ds, resolution=12, mode="covering"))
    assert chips, "covering must yield >=1 chip"
    for cellid, raster in chips:
        assert isinstance(cellid, int) and cellid != 0
        assert raster  # non-empty GTiff bytes
```

- [ ] **Step 2: Run to verify it fails** — `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_tessellate_quadbin.py`. Expected: FAIL (`iter_tessellate_quadbin` undefined).

- [ ] **Step 3: Implement** — Clone `iter_tessellate_h3`'s structure into `iter_tessellate_quadbin`. Substitute only: cell enumeration (`pygx._quadbin.polyfill` on the raster bbox for covering; per-pixel `pygx._quadbin.point_as_cell` for centroid), cell geometry (shapely from `pygx._quadbin.as_wkb(cell)`), Long id tag. 4326-native (no warp). Reuse the module's shared clip/emit machinery. Add `_RstQuadbinTessellateUDTF` (mirror `_RstH3TessellateUDTF`), register `gbx_rst_quadbin_tessellate` in the `udtfs` list, add the `rst_quadbin_tessellate` NotImplementedError-pointer wrapper.

- [ ] **Step 4: Run to verify it passes** — same command. Expected: PASS.

- [ ] **Step 5: Commit** — `git add pyrx/core/tessellate.py pyrx/functions.py test/pyrx/test_tessellate_quadbin.py && git commit -m "feat(pyrx): light quadbin tessellate"`

---

## Task 2: Light quadbin rasterize_agg + cellraster grid-adapter refactor

**Files:**
- Modify: `pyrx/core/cellraster.py` (grid-adapter refactor + quadbin binding)
- Modify: `pyrx/functions.py` (`_rst_quadbin_rasterize_agg_udf` + register + wrapper)
- Test: `python/geobrix/test/pyrx/test_cellraster_grids.py`
- Reference: `cellraster.py` (`_h3_str`, `_resolution`, `compute_gridspec`, `cell_bbox`, `cells_to_raster`); `_rst_h3_rasterize_agg_udf` in functions.py; `pygx._quadbin` (`resolution`, `centroid`, `k_ring`).

**Interfaces:**
- Consumes: a new per-grid adapter in cellraster (`grid` param → `to_str`/`resolution`/`cell_center`/`cell_boundary`/`edge_size`); `pygx._quadbin`.
- Produces: `cellraster` functions accept `grid="h3"|"quadbin"|"bng"`; `_rst_quadbin_rasterize_agg_udf`; `gbx_rst_quadbin_rasterize_agg` registered.

- [ ] **Step 1: Write the failing test** — `test_cellraster_grids.py`: (a) H3 rasterize unchanged (regression: same output as before the refactor for a fixed cell set); (b) quadbin rasterize_agg burns a small quadbin cell set to a raster whose band nodata == -9999 and recovers the per-cell values via a round-trip through `gridagg.raster_to_grid(..., "quadbin", "avg")`.

```python
def test_quadbin_rasterize_roundtrip_and_nodata():
    from databricks.labs.gbx.pyrx.core import cellraster as cr, gridagg
    # fixed quadbin cell set + values -> raster -> read back -> exact recover; nodata==-9999
    ...
```

- [ ] **Step 2: Run to verify it fails** — `gbx-test-python.sh --path .../test_cellraster_grids.py`. Expected: FAIL.

- [ ] **Step 3: Implement** — Refactor cellraster's H3-hardcoded helpers to dispatch on `grid` via a small adapter object/dict: `to_str(id)`, `resolution(ids)`, `cell_center(id)->(lon,lat or e,n)`, `cell_boundary(id)`, `edge_size(res)`, and the sample-point CRS (`4326` for h3/quadbin). H3 adapter binds the current `h3.*` calls (behavior identical). Quadbin adapter binds `pygx._quadbin`. `compute_gridspec`/`cell_bbox`/`cells_to_raster` take `grid` and use the adapter. Add `_rst_quadbin_rasterize_agg_udf` (mirror H3's, `grid="quadbin"`), register `gbx_rst_quadbin_rasterize_agg`, add the callable wrapper.

- [ ] **Step 4: Run to verify it passes** — also run the existing H3 cellraster/rasterize tests to confirm the refactor didn't regress H3: `gbx-test-python.sh --path python/geobrix/test/pyrx/`. Expected: PASS (new + all existing H3).

- [ ] **Step 5: Commit** — `feat(pyrx): light quadbin rasterize_agg + grid-adapter cellraster refactor`

---

## Task 3: Light BNG rastertogrid reducers (×5)

**Files:**
- Modify: `pyrx/core/gridagg.py` (`_bng_cells` + `"bng"` branch + 27700 warp + String yield)
- Modify: `pyrx/functions.py` (`_GRID_FLAT_STRING_SCHEMA`; 5 `_RstBngRasterToGrid*UDTF`; register; wrappers)
- Test: `python/geobrix/test/pyrx/test_gridagg_bng.py`
- Reference: `raster_to_grid` / `_h3_cells` / `_grouped_measures` in gridagg.py; `_make_rastertogrid_udtf` + `_GRID_FLAT_*` schemas in functions.py; `pygx._bng` (`point_to_cell_id`, `format`, `is_valid`).

**Interfaces:**
- Consumes: `pygx._bng.point_to_cell_id(e,n,res)`, `pygx._bng.format(id)`, `pygx._bng.is_valid(id)`; `rasterio.warp` to 27700.
- Produces: `gridagg.raster_to_grid(ds, res, "bng", agg)` returns `[{"cellID": str, "measure": ...}]`; 5 `gbx_rst_bng_rastertogrid{avg,count,max,min,median}` registered UDTFs; `_GRID_FLAT_STRING_SCHEMA`.

- [ ] **Step 1: Write the failing test** — `test_gridagg_bng.py`: (a) a small EPSG:27700 London raster (all valid) → `raster_to_grid(ds, 3, "bng", "avg")` yields String cell ids matching `^[A-Z]{2}\d*$` with correct mean; (b) all-nodata band yields `[]` (no zero-valid-pixel cell, §2.6); (c) a 4326 input raster is auto-warped to 27700 and yields valid BNG cells; (d) out-of-GB pixels dropped (`is_valid`).

- [ ] **Step 2: Run to verify it fails** — `gbx-test-python.sh --path .../test_gridagg_bng.py`. Expected: FAIL (`"bng"` grid unknown).

- [ ] **Step 3: Implement** — In gridagg: add `_bng_cells(e, n, res)` scalar loop over valid pixels calling `pygx._bng.point_to_cell_id` (like `_h3_cells`); in `raster_to_grid`, for `grid=="bng"` warp `ds` to EPSG:27700 (`rasterio.warp`, nearest) first, compute e/n from the warped geotransform, drop cells failing `pygx._bng.is_valid`, and render `pygx._bng.format(id)` as the String `cellID` at yield (keep grouping Long-keyed). Extend `_validate_resolution` for bng (±1..±6 / `pygx._bng.get_resolution`). In functions.py: `_GRID_FLAT_STRING_SCHEMA` (cellID StringType; count = String cellID + Integer measure), 5 `_RstBngRasterToGrid*UDTF` via `_make_rastertogrid_udtf("bng", agg, schema)` (the UDTF yield must emit String cellID — adjust the factory to not `int()` the cellID for bng, or add a String-cellID factory variant), register the 5, add NotImplementedError-pointer wrappers.

- [ ] **Step 4: Run to verify it passes** — same, plus the existing H3/quadbin gridagg tests (confirm no regression). Expected: PASS.

- [ ] **Step 5: Commit** — `feat(pyrx): light BNG rastertogrid reducers (avg/count/max/min/median)`

---

## Task 4: Light BNG tessellate

**Files:**
- Modify: `pyrx/core/tessellate.py` (`iter_tessellate_bng`)
- Modify: `pyrx/functions.py` (`_RstBngTessellateUDTF` + register + wrapper)
- Test: `python/geobrix/test/pyrx/test_tessellate_bng.py`
- Reference: `iter_tessellate_quadbin` (Task 1); `pygx._bng` (`polyfill`, `cell_id_to_geometry`, `format`, `is_valid`, `point_to_cell_id`).

**Interfaces:**
- Produces: `iter_tessellate_bng(ds, resolution, mode="covering") -> Iterator[(str cellid, bytes raster)]`; `gbx_rst_bng_tessellate` registered.

- [ ] **Step 1: Write the failing test** — `test_tessellate_bng.py`: covering over a GB raster (27700, or 4326 auto-warped) yields ≥1 chip each tagged with a BNG String id `^[A-Z]{2}\d*$`; only areal chips; centroid single-assigns; unknown mode raises; a 4326 input triggers the warp.

- [ ] **Step 2: Run to verify it fails** — Expected: FAIL.

- [ ] **Step 3: Implement** — Clone `iter_tessellate_quadbin` into `iter_tessellate_bng`; warp raster to 27700 first; enumerate via `pygx._bng.polyfill(bbox_poly, res)` (buffer the bbox before polyfill — mirror the heavy-tier fix so boundary cells aren't dropped; use `pygx._bng` buffer-radius helper if present, else a resolution-derived buffer), cell geometry from `pygx._bng.cell_id_to_geometry`, drop out-of-GB via `is_valid`, String id tag via `format`. covering = geometric-overlap keep-test; centroid = per-pixel `point_to_cell_id` on 27700 coords. Add `_RstBngTessellateUDTF`, register `gbx_rst_bng_tessellate`, wrapper.

- [ ] **Step 4: Run to verify it passes** — Expected: PASS.

- [ ] **Step 5: Commit** — `feat(pyrx): light BNG tessellate (27700 warp, boundary-complete covering)`

---

## Task 5: Light BNG rasterize_agg

**Files:**
- Modify: `pyrx/core/cellraster.py` (BNG adapter binding — 27700-native)
- Modify: `pyrx/functions.py` (`_rst_bng_rasterize_agg_udf` + register + wrapper)
- Test: `python/geobrix/test/pyrx/test_cellraster_grids.py` (extend)
- Reference: Task 2 grid-adapter; `pygx._bng` (`parse`, `format`, `cell_id_to_geometry`, `get_edge_size`, `get_resolution_from_digits`).

**Interfaces:**
- Produces: cellraster BNG adapter (27700, no WGS84 hop); `_rst_bng_rasterize_agg_udf` (cellid input STRING, parsed via `pygx._bng.parse`); `gbx_rst_bng_rasterize_agg` registered.

- [ ] **Step 1: Write the failing test** — extend `test_cellraster_grids.py`: BNG rasterize_agg burns a small BNG cell set (String ids) to a 27700 raster (band nodata == -9999); round-trip through `raster_to_grid(..., "bng", "avg")` recovers the per-cell values. Assert the srid used is 27700.

- [ ] **Step 2: Run to verify it fails** — Expected: FAIL.

- [ ] **Step 3: Implement** — Bind the BNG adapter in cellraster: `to_str` = `pygx._bng.format`, `resolution(ids)` via `pygx._bng.get_resolution_from_digits`, `cell_center`/`cell_boundary` from `pygx._bng.cell_id_to_geometry` (27700 coords — **no 4326 reproject**; sample-point CRS = 27700), `edge_size` = `pygx._bng.get_edge_size`. `_rst_bng_rasterize_agg_udf`: parse String cellid → Long via `pygx._bng.parse`, `grid="bng"`, srid forced 27700 (document the srid arg is a no-op for BNG). Register `gbx_rst_bng_rasterize_agg`, callable wrapper.

- [ ] **Step 4: Run to verify it passes** — full `test/pyrx/` (confirm H3+quadbin+BNG all green). Expected: PASS.

- [ ] **Step 5: Commit** — `feat(pyrx): light BNG rasterize_agg (27700-native, String cellid)`

---

## Task 6: Cross-tier parity tests (all 9, Docker)

**Files:**
- Test: cross-tier parity suite under `python/geobrix/test/` (follow the existing light-vs-heavy parity test pattern — grep for an existing `*parity*` or a test that registers both tiers).
- Docker: heavy tier needs the JAR + sample data.

- [ ] **Step 1: Write the tests** — for each of the 9: run the light SQL function and the heavy SQL function on the SAME real sample raster + resolution, assert **exact cell-set equality** (BNG String / quadbin Long ids) and measures within tolerance (1e-9 numeric, 1e-6 geometry). For tessellate: same emitted cell-id set + per-chip pixel parity within tolerance. For rasterize_agg: same output raster (band-wise within NoData-aware tolerance) + band nodata == -9999.

- [ ] **Step 2: Run in Docker** — `bash scripts/commands/gbx-test-scala.sh` is heavy; the cross-tier test is Python-driven but needs the JAR-backed heavy functions — run via the doc/integration Docker path (`gbx:test:*-docs` or the existing parity harness). Report the command. If a real production divergence appears, STOP and report BLOCKED with expected-vs-actual — do NOT weaken the tolerance.

- [ ] **Step 3: Commit** — `test(pyrx): cross-tier light-vs-heavy parity for 9 BNG/quadbin raster-grid fns`

---

## Task 7: function-info + binding parity (light entries)

**Files:**
- The 9 already have function-info examples (Phase 1). Verify light registration doesn't break `gbx:test:bindings`; if the light SQL functions need any function-info/registered_functions adjustment, make it here.

- [ ] **Step 1** — run `bash scripts/commands/gbx-test-bindings.sh --log bindings.log`. Expected: still PASS 165/165 (parity is name-level; light adds impls under the same names). If it fails, address upstream.
- [ ] **Step 2: Commit** if any change — else note "no change needed; parity holds".

---

## Task 8: Docs + diagram + badges (flip heavy→both)

**Files:**
- `docs/docs/api/raster-functions.mdx`, `docs/docs/api/execution-tiers.mdx`, `docs/docs/api/performance.mdx`, `docs/docs/api/beta-release-notes.mdx`, `resources/images/generators/rasterx-function-categories.py`

- [ ] **Step 1: raster-functions.mdx** — flip all 9 `<Tier heavy/>` → `<Tier both/>`; add per-function `:::note Lightweight tier (pyrx)` admonitions (backing lib: `pygx._bng`/`_quadbin` + rasterio; BNG notes 27700 warp). **Fix the now-true page-level claim** (lines ~20, ~157): "every RasterX function is available in both tiers" is TRUE again once these land — remove the Phase-1 "except…" qualifier that Phase-1 docs added. Fix the BNG-Grid section's tier language (drop "heavy-tier only / pending pygx BNG raster integration"; it's both-tier now).
- [ ] **Step 2: execution-tiers.mdx** — move the 9 from heavy-only to both in the raster-grid tier table.
- [ ] **Step 3: performance.mdx** — the 9 now have light impls; update the "Heavy-tier variants" framing added in Phase 1 to reflect both-tier availability; classify into existing families (no new shape).
- [ ] **Step 4: rasterx-function-categories diagram** — update `resources/images/generators/rasterx-function-categories.py`: header comment 108→117; add the 2 tessellate to Generators, 2 rasterize_agg to Aggregators, and a **BNG Grid** card (mirror H3/Quadbin Grid) with the 5 BNG reducers. Regenerate PNG+SVG (portrait + landscape) via the Chrome-headless commands in the script docstring. Verify `bash docs/scripts/check-diagram-coverage.py` passes (coverage + count).
- [ ] **Step 5: beta-release-notes.mdx** — note the 9 raster BNG/quadbin functions are now both-tier (lightweight parity), building toward v0.4.3.
- [ ] **Step 6: verify** — `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/` (clean); `bash docs/scripts/check-diagram-coverage.py` (pass); user-facing voice.
- [ ] **Step 7: Commit** — `docs(rasterx): light-tier parity — flip 9 grid fns to both-tier + diagram/badges`

---

## Final gate: pre-push checks (after Task 8 — do NOT push, user-gated)

- [ ] Run and report: `bash scripts/commands/gbx-lint-python.sh --check` (verify with Docker `--check` too, host black may differ); `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/`; `bash scripts/commands/gbx-test-bindings.sh`; `bash docs/scripts/check-diagram-coverage.py`. Hold for user go-ahead before push to `issues/49`.

---

## Self-Review notes (author)

- **Spec §4.1 (light design):** Task 1 (quadbin tessellate), Task 2 (quadbin rasterize + adapter refactor), Task 3 (5 BNG reducers), Task 4 (BNG tessellate), Task 5 (BNG rasterize) = all 9. ✅
- **Single-source cell math:** every task delegates to `pygx` (`_bng`/`_quadbin`); no numpy BNG reimpl (global constraint). ✅
- **H3 unchanged:** Task 2's cellraster refactor is the only H3 restructure (refactor-only; existing H3 tests gate it in Step 4). Tasks 1/3/4 are additive. ✅
- **BNG 27700 + String + is_valid:** Tasks 3/4/5 each warp to 27700, use `is_valid`, render String ids. ✅
- **BNG tessellate boundary completeness:** Task 4 buffers bbox before polyfill (carries the heavy-tier final-review fix into light). ✅
- **Parity bar:** Task 6 cross-tier exact-cell-set + tolerance. ✅
- **§2.6:** Task 3 Step-1 (b) empty-band → [] ; Task 2/5 nodata==-9999 + round-trip. ✅
- **Docs/diagram/badges (deferred from Phase 1):** Task 8 flips badges, fixes the page-level claim + BNG reason, regenerates the coverage-enforced diagram (108→117 + BNG card). ✅
- **Light-CI:** no new deps; pyrx/pygx test dirs already in `_LIGHT_TEST_DIRS`. ✅
- **Open impl-time decision:** the `_make_rastertogrid_udtf` factory currently `int()`s the cellID; Task 3 must add a String-cellID path (factory variant or param) — flagged in Task 3 Step 3, not a blocker.
