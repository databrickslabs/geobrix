# Issue #59 — Covering Tessellation All-Nodata Standardization (emit + NULL) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Lock in and document that covering-mode H3 tessellation (`gbx_rst_h3_tessellate`) treats a cell that overlaps the raster bbox but clips to all-NoData identically on both tiers — the cell **is emitted** (a chip row is produced), and reducing that chip yields SQL `NULL` (already true after the issue #59 reducer fix). This is the "emit + NULL" (B1) end state, folded into the `issues/59` branch (no separate issue).

**Architecture:** An empirical parity probe (already run, throwaway code removed) proved the two tiers **already emit the exact same covering-cell set** on interior-hole and swath-edge rasters (heavy_only=0, light_only=0 across 2 rasters × 2 resolutions), including all-nodata cells, with matching valid-pixel counts. Heavy's `polyfill(bbox.buffer)+intersects` and light's `polygon_to_cells_experimental(contain="overlap")` are functionally equivalent. **No product code change is needed.** The remaining work is (1) a both-tier regression test that locks the emit-all-nodata-cells + reduce-to-NULL contract so it can't silently regress, and (2) a doc sentence making the contract explicit for users.

**Tech Stack:** Python 3.12 / rasterio / numpy / pytest (light, no Docker); Scala 2.13 / GDAL / ScalaTest in the `geobrix-dev` Docker container via `gbx:*` commands (heavy); Docusaurus MDX docs.

## Global Constraints

- **The emit + NULL contract (this plan's invariant):** covering mode emits a chip row for **every** H3 cell whose hexagon geometrically overlaps the raster bbox — including cells that clip to entirely NoData. It does **not** drop all-nodata-but-overlapping cells. Reducing an all-nodata chip (`gbx_rst_max/min/avg/median`) returns SQL `NULL` (from the issue #59 reducer fix already on this branch); `gbx_rst_pixelcount` returns `0`. A cell is dropped **only** on true geometric non-overlap.
- **No product code change.** Probe-confirmed the tiers already agree. If any task discovers a real behavioral divergence (a cell one tier emits and the other doesn't, or a disagreement on all-nodata → NULL), STOP and escalate — that contradicts the probe and changes scope.
- **Version 0.4.2 (in-flight beta).** Docs/release-notes target 0.4.2; do not bump the version banner. No function aliases/renames; no new functions.
- **User-facing docs voice** — no internal planning vocabulary under `docs/docs/` (no "wave N", no subagent/dispatch talk). QC judge enforces `internals-leak`.
- **Heavy work runs in Docker** via `gbx:*` commands; never `mvn` on the host. Long Scala/Maven runs get a progress line ~every 30s.
- **Heavy /vsimem test fixtures that warp/reproject must set a geotransform + projection** (`SetGeoTransform` + `SetProjection(EPSG:4326)`) — bare `Create`+`WriteRaster` rasters make GDAL warp/clip paths misbehave (learned in the reducer work).
- **gh account** — `gh auth switch --user mjohns-databricks` before any push/PR/comment to `databrickslabs/geobrix`.

---

## Task 1: Light-tier parity test — covering emits all-nodata cells that reduce to NULL

**Files:**
- Test: `python/geobrix/test/pyrx/test_core_tessellate.py` (append; imports already present: `h3`, `pytest`, `numpy as np`, `_serde`, `tessellate`, `make_geotiff_bytes`)

**Interfaces:**
- Consumes: `tessellate.tessellate_h3(ds, resolution) -> [(cellid_int, gtiff_bytes)]` (covering mode); `accessors.maximum/pixelcount(ds) -> List[Optional[float]] / List[int]` (issue #59 behavior: empty band → `None` / `0`). `make_geotiff_bytes(width, height, epsg, nodata)` builds a north-up 4326 GTiff at origin (10.0, 50.0), 0.5° pixels.
- Produces: a regression test asserting covering emits an all-nodata-overlapping chip whose reducer is `None`.

- [ ] **Step 1: Write the failing/guard test**

Add to `python/geobrix/test/pyrx/test_core_tessellate.py`. The helper builds a raster with an interior NoData hole (a real value everywhere except a central block), tessellates in covering mode, and asserts that (a) at least one emitted chip is all-nodata (`pixelcount == 0`), (b) every such chip reduces to `None` (not NaN, not a number), and (c) chips with data still reduce to a real value.

```python
def _raster_with_interior_hole(nodata=-9999.0):
    """9x9 EPSG:4326 raster, all pixels = 42.0 except a 3x3 interior NoData block."""
    import numpy as np
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = np.full((9, 9), 42.0, dtype="float32")
    data[3:6, 3:6] = nodata
    profile = dict(
        driver="GTiff", width=9, height=9, count=1, dtype="float32",
        crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.05, 0.05), nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data, 1)
        return mf.read()


def test_covering_emits_all_nodata_cells_that_reduce_to_null():
    # Contract (issue #59, emit + NULL): covering mode emits a chip for every
    # overlapping cell, INCLUDING cells that clip to entirely NoData; reducing
    # such a chip yields None (SQL NULL), never NaN. Data-bearing cells keep a
    # real value. A cell is dropped only on true geometric non-overlap.
    from databricks.labs.gbx.pyrx.core import accessors

    src = _raster_with_interior_hole()
    empty_seen = data_seen = 0
    with _serde.open_tile(src) as ds:
        chips = tessellate.tessellate_h3(ds, 6)
    assert chips, "covering must emit at least one chip"
    for _cellid, raster in chips:
        with _serde.open_tile(raster) as chip:
            pc = accessors.pixelcount(chip)[0]
            mx = accessors.maximum(chip)[0]
        if pc == 0:
            empty_seen += 1
            assert mx is None, f"all-nodata chip must reduce to None, got {mx!r}"
        else:
            data_seen += 1
            assert mx is not None and mx == 42.0
    # The hole must actually produce >=1 all-nodata cell, else the test proves nothing.
    assert empty_seen > 0, "interior hole should yield >=1 all-nodata covering cell"
    assert data_seen > 0
```

- [ ] **Step 2: Run the test**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_tessellate.py -k "all_nodata_cells_that_reduce_to_null" -v`
Expected: PASS. (Tasks 1–5 of the reducer fix already make `accessors.maximum` return `None` for an empty band, so this passes now — it is a regression guard. If `empty_seen == 0`, adjust the hole size / resolution until the interior block yields at least one fully-NoData covering cell; res 6 on a 9×9 @ 0.05° raster with a 3×3 hole should. If `mx` comes back as `NaN`, the reducer fix regressed — STOP and escalate.)

- [ ] **Step 3: Run the full tessellate test file to confirm no regression**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_tessellate.py -v`
Expected: all PASS (including the pre-existing `test_tessellate_drops_zero_coverage_fringe_cells`, which asserts the complementary invariant — no *zero-geometric-coverage* fringe cells; the two are consistent: overlap-but-nodata is emitted, no-overlap is not).

- [ ] **Step 4: Lint**

Run: `bash scripts/commands/gbx-lint-python.sh --check` (run `--fix` on host first if it flags formatting, then re-check).
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add python/geobrix/test/pyrx/test_core_tessellate.py
git commit -m "test(pyrx): covering emits all-nodata cells that reduce to NULL (#59)

Locks the emit+NULL contract on the light tier: covering-mode tessellation
emits a chip for every overlapping H3 cell including all-nodata ones, and
reducing such a chip yields None (not NaN). Guards against a silent
regression to dropping empty cells or to the pre-#59 NaN sentinel.

Co-authored-by: Isaac"
```

---

## Task 2: Heavy-tier parity test — covering emits all-nodata cells with a null reducer

**Files:**
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/operations/RasterTessellateTest.scala` (append; reuses `beforeAll` GDAL registration and the `validPixelCount(chip)` helper already in the suite)

**Interfaces:**
- Consumes: `RasterTessellate.tessellateH3Iter(ds, options, resolution, mode) -> Iterator[(Long cellId, Dataset chip, Map)]` (covering default); `RST_Max.execute(chip): Array[java.lang.Double]` (issue #59: empty band → `null` element); the suite's existing `validPixelCount(chip: Dataset): Long`.
- Produces: a heavy regression test mirroring Task 1 — an all-nodata-overlapping chip is emitted and `RST_Max` on it is `null`.

- [ ] **Step 1: Add a georeferenced interior-hole raster helper + the test**

Append to `RasterTessellateTest.scala` (inside the class). The helper builds a `/vsimem` GTiff with a geotransform + EPSG:4326 projection (required for the covering clip/warp path) and an interior NoData block. `RST_Max` must be imported.

```scala
    /** 9x9 Float32 /vsimem raster (EPSG:4326, georeferenced) = 42.0 except a 3x3 interior NoData block. */
    private def interiorHoleDs(): Dataset = {
        val path = s"/vsimem/tess_hole_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv = gdal.GetDriverByName("GTiff")
        val d = drv.Create(path, 9, 9, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float32)
        d.SetGeoTransform(Array(10.0, 0.05, 0.0, 50.0, 0.0, -0.05))
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(4326)
        d.SetProjection(srs.ExportToWkt())
        srs.delete()
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(-9999.0)
        val buf = Array.fill[Double](81)(42.0)
        for (r <- 3 to 5; c <- 3 to 5) buf(r * 9 + c) = -9999.0  // interior 3x3 hole
        band.WriteRaster(0, 0, 9, 9, buf)
        band.FlushCache()
        d.FlushCache()
        band.delete()
        d
    }

    test("covering emits all-nodata cells whose reducer is null (issue #59 emit+NULL)") {
        val iter = RasterTessellate.tessellateH3Iter(interiorHoleDs(), Map.empty, 6, "covering")
        var emptySeen = 0
        var dataSeen = 0
        try {
            iter.foreach { case (_, chip, _) =>
                val vc = validPixelCount(chip)
                val mx = RST_Max.execute(chip).headOption.orNull
                if (vc == 0L) { emptySeen += 1; mx shouldBe null }
                else { dataSeen += 1; mx should not be null }
                RasterDriver.releaseDataset(chip)
            }
        } finally iter match {
            case ac: AutoCloseable => ac.close()
            case _                 =>
        }
        emptySeen should be > 0  // the hole must yield >=1 all-nodata covering cell
        dataSeen should be > 0
    }
```

(If `RST_Max` isn't imported in this suite, add `import com.databricks.labs.gbx.rasterx.expressions.accessors.RST_Max`. `RasterDriver` is already imported.)

- [ ] **Step 2: Run the test in Docker**

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.operations.RasterTessellateTest' --log tess-null.log`
Expected: PASS, including the new test and all pre-existing covering/centroid tests. (Compile + run takes a few minutes; be patient.) If `emptySeen == 0`, the hole didn't produce a fully-NoData covering cell at res 6 — bump resolution to 7 (finer cells → more likely an interior cell lands entirely in the hole) and re-run. If the empty chip's `RST_Max` is not `null`, the reducer fix regressed — STOP and escalate.

- [ ] **Step 3: Scalastyle**

Run: `bash scripts/commands/gbx-lint-scalastyle.sh`
Expected: 0 errors.

- [ ] **Step 4: Commit**

```bash
git add src/test/scala/com/databricks/labs/gbx/rasterx/operations/RasterTessellateTest.scala
git commit -m "test(rasterx): covering emits all-nodata cells with null reducer (#59)

Heavy-tier counterpart to the pyrx test: covering-mode tessellation emits
a chip for every overlapping H3 cell including all-nodata ones, and
RST_Max on such a chip is a null element. Locks the emit+NULL contract on
the heavy tier so it stays cross-tier consistent.

Co-authored-by: Isaac"
```

---

## Task 3: Document the covering all-nodata contract

**Files:**
- Modify: `docs/docs/api/h3-raster-tessellation.mdx` (the covering-mode prose, around the `**covering:**` paragraph at line ~52 and the cross-tier note at ~169)

**Interfaces:** none (docs only). Consumes nothing; produces user-facing contract text.

- [ ] **Step 1: Add the emit + NULL sentence to the covering description**

In `docs/docs/api/h3-raster-tessellation.mdx`, in the `**covering:**` explanation paragraph (~line 52, which currently ends with the border-cell union note), append:

```markdown
A cell that overlaps the tile but clips to entirely NoData is still emitted as a chip (a cell is omitted only when its hexagon does not geometrically overlap the tile at all). Such an all-NoData chip has a valid-pixel count of `0`, and the value reducers (`gbx_rst_max`, `gbx_rst_min`, `gbx_rst_avg`, `gbx_rst_median`) return SQL `NULL` for it on both tiers — filter these with `WHERE measure IS NULL`. This lets a downstream query distinguish *missing data* (a chip is present but its measure is `NULL`) from *outside the coverage area* (no chip for that cell).
```

- [ ] **Step 2: Reinforce the cross-tier parity note**

In the cross-tier mechanism note (~line 169, which already states the covering *set* is identical across tiers and "verified by the per-mode parity tests"), extend it so the all-nodata behavior is explicitly covered:

```markdown
Both tiers emit the same covering set — including cells that clip to all-NoData — and both reduce an all-NoData chip to SQL `NULL`; this is verified by the per-mode parity tests and the all-nodata regression tests on each tier.
```

- [ ] **Step 3: Verify no internal-vocab leak**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/`
Expected: prints nothing.

- [ ] **Step 4: Commit**

```bash
git add docs/docs/api/h3-raster-tessellation.mdx
git commit -m "docs(issue-59): document covering emit+NULL for all-nodata cells

State the covering-mode contract explicitly: an overlapping-but-all-NoData
cell is emitted (not dropped) and its reducers return SQL NULL on both
tiers; a cell is omitted only on true geometric non-overlap. Clarifies the
missing-data vs outside-coverage distinction for downstream filtering.

Co-authored-by: Isaac"
```

---

## Task 4: Verification gate

**Files:** none (verification only).

- [ ] **Step 1: Run both affected test files**

Light: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_tessellate.py -v`
Heavy (Docker): `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.operations.RasterTessellateTest' --log tess-final.log`
Expected: both green.

- [ ] **Step 2: Lint both tiers**

Run: `bash scripts/commands/gbx-lint-python.sh --check` and `bash scripts/commands/gbx-lint-scalastyle.sh`
Expected: both clean.

- [ ] **Step 3: Confirm no product code changed in this plan**

Run: `git diff --stat <this-plan's-base>..HEAD -- src/main python/geobrix/src`
Expected: **empty** — this plan adds only tests and docs. If any `src/main` or `pyrx/src` file changed, that contradicts the probe (which proved no code change is needed); review and escalate before proceeding.

---

## Self-Review

**Spec/decision coverage:**
- B1 "emit + NULL" end state, no separate issue, folded into issues/59 → whole plan. ✓
- Probe finding (sets already match, no code change) → Global Constraints + Task 4 Step 3 guard. ✓
- Both-tier parity test (emit all-nodata cell + reduce to NULL) → Task 1 (light) + Task 2 (heavy). ✓
- Doc sentence on the covering contract + missing-vs-outside distinction → Task 3. ✓
- Depends on the issue #59 reducer fix already on this branch (empty → None/null) → Tasks 1/2 assert it end-to-end; escalate if it regressed. ✓

**Placeholder scan:** none — every test/doc step has full code/prose. ✓

**Type consistency:** light asserts `accessors.maximum(...)[0] is None` / `== 42.0`, `pixelcount(...)[0] == 0`; heavy asserts `RST_Max.execute(chip).headOption.orNull shouldBe null` (boxed `java.lang.Double` from the #59 change) and uses the suite's existing `validPixelCount`. Consistent with the reducer work already on the branch. ✓

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-20-issue59-tessellation-nodata-standardization.md`.
