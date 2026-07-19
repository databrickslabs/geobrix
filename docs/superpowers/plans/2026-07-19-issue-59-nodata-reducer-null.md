# Issue #59 — NULL for Zero-Valid-Pixel Raster Reducers — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `gbx_rst_max` / `gbx_rst_min` / `gbx_rst_avg` / `gbx_rst_median` return SQL `NULL` (not NaN on light, not `0.0` on heavy) for a raster band with zero valid pixels, identically on both tiers, and align light `rst_isempty` to heavy's all-nodata-aware semantics.

**Architecture:** Light tier (pyrx) is pure Python — reducers live in `core/accessors.py` and already detect the empty case via `if vals.size`; the change is `float("nan")` → `None` plus an all-nodata branch in `isempty`. Heavy tier (rasterx) reducers return dense `Array[Double]` which cannot carry a per-band NULL, so all four `execute` methods change to `Array[java.lang.Double]` and gain a `stats.getValid_count == 0` guard (the same signal `RST_PixelCount` uses). No internal Scala code calls these `execute` methods, so the signature change is contained. Docs (release notes, docstrings, function-info, reducer pages) are updated to state NULL-on-empty.

**Tech Stack:** Scala 2.13.16 / Spark 4.0.0 / GDAL Java bindings (heavy, built + tested in the `geobrix-dev` Docker container via Maven); Python 3.12 / rasterio / numpy / PySpark (light); Docusaurus MDX docs; `gbx:*` command palette.

## Global Constraints

- **Version is 0.4.1 (beta).** Beta = APIs may break to stabilize; **no function aliases** — one canonical name per function.
- **Convention (the invariant this plan enforces):** a per-band element of `rst_max`/`rst_min`/`rst_avg`/`rst_median` is SQL `NULL` **iff** that band has zero valid pixels. `rst_pixelcount` stays `0` for an empty band — do **not** change it.
- **Binding parity is enforced** — every function name must exist as a Scala `override def name`, a Python `functions.py` binding, and a `function-info.json` key. The four reducers already exist in all three; this plan changes behavior/return representation, not the name set, so no new names are added.
- **User-facing docs voice** — no internal planning vocabulary (no "wave N", no subagent/dispatch references) anywhere under `docs/docs/`. QC judge enforces via `internals-leak`.
- **Heavy work runs in Docker.** Never run Maven/Scala suites inline on the host; dispatch via a Task subagent using the `gbx:*` commands, and give a one-line progress update ~every 30s on long runs.
- **gh account** — `gh auth switch --user mjohns-databricks` before any push/PR/comment to `databrickslabs/geobrix`.
- **Deferred (do NOT touch in this plan):** covering-mode tessellation divergence for geometrically-overlapping-but-all-nodata chips (`RasterTessellate.scala` / `tessellate.py:183-184`). Filed as a separate follow-up issue.

---

## Task 1: Light tier — reducers return None + isempty all-nodata parity

**Files:**
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/core/accessors.py` (reducers `:120-153`, `isempty` `:93-94`)
- Modify: `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py` (docstrings `:2314-2350`, `rst_isempty` `:2263-2264`)
- Test: `python/geobrix/test/pyrx/test_core_accessors_stats.py` (replace `:70-78`), and a new `test_core_accessors_isempty.py` (or extend the stats file) for isempty

**Interfaces:**
- Consumes: existing `_valid_values(ds, band_index) -> np.ndarray` (`accessors.py:113-117`), which returns an empty array for an all-masked band.
- Produces: `accessors.avg/minimum/maximum/median(ds) -> List[Optional[float]]` where an empty band yields `None`; `accessors.pixelcount(ds) -> List[int]` **unchanged** (empty → `0`); `accessors.isempty(ds) -> bool` returns `True` when width/height/count is 0 **or** every band has zero valid pixels.

- [ ] **Step 1: Replace the NaN-asserting test with a None-asserting test**

In `python/geobrix/test/pyrx/test_core_accessors_stats.py`, replace the whole `test_stats_all_invalid_band_is_nan_zero` function (lines 70-78) with:

```python
def test_stats_all_invalid_band_is_null_zero():
    # A band that is entirely NoData has zero valid pixels: reducers must return
    # None (SQL NULL), never NaN or 0.0 (issue #59). pixelcount stays 0.
    data = np.full((2, 2), -9999.0, dtype="float32")
    raster = _custom_raster(data)
    with _serde.open_tile(raster) as ds:
        assert accessors.avg(ds) == [None]
        assert accessors.minimum(ds) == [None]
        assert accessors.maximum(ds) == [None]
        assert accessors.median(ds) == [None]
        assert accessors.pixelcount(ds) == [0]


def test_stats_genuine_zero_is_not_null():
    # A band of genuine 0.0 valid pixels must return 0.0, not None — the
    # zero-not-null trap. nodata is a sentinel that no pixel equals.
    data = np.zeros((2, 2), dtype="float32")
    raster = _custom_raster(data)  # nodata defaults to -9999.0, unmatched
    with _serde.open_tile(raster) as ds:
        assert accessors.avg(ds) == [pytest.approx(0.0)]
        assert accessors.minimum(ds) == [pytest.approx(0.0)]
        assert accessors.maximum(ds) == [pytest.approx(0.0)]
        assert accessors.median(ds) == [pytest.approx(0.0)]
        assert accessors.pixelcount(ds) == [4]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_accessors_stats.py -k "all_invalid_band_is_null_zero or genuine_zero_is_not_null"`
Expected: `test_stats_all_invalid_band_is_null_zero` FAILS (reducers currently return NaN, `[nan] == [None]` is False); `test_stats_genuine_zero_is_not_null` PASSES (0.0 already works).

- [ ] **Step 3: Change the four reducers from NaN to None**

In `python/geobrix/src/databricks/labs/gbx/pyrx/core/accessors.py`, edit the four reducers so the empty-band branch yields `None` and update each docstring. Replace lines 120-153 with:

```python
def avg(ds) -> List[Optional[float]]:
    """Per-band mean of valid pixels; None (SQL NULL) for empty/all-invalid bands."""
    out: List[Optional[float]] = []
    for bi in range(1, ds.count + 1):
        vals = _valid_values(ds, bi)
        out.append(float(np.mean(vals)) if vals.size else None)
    return out


def minimum(ds) -> List[Optional[float]]:
    """Per-band min of valid pixels; None (SQL NULL) for empty/all-invalid bands."""
    out: List[Optional[float]] = []
    for bi in range(1, ds.count + 1):
        vals = _valid_values(ds, bi)
        out.append(float(np.min(vals)) if vals.size else None)
    return out


def maximum(ds) -> List[Optional[float]]:
    """Per-band max of valid pixels; None (SQL NULL) for empty/all-invalid bands."""
    out: List[Optional[float]] = []
    for bi in range(1, ds.count + 1):
        vals = _valid_values(ds, bi)
        out.append(float(np.max(vals)) if vals.size else None)
    return out


def median(ds) -> List[Optional[float]]:
    """Per-band median of valid pixels; None (SQL NULL) for empty/all-invalid bands."""
    out: List[Optional[float]] = []
    for bi in range(1, ds.count + 1):
        vals = _valid_values(ds, bi)
        out.append(float(np.median(vals)) if vals.size else None)
    return out
```

(`Optional` is already imported in this module — it is used by `getnodata` at `:102`. If a flake8 run reports it missing, add `Optional` to the existing `from typing import ...` line.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_accessors_stats.py -k "all_invalid_band_is_null_zero or genuine_zero_is_not_null"`
Expected: both PASS.

- [ ] **Step 5: Write the failing isempty parity test**

Create `python/geobrix/test/pyrx/test_core_accessors_isempty.py`:

```python
"""isempty parity: an all-nodata raster is empty (matches heavy RasterAccessors.isEmpty)."""
import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import accessors


def _raster(data, nodata=-9999.0):
    h, w = data.shape
    profile = dict(
        driver="GTiff", width=w, height=h, count=1, dtype="float32",
        crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.5, 0.5), nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


def test_isempty_all_nodata_is_empty():
    data = np.full((2, 2), -9999.0, dtype="float32")
    with _serde.open_tile(_raster(data)) as ds:
        assert accessors.isempty(ds) is True


def test_isempty_has_valid_pixels_is_not_empty():
    data = np.array([[1.0, 2.0], [3.0, 4.0]], dtype="float32")
    with _serde.open_tile(_raster(data)) as ds:
        assert accessors.isempty(ds) is False
```

- [ ] **Step 6: Run the isempty test to verify the all-nodata case fails**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_accessors_isempty.py -v`
Expected: `test_isempty_all_nodata_is_empty` FAILS (current isempty only checks dimensions, returns False); `test_isempty_has_valid_pixels_is_not_empty` PASSES.

- [ ] **Step 7: Extend isempty with an all-nodata branch**

In `python/geobrix/src/databricks/labs/gbx/pyrx/core/accessors.py`, replace lines 93-94:

```python
def isempty(ds) -> bool:
    """True if the raster has no size, or every band has zero valid pixels.

    Mirrors heavyweight RasterAccessors.isEmpty (null / no size / all bands
    fully NoData). A dimensionally-valid raster whose every band is NoData is
    still empty (issue #59).
    """
    if int(ds.width) == 0 or int(ds.height) == 0 or int(ds.count) == 0:
        return True
    return all(_valid_values(ds, bi).size == 0 for bi in range(1, ds.count + 1))
```

- [ ] **Step 8: Run the isempty test to verify it passes**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_accessors_isempty.py -v`
Expected: both PASS.

- [ ] **Step 9: Update the public docstrings in functions.py**

In `python/geobrix/src/databricks/labs/gbx/pyrx/functions.py`, update the four reducer docstrings (lines 2314-2342) — change each `Empty / all-invalid bands return NaN.` to `Empty / all-invalid bands return NULL.`. Leave `rst_pixelcount` (`:2346-2348`, "return 0") unchanged. Then give `rst_isempty` (`:2263-2264`) a docstring:

```python
def rst_isempty(tile: ColLike) -> Column:
    """True if the raster has no size or every band is entirely NoData; BOOLEAN."""
    return _u_isempty(_raster_field(_col(tile)))
```

- [ ] **Step 10: Run the full pyrx accessors suite + lint**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_core_accessors_stats.py --path python/geobrix/test/pyrx/test_core_accessors_isempty.py -v`
Then: `bash scripts/commands/gbx-lint-python.sh --check`
Expected: all tests PASS; lint clean (run `--fix` on host first if black/isort/flake8 complain, then re-run `--check`).

- [ ] **Step 11: Commit**

```bash
git add python/geobrix/src/databricks/labs/gbx/pyrx/core/accessors.py \
        python/geobrix/src/databricks/labs/gbx/pyrx/functions.py \
        python/geobrix/test/pyrx/test_core_accessors_stats.py \
        python/geobrix/test/pyrx/test_core_accessors_isempty.py
git commit -m "fix(pyrx): reducers return NULL (not NaN) for zero-valid-pixel bands

gbx_rst_max/min/avg/median now emit None for an all-nodata band, and
rst_isempty is all-nodata-aware, matching heavy RasterAccessors.isEmpty.
Addresses issue #59 on the light tier. pixelcount unchanged (empty -> 0).

Co-authored-by: Isaac"
```

---

## Task 2: Heavy tier — RST_Avg returns NULL for zero-valid-pixel bands

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Avg.scala` (`execute` `:54-64`)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AccessorsExecuteTest.scala`

**Interfaces:**
- Consumes: `band.AsMDArray().GetStatistics()` (GDAL) — a `Statistics` object with `getMean` and `getValid_count`; already called in `RST_Avg.execute`.
- Produces: `RST_Avg.execute(ds: Dataset): Array[java.lang.Double]` — element is `null` for a null band or `getValid_count == 0`, else the boxed mean. `ArrayData.toArrayData` accepts `Array[java.lang.Double]` and maps `null` to a SQL NULL element (the expression already declares `dataType = ArrayType(DoubleType)`, `nullable = true`).

- [ ] **Step 1: Add an all-nodata + genuine-zero test to the Scala accessor suite**

The suite opens a MODIS TIF in `beforeAll`. Add a helper that builds a synthetic all-nodata raster in `/vsimem`, and two tests. Append inside `RST_AccessorsExecuteTest` (before the closing brace at line 246):

```scala
    /** Build a 4x4 single-band Float32 /vsimem raster where every pixel == nodata. */
    private def allNodataDs(nodata: Double = -9999.0): Dataset = {
        val path = s"/vsimem/all_nodata_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv = gdal.GetDriverByName("GTiff")
        val d = drv.Create(path, 4, 4, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float32)
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(nodata)
        val buf = Array.fill[Double](16)(nodata)
        band.WriteRaster(0, 0, 4, 4, buf)
        band.FlushCache()
        d.FlushCache()
        band.delete()
        d
    }

    /** Build a 4x4 single-band Float32 /vsimem raster of genuine 0.0 pixels. */
    private def allZeroDs(nodata: Double = -9999.0): Dataset = {
        val path = s"/vsimem/all_zero_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv = gdal.GetDriverByName("GTiff")
        val d = drv.Create(path, 4, 4, 1, org.gdal.gdalconst.gdalconstConstants.GDT_Float32)
        val band = d.GetRasterBand(1)
        band.SetNoDataValue(nodata)
        val buf = Array.fill[Double](16)(0.0)
        band.WriteRaster(0, 0, 4, 4, buf)
        band.FlushCache()
        d.FlushCache()
        band.delete()
        d
    }

    test("RST_Avg returns null for an all-nodata band (issue #59)") {
        val empty = allNodataDs()
        RST_Avg.execute(empty).head shouldBe null
        empty.delete()
    }

    test("RST_Avg returns 0.0 (not null) for a genuine-zero band") {
        val zeros = allZeroDs()
        RST_Avg.execute(zeros).head shouldBe (0.0: java.lang.Double)
        zeros.delete()
    }
```

- [ ] **Step 2: Run the RST_Avg tests to verify the all-nodata case fails** (Docker — dispatch a Task subagent)

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsExecuteTest' --log issue59-avg.log`
Expected: `RST_Avg returns null for an all-nodata band` FAILS — today `execute` returns `Array[Double]` (primitive), so `.head` is `0.0`, and also the code returns `stats.getMean` (0.0) for the empty band; the boxed-null assertion cannot even compile against `Array[Double]`. This step confirms the compile/behavior gap. The genuine-zero test may fail to compile until Step 3 changes the return type — expect a compile error naming `RST_Avg.execute` return type; that is the failing signal.

- [ ] **Step 3: Change RST_Avg.execute to Array[java.lang.Double] with a valid-count guard**

In `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Avg.scala`, replace `execute` (lines 54-64):

```scala
    def execute(ds: Dataset): Array[java.lang.Double] = {
        (1 to ds.GetRasterCount()).map { bandIndex =>
            val band = ds.GetRasterBand(bandIndex)
            if (band == null) null
            else {
                val md = band.AsMDArray()
                val stats = md.GetStatistics()
                val res: java.lang.Double =
                    if (stats == null || stats.getValid_count == 0) null
                    else stats.getMean
                if (stats != null) stats.delete()
                md.delete()
                band.delete()
                res
            }
        }.toArray
    }
```

Also update the class doc comment (line 14) to note: `an all-nodata band (zero valid pixels) yields a NULL element`.

- [ ] **Step 4: Run the RST_Avg tests to verify they pass** (Docker)

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsExecuteTest' --log issue59-avg.log`
Expected: both new tests PASS. The pre-existing `RST_Avg should return the average px value` test (line 28-32) still PASSES — `avg.foreach(a => a shouldBe expected)` compares boxed `java.lang.Double` to a primitive `Double`; scalatest `shouldBe` handles the boxing. If it fails on a type mismatch, change that assertion to `a shouldBe (expected: java.lang.Double)`.

- [ ] **Step 5: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Avg.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AccessorsExecuteTest.scala
git commit -m "fix(rasterx): RST_Avg returns NULL for zero-valid-pixel bands (#59)

execute now returns Array[java.lang.Double] and emits null when a band
has zero valid pixels (getValid_count == 0), replacing the leaked 0.0.

Co-authored-by: Isaac"
```

---

## Task 3: Heavy tier — RST_Max and RST_Min return NULL for zero-valid-pixel bands

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Max.scala` (`execute` `:54-64`)
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Min.scala` (`execute` `:54-64`)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AccessorsExecuteTest.scala`

**Interfaces:**
- Consumes: `BandAccessors.getMinMax(band): (Double, Double)` (`BandAccessors.scala:23-30`) — **leave this helper unchanged**; it is also used by `RST_TileXYZ.scala:157`. The empty-band decision is made inside the reducer via a `getValid_count` check, not inside `getMinMax`.
- Produces: `RST_Max.execute(ds): Array[java.lang.Double]` and `RST_Min.execute(ds): Array[java.lang.Double]` — `null` element for a null band or `getValid_count == 0`, else the boxed max/min.

- [ ] **Step 1: Add all-nodata + genuine-zero tests for Max and Min**

In `RST_AccessorsExecuteTest.scala`, append (the `allNodataDs`/`allZeroDs` helpers from Task 2 already exist):

```scala
    test("RST_Max returns null for an all-nodata band (issue #59)") {
        val empty = allNodataDs()
        RST_Max.execute(empty).head shouldBe null
        empty.delete()
    }

    test("RST_Max returns 0.0 (not null) for a genuine-zero band") {
        val zeros = allZeroDs()
        RST_Max.execute(zeros).head shouldBe (0.0: java.lang.Double)
        zeros.delete()
    }

    test("RST_Min returns null for an all-nodata band (issue #59)") {
        val empty = allNodataDs()
        RST_Min.execute(empty).head shouldBe null
        empty.delete()
    }

    test("RST_Min returns 0.0 (not null) for a genuine-zero band") {
        val zeros = allZeroDs()
        RST_Min.execute(zeros).head shouldBe (0.0: java.lang.Double)
        zeros.delete()
    }
```

- [ ] **Step 2: Run to verify failure/compile gap** (Docker)

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsExecuteTest' --log issue59-minmax.log`
Expected: the new tests FAIL or fail to compile (execute still returns `Array[Double]`, all-nodata leaks `0.0`, and the null assertion needs boxed elements).

- [ ] **Step 3: Change RST_Max.execute with a valid-count guard**

In `RST_Max.scala`, replace `execute` (lines 54-64):

```scala
    def execute(ds: Dataset): Array[java.lang.Double] = {
        (1 to ds.GetRasterCount()).map { bandIndex =>
            val band = ds.GetRasterBand(bandIndex)
            if (band == null) null
            else {
                val md = band.AsMDArray()
                val stats = md.GetStatistics()
                val res: java.lang.Double =
                    if (stats == null || stats.getValid_count == 0) null
                    else {
                        val (_, max) = BandAccessors.getMinMax(band)
                        max
                    }
                if (stats != null) stats.delete()
                md.delete()
                band.delete()
                res
            }
        }.toArray
    }
```

Update the class doc comment (line 15) to note the NULL-on-empty behavior.

- [ ] **Step 4: Change RST_Min.execute with a valid-count guard**

In `RST_Min.scala`, replace `execute` (lines 54-64):

```scala
    def execute(ds: Dataset): Array[java.lang.Double] = {
        (1 to ds.GetRasterCount()).map { bandIndex =>
            val band = ds.GetRasterBand(bandIndex)
            if (band == null) null
            else {
                val md = band.AsMDArray()
                val stats = md.GetStatistics()
                val res: java.lang.Double =
                    if (stats == null || stats.getValid_count == 0) null
                    else {
                        val (min, _) = BandAccessors.getMinMax(band)
                        min
                    }
                if (stats != null) stats.delete()
                md.delete()
                band.delete()
                res
            }
        }.toArray
    }
```

Update the class doc comment (line 15) to note the NULL-on-empty behavior.

- [ ] **Step 5: Run to verify all Max/Min tests pass** (Docker)

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsExecuteTest' --log issue59-minmax.log`
Expected: the four new tests PASS; the pre-existing `RST_Max`/`RST_Min` tests (lines 92-95, 128-131) still PASS (boxed-vs-primitive `shouldBe` handles boxing; if not, box the expected value as in Task 2 Step 4).

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Max.scala \
        src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Min.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AccessorsExecuteTest.scala
git commit -m "fix(rasterx): RST_Max/RST_Min return NULL for zero-valid-pixel bands (#59)

execute methods now return Array[java.lang.Double] and emit null when a
band has zero valid pixels (getValid_count == 0), replacing the leaked
0.0 that ComputeRasterMinMax's zero-initialized array produced.
BandAccessors.getMinMax is unchanged (still used by RST_TileXYZ).

Co-authored-by: Isaac"
```

---

## Task 4: Heavy tier — RST_Median returns NULL for zero-valid-pixel bands

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Median.scala` (`execute` `:54-65`)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AccessorsExecuteTest.scala`

**Interfaces:**
- Consumes: `GDALWarp.executeWarp` (warps to a 1×1 raster via `gdalwarp -r med -ts 1 1`), then `resDs.GetRasterBand(i).AsMDArray().GetStatistics()`.
- Produces: `RST_Median.execute(ds: Dataset, options: Map[String, String]): Array[java.lang.Double]` — `null` element when the warped 1×1 band's stats are null or `getValid_count == 0`, else the boxed median (`getMax` of the 1×1 med-warp). Adds the currently-missing null-check on `GetStatistics()`.

- [ ] **Step 1: Add all-nodata + genuine-zero tests for Median**

In `RST_AccessorsExecuteTest.scala`, append:

```scala
    test("RST_Median returns null for an all-nodata band (issue #59)") {
        val empty = allNodataDs()
        RST_Median.execute(empty, Map.empty).head shouldBe null
        empty.delete()
    }

    test("RST_Median returns 0.0 (not null) for a genuine-zero band") {
        val zeros = allZeroDs()
        RST_Median.execute(zeros, Map.empty).head shouldBe (0.0: java.lang.Double)
        zeros.delete()
    }
```

- [ ] **Step 2: Run to verify failure** (Docker)

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsExecuteTest' --log issue59-median.log`
Expected: the all-nodata Median test FAILS (today `getMax` of the 1×1 med-warp of an all-nodata input is not null, and the return type is primitive `Array[Double]`).

- [ ] **Step 3: Change RST_Median.execute with a null/valid-count guard**

In `RST_Median.scala`, replace `execute` (lines 54-65):

```scala
    def execute(ds: Dataset, options: Map[String, String]): Array[java.lang.Double] = {
        val outShortName = ds.GetDriver().getShortName
        val uuid = java.util.UUID.randomUUID().toString.replace("-", "")
        val extension = GDAL.getExtension(outShortName)
        val resultPath = s"/vsimem/rst_median_$uuid.$extension"
        val cmd = s"gdalwarp -r med -ts 1 1"
        val (resDs, _) = GDALWarp.executeWarp(resultPath, Array(ds), options, cmd)
        val medians: Array[java.lang.Double] = (1 to resDs.GetRasterCount()).map { i =>
            val md = resDs.GetRasterBand(i).AsMDArray()
            val stats = md.GetStatistics()
            val res: java.lang.Double =
                if (stats == null || stats.getValid_count == 0) null
                else stats.getMax
            if (stats != null) stats.delete()
            md.delete()
            res
        }.toArray
        resDs.delete()
        gdal.Unlink(resultPath)
        medians
    }
```

Update the class doc comment (line 15) to note NULL-on-empty behavior.

- [ ] **Step 4: Run to verify Median tests pass** (Docker)

Run: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsExecuteTest' --log issue59-median.log`
Expected: both new tests PASS; the pre-existing `RST_Median should return approximated median` test (lines 97-106) still PASSES.

- [ ] **Step 5: Run the broader eval suites to catch ArrayData/nullable regressions** (Docker)

Run: `bash scripts/commands/gbx-test-scala.sh --suites 'com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsEvalTest,com.databricks.labs.gbx.rasterx.expressions.RST_AccessorsExecuteTest,com.databricks.labs.gbx.rasterx.RasterXFunctionsTest' --log issue59-eval.log`
Expected: PASS. These exercise the `eval` → `ArrayData.toArrayData` path end-to-end, confirming a boxed `null` element round-trips to a SQL NULL array element for all four reducers.

- [ ] **Step 6: Commit**

```bash
git add src/main/scala/com/databricks/labs/gbx/rasterx/expressions/accessors/RST_Median.scala \
        src/test/scala/com/databricks/labs/gbx/rasterx/expressions/RST_AccessorsExecuteTest.scala
git commit -m "fix(rasterx): RST_Median returns NULL for zero-valid-pixel bands (#59)

execute returns Array[java.lang.Double], adds the previously-missing null
check on GetStatistics, and emits null when the med-warped band has zero
valid pixels. Completes the reducer NULL convention on the heavy tier.

Co-authored-by: Isaac"
```

---

## Task 5: Cross-tier seam-reconciliation regression test

**Files:**
- Test (light): `python/geobrix/test/pyrx/test_issue59_seam_reconciliation.py` (new)

**Interfaces:**
- Consumes: the light `accessors.maximum` (now None-on-empty) and standard Spark `GROUP BY ... MAX()`. This is the concrete harm from the issue: NaN sorts high and overwrites a real value in `MAX()`; NULL is ignored by `MAX()`.

- [ ] **Step 1: Write the regression test that models the seam GROUP BY**

Create `python/geobrix/test/pyrx/test_issue59_seam_reconciliation.py`:

```python
"""Issue #59 regression: an all-nodata chip's reducer must not poison MAX() in a
seam-reconciliation GROUP BY. Pure-accessor level (Spark-free) — asserts that the
empty-band reducer yields None and that None is ignored by a max() over a group
where a real value also exists, whereas the old NaN would win.
"""
import numpy as np
from rasterio.io import MemoryFile
from rasterio.transform import from_origin

from databricks.labs.gbx.pyrx import _serde
from databricks.labs.gbx.pyrx.core import accessors


def _raster(data, nodata=-9999.0):
    h, w = data.shape
    profile = dict(
        driver="GTiff", width=w, height=h, count=1, dtype="float32",
        crs="EPSG:4326", transform=from_origin(10.0, 50.0, 0.5, 0.5), nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


def test_all_nodata_chip_does_not_poison_group_max():
    # One H3 cell reconciled across two chips at a tile seam: a real-data chip
    # (max 42.0) and an all-nodata chip (empty -> None). MAX over the group must
    # be 42.0, not None and not NaN.
    real = _raster(np.array([[42.0, 1.0], [2.0, 3.0]], dtype="float32"))
    empty = _raster(np.full((2, 2), -9999.0, dtype="float32"))

    with _serde.open_tile(real) as r, _serde.open_tile(empty) as e:
        real_max = accessors.maximum(r)[0]
        empty_max = accessors.maximum(e)[0]

    assert empty_max is None
    # SQL MAX() ignores NULL: emulate the reconciliation with a NULL-skipping max.
    group = [v for v in (real_max, empty_max) if v is not None]
    assert max(group) == 42.0
```

- [ ] **Step 2: Run the regression test** (Task 1 must be merged/applied so `maximum` returns None)

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_issue59_seam_reconciliation.py -v`
Expected: PASS. (Before Task 1, `empty_max` was NaN — `empty_max is None` would be False — so this test also guards against regressing the light fix.)

- [ ] **Step 3: Commit**

```bash
git add python/geobrix/test/pyrx/test_issue59_seam_reconciliation.py
git commit -m "test(pyrx): guard seam-reconciliation MAX against all-nodata chips (#59)

Regression test for the core harm in issue #59 — an all-nodata chip's
reducer must be NULL (ignored by MAX), never NaN (which poisons the
GROUP BY seam reconciliation).

Co-authored-by: Isaac"
```

---

## Task 6: Docs — beta release notes, function-info regen, reducer/isempty pages

**Files:**
- Modify: `docs/docs/beta-release-notes.mdx` (v0.4.1 section, after the last bullet in "What's new in v0.4.1")
- Modify: `docs/docs/api/raster-functions.mdx` (`rst_avg` `:164`, `rst_max` `:289`, `rst_median` `:303`, `rst_min` `:345`, `rst_isempty` `:1339` sections)
- Regenerate: `src/main/resources/com/databricks/labs/gbx/function-info.json` (via command, not by hand)

**Interfaces:**
- Consumes: the doc SQL examples in `docs/tests/python/api/rasterx_functions_sql.py` (`rst_avg_sql_example` etc.) feed `function-info.json`. No new function names — only descriptive text and the regenerated file change.

- [ ] **Step 1: Add the breaking-change entry to the v0.4.1 release notes**

In `docs/docs/beta-release-notes.mdx`, append a bullet to the end of the "What's new in v0.4.1" list (before the `---` that closes the section):

```markdown
- **Raster value reducers return `NULL` for all-nodata bands (behavior change).** `gbx_rst_max`, `gbx_rst_min`, `gbx_rst_avg`, and `gbx_rst_median` now return SQL `NULL` for a band with zero valid pixels — for example an H3 covering-tessellation cell that clips only NoData — on **both** the lightweight and heavyweight tiers. Previously the lightweight tier returned `NaN` and the heavyweight tier returned `0.0`; neither was catchable or aggregation-safe. `NaN` silently passed `WHERE measure IS NOT NULL` and could overwrite a real value in `MAX()` during a `GROUP BY` seam reconciliation (NaN sorts greater than everything); `0.0` was indistinguishable from a genuine zero. The new `NULL` is catchable via `WHERE measure IS NULL` and ignored by aggregates like `MAX`/`MIN`/`AVG`. `gbx_rst_pixelcount` is unchanged — an all-nodata band still returns `0` (a count of zero is meaningful). Relatedly, the lightweight `gbx_rst_isempty` is now all-nodata-aware: a dimensionally-valid raster whose every band is entirely NoData now returns `true`, matching the heavyweight tier. See [Raster Functions](./api/raster-functions).
```

- [ ] **Step 2: Update the reducer + isempty descriptions in raster-functions.mdx**

In `docs/docs/api/raster-functions.mdx`, in each of the `### rst_avg`, `### rst_max`, `### rst_median`, `### rst_min` sections (near lines 164/289/303/345), add one sentence after the signature line describing empty-band behavior:

```markdown
Returns `NULL` for a band with zero valid pixels (all NoData) on both tiers.
```

In the `### rst_isempty` section (near line 1339), add:

```markdown
Returns `true` when the raster has no size **or** every band is entirely NoData.
```

Verify no internal vocabulary leaked: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/` must print nothing.

- [ ] **Step 3: Regenerate function-info.json** (Docker — needs the doc SQL example pipeline)

Run: `bash scripts/commands/gbx-docs-function-info.sh`
Then confirm the reducer entries still have non-empty usage:
Run: `git diff --stat src/main/resources/com/databricks/labs/gbx/function-info.json`
Expected: the file regenerates cleanly (the SQL examples already exist for all four reducers + pixelcount + isempty; usage is non-empty). If the generator errors on empty usage, fix the upstream SQL example — do not hand-edit the JSON.

- [ ] **Step 4: Verify docs render + no dead references**

Run: `grep -n -iE "return NaN|returns NaN|-> NaN" docs/docs/api/raster-functions.mdx`
Expected: no reducer line still claims NaN. (There may be unrelated NaN mentions; scan that none refer to the four reducers.)

- [ ] **Step 5: Commit**

```bash
git add docs/docs/beta-release-notes.mdx docs/docs/api/raster-functions.mdx \
        src/main/resources/com/databricks/labs/gbx/function-info.json
git commit -m "docs(issue-59): document NULL-on-empty reducers + isempty change

Beta release notes gain a behavior-change entry; raster-functions.mdx
reducer and isempty sections state the NULL-on-all-nodata contract;
function-info.json regenerated.

Co-authored-by: Isaac"
```

---

## Task 7: Binding parity + affected-package verification before push

**Files:** none (verification only)

**Interfaces:** consumes the full changed tree; produces a green binding-parity + lint gate.

- [ ] **Step 1: Run binding parity** (Docker)

Run: `bash scripts/commands/gbx-test-bindings.sh --log issue59-bindings.log`
Expected: PASS — every reducer name resolves across Scala `override def name`, Python `functions.py`, and `function-info.json`. (No names changed, but the element-type/behavior change must not break the parity harness.)

- [ ] **Step 2: Run the full affected light package suite**

Run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/ --log issue59-pyrx.log`
Expected: PASS. Confirms no other pyrx test depended on the old NaN/dimension-only-isempty behavior.

- [ ] **Step 3: Run scalastyle + python lint (matches CI)**

Run: `bash scripts/commands/gbx-lint-scalastyle.sh` and `bash scripts/commands/gbx-lint-python.sh --check`
Expected: both clean.

- [ ] **Step 4: Grep for any remaining callers/docs asserting the old behavior**

Run:
```bash
grep -rn -iE "all-invalid.*NaN|return NaN|isnan\(accessors\.(avg|min|max|median)" python/geobrix docs/docs src/main
```
Expected: no reducer still documents/asserts NaN. Fix any stragglers, then re-run the relevant suite.

- [ ] **Step 5: Final commit if Step 4 changed anything, else proceed**

```bash
git add -A && git commit -m "chore(issue-59): clean up residual NaN references" || echo "nothing to clean"
```

---

## Self-Review

**Spec coverage:**
- Convention (NULL iff zero valid pixels, both tiers; pixelcount stays 0) → Tasks 1–4 + Task 6 docs. ✓
- Light reducers `float("nan")`→`None` → Task 1. ✓
- Light `isempty` all-nodata parity → Task 1. ✓
- Heavy `Array[Double]`→`Array[java.lang.Double]` + `getValid_count == 0` guard, all four → Tasks 2–4. ✓
- Heavy `RST_Median` missing null-check → Task 4 Step 3. ✓
- Caller audit (NPE risk) → resolved during planning: `grep` found **no** internal callers of the four reducer `execute` methods, and `getMinMax` (shared with `RST_TileXYZ`) is left unchanged. The eval-path round-trip is covered by Task 4 Step 5. ✓
- Tests: replace NaN test, zero-not-null trap (both tiers), seam-reconciliation regression, isempty parity → Tasks 1, 2, 3, 4, 5. ✓
- Docs: beta release notes, docstrings, function-info regen, reducer pages, isempty note → Tasks 1 (docstrings) + 6. ✓
- Binding parity + lint gate → Task 7. ✓
- Deferred tessellation explicitly out of scope → Global Constraints. ✓

**Placeholder scan:** No TBD/TODO; every code step shows full code. ✓

**Type consistency:** All four heavy `execute` methods use `Array[java.lang.Double]` and the `val res: java.lang.Double = if (...) null else <boxed>` idiom uniformly; `RST_Median` keeps its `(ds, options)` signature. Light reducers return `List[Optional[float]]`; `pixelcount` stays `List[int]`. ✓

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-07-19-issue-59-nodata-reducer-null.md`.
