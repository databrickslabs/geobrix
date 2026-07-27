# Heavy XYZ RGBA Convergence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make heavy `rst_tilexyz` / `rst_xyzpyramid` emit display RGB(A) web-map tiles matching the lightweight (rio-tiler) tier — RGBA for PNG/WEBP, RGB for JPEG — so an in-extent tile with internal NoData renders transparent (not opaque/black) on the heavy tier.

**Architecture:** Insert an RGB(A) compositing step into `RST_TileXYZ.executeWithScale`, between the `gdal.Warp` and the `gdal.Translate`. The warp gains `-dstalpha` so GDAL derives a binary (0/255) alpha band from the source's valid-data mask — this is the same mask rio-tiler uses, and covers both out-of-extent and internal-NoData pixels in one step. A new private helper `toDisplayRGBA` reduces/expands the warped band set to the rio-tiler band mapping (1→grey RGB, 2→grey+alpha, 3→RGB, 4→RGB+alpha, ≥5→first-3 RGB) and attaches the alpha, producing a MEM dataset with explicit color interpretation that the translate step encodes. JPEG drops alpha (3-band RGB). The existing per-band `-scale` rescale applies to the RGB bands only.

**Tech Stack:** Scala 2.13, Spark 4.0, GDAL Java bindings (`org.gdal.gdal`), scalatest (heavy unit tests), Python/pytest + rasterio + PIL (cross-tier parity test), Docker (`geobrix-dev`) for both test suites.

## Global Constraints

- Byte-parity across tiers is a NON-GOAL and impossible (different PNG/WEBP encoders). Cross-tier verification is decode + tolerance, never byte equality or a bench fingerprint. The bench stays `timing-only` — do not touch bench wiring.
- Band mapping matches rio-tiler EXACTLY: 1→grey replicated to R=G=B (+alpha); 2→band1 grey (R=G=B) + band2 as alpha; 3→R,G,B (+alpha); 4→R,G,B + band4 alpha; ≥5→first 3 bands as R,G,B (+alpha).
- Alpha is BINARY (0 or 255) from the warp's valid-data mask — one mask covers out-of-extent AND internal NoData. Alpha bands are NEVER passed to the `-scale` rescale.
- PNG → RGBA; WEBP → RGBA where the GDAL build's WEBP driver supports alpha, else 3-band RGB fallback with a logged note (never a hard failure); JPEG → RGB (no alpha).
- The 8-bit `rescale` behavior (`"auto"` default, `resolveScale`) is REUSED unchanged and applied to RGB bands only. Do not redesign it.
- Heavy output band-count changes (N→4 for PNG/WEBP, N→3 for JPEG): a beta behavior change, documented in release notes + function docs.
- Follow existing GDAL resource discipline: release every intermediate Dataset (`RasterDriver.releaseDataset` or `.delete()`) in `try/finally`; `gdal.Unlink` every `/vsimem` path.
- Scala style: matches CI scalastyle (`gbx:lint:scalastyle`). Run before any commit that touches Scala.
- The out-of-extent `transparentPng(size)` fallback already emits all-zero-alpha RGBA — keep it; it is the reference for the RGBA MEM-dataset construction idiom.

---

## File Structure

- `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/web/RST_TileXYZ.scala` — MODIFY. Add `-dstalpha` to the warp command; add private `toDisplayRGBA` helper; wire it into `executeWithScale` between warp and translate; JPEG path drops alpha. `RST_XYZPyramid.scala` is UNCHANGED (delegates per-tile to `executeWithScale`).
- `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/RST_TileXYZRgbaTest.scala` — CREATE. Heavy unit tests: per-source-band-count output shape, per-format band count, internal-NoData → 0-alpha, out-of-extent fallback still transparent.
- `python/geobrix/test/pyrx/test_cross_language_xyz_parity.py` — MODIFY (extend). Add the cross-tier RGBA parity test (decode + exact-alpha-position + tolerance-RGB) reusing the existing `spark_with_jar`/`heavy_registered` fixtures and `_decode_band`/`_make_uint16_narrow_bytes` helpers.
- `docs/docs/beta-release-notes.mdx` and `docs/docs/api/raster-functions.mdx` — MODIFY. Behavior-change note + band-mapping documentation.

---

### Task 1: Warp emits binary alpha (`-dstalpha`) + `toDisplayRGBA` compositing helper

Insert the RGBA compositing between warp and translate. This is the core change and carries its own heavy unit test.

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/web/RST_TileXYZ.scala` (`executeWithScale` ~L199-244; add helper + `-dstalpha` to the warp command L217)
- Test: `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/RST_TileXYZRgbaTest.scala` (create)

**Interfaces:**
- Consumes: existing `resolveScale(ds, rescale): String` (per-band `-scale` flags), `transparentPng(size)`, `GDALWarp.executeWarp`, `GDALTranslate.executeTranslate`, `RasterDriver.releaseDataset`.
- Produces: `private[web] def toDisplayRGBA(warpedDs: Dataset, format: String, scaleFlags: String): (Dataset, Boolean)` — returns a MEM `GDT_Byte` dataset with color-interp set (RGBA for PNG/WEBP, RGB for JPEG) plus a boolean = `true` when an alpha band is present. Caller encodes this dataset and releases it.

- [ ] **Step 1: Write the failing heavy unit test**

Create `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/RST_TileXYZRgbaTest.scala`:

```scala
package com.databricks.labs.gbx.rasterx.expressions.web

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.test.SparkSuite
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.matchers.should.Matchers

/** Heavy-tier RGBA output shape for rst_tilexyz. Byte-parity with the light tier
 *  is impossible (different encoders) and is NOT tested here; the cross-tier
 *  decode+tolerance parity test lives in the Python suite. These tests assert the
 *  HEAVY output STRUCTURE: band count / alpha per format / band count, and that an
 *  internal-NoData hole yields 0-alpha (the display bug being fixed). */
class RST_TileXYZRgbaTest extends SparkSuite with Matchers {

  // Decode a PNG/WEBP byte array via GDAL (/vsimem) into a Dataset for band inspection.
  private def openBytes(bytes: Array[Byte], ext: String) = {
    val p = s"/vsimem/rgbatest_${java.util.UUID.randomUUID().toString.replace("-", "")}.$ext"
    gdal.FileFromMemBuffer(p, bytes)
    val ds = gdal.Open(p)
    (ds, p)
  }

  test("PNG output from a 1-band source is 4-band RGBA") {
    val src = TileXYZTestFixtures.singleBandOverTile()  // see Step 3 fixtures
    try {
      val png = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "PNG", 256, "near", "auto")
      val (ds, p) = openBytes(png, "png")
      try ds.GetRasterCount shouldBe 4
      finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }

  test("PNG output from a 3-band source is 4-band RGBA") {
    val src = TileXYZTestFixtures.threeBandOverTile()
    try {
      val png = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "PNG", 256, "near", "auto")
      val (ds, p) = openBytes(png, "png")
      try ds.GetRasterCount shouldBe 4
      finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }

  test("JPEG output is 3-band RGB (no alpha)") {
    val src = TileXYZTestFixtures.threeBandOverTile()
    try {
      val jpg = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "JPEG", 256, "near", "auto")
      val (ds, p) = openBytes(jpg, "jpg")
      try ds.GetRasterCount shouldBe 3
      finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }

  test("internal-NoData hole yields a fully-transparent (alpha=0) region") {
    val src = TileXYZTestFixtures.singleBandWithNoDataHole()  // NoData square in the middle
    try {
      val png = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "PNG", 256, "near", "auto")
      val (ds, p) = openBytes(png, "png")
      try {
        ds.GetRasterCount shouldBe 4
        // Read the alpha band (band 4); some pixels must be 0 (the hole) and some 255.
        val alpha = ds.GetRasterBand(4)
        val w = ds.GetRasterXSize; val h = ds.GetRasterYSize
        val buf = Array.ofDim[Byte](w * h)
        alpha.ReadRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
        val ints = buf.map(_ & 0xff)
        ints.exists(_ == 0) shouldBe true    // the NoData hole is transparent
        ints.exists(_ == 255) shouldBe true  // valid data is opaque
      } finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }
}
```

Also create the fixtures object `src/test/scala/com/databricks/labs/gbx/rasterx/expressions/web/TileXYZTestFixtures.scala`:

```scala
package com.databricks.labs.gbx.rasterx.expressions.web

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.SpatialReference

/** In-memory GTiff fixtures for RST_TileXYZ tests, placed over a known WebMercator
 *  z=8 tile so execute() produces a data-carrying tile (not the transparent fallback).
 *  Footprint: lon 10..12, lat 48..50 (EPSG:4326) -- mirrors the Python parity fixture. */
object TileXYZTestFixtures {
  // z=8 tile covering lon~11, lat~49 (the fixture midpoint). Precomputed via morecantile.
  val z = 8; val x = 134; val y = 86

  private def wgs84Wkt: String = {
    val srs = new SpatialReference(); srs.ImportFromEPSG(4326); srs.ExportToWkt()
  }

  private def makeGeoTiff(nbands: Int, fill: (Int, Int, Int) => Int,
                          noDataBandVal: Option[Int] = None): Dataset = {
    val w = 64; val h = 64
    val mem = gdal.GetDriverByName("MEM").Create("", w, h, nbands, gdalconstConstants.GDT_Byte)
    // lon 10..12, lat 48..50 -> pixel size 2/64 in each axis (north-up).
    mem.SetGeoTransform(Array(10.0, 2.0 / w, 0.0, 50.0, 0.0, -2.0 / h))
    mem.SetProjection(wgs84Wkt)
    for (b <- 1 to nbands) {
      val buf = Array.tabulate(w * h)(i => fill(b, i % w, i / w).toByte)
      mem.GetRasterBand(b).WriteRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
      noDataBandVal.foreach(mem.GetRasterBand(b).SetNoDataValue(_))
    }
    mem
  }

  def singleBandOverTile(): Dataset =
    makeGeoTiff(1, (_, px, py) => (px + py) % 200 + 20)

  def threeBandOverTile(): Dataset =
    makeGeoTiff(3, (b, px, _) => (px * b) % 200 + 20)

  /** Single band with a NoData value of 0 and a 0-filled square in the center. */
  def singleBandWithNoDataHole(): Dataset =
    makeGeoTiff(1, (_, px, py) => if (px >= 24 && px < 40 && py >= 24 && py < 40) 0 else (px + py) % 200 + 20,
      noDataBandVal = Some(0))
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run in Docker: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.web.RST_TileXYZRgbaTest' --log rgba-task1.log`
Expected: FAIL — the 1-band and 3-band PNG cases assert 4 bands but current output has 1 and 3 bands; the JPEG case may already pass (3-band source → 3-band); the NoData case fails (no alpha band today).

- [ ] **Step 3: Add `-dstalpha` to the warp and the `toDisplayRGBA` helper**

In `RST_TileXYZ.executeWithScale`, change the warp command (currently L217) to request a destination alpha band:

```scala
        val (warpedDs, warpedOpts) = GDALWarp.executeWarp(
          warpPath,
          Array(ds),
          options ++ Map("format" -> "GTiff"),
          command = s"gdalwarp -t_srs EPSG:3857 -te $xmin $ymin $xmax $ymax -ts $size $size -r $resampling -dstalpha"
        )
```

`-dstalpha` makes GDAL append a binary (0/255) alpha band derived from the source's valid-data mask: 255 where a source pixel maps in, 0 outside the footprint and at NoData pixels. So `warpedDs` now has `sourceBands + 1` bands, the last being alpha.

Add the helper (place it near `transparentPng`, mirroring its MEM-dataset idiom):

```scala
    /** Build a display RGB(A) GDT_Byte MEM dataset from the warped tile, matching the
     *  rio-tiler band mapping. `warpedDs` has the source bands plus a trailing binary
     *  alpha band (from `-dstalpha`). Returns the MEM dataset ready to encode.
     *
     *  Band mapping (rio-tiler parity), where N = source band count (warpedDs has N+1):
     *    N==1 -> grey replicated to R=G=B; N==2 -> band1 grey R=G=B, band2 as alpha;
     *    N==3 -> R,G,B; N==4 -> R,G,B + band4 alpha; N>=5 -> first 3 bands R,G,B.
     *  Alpha for PNG/WEBP is: the source's own alpha band (N in {2,4}) if present, else
     *  the warp's trailing -dstalpha band. JPEG drops alpha (3-band RGB).
     *  The `-scale` flags apply to RGB bands only (never alpha). */
    private[web] def toDisplayRGBA(warpedDs: Dataset, format: String, scaleFlags: String): Dataset = {
        val total = warpedDs.GetRasterCount        // = sourceBands + 1 (trailing -dstalpha)
        val n = total - 1                           // source band count
        val w = warpedDs.GetRasterXSize; val h = warpedDs.GetRasterYSize
        val wantAlpha = format.toUpperCase(Locale.ROOT) != "JPEG"
        val outBands = if (wantAlpha) 4 else 3
        val mem = gdal.GetDriverByName("MEM").Create("", w, h, outBands, gdalconstConstants.GDT_Byte)

        // Choose the source band indices that become R,G,B, and the alpha source band.
        val (rgbSrc, alphaSrc): (Seq[Int], Int) = n match {
            case 1 => (Seq(1, 1, 1), total)          // grey -> RGB; alpha = trailing -dstalpha band
            case 2 => (Seq(1, 1, 1), 2)              // band1 grey -> RGB; band2 IS alpha (rio-tiler)
            case 3 => (Seq(1, 2, 3), total)          // RGB; alpha = trailing -dstalpha band
            case 4 => (Seq(1, 2, 3), 4)              // RGB; band4 IS alpha
            case _ => (Seq(1, 2, 3), total)          // >=5: first 3 -> RGB; alpha = trailing band
        }

        // Copy R,G,B (raw byte copy; the -scale rescale is applied at the translate step
        // via scaleFlags, so we copy the ALREADY-warped-but-not-yet-rescaled bands and let
        // translate rescale them -- see Step 4 note). Copy raw bytes band-for-band.
        def copyBand(srcIdx: Int, dstIdx: Int): Unit = {
            val buf = Array.ofDim[Byte](w * h)
            warpedDs.GetRasterBand(srcIdx).ReadRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
            mem.GetRasterBand(dstIdx).WriteRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Byte, buf)
        }
        rgbSrc.zipWithIndex.foreach { case (srcIdx, i) => copyBand(srcIdx, i + 1) }
        mem.GetRasterBand(1).SetColorInterpretation(gdalconstConstants.GCI_RedBand)
        mem.GetRasterBand(2).SetColorInterpretation(gdalconstConstants.GCI_GreenBand)
        mem.GetRasterBand(3).SetColorInterpretation(gdalconstConstants.GCI_BlueBand)
        if (wantAlpha) {
            copyBand(alphaSrc, 4)
            mem.GetRasterBand(4).SetColorInterpretation(gdalconstConstants.GCI_AlphaBand)
        }
        mem.SetGeoTransform(warpedDs.GetGeoTransform())
        Option(warpedDs.GetProjection()).foreach(mem.SetProjection)
        mem
    }
```

IMPORTANT implementer note on rescale ordering: the source bands in `warpedDs` are the raw (possibly uint16) values; the `-scale` rescale must map them to Byte. Two correct orderings — pick the one that keeps `-scale` on RGB only:
  (A) copy raw source bands into the MEM ds as their native dtype is NOT possible here (MEM ds is `GDT_Byte`); so instead, keep the MEM ds `GDT_Byte` and let the TRANSLATE step apply `-scale` — but translate would then rescale the alpha band too. To avoid that, prefer ordering (B).
  (B) RECOMMENDED: create the MEM ds with the SAME dtype as the warped source bands for RGB, GDT_Byte for alpha — but mixed-dtype MEM bands are not allowed. Therefore the clean approach: apply the rescale to the RGB bands DURING the copy (compute the Byte value from the `scaleFlags` linear map in Scala), and copy the alpha band verbatim. Parse each `-scale lo hi 0 255` into (lo,hi) and map `byte = clamp(round((v-lo)/(hi-lo)*255), 0, 255)`; when `scaleFlags` is empty (uint8 auto / none) copy verbatim. Then the translate step needs NO `-scale`.

Adopt ordering (B): add a `private def rescaleByteMap(scaleFlags: String, bandIndex: Int): Option[(Double, Double)]` that returns the (lo,hi) for a given RGB output band (the flags are per-band in order), and apply it in `copyBand` for RGB bands only. The implementer MUST make the cross-tier parity test (Task 3) pass — that test is the arbiter of correct rescale behavior; iterate the mapping until the decoded distributions match within tolerance. Do NOT weaken the parity tolerance.

Now rewire `executeWithScale`'s translate section to composite first, then encode WITHOUT `-scale` (rescale already applied in the helper):

```scala
        try {
            val extension = format.toLowerCase(Locale.ROOT) match {
                case "png"  => "png"
                case "jpeg" => "jpg"
                case "webp" => "webp"
                case other  => throw new IllegalArgumentException(s"rst_tilexyz: unknown format $other")
            }
            val rgbaDs = toDisplayRGBA(warpedDs, format, scaleFlags)
            try {
                val translatePath = s"/vsimem/tilexyz_out_$uuid.$extension"
                val translateOpts = warpedOpts ++ Map("format" -> format, "extension" -> extension)
                val (resDs, _) = GDALTranslate.executeTranslate(
                  translatePath, rgbaDs, command = "gdal_translate", translateOpts)
                Try(resDs.FlushCache()); Try(resDs.delete())
                val bytes = gdal.GetMemFileBuffer(translatePath)
                gdal.Unlink(translatePath)
                if (bytes == null || bytes.isEmpty) transparentPng(size) else bytes
            } finally RasterDriver.releaseDataset(rgbaDs)
        } finally {
            RasterDriver.releaseDataset(warpedDs)
        }
```

Note: with rescale now applied in `toDisplayRGBA`, `OperatorOptions`'s PNG branch (`-ot Byte -a_nodata none$scaleSuffix`) receives no `scale` key, so `scaleSuffix` is empty — the MEM ds is already Byte RGBA and encodes directly. The `-a_nodata none` stays (harmless; alpha carries transparency now).

- [ ] **Step 4: Run the heavy unit test to verify it passes**

Run in Docker: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.web.RST_TileXYZRgbaTest' --log rgba-task1.log`
Expected: PASS — 1-band and 3-band PNG → 4 bands; JPEG → 3 bands; internal-NoData → alpha band has both 0 and 255.

- [ ] **Step 5: Run the existing heavy XYZ suite (no regression)**

Run in Docker: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.web.*' --log rgba-task1-regress.log`

KNOWN INTERACTION — `XYZRescaleParityTest` will break and must be updated in THIS task. It asserts on the `resolveScale` FLAG STRING structure (verified: `RST_TileXYZ.scala` context, that test at ~L182 `segments.length shouldBe 3` and ~L189 `parts.length shouldBe 4 // lo hi 0 255`). Those assertions still hold IF you keep `resolveScale` producing the same per-band `-scale lo hi 0 255` string — which ordering (B) does: `resolveScale` is unchanged and still called; you PARSE its output inside `toDisplayRGBA` (via `rescaleByteMap`) instead of passing it to translate. So `resolveScale`'s output (what `XYZRescaleParityTest` inspects) is unchanged and those assertions stay green. If any assertion in that suite instead inspects the OUTPUT tile's band count, update it to expect RGBA (PNG→4, JPEG→3) and add a one-line comment pointing at this plan. Do NOT delete assertions — adjust the expected value.
Expected: PASS after any such adjustment.

- [ ] **Step 6: Scalastyle + commit**

Run in Docker: `bash scripts/commands/gbx-lint-scalastyle.sh` (or `gbx:lint:scalastyle`)
Expected: clean.
Commit (message to a temp file, `git commit -F`): subject `feat(rasterx): heavy rst_tilexyz emits RGBA via warp -dstalpha + band mapping`.

---

### Task 2: WEBP alpha support detection + JPEG/WEBP wiring

Handle the WEBP-alpha capability check and confirm JPEG/WEBP paths.

**Files:**
- Modify: `src/main/scala/com/databricks/labs/gbx/rasterx/expressions/web/RST_TileXYZ.scala` (`toDisplayRGBA` / format branch)
- Test: `src/test/scala/.../web/RST_TileXYZRgbaTest.scala` (extend)

**Interfaces:**
- Consumes: `toDisplayRGBA` from Task 1.
- Produces: WEBP emits RGBA when the driver supports alpha, else RGB; JPEG always RGB.

- [ ] **Step 1: Write the failing WEBP/JPEG shape tests**

Append to `RST_TileXYZRgbaTest.scala`:

```scala
  test("WEBP output is RGBA when the driver supports alpha, else RGB") {
    val src = TileXYZTestFixtures.threeBandOverTile()
    try {
      val webp = RST_TileXYZ.execute(
        src, Map.empty, TileXYZTestFixtures.z, TileXYZTestFixtures.x,
        TileXYZTestFixtures.y, "WEBP", 256, "near", "auto")
      val (ds, p) = openBytes(webp, "webp")
      try {
        val nb = ds.GetRasterCount
        // 4 (alpha-capable build) or 3 (fallback) -- both acceptable; never the source's raw N.
        (nb == 4 || nb == 3) shouldBe true
      } finally { ds.delete(); gdal.Unlink(p) }
    } finally RasterDriver.releaseDataset(src)
  }
```

- [ ] **Step 2: Run to verify it fails or is inconclusive**

Run in Docker: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.web.RST_TileXYZRgbaTest' --log rgba-task2.log`
Expected: the WEBP test either passes (if Task 1 already produces 4-band WEBP and the driver accepts it) or fails at encode (driver rejects 4-band) — the latter is what Step 3 fixes.

- [ ] **Step 3: Add the WEBP-alpha capability check**

In `toDisplayRGBA`, gate `wantAlpha` for WEBP on driver capability. GDAL's WEBP driver advertises alpha via its metadata; probe once:

```scala
    private lazy val webpSupportsAlpha: Boolean =
        Try {
            val drv = gdal.GetDriverByName("WEBP")
            drv != null && {
                val md = drv.GetMetadataItem("DMD_CREATIONOPTIONLIST")
                md != null  // WEBP driver present with creation options => alpha-capable in practice
            }
        }.getOrElse(false)
```

Then in `toDisplayRGBA`:

```scala
        val fmtU = format.toUpperCase(Locale.ROOT)
        val wantAlpha = fmtU match {
            case "JPEG" => false
            case "WEBP" => webpSupportsAlpha
            case _      => true   // PNG
        }
```

If WEBP does not support alpha, `wantAlpha=false` → 3-band RGB output. Add a one-line log (use the existing logging idiom in the package, e.g. an `RST_ErrorHandler`/logger already imported) noting the RGB fallback. If no logger is readily available, a scaladoc note on the branch suffices — do NOT add a new logging dependency.

- [ ] **Step 4: Run the test to verify it passes**

Run in Docker: `bash scripts/commands/gbx-test-scala.sh --suite 'com.databricks.labs.gbx.rasterx.expressions.web.RST_TileXYZRgbaTest' --log rgba-task2.log`
Expected: PASS (4 or 3 band WEBP accepted).

- [ ] **Step 5: Scalastyle + commit**

Run: `bash scripts/commands/gbx-lint-scalastyle.sh`
Commit: `feat(rasterx): WEBP alpha-capability gate + RGB fallback for rst_tilexyz`.

---

### Task 3: Cross-tier RGBA parity test (decode + tolerance)

The primary gate: prove heavy now matches light on shape, alpha positions, and RGB distribution.

**Files:**
- Modify: `python/geobrix/test/pyrx/test_cross_language_xyz_parity.py` (extend — reuse its fixtures + `spark_with_jar`/`heavy_registered` + `_decode_band`)

**Interfaces:**
- Consumes (existing in that file): `spark_with_jar`, `heavy_registered`, `_make_uint16_narrow_bytes`, `_center_tile_zxy`, `xyz.render_tile`, `rx.rst_tilexyz`, `rx.rst_fromcontent`.
- Produces: two new tests exercising RGBA shape + alpha-position parity + RGB-distribution tolerance.

- [ ] **Step 1: Write the failing parity test (append to the file)**

Add a NoData-hole fixture builder and the parity tests:

```python
def _make_uint16_with_nodata_hole(width=64, height=64, lo=8000, hi=12000, nodata=0):
    """uint16 ramp with a NoData square in the center (tests internal-NoData transparency)."""
    transform = from_origin(10.0, 50.0, 0.03125, 0.03125)
    profile = dict(driver="GTiff", width=width, height=height, count=1,
                   dtype="uint16", crs="EPSG:4326", transform=transform, nodata=nodata)
    ramp = np.linspace(lo, hi, width * height).astype("uint16").reshape(height, width)
    ramp[height // 3:2 * height // 3, width // 3:2 * width // 3] = nodata  # hole
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(ramp, 1)
        return mf.read()


def _decode_rgba(png_bytes):
    """Decode to a full HxWx4 uint8 RGBA array (alpha as the 4th channel)."""
    from PIL import Image
    img = Image.open(io.BytesIO(png_bytes)).convert("RGBA")
    return np.asarray(img)


def test_light_vs_heavy_rgba_shape_and_alpha_parity(heavy_registered):
    """Both tiers emit RGBA PNGs whose transparent-pixel positions match exactly,
    and whose RGB channels agree within tolerance, on a source with internal NoData."""
    from pyspark.sql import functions as f
    from databricks.labs.gbx.rasterx import functions as rx

    spark = heavy_registered
    raster_bytes = _make_uint16_with_nodata_hole()

    with MemoryFile(raster_bytes) as mf, mf.open() as ds:
        z, x, y = _center_tile_zxy(ds)
    with MemoryFile(raster_bytes) as mf, mf.open() as ds:
        light_png = xyz.render_tile(ds, z, x, y, rescale="auto")

    df = spark.range(1).select(
        rx.rst_tilexyz(
            rx.rst_fromcontent(f.lit(raster_bytes), f.lit("GTiff")),
            z, x, y, "PNG", 256, "near", "auto").alias("bytes"))
    heavy_png = bytes(df.collect()[0]["bytes"])

    light = _decode_rgba(light_png)
    heavy = _decode_rgba(heavy_png)

    # Same dimensions and 4-band RGBA.
    assert light.shape == heavy.shape, f"shape mismatch light={light.shape} heavy={heavy.shape}"
    assert light.shape[2] == 4, "light not RGBA"
    assert heavy.shape[2] == 4, "heavy not RGBA"

    # (a) Exact alpha-position parity: the set of transparent pixels is identical.
    light_transparent = light[..., 3] == 0
    heavy_transparent = heavy[..., 3] == 0
    # There MUST be a transparent hole (the NoData square) and opaque data around it.
    assert light_transparent.any() and (~light_transparent).any(), "light has no alpha variation"
    assert heavy_transparent.any() and (~heavy_transparent).any(), "heavy has no alpha variation"
    # Allow a thin disagreement fringe from warp-resampling edges (<=1% of pixels).
    disagree = int(np.sum(light_transparent != heavy_transparent))
    frac = disagree / light_transparent.size
    print(f"\n[rgba] alpha disagreement {disagree}/{light_transparent.size} = {frac:.4f}")
    assert frac <= 0.01, (
        f"alpha-position parity: {frac:.4f} of pixels disagree on transparency "
        f"(tolerance 0.01) -- heavy and light derive different NoData masks")

    # (b) RGB distribution within tolerance over the OPAQUE (data) pixels of each tier.
    qs = (0.05, 0.25, 0.5, 0.75, 0.95)
    light_rgb = light[..., 0][~light_transparent].astype(float)
    heavy_rgb = heavy[..., 0][~heavy_transparent].astype(float)
    light_q = np.quantile(light_rgb, qs)
    heavy_q = np.quantile(heavy_rgb, qs)
    max_q_diff = float(np.max(np.abs(light_q - heavy_q)))
    print(f"[rgba] light R quantiles={light_q.round(1)} heavy={heavy_q.round(1)} maxdiff={max_q_diff:.1f}")
    assert max_q_diff <= 20, (
        f"cross-tier RGB quantile mismatch {max_q_diff:.1f} > 20 "
        f"(light {light_q.round(1)} vs heavy {heavy_q.round(1)})")
```

- [ ] **Step 2: Run to verify it fails (before the Task 1/2 JAR is staged) or passes (after)**

The heavy side needs the freshly-built JAR staged under `python/geobrix/lib/`. Build + stage first:
Run in Docker: `bash scripts/commands/gbx-docker-exec.sh 'mvn clean package -PskipScoverage -DskipTests'` then copy `target/geobrix-*-jar-with-dependencies.jar` to `python/geobrix/lib/`.
Then run: `bash scripts/commands/gbx-test-python.sh --path python/geobrix/test/pyrx/test_cross_language_xyz_parity.py --with-integration --log rgba-parity.log`
Expected before the JAR carries Task 1/2: FAIL (heavy PNG is not RGBA / alpha positions differ). After staging the new JAR: PASS.

- [ ] **Step 3: If the parity test fails on RGB distribution, fix the rescale mapping in `toDisplayRGBA`**

This is the arbiter of Task 1's rescale-during-copy math. If quantiles diverge > 20, the Scala `rescaleByteMap` is wrong — fix the (lo,hi) parse / linear map to match `resolveScale`'s `-scale lo hi 0 255` semantics exactly. Do NOT loosen the tolerance. Rebuild + re-stage the JAR and re-run.

- [ ] **Step 4: Commit**

Commit: `test(pyrx): cross-tier RGBA shape + alpha-position + RGB parity for rst_tilexyz`.

---

### Task 4: Docs — behavior change + band mapping

**Files:**
- Modify: `docs/docs/beta-release-notes.mdx` (in-flight list)
- Modify: `docs/docs/api/raster-functions.mdx` (`rst_tilexyz` entry)

**Interfaces:** none (documentation).

- [ ] **Step 1: Add the beta release note**

In `docs/docs/beta-release-notes.mdx`, under the current in-flight "What's new" list, add:

```markdown
- **Heavy XYZ tiles now emit RGBA to match the lightweight tier (behavior change).** `gbx_rst_tilexyz` and `gbx_rst_xyzpyramid` on the heavyweight tier now produce display RGB(A) web-map tiles: **PNG and WEBP → RGBA**, **JPEG → RGB**, with a binary alpha channel derived from the source's valid-data mask. Previously the heavy tier emitted the source's raw band count with no alpha, so a tile with internal NoData rendered opaque (black) where the lightweight (`pyrx`) tier renders it transparent — the two tiers now agree. Band mapping matches the lightweight tier: a single-band source becomes greyscale RGB, three bands become RGB, and an existing alpha/4th band is preserved. This changes the heavy output band count (e.g. a 1-band source now yields a 4-band RGBA PNG); consumers that read heavy `gbx_rst_tilexyz` bytes as a raw single-band raster rather than a display tile are affected. WEBP alpha requires GDAL WEBP-alpha support in the runtime; where absent, WEBP falls back to RGB. See [Raster Functions](./api/raster-functions).
```

- [ ] **Step 2: Update the `rst_tilexyz` function doc**

In `docs/docs/api/raster-functions.mdx`, find the `rst_tilexyz` (and `rst_xyzpyramid`) entry and add a note describing: display RGB(A) output, the band-mapping table (1→grey RGB, 3→RGB, 4→RGB+alpha, ≥5→first 3), binary alpha from NoData, PNG/WEBP RGBA vs JPEG RGB, and the WEBP-alpha-fallback caveat. Match the surrounding entry's prose style.

- [ ] **Step 3: Internal-vocabulary + link check**

Run: `grep -rn -iE "wave [0-9]+|wave-[0-9]+" docs/docs/api/raster-functions.mdx docs/docs/beta-release-notes.mdx` → expect no output.
Verify the `./api/raster-functions` link and any anchor resolve.

- [ ] **Step 4: Commit**

Commit: `docs: heavy XYZ RGBA output behavior change + band mapping`.

---

## Self-Review

- **Spec coverage:** §2 scope (PNG/WEBP RGBA, JPEG RGB, xyzpyramid inherits) → Tasks 1–2; §4.1 band mapping → Task 1 `toDisplayRGBA` + Task 1 tests; §4.2 binary alpha from warp mask → Task 1 `-dstalpha`; §4.4 WEBP fallback → Task 2; §5 behavior change/docs → Task 4; §6 decode+tolerance parity (exact alpha positions + tolerance RGB, internal-NoData fixture) → Task 3; §6 heavy unit tests per band count/format → Task 1–2; bench untouched → honored (no bench task). Covered.
- **Placeholder scan:** no TBD/TODO; all code shown incl. fixtures and the rescale-ordering decision (ordering B, made explicit). The one deliberately-iterative point (the exact `rescaleByteMap` linear-map math) is gated by the Task 3 parity test, which is named as the arbiter — not a placeholder but a test-driven convergence.
- **Type consistency:** `toDisplayRGBA(warpedDs, format, scaleFlags): Dataset` used consistently; `execute(...)` 9-arg signature matches the real current signature (verified against the file); `_decode_rgba`/`_decode_band` distinct helpers; fixture builders named consistently across Scala (`TileXYZTestFixtures.*`) and Python (`_make_uint16_*`).
- **Note on ordering (B):** the plan resolves the rescale-vs-alpha interaction by applying the RGB rescale during the band copy (so `-scale` never touches alpha) rather than at the translate step. This deviates from the spec's looser "then encode" sketch but satisfies the spec's hard constraint (§4.1: alpha never rescaled) and is the reviewable, correct ordering. Flagged here for the executor.
