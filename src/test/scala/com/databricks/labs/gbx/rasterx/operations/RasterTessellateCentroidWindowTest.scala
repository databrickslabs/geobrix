package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.{CoordinateTransformation, SpatialReference, osrConstants}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.util.UUID
import scala.collection.mutable

/**
  * Shape and content of heavy centroid tessellation chips ([[RasterTessellate.tessellateH3Iter]] with
  * `assignment = "centroid"`):
  *
  *  - each chip is cropped to the pixel window of its cell's assigned pixels (not the source extent);
  *  - every valid chip pixel maps back to exactly one source pixel whose centre falls in the chip's
  *    cell (an independent `H3.pointToCellID` oracle), with the source value;
  *  - a covered cell that caught no pixel (`coverage = "complete"`) is a 1x1 all-NoData chip on the
  *    clamped pixel holding the cell polygon's centroid;
  *  - a source without NoData blanks with the dtype constant (`GDAL.getNoDataConstant`), so real 0s
  *    survive and float chips carry `Float.MinValue`, not NaN;
  *  - assignment runs in row strips of at most 1M pixels; the strip height changes nothing, and
  *    assigned chips come in first-seen pixel order, before the synthetic empties;
  *  - the BNG warp lives until the iterator is exhausted or closed, and is released once, also when
  *    setup fails;
  *  - on a reprojected (UTM) raster, empties sit on the cell centroid moved into the raster CRS.
  *
  * Fixtures are synthesized in /vsimem (or MEM) so every attribute under test is controlled.
  */
class RasterTessellateCentroidWindowTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit =
        GDALManager.init(ExpressionConfig(Map.empty[String, String], new SerializableConfiguration(new Configuration())))

    private val Res = 7
    private val NoData = -9999.0

    /** EPSG:4326 over lon -122.60..-122.40, lat 37.60..37.80 at `n` x `n` pixels. */
    private def gt4326(n: Int): Array[Double] = Array(-122.60, 0.2 / n, 0.0, 37.80, 0.0, -0.2 / n)

    /** A rotated EPSG:4326 geotransform (non-zero row/column rotation terms). */
    private val GtRotated = Array(-122.60, 0.0004, 0.00005, 37.80, 0.00005, -0.0004)

    /** EPSG:32618 (UTM 18N, around New York), 30 m pixels. */
    private val GtUtm = Array(580000.0, 30.0, 0.0, 4515000.0, 0.0, -30.0)

    private def mem(path: String): String = s"/vsimem/wetest_${UUID.randomUUID().toString.replace("-", "")}_$path"

    /** Builds a single-/multi-band raster with a known per-pixel ramp; band i pixel = base*i + idx.
      * Copied from `WindowedExtractTest`, plus the CRS (`epsg`) and an in-memory MEM option (`inMem`,
      * which keeps per-band NoData; GTiff stores one NoData per dataset). */
    private def buildRamp(
        path: String,
        w: Int,
        h: Int,
        bands: Int,
        dtype: Int,
        gt: Array[Double],
        nodata: Option[Double] = None,
        epsg: Int = 4326,
        inMem: Boolean = false
    ): Dataset = {
        val drv = if (inMem) GDALManager.memDriver() else GDALManager.gtiffDriver()
        val ds = drv.Create(if (inMem) "" else path, w, h, bands, dtype)
        ds.SetGeoTransform(gt)
        val srs = new org.gdal.osr.SpatialReference()
        srs.ImportFromEPSG(epsg)
        ds.SetProjection(srs.ExportToWkt())
        srs.delete()
        var b = 1
        while (b <= bands) {
            val band = ds.GetRasterBand(b)
            val arr = new Array[Double](w * h)
            var i = 0
            while (i < w * h) { arr(i) = (b * 1000 + i).toDouble % 250.0; i += 1 }
            band.WriteRaster(0, 0, w, h, arr)
            nodata.foreach(band.SetNoDataValue)
            b += 1
        }
        ds.FlushCache()
        ds
    }

    /** Overwrites the `w` x `h` block at (`x`, `y`) of band `b` with `v`. */
    private def fillRect(ds: Dataset, b: Int, x: Int, y: Int, w: Int, h: Int, v: Double): Unit = {
        ds.GetRasterBand(b).WriteRaster(x, y, w, h, Array.fill(w * h)(v))
        ds.FlushCache()
    }

    /** Band `b` values and mask (0 = invalid) over the whole dataset. */
    private def readBand(ds: Dataset, b: Int): (Array[Double], Array[Byte]) = {
        val (w, h) = (ds.getRasterXSize, ds.getRasterYSize)
        val vals = new Array[Double](w * h)
        val mask = new Array[Byte](w * h)
        val band = ds.GetRasterBand(b)
        band.ReadRaster(0, 0, w, h, vals)
        band.GetMaskBand().ReadRaster(0, 0, w, h, mask)
        (vals, mask)
    }

    private def validCount(ds: Dataset, b: Int): Long = readBand(ds, b)._2.count(_ != 0).toLong

    private def noDataOf(ds: Dataset, b: Int): Option[Double] = {
        val nd = new Array[java.lang.Double](1)
        ds.GetRasterBand(b).GetNoDataValue(nd)
        Option(nd(0)).map(_.doubleValue())
    }

    /** Inverse of a 6-term geotransform, in GDAL's layout (pixel = i0 + X*i1 + Y*i2, line = i3 + X*i4 + Y*i5).
      * Computed independently of the implementation under test. */
    private def invert(gt: Array[Double]): Array[Double] = {
        val det = gt(1) * gt(5) - gt(2) * gt(4)
        Array(
          (gt(2) * gt(3) - gt(0) * gt(5)) / det, gt(5) / det, -gt(2) / det,
          (gt(4) * gt(0) - gt(1) * gt(3)) / det, -gt(4) / det, gt(1) / det
        )
    }

    private def epsgSR(epsg: Int): SpatialReference = {
        val sr = new SpatialReference()
        sr.ImportFromEPSG(epsg)
        sr.SetAxisMappingStrategy(osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        sr
    }

    /** Drains a centroid tessellation of `src`, handing each chip to `f` and releasing it. Returns the chip count. */
    private def forEachChip(src: Dataset, coverage: String)(f: (Long, Dataset) => Unit): Int = {
        val it = RasterTessellate.tessellateH3Iter(src, Map.empty[String, String], Res, "centroid", coverage)
        var n = 0
        try {
            while (it.hasNext) {
                val (cell, chip, _) = it.next()
                try f(cell, chip) finally RasterDriver.releaseDataset(chip)
                n += 1
            }
        } finally it.asInstanceOf[AutoCloseable].close()
        n
    }

    /** (a): every chip fits the pixel window of its cell polygon (vertices mapped through the inverse GT,
      * after `toRaster` moves them from EPSG:4326 into the raster CRS), plus 2 px of slack. */
    private def checkWindows(src: Dataset, toRaster: Option[CoordinateTransformation]): Unit = {
        val size = src.getRasterXSize
        val inv = invert(src.GetGeoTransform)
        val n = forEachChip(src, "complete") { (cell, chip) =>
            var (minC, maxC, minR, maxR) = (Double.MaxValue, -Double.MaxValue, Double.MaxValue, -Double.MaxValue)
            H3.cellIdToGeometry(cell).getCoordinates.foreach { c =>
                val (x, y) = toRaster.map { t => val p = t.TransformPoint(c.x, c.y); (p(0), p(1)) }.getOrElse((c.x, c.y))
                val col = inv(0) + x * inv(1) + y * inv(2)
                val row = inv(3) + x * inv(4) + y * inv(5)
                minC = math.min(minC, col); maxC = math.max(maxC, col)
                minR = math.min(minR, row); maxR = math.max(maxR, row)
            }
            val (winW, winH) = (maxC - minC, maxR - minR)
            val (w, h) = (chip.getRasterXSize, chip.getRasterYSize)
            assert(w <= winW + 2 && h <= winH + 2, s"cell $cell chip ${w}x$h exceeds its window ${winW}x$winH")
            assert(w < size / 4 && h < size / 4, s"cell $cell chip ${w}x$h is not window-sized (source $size)")
        }
        assert(n > 1, s"expected several chips, got $n")
    }

    /** (b): per-pixel oracle. Every valid chip pixel maps back to a distinct source pixel, carries its
      * value, and that pixel's centre (moved to lon/lat by `toLonLat`) lies in the chip's H3 cell; the
      * chips' pixels are exactly the valid source pixels and the emitted cells are the oracle's cells. */
    private def checkOracle(src: Dataset, toLonLat: Option[CoordinateTransformation]): Unit = {
        val (xSize, ySize) = (src.getRasterXSize, src.getRasterYSize)
        val gt = src.GetGeoTransform
        val inv = invert(gt)
        val (srcVals, srcMask) = readBand(src, 1)

        def oracleCell(p: Int): Long = {
            val (r, c) = (p / xSize, p % xSize)
            val x = gt(0) + (c + 0.5) * gt(1) + (r + 0.5) * gt(2)
            val y = gt(3) + (c + 0.5) * gt(4) + (r + 0.5) * gt(5)
            val (lon, lat) = toLonLat.map { t => val q = t.TransformPoint(x, y); (q(0), q(1)) }.getOrElse((x, y))
            H3.pointToCellID(lon, lat, Res)
        }

        val seen = new Array[Boolean](xSize * ySize)
        val emitted = mutable.Set.empty[Long]
        var (outside, dups, badValue, badCell) = (0, 0, 0, 0)
        var firstBad = ""
        forEachChip(src, "sparse") { (cell, chip) =>
            assert(emitted.add(cell), s"cell $cell emitted twice")
            val cgt = chip.GetGeoTransform
            val w = chip.getRasterXSize
            val (vals, mask) = readBand(chip, 1)
            var k = 0
            while (k < vals.length) {
                if (mask(k) != 0) {
                    val (i, j) = (k % w, k / w)
                    val gx = cgt(0) + (i + 0.5) * cgt(1) + (j + 0.5) * cgt(2)
                    val gy = cgt(3) + (i + 0.5) * cgt(4) + (j + 0.5) * cgt(5)
                    val sc = math.floor(inv(0) + gx * inv(1) + gy * inv(2)).toInt
                    val sr = math.floor(inv(3) + gx * inv(4) + gy * inv(5)).toInt
                    if (sc < 0 || sc >= xSize || sr < 0 || sr >= ySize) outside += 1
                    else {
                        val p = sr * xSize + sc
                        if (seen(p)) dups += 1
                        seen(p) = true
                        if (vals(k) != srcVals(p)) badValue += 1
                        if (oracleCell(p) != cell) {
                            badCell += 1
                            if (firstBad.isEmpty) firstBad = s"source ($sr,$sc) in chip of cell $cell"
                        }
                    }
                }
                k += 1
            }
        }
        assert(outside == 0, s"$outside chip pixels map outside the source")
        assert(dups == 0, s"$dups source pixels appear in more than one chip")
        assert(badValue == 0, s"$badValue chip pixels differ from their source value")
        assert(badCell == 0, s"$badCell chip pixels are not in their chip's cell, e.g. $firstBad")

        val oracleCells = mutable.Set.empty[Long]
        var missing = 0
        var p = 0
        while (p < seen.length) {
            val valid = srcMask(p) != 0
            if (valid) oracleCells += oracleCell(p)
            if (valid != seen(p)) missing += 1
            p += 1
        }
        assert(missing == 0, s"$missing pixels differ between the chips' union and the valid source pixels")
        assert(emitted.toSet == oracleCells.toSet, "emitted cell set differs from the pointToCellID oracle")
    }

    test("(a) centroid chips are cropped to their cell's pixel window (EPSG:4326 and UTM)") {
        val src = buildRamp(mem("a4326.tif"), 1024, 1024, 1, gdalconstConstants.GDT_Float32, gt4326(1024))
        try checkWindows(src, None) finally RasterDriver.releaseDataset(src)

        val (wgs, utm) = (epsgSR(4326), epsgSR(32618))
        val toUtm = new CoordinateTransformation(wgs, utm)
        val srcUtm = buildRamp(mem("autm.tif"), 512, 512, 1, gdalconstConstants.GDT_Float32, GtUtm, epsg = 32618)
        try checkWindows(srcUtm, Some(toUtm))
        finally { RasterDriver.releaseDataset(srcUtm); toUtm.delete(); wgs.delete(); utm.delete() }
    }

    test("(b) every chip pixel matches the pointToCellID oracle (EPSG:4326, UTM, rotated GT)") {
        val src = buildRamp(mem("b4326.tif"), 1024, 1024, 1, gdalconstConstants.GDT_Float32, gt4326(1024))
        try checkOracle(src, None) finally RasterDriver.releaseDataset(src)

        val (wgs, utm) = (epsgSR(4326), epsgSR(32618))
        val toLonLat = new CoordinateTransformation(utm, wgs)
        val srcUtm = buildRamp(mem("butm.tif"), 512, 512, 1, gdalconstConstants.GDT_Float32, GtUtm, epsg = 32618)
        try checkOracle(srcUtm, Some(toLonLat))
        finally { RasterDriver.releaseDataset(srcUtm); toLonLat.delete(); wgs.delete(); utm.delete() }

        val srcRot = buildRamp(mem("brot.tif"), 512, 512, 1, gdalconstConstants.GDT_Float32, GtRotated)
        try checkOracle(srcRot, None) finally RasterDriver.releaseDataset(srcRot)
    }

    test("(c) covered-but-empty cells are 1x1 NoData chips on the clamped centroid pixel") {
        val n = 1024
        val gt = gt4326(n)
        val inv = invert(gt)
        val src = buildRamp(mem("c.tif"), n, n, 1, gdalconstConstants.GDT_Float32, gt, Some(NoData))
        fillRect(src, 1, 0, 0, 400, n, NoData)
        var (empties, clamped) = (0, 0)
        try {
            forEachChip(src, "complete") { (cell, chip) =>
                val (vals, mask) = readBand(chip, 1)
                if (!mask.exists(_ != 0)) {
                    empties += 1
                    assert(chip.getRasterXSize == 1 && chip.getRasterYSize == 1,
                        s"empty cell $cell chip is ${chip.getRasterXSize}x${chip.getRasterYSize}, expected 1x1")
                    val cen = H3.cellIdToGeometry(cell).getCentroid
                    val colF = math.floor(inv(0) + cen.getX * inv(1) + cen.getY * inv(2)).toInt
                    val rowF = math.floor(inv(3) + cen.getX * inv(4) + cen.getY * inv(5)).toInt
                    val col = math.min(math.max(colF, 0), n - 1)
                    val row = math.min(math.max(rowF, 0), n - 1)
                    if (col != colF || row != rowF) clamped += 1
                    val cgt = chip.GetGeoTransform
                    val (ex, ey) = (gt(0) + col * gt(1) + row * gt(2), gt(3) + col * gt(4) + row * gt(5))
                    assert(math.abs(cgt(0) - ex) < 1e-9 && math.abs(cgt(3) - ey) < 1e-9,
                        s"empty cell $cell chip origin (${cgt(0)}, ${cgt(3)}) != centroid pixel ($col,$row) origin ($ex, $ey)")
                    assert(cgt(1) == gt(1) && cgt(2) == gt(2) && cgt(4) == gt(4) && cgt(5) == gt(5),
                        s"empty cell $cell chip pixel size/rotation differs from the source")
                    assert(vals(0) == NoData && noDataOf(chip, 1).contains(NoData), s"empty cell $cell chip is not NoData")
                }
            }
        } finally RasterDriver.releaseDataset(src)
        assert(empties > 0, "expected covered-but-empty cells over the NoData columns")
        assert(clamped > 0, "expected at least one empty cell whose centroid lies outside the raster (clamped)")
    }

    test("(d) uint16 without NoData: chip NoData is 65535 and real 0 pixels stay valid") {
        val src = buildRamp(mem("d.tif"), 256, 256, 1, gdalconstConstants.GDT_UInt16, gt4326(256))
        fillRect(src, 1, 100, 100, 16, 16, 0.0)
        val srcValid = validCount(src, 1)
        var chipValid = 0L
        val chipNoData = mutable.Set.empty[Option[Double]]
        try {
            forEachChip(src, "complete") { (_, chip) =>
                chipNoData += noDataOf(chip, 1)
                chipValid += validCount(chip, 1)
            }
        } finally RasterDriver.releaseDataset(src)
        assert(srcValid == 256L * 256L)
        assert(chipValid == srcValid, s"chips hold $chipValid valid pixels, source has $srcValid")
        assert(chipNoData == Set(Some(65535.0)), s"chip NoData values $chipNoData, expected 65535")
    }

    test("(e) float32 without NoData: chip NoData is Float.MinValue") {
        val src = buildRamp(mem("e.tif"), 256, 256, 1, gdalconstConstants.GDT_Float32, gt4326(256))
        val expected = Float.MinValue.toDouble
        val srcValid = validCount(src, 1)
        var chipValid = 0L
        try {
            forEachChip(src, "complete") { (cell, chip) =>
                assert(noDataOf(chip, 1).contains(expected), s"cell $cell chip NoData ${noDataOf(chip, 1)}, expected $expected")
                chipValid += validCount(chip, 1)
            }
        } finally RasterDriver.releaseDataset(src)
        assert(chipValid == srcValid, s"chips hold $chipValid valid pixels, source has $srcValid")
    }

    test("(f) 3 bands, band 2 NoData on the left half: per-band valid pixel counts are preserved") {
        val src = buildRamp("", 256, 256, 3, gdalconstConstants.GDT_Float32, gt4326(256), inMem = true)
        src.GetRasterBand(2).SetNoDataValue(NoData)
        fillRect(src, 2, 0, 0, 128, 256, NoData)
        val srcValid = (1 to 3).map(validCount(src, _))
        val chipValid = Array.fill(3)(0L)
        try {
            forEachChip(src, "complete") { (_, chip) =>
                (1 to 3).foreach(b => chipValid(b - 1) += validCount(chip, b))
            }
        } finally RasterDriver.releaseDataset(src)
        assert(srcValid == Seq(65536L, 32768L, 65536L), s"fixture valid counts $srcValid")
        assert(chipValid.toSeq == srcValid, s"chip valid counts ${chipValid.toSeq} != source $srcValid")
    }

    // ---------------------------------------------------------------------------------------------
    // Bounded-memory assignment: row strips, native lifetimes, empty placement under reprojection.
    // ---------------------------------------------------------------------------------------------

    /** EPSG:4326 over Great Britain (lon -1.20..-1.00, lat 52.00..52.20) at `n` x `n` pixels. */
    private def gtGb(n: Int): Array[Double] = Array(-1.20, 0.2 / n, 0.0, 52.20, 0.0, -0.2 / n)

    /** BNG resolution index 3 (1 km cells). */
    private val BngRes = 3

    /** Every chip as plain values, in emission order: (cell, width, height, geotransform, band-1 values). */
    private def snapshot(src: Dataset, coverage: String): Seq[(Long, Int, Int, Seq[Double], Seq[Double])] = {
        val out = mutable.ArrayBuffer.empty[(Long, Int, Int, Seq[Double], Seq[Double])]
        forEachChip(src, coverage) { (cell, chip) =>
            out += ((cell, chip.getRasterXSize, chip.getRasterYSize, chip.GetGeoTransform.toSeq, readBand(chip, 1)._1.toSeq))
        }
        out.toSeq
    }

    /** Runs `body` with the centroid assignment strip height forced to `rows`. */
    private def withStripRows[T](rows: Int)(body: => T): T = {
        RasterTessellate.centroidStripRowsOverride = rows
        try body finally RasterTessellate.centroidStripRowsOverride = 0
    }

    /** H3 cells of the valid pixels of band 1 of an EPSG:4326 raster, in first-seen (row-major) order. */
    private def firstSeenCells(src: Dataset): Seq[Long] = {
        val (xSize, ySize) = (src.getRasterXSize, src.getRasterYSize)
        val gt = src.GetGeoTransform
        val mask = readBand(src, 1)._2
        val seen = mutable.LinkedHashSet.empty[Long]
        var p = 0
        while (p < xSize * ySize) {
            if (mask(p) != 0) {
                val (r, c) = (p / xSize, p % xSize)
                seen += H3.pointToCellID(gt(0) + (c + 0.5) * gt(1) + (r + 0.5) * gt(2), gt(3) + (c + 0.5) * gt(4) + (r + 0.5) * gt(5), Res)
            }
            p += 1
        }
        seen.toSeq
    }

    /** Number of entries at the top of /vsimem/ (each dataset this process holds in memory). */
    private def vsimemCount(): Int = Option(gdal.ReadDir("/vsimem/")).map(_.size).getOrElse(0)

    test("(g) row strips of 64 and 100 rows give the same chips, in first-seen pixel order, as one strip") {
        val n = 1024
        val src = buildRamp(mem("g.tif"), n, n, 1, gdalconstConstants.GDT_Float32, gt4326(n), Some(NoData))
        fillRect(src, 1, 300, 200, 150, 500, NoData)
        try {
            assert(RasterTessellate.centroidStripRows(n, n, src.GetRasterBand(1).GetBlockYSize) == n,
                "a 1024 x 1024 raster should be assigned in a single strip by default")
            val one = snapshot(src, "complete")
            Seq(64, 100).foreach { rows =>
                val strips = withStripRows(rows)(snapshot(src, "complete"))
                assert(strips.length == one.length, s"$rows-row strips emit ${strips.length} chips, one strip ${one.length}")
                val firstDiff = strips.indices.find(i => strips(i) != one(i))
                assert(firstDiff.isEmpty, s"$rows-row strips differ from one strip at chip ${firstDiff.getOrElse(-1)}")
                assert(RasterTessellate.lastAssignmentMaxRows == rows, s"max strip rows ${RasterTessellate.lastAssignmentMaxRows}")
            }
            withStripRows(64)(checkOracle(src, None))
            // Assigned chips come in first-seen pixel order (row-major); synthetic empties follow them.
            val sparse = snapshot(src, "sparse").map(_._1)
            assert(sparse == firstSeenCells(src), "assigned chips are not in first-seen pixel order")
            assert(one.take(sparse.length).map(_._1) == sparse, "complete coverage must emit the assigned chips first")
            assert(one.drop(sparse.length).forall(_._5.forall(_ == NoData)), "chips after the assigned ones must be empties")
        } finally RasterDriver.releaseDataset(src)
    }

    test("(h) assignment reads at most 1M pixels per strip, in whole source blocks") {
        val n = 2048
        val src = buildRamp(mem("h.tif"), n, n, 1, gdalconstConstants.GDT_Float32, gt4326(n))
        RasterTessellate.lastAssignmentMaxRows = 0
        try {
            val chips = forEachChip(src, "sparse")((_, _) => ())
            assert(chips > 1, s"expected several chips, got $chips")
            val maxRows = RasterTessellate.lastAssignmentMaxRows
            assert(maxRows > 0 && maxRows <= 512, s"assignment read strips of $maxRows rows of a ${n}x$n raster")
        } finally RasterDriver.releaseDataset(src)

        // rows = max(1, min(ySize, 1M / xSize)), rounded down to whole blocks when the block is shorter.
        assert(RasterTessellate.centroidStripRows(2048, 2048, 1) == 512)
        assert(RasterTessellate.centroidStripRows(2048, 2048, 384) == 384)
        assert(RasterTessellate.centroidStripRows(2048, 2048, 512) == 512)
        assert(RasterTessellate.centroidStripRows(2048, 2048, 4096) == 512)
        assert(RasterTessellate.centroidStripRows(2048, 100, 1) == 100)
        assert(RasterTessellate.centroidStripRows(3000000, 10, 1) == 1)
    }

    test("(i) BNG on an EPSG:4326 raster: the warp lives until the iterator ends and is released once") {
        val src = buildRamp(mem("i.tif"), 256, 256, 1, gdalconstConstants.GDT_Float32, gtGb(256))
        try {
            val before = vsimemCount()
            val it = RasterTessellate.tessellateBngIter(src, Map.empty[String, String], BngRes, "centroid", "complete")
            assert(vsimemCount() > before, "the EPSG:27700 warp should be open while chips are produced")
            var chips = 0
            while (it.hasNext) {
                val (_, chip, _) = it.next()
                RasterDriver.releaseDataset(chip)
                chips += 1
            }
            assert(chips > 1, s"expected several chips, got $chips")
            assert(vsimemCount() == before, "the warp must be released when the iterator is exhausted")
            it.asInstanceOf[AutoCloseable].close()
            it.asInstanceOf[AutoCloseable].close()
            assert(vsimemCount() == before)

            // Abandoned after two chips (a cancelled task): close() releases the warp, and is idempotent.
            val it2 = RasterTessellate.tessellateBngIter(src, Map.empty[String, String], BngRes, "centroid", "complete")
            (1 to 2).foreach(_ => RasterDriver.releaseDataset(it2.next()._2))
            it2.asInstanceOf[AutoCloseable].close()
            it2.asInstanceOf[AutoCloseable].close()
            assert(!it2.hasNext, "a closed iterator has no more chips")
            assert(vsimemCount() == before, "close() must release the warp of an abandoned iterator")
        } finally RasterDriver.releaseDataset(src)

        // No chips at all (all NoData, sparse): the first hasNext already releases the warp.
        val empty = buildRamp(mem("i0.tif"), 64, 64, 1, gdalconstConstants.GDT_Float32, gtGb(64), Some(NoData))
        fillRect(empty, 1, 0, 0, 64, 64, NoData)
        try {
            val before = vsimemCount()
            val it = RasterTessellate.tessellateBngIter(empty, Map.empty[String, String], BngRes, "centroid", "sparse")
            assert(!it.hasNext, "an all-NoData raster has no centroid chips")
            assert(vsimemCount() == before, "an iterator with no chips must release the warp once it reports none")
        } finally RasterDriver.releaseDataset(empty)
    }

    test("(j) reprojected empties (UTM): 1x1 NoData chips on the cell centroid moved into the raster CRS") {
        val n = 512
        val src = buildRamp(mem("j.tif"), n, n, 1, gdalconstConstants.GDT_Float32, GtUtm, Some(NoData), epsg = 32618)
        // A 7.7 km NoData square: several res-7 cells (about 2.6 km across) lie wholly inside it.
        fillRect(src, 1, 128, 128, 256, 256, NoData)
        val inv = invert(GtUtm)
        val (wgs, utm) = (epsgSR(4326), epsgSR(32618))
        val toUtm = new CoordinateTransformation(wgs, utm)
        var (empties, interior) = (0, 0)
        try {
            forEachChip(src, "complete") { (cell, chip) =>
                val (vals, mask) = readBand(chip, 1)
                if (!mask.exists(_ != 0)) {
                    empties += 1
                    assert(chip.getRasterXSize == 1 && chip.getRasterYSize == 1,
                        s"empty cell $cell chip is ${chip.getRasterXSize}x${chip.getRasterYSize}, expected 1x1")
                    assert(vals(0) == NoData && noDataOf(chip, 1).contains(NoData), s"empty cell $cell chip is not NoData")
                    val cen = H3.cellIdToGeometry(cell).getCentroid
                    val p = toUtm.TransformPoint(cen.getX, cen.getY)
                    val colF = math.floor(inv(0) + p(0) * inv(1) + p(1) * inv(2)).toInt
                    val rowF = math.floor(inv(3) + p(0) * inv(4) + p(1) * inv(5)).toInt
                    val col = math.min(math.max(colF, 0), n - 1)
                    val row = math.min(math.max(rowF, 0), n - 1)
                    if (col == colF && row == rowF && col > 0 && col < n - 1 && row > 0 && row < n - 1) interior += 1
                    val cgt = chip.GetGeoTransform
                    val (ex, ey) = (GtUtm(0) + col * GtUtm(1) + row * GtUtm(2), GtUtm(3) + col * GtUtm(4) + row * GtUtm(5))
                    assert(math.abs(cgt(0) - ex) < 1e-6 && math.abs(cgt(3) - ey) < 1e-6,
                        s"empty cell $cell chip origin (${cgt(0)}, ${cgt(3)}) != centroid pixel ($col,$row) origin ($ex, $ey)")
                }
            }
        } finally { RasterDriver.releaseDataset(src); toUtm.delete(); wgs.delete(); utm.delete() }
        assert(empties > 0, "expected covered-but-empty cells over the NoData square")
        assert(interior > 0, "expected an empty cell whose centroid pixel lies strictly inside the raster (not clamped)")
    }

    test("(k) a failure during centroid setup releases the BNG warp before it propagates") {
        val src = buildRamp(mem("k.tif"), 64, 64, 1, gdalconstConstants.GDT_Float32, gtGb(64))
        val before = vsimemCount()
        RasterTessellate.centroidSetupFault = new IllegalStateException("injected setup fault")
        try {
            val e = intercept[IllegalStateException] {
                RasterTessellate.tessellateBngIter(src, Map.empty[String, String], BngRes, "centroid", "complete")
            }
            assert(e.getMessage == "injected setup fault")
            assert(vsimemCount() == before, "the EPSG:27700 warp leaked after a setup failure")
        } finally {
            RasterTessellate.centroidSetupFault = null
            RasterDriver.releaseDataset(src)
        }
    }
}
