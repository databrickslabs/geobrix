package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operator.WarpSource
import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.osr.{CoordinateTransformation, SpatialReference, osrConstants}
import org.locationtech.jts.algorithm.Orientation
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
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
  *  - on a reprojected (UTM) raster, empties sit on the cell centroid moved into the raster CRS;
  *  - covering and complete coverage keep cells against the densified reprojected footprint
  *    ([[BoundingBox.densifiedBBox]]), so a projected raster loses no edge cell; a footprint that
  *    wraps in lon/lat fails up front;
  *  - the tessellation owns its input: it is released when the iterator ends or is closed, or when
  *    setup fails (covering and centroid);
  *  - covering chips of a source without NoData carry the dtype constant, so the clip fill is NoData,
  *    and a source's own mask band still masks its pixels, also under hexagon cutlines and on a source
  *    that declares NoData, with the source's bands, type and chip windows;
  *  - the BNG warp of a source without NoData marks its fill as NoData (the dtype constant), so no
  *    valid chip pixel lies outside the source footprint and real source pixels equal to the constant
  *    are NoData too; a source's own mask band survives the warp, also when the source declares NoData;
  *  - covering builds one mask view for all of a masked source's clips.
  *
  * Fixtures are synthesized in /vsimem (or MEM) so every attribute under test is controlled. The
  * tessellation releases its input, so a test that keeps reading a fixture hands it a copy
  * ([[handOff]]).
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

    /** A copy of `src` (same driver) for a tessellation to own: the tessellation releases its input
      * when it ends, so a test that goes on reading `src` hands it this copy instead. */
    private def handOff(src: Dataset): Dataset = {
        val drv = src.GetDriver()
        drv.CreateCopy(if (drv.getShortName == "MEM") "" else mem("copy.tif"), src)
    }

    /** Drains a centroid tessellation of a copy of `src`, handing each chip to `f` and releasing it.
      * Returns the chip count. */
    private def forEachChip(src: Dataset, coverage: String)(f: (Long, Dataset) => Unit): Int = {
        val it = RasterTessellate.tessellateH3Iter(handOff(src), Map.empty[String, String], Res, "centroid", coverage)
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

    test("(i) BNG on an EPSG:4326 raster: the input and the warp live until the iterator ends, released once") {
        val src = buildRamp(mem("i.tif"), 256, 256, 1, gdalconstConstants.GDT_Float32, gtGb(256))
        try {
            val before = vsimemCount()
            val it = RasterTessellate.tessellateBngIter(handOff(src), Map.empty[String, String], BngRes, "centroid", "complete")
            assert(vsimemCount() >= before + 2, "the input and its EPSG:27700 warp should be open while chips are produced")
            var chips = 0
            while (it.hasNext) {
                val (_, chip, _) = it.next()
                RasterDriver.releaseDataset(chip)
                chips += 1
            }
            assert(chips > 1, s"expected several chips, got $chips")
            assert(vsimemCount() == before, "the input and the warp must be released when the iterator is exhausted")
            it.asInstanceOf[AutoCloseable].close()
            it.asInstanceOf[AutoCloseable].close()
            assert(vsimemCount() == before)

            // Abandoned after two chips (a cancelled task): close() releases them, and is idempotent.
            val it2 = RasterTessellate.tessellateBngIter(handOff(src), Map.empty[String, String], BngRes, "centroid", "complete")
            (1 to 2).foreach(_ => RasterDriver.releaseDataset(it2.next()._2))
            it2.asInstanceOf[AutoCloseable].close()
            it2.asInstanceOf[AutoCloseable].close()
            assert(!it2.hasNext, "a closed iterator has no more chips")
            assert(vsimemCount() == before, "close() must release the input and the warp of an abandoned iterator")
        } finally RasterDriver.releaseDataset(src)

        // No chips at all (all NoData, sparse): the first hasNext already releases them.
        val before = vsimemCount()
        val empty = buildRamp(mem("i0.tif"), 64, 64, 1, gdalconstConstants.GDT_Float32, gtGb(64), Some(NoData))
        fillRect(empty, 1, 0, 0, 64, 64, NoData)
        val it = RasterTessellate.tessellateBngIter(empty, Map.empty[String, String], BngRes, "centroid", "sparse")
        assert(!it.hasNext, "an all-NoData raster has no centroid chips")
        assert(vsimemCount() == before, "an iterator with no chips must release the input and the warp once it reports none")
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

    test("(k) a failure during centroid setup releases the input and the BNG warp before it propagates") {
        val before = vsimemCount()
        val src = buildRamp(mem("k.tif"), 64, 64, 1, gdalconstConstants.GDT_Float32, gtGb(64))
        RasterTessellate.centroidSetupFault = new IllegalStateException("injected setup fault")
        try {
            val e = intercept[IllegalStateException] {
                RasterTessellate.tessellateBngIter(src, Map.empty[String, String], BngRes, "centroid", "complete")
            }
            assert(e.getMessage == "injected setup fault")
            assert(vsimemCount() == before, "the input or the EPSG:27700 warp leaked after a setup failure")
        } finally RasterTessellate.centroidSetupFault = null
    }

    // ---------------------------------------------------------------------------------------------
    // Densified footprint (covering and complete keep-tests) and input ownership.
    // ---------------------------------------------------------------------------------------------

    /** EPSG:32618 (UTM 18N) scene: 1098 x 1098 pixels at 100 m from (499980, 4500000). */
    private val GtScene = Array(499980.0, 100.0, 0.0, 4500000.0, 0.0, -100.0)
    private val SceneSize = 1098

    /** The UTM scene, every pixel 1, NoData 0 (all pixels valid). */
    private def utmScene(): Dataset = {
        val ds = GDALManager.gtiffDriver().Create(mem("scene.tif"), SceneSize, SceneSize, 1, gdalconstConstants.GDT_Byte)
        ds.SetGeoTransform(GtScene)
        val srs = epsgSR(32618)
        ds.SetProjection(srs.ExportToWkt())
        srs.delete()
        val band = ds.GetRasterBand(1)
        band.SetNoDataValue(0.0)
        band.Fill(1.0)
        ds.FlushCache()
        ds
    }

    /** Drains an H3 tessellation of `input` (which it owns) and closes it: each emitted cell, with
      * whether its chip holds a valid pixel. */
    private def h3Cells(input: Dataset, assignment: String, coverage: String): Map[Long, Boolean] = {
        val it = RasterTessellate.tessellateH3Iter(input, Map.empty[String, String], Res, assignment, coverage)
        val out = mutable.Map.empty[Long, Boolean]
        try {
            while (it.hasNext) {
                val (cell, chip, _) = it.next()
                try {
                    assert(!out.contains(cell), s"cell $cell emitted twice")
                    out(cell) = validCount(chip, 1) > 0
                } finally RasterDriver.releaseDataset(chip)
            }
        } finally it.asInstanceOf[AutoCloseable].close()
        out.toMap
    }

    test("(l) UTM scene: covering keeps every cell that holds valid pixels; complete covers the same cells") {
        val covering = h3Cells(utmScene(), "covering", "complete")
        val centroid = h3Cells(utmScene(), "centroid", "sparse").keySet
        val complete = h3Cells(utmScene(), "centroid", "complete").keySet
        val coveringValid = covering.collect { case (cell, true) => cell }.toSet
        info(s"H3 res $Res: covering ${covering.size} cells (${coveringValid.size} with valid pixels), " +
          s"centroid+sparse ${centroid.size}, centroid+complete ${complete.size}")
        // Every cell holding a pixel centre holds valid pixels, so covering must keep it.
        val dropped = centroid -- covering.keySet
        assert(dropped.isEmpty, s"covering dropped ${dropped.size} cells that hold pixel centres, e.g. ${dropped.take(5)}")
        // Every pixel is valid, so a cell kept against the true footprint holds a valid pixel: an all-NoData
        // covering chip means the footprint reached past the raster edge. (Covering may still hold cells
        // with no pixel centre: its clip takes every pixel a cell touches.)
        val empty = covering.keySet -- coveringValid
        assert(empty.isEmpty, s"${empty.size} covering chips hold no valid pixel, e.g. ${empty.take(5)}")
        assert(covering.keySet == complete,
          s"covering and centroid+complete differ: covering-only ${(covering.keySet -- complete).take(5)}, " +
            s"complete-only ${(complete -- covering.keySet).take(5)}")
    }

    test("(m) densifiedBBox follows the reprojected raster edge; without a transform it is the four corners") {
        val src = utmScene()
        val (wgs, utm) = (epsgSR(4326), epsgSR(32618))
        val toLonLat = new CoordinateTransformation(utm, wgs)
        val factory = new GeometryFactory()
        try {
            val fp = BoundingBox.densifiedBBox(src, GDAL.WSG84)
            assert(fp.getCoordinates.length == 4 * 20 + 1, s"expected 21 points per edge, got ${fp.getCoordinates.length} coordinates")
            assert(Orientation.isCCW(fp.getCoordinates), "the footprint ring must be counter-clockwise")
            assert(fp.getSRID == 4326 && fp.isValid, s"footprint SRID ${fp.getSRID}, valid ${fp.isValid}")
            // 101 points per edge of the pixel-grid rectangle, corners included, moved to lon/lat.
            val n = SceneSize.toDouble
            val samples = (0 to 100).map(_ / 100.0).flatMap(f => Seq((f * n, 0.0), (n, f * n), ((1 - f) * n, n), (0.0, (1 - f) * n)))
            val points = samples.map { case (col, row) =>
                val p = toLonLat.TransformPoint(GtScene(0) + col * GtScene(1), GtScene(3) + row * GtScene(5))
                factory.createPoint(new Coordinate(p(0), p(1)))
            }
            val outside = points.map(fp.distance).max
            val chordOutside = points.map(BoundingBox.bbox(src, GDAL.WSG84).distance).max
            info(f"farthest edge point outside: densified $outside%.3e deg, 4-corner chord $chordOutside%.3e deg")
            // A 100 m pixel is about 9e-4 degrees; the densified ring stays within ~1% of one.
            assert(outside < 1e-5, s"an edge point lies $outside degrees outside the densified footprint")
            assert(chordOutside > 1e-4, s"the 4-corner chord should miss the curved edge, but misses by only $chordOutside")
        } finally { RasterDriver.releaseDataset(src); toLonLat.delete(); wgs.delete(); utm.delete() }

        // Same CRS (EPSG:4326 under H3): no transform, the straight edges stay the four corners.
        val src4326 = buildRamp(mem("m4326.tif"), 64, 64, 1, gdalconstConstants.GDT_Float32, gt4326(64))
        try {
            val fp = BoundingBox.densifiedBBox(src4326, GDAL.WSG84)
            assert(fp.getCoordinates.length == 5, s"expected the 4 corners, got ${fp.getCoordinates.length} coordinates")
            assert(fp.equalsExact(BoundingBox.bbox(src4326, GDAL.WSG84)), "an untransformed footprint must equal bbox")
        } finally RasterDriver.releaseDataset(src4326)
    }

    test("(n) a footprint that crosses the antimeridian fails before any chip, naming the raster CRS") {
        // UTM zone 1N, 100 km wide around 50N: its west part lies beyond -180, so longitudes wrap.
        def wrapScene(): Dataset = {
            val ds = GDALManager.gtiffDriver().Create(mem("wrap.tif"), 100, 100, 1, gdalconstConstants.GDT_Byte)
            ds.SetGeoTransform(Array(200000.0, 1000.0, 0.0, 5600000.0, 0.0, -1000.0))
            val srs = epsgSR(32601)
            ds.SetProjection(srs.ExportToWkt())
            srs.delete()
            ds.GetRasterBand(1).Fill(1.0)
            ds.FlushCache()
            ds
        }
        val expected = "rst_h3_tessellate: the raster footprint crosses the antimeridian or encloses a pole in EPSG:32601; " +
            "reproject it (e.g. to EPSG:4326) or split it before tessellating"
        val before = vsimemCount()
        Seq(("covering", "complete"), ("covering", "sparse"), ("centroid", "complete")).foreach { case (assignment, coverage) =>
            val e = intercept[IllegalArgumentException] {
                RasterTessellate.tessellateH3Iter(wrapScene(), Map.empty[String, String], Res, assignment, coverage)
            }
            assert(e.getMessage == expected, s"$assignment+$coverage: ${e.getMessage}")
            assert(vsimemCount() == before, s"$assignment+$coverage: the input leaked after the footprint error")
        }
    }

    test("(o) the tessellation releases its input: /vsimem is back to its count after a drain and close") {
        // The input arrives as the SQL functions open a tile: a /vsimem copy of the tile bytes.
        def asTile(ds: Dataset): Dataset = RasterDriver.readFromBytes(RasterDriver.writeToBytes(ds, Map.empty), Map("driver" -> "GTiff"))
        def drain(it: Iterator[(Any, Dataset, Map[String, String])]): Int = {
            var chips = 0
            while (it.hasNext) { RasterDriver.releaseDataset(it.next()._2); chips += 1 }
            it.asInstanceOf[AutoCloseable].close()
            chips
        }
        val src4326 = buildRamp(mem("o4326.tif"), 256, 256, 1, gdalconstConstants.GDT_Float32, gt4326(256))
        val srcGb = buildRamp(mem("ogb.tif"), 256, 256, 1, gdalconstConstants.GDT_Float32, gtGb(256))
        try {
            val before = vsimemCount()
            val centroidChips = drain(RasterTessellate.tessellateH3Iter(asTile(src4326), Map.empty[String, String], Res, "centroid", "complete"))
            assert(centroidChips > 1, s"expected several centroid chips, got $centroidChips")
            assert(vsimemCount() == before, "centroid (H3, EPSG:4326) left its input in /vsimem")
            val coveringChips = drain(RasterTessellate.tessellateBngIter(asTile(srcGb), Map.empty[String, String], BngRes, "covering", "complete"))
            assert(coveringChips > 1, s"expected several covering chips, got $coveringChips")
            assert(vsimemCount() == before, "covering (BNG from EPSG:4326) left its input or its warp in /vsimem")
            // Closed after hasNext fetched a chip that next() never handed out (a cancelled task).
            val it = RasterTessellate.tessellateH3Iter(asTile(src4326), Map.empty[String, String], Res, "covering", "complete")
            assert(it.hasNext, "expected a covering chip")
            it.asInstanceOf[AutoCloseable].close()
            assert(vsimemCount() == before, "close() left the fetched covering chip or the input in /vsimem")
        } finally { RasterDriver.releaseDataset(src4326); RasterDriver.releaseDataset(srcGb) }
    }

    test("(p) a failure during covering setup releases the input and the BNG warp before it propagates") {
        val before = vsimemCount()
        val src = buildRamp(mem("p.tif"), 64, 64, 1, gdalconstConstants.GDT_Float32, gtGb(64))
        RasterTessellate.coveringSetupFault = new IllegalStateException("injected setup fault")
        try {
            val e = intercept[IllegalStateException] {
                RasterTessellate.tessellateBngIter(src, Map.empty[String, String], BngRes, "covering", "complete")
            }
            assert(e.getMessage == "injected setup fault")
            assert(vsimemCount() == before, "the input or the EPSG:27700 warp leaked after a covering setup failure")
        } finally RasterTessellate.coveringSetupFault = null
    }

    test("(q) covering on a source without NoData: the clip fill and real 65535 pixels are NoData, 0s stay valid") {
        val src = buildRamp(mem("q.tif"), 64, 64, 1, gdalconstConstants.GDT_UInt16, gt4326(64))
        fillRect(src, 1, 0, 30, 64, 2, 65535.0) // real pixels equal to the uint16 fallback
        val (xSize, ySize) = (src.getRasterXSize, src.getRasterYSize)
        val inv = invert(src.GetGeoTransform)
        val (srcVals, _) = readBand(src, 1)
        val chipNoData = mutable.Set.empty[Option[Double]]
        var (chips, filled, outside, badValue, zeros, fallbackValid) = (0, 0L, 0, 0, 0, 0)
        val it = RasterTessellate.tessellateH3Iter(handOff(src), Map.empty[String, String], Res, "covering", "complete")
        try {
            while (it.hasNext) {
                val (_, chip, _) = it.next()
                try {
                    chips += 1
                    chipNoData += noDataOf(chip, 1)
                    val cgt = chip.GetGeoTransform
                    val w = chip.getRasterXSize
                    val (vals, mask) = readBand(chip, 1)
                    var k = 0
                    while (k < vals.length) {
                        if (mask(k) == 0) filled += 1
                        else {
                            val (i, j) = (k % w, k / w)
                            val gx = cgt(0) + (i + 0.5) * cgt(1) + (j + 0.5) * cgt(2)
                            val gy = cgt(3) + (i + 0.5) * cgt(4) + (j + 0.5) * cgt(5)
                            val sc = math.floor(inv(0) + gx * inv(1) + gy * inv(2)).toInt
                            val sr = math.floor(inv(3) + gx * inv(4) + gy * inv(5)).toInt
                            if (sc < 0 || sc >= xSize || sr < 0 || sr >= ySize) outside += 1
                            else if (srcVals(sr * xSize + sc) == 65535.0) fallbackValid += 1
                            else if (vals(k) != srcVals(sr * xSize + sc)) badValue += 1
                            else if (vals(k) == 0.0) zeros += 1
                        }
                        k += 1
                    }
                } finally RasterDriver.releaseDataset(chip)
            }
        } finally {
            it.asInstanceOf[AutoCloseable].close()
            RasterDriver.releaseDataset(src)
        }
        assert(chips > 1, s"expected several covering chips, got $chips")
        assert(chipNoData == Set(Some(65535.0)), s"chip NoData values $chipNoData, expected 65535")
        assert(filled > 0, "fixture must clip some pixels outside a hexagon")
        assert(outside == 0, s"$outside valid chip pixels lie outside the source (clip fill read as data)")
        assert(badValue == 0, s"$badValue valid chip pixels differ from the source (clip fill read as data)")
        assert(fallbackValid == 0, s"$fallbackValid source pixels equal to 65535 stayed valid (gdalwarp moved them)")
        assert(zeros > 0, "the source's real 0 pixels must stay valid")
    }

    test("(r) covering on a source without NoData but with its own mask: masked pixels stay NoData") {
        // 16x16 EPSG:4326 at 0.01 deg from lon -0.13: quadbin res-8 cells split it on a pixel edge (lon 0).
        // Rows 2-3 are masked; row 6 holds real 65535s (the uint16 fallback), which make gdalwarp drop the
        // mask once a source NoData is set, and which must be NoData in the chips, as on an unmasked source.
        val gt = Array(-0.13, 0.01, 0.0, 51.55, 0.0, -0.01)
        val src = buildRamp(mem("r.tif"), 16, 16, 1, gdalconstConstants.GDT_UInt16, gt)
        fillRect(src, 1, 0, 6, 16, 1, 65535.0)
        src.CreateMaskBand(gdalconstConstants.GMF_PER_DATASET)
        val srcMask = Array.tabulate[Byte](16 * 16)(i => if (i / 16 == 2 || i / 16 == 3) 0.toByte else 255.toByte)
        src.GetRasterBand(1).GetMaskBand().WriteRaster(0, 0, 16, 16, srcMask)
        src.FlushCache()
        val (xSize, ySize) = (src.getRasterXSize, src.getRasterYSize)
        val inv = invert(src.GetGeoTransform)
        val (srcVals, _) = readBand(src, 1)
        val input = handOff(src)
        assert(input.GetRasterBand(1).GetMaskFlags() == gdalconstConstants.GMF_PER_DATASET, "fixture needs a per-dataset mask")
        var (chips, maskedValid, badValue, validSeen, fallbackValid) = (0, 0, 0, 0, 0)
        val it = RasterTessellate.tessellateQuadbinIter(input, Map.empty[String, String], 8, "covering", "complete")
        try {
            while (it.hasNext) {
                val (_, chip, _) = it.next()
                try {
                    chips += 1
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
                            if (sc >= 0 && sc < xSize && sr >= 0 && sr < ySize) {
                                val p = sr * xSize + sc
                                if (srcMask(p) == 0) maskedValid += 1
                                else if (srcVals(p) == 65535.0) fallbackValid += 1
                                else if (vals(k) != srcVals(p)) badValue += 1
                                else validSeen += 1
                            }
                        }
                        k += 1
                    }
                } finally RasterDriver.releaseDataset(chip)
            }
        } finally {
            it.asInstanceOf[AutoCloseable].close()
            RasterDriver.releaseDataset(src)
        }
        assert(chips > 1, s"expected several covering chips, got $chips")
        assert(maskedValid == 0, s"$maskedValid masked source pixels became valid in the covering chips")
        assert(fallbackValid == 0, s"$fallbackValid source pixels equal to 65535 stayed valid (gdalwarp moved them)")
        assert(badValue == 0, s"$badValue valid chip pixels differ from the source")
        assert(validSeen > 0, "the unmasked pixels must stay valid")
    }

    /** A 64x64 uint16 EPSG:4326 ramp with `bands` bands, the source NoData `declared` (or none) and, when
      * `masked`, an internal per-dataset mask that hides rows 10-29 of columns 12-43. Row 40 holds real
      * 65535s (the uint16 fallback); with `declared`, the ramp's pixels equal to it are real NoData. Returns
      * the copy a tessellation owns (the GTiff reopened as tiles are), its values per band and the mask. */
    private def maskedCoveringSource(
        bands: Int,
        declared: Option[Double],
        masked: Boolean
    ): (Dataset, Seq[Array[Double]], Array[Byte]) = {
        val n = 64
        val src = buildRamp(mem(s"w_${bands}_${declared.isDefined}_$masked.tif"), n, n, bands, gdalconstConstants.GDT_UInt16, gt4326(n))
        (1 to bands).foreach(b => fillRect(src, b, 0, 40, n, 1, 65535.0))
        declared.foreach(nd => (1 to bands).foreach(b => src.GetRasterBand(b).SetNoDataValue(nd)))
        val srcMask = Array.tabulate[Byte](n * n) { i =>
            val (r, c) = (i / n, i % n)
            if (masked && r >= 10 && r <= 29 && c >= 12 && c <= 43) 0.toByte else 255.toByte
        }
        if (masked) {
            src.CreateMaskBand(gdalconstConstants.GMF_PER_DATASET)
            src.GetRasterBand(1).GetMaskBand().WriteRaster(0, 0, n, n, srcMask)
        }
        src.FlushCache()
        val vals = (1 to bands).map(b => readBand(src, b)._1)
        val input = handOff(src)
        RasterDriver.releaseDataset(src)
        val flags = input.GetRasterBand(1).GetMaskFlags()
        if (masked) assert(flags == gdalconstConstants.GMF_PER_DATASET, s"fixture needs a per-dataset mask, flags $flags")
        else assert((flags & gdalconstConstants.GMF_PER_DATASET) == 0, s"twin fixture must have no mask band, flags $flags")
        (input, vals, srcMask)
    }

    /** One covering chip, as compared below: band count, band data types, NoData per band, size and
      * geotransform. */
    private def chipLayout(chip: Dataset): (Int, Seq[Int], Seq[Option[Double]], Int, Int, Seq[Double]) = {
        val bands = 1 to chip.GetRasterCount()
        (
          chip.GetRasterCount(),
          bands.map(b => chip.GetRasterBand(b).getDataType),
          bands.map(b => noDataOf(chip, b)),
          chip.getRasterXSize,
          chip.getRasterYSize,
          chip.GetGeoTransform.toSeq
        )
    }

    for ((bands, declared) <- Seq((1, None), (3, None), (1, Some(7.0)))) {
        val what = declared.fold("without NoData")(nd => s"with NoData ${nd.toInt}")
        test(s"(w) h3 covering of a $bands-band source $what and its own mask: masked pixels stay NoData under hexagon cutlines") {
            // H3 cells are hexagons, which gdalwarp rasterizes all-touched as cutlines; it then ignored a
            // per-dataset source mask (and a source NoData makes it ignore the mask under any cutline).
            val n = 64
            val expectedNoData = declared.getOrElse(65535.0)
            val inv = invert(gt4326(n))
            val (input, srcVals, srcMask) = maskedCoveringSource(bands, declared, masked = true)
            val (twin, _, _) = maskedCoveringSource(bands, declared, masked = false)
            def layouts(ds: Dataset)(each: Dataset => Unit): Map[Long, (Int, Seq[Int], Seq[Option[Double]], Int, Int, Seq[Double])] = {
                val out = mutable.Map.empty[Long, (Int, Seq[Int], Seq[Option[Double]], Int, Int, Seq[Double])]
                val it = RasterTessellate.tessellateH3Iter(ds, Map.empty[String, String], Res, "covering", "complete")
                try {
                    while (it.hasNext) {
                        val (cell, chip, _) = it.next()
                        try {
                            out(cell) = chipLayout(chip)
                            each(chip)
                        } finally RasterDriver.releaseDataset(chip)
                    }
                } finally it.asInstanceOf[AutoCloseable].close()
                out.toMap
            }
            var (maskedValid, noDataValid, badValue, outside, validSeen) = (0L, 0L, 0L, 0L, 0L)
            val masked = layouts(input) { chip =>
                val cgt = chip.GetGeoTransform
                val w = chip.getRasterXSize
                (1 to chip.GetRasterCount()).foreach { b =>
                    val (vals, mask) = readBand(chip, b)
                    var k = 0
                    while (k < vals.length) {
                        if (mask(k) != 0) {
                            val (i, j) = (k % w, k / w)
                            val gx = cgt(0) + (i + 0.5) * cgt(1) + (j + 0.5) * cgt(2)
                            val gy = cgt(3) + (i + 0.5) * cgt(4) + (j + 0.5) * cgt(5)
                            val sc = math.floor(inv(0) + gx * inv(1) + gy * inv(2)).toInt
                            val sr = math.floor(inv(3) + gx * inv(4) + gy * inv(5)).toInt
                            if (sc < 0 || sc >= n || sr < 0 || sr >= n) outside += 1
                            else {
                                val p = sr * n + sc
                                if (srcMask(p) == 0) maskedValid += 1
                                else if (srcVals(b - 1)(p) == expectedNoData) noDataValid += 1
                                else if (vals(k) != srcVals(b - 1)(p)) badValue += 1
                                else validSeen += 1
                            }
                        }
                        k += 1
                    }
                }
            }
            val unmasked = layouts(twin)(_ => ())
            assert(masked.size > 10, s"expected many hexagon chips, got ${masked.size}")
            assert(maskedValid == 0, s"$maskedValid masked source pixels became valid in the covering chips")
            assert(noDataValid == 0, s"$noDataValid source pixels equal to the NoData $expectedNoData stayed valid")
            assert(outside == 0, s"$outside valid chip pixels lie outside the source (clip fill read as data)")
            assert(badValue == 0, s"$badValue valid chip pixels differ from the source")
            assert(validSeen > 0, "the unmasked pixels must stay valid")
            val expectedLayout = (bands, Seq.fill(bands)(gdalconstConstants.GDT_UInt16), Seq.fill(bands)(Some(expectedNoData)))
            val wrongLayout = masked.collect { case (c, l) if (l._1, l._2, l._3) != expectedLayout => c -> l }
            assert(wrongLayout.isEmpty, s"chips must keep the source's bands, type and NoData $expectedLayout: ${wrongLayout.take(3)}")
            // the mask changes which pixels are valid, never which cells are emitted or a chip's window
            assert(masked == unmasked, "the masked source's chips differ in cells, size or geotransform from its unmasked twin")
        }
    }

    test("(x) h3 covering of a masked source builds one mask view for all its cells' clips") {
        val (input, _, _) = maskedCoveringSource(1, None, masked = true)
        val built = WarpSource.viewsBuilt.get()
        var chips = 0
        val it = RasterTessellate.tessellateH3Iter(input, Map.empty[String, String], Res, "covering", "complete")
        try {
            while (it.hasNext) {
                val (_, chip, _) = it.next()
                chips += 1
                RasterDriver.releaseDataset(chip)
            }
        } finally it.asInstanceOf[AutoCloseable].close()
        assert(chips > 10, s"expected many hexagon chips, got $chips")
        assert(WarpSource.viewsBuilt.get() - built == 1, s"${WarpSource.viewsBuilt.get() - built} mask views for $chips chips")
    }

    /** Valid chip pixels of a BNG tessellation of the uint16 source below, split into `(valid, invented,
      * fallbackValid, maskedValid, chipNoData, declaredValid)`: `invented` counts valid pixels that do not map
      * back to the source pixel their value names (a 0, or a pixel outside the footprint); `fallbackValid`
      * counts valid pixels equal to the dtype constant, or to it moved by one (65534: the source holds none);
      * `maskedValid` counts valid pixels of a source pixel its mask hides; `declaredValid` counts valid pixels
      * equal to the `declared` NoData. Source pixel `p` holds `p + 1`; there is no 0. Row 3 holds 65535 (the
      * uint16 fallback); when `masked`, rows 10-11 are hidden by a per-dataset mask band. `declared` sets the
      * source's own NoData instead of leaving it undeclared. */
    private def bngWarpFill(
        assignment: String,
        coverage: String,
        masked: Boolean = false,
        declared: Option[Double] = None
    ): (Long, Long, Long, Long, Set[Option[Double]], Long) = {
        val n = 40
        // Norfolk, far from the 27700 central meridian (-2): the warp rotates the grid ~2.4 degrees, so the
        // EPSG:27700 raster has corners outside the source footprint.
        val gt = Array(1.00, 0.2 / n, 0.0, 52.60, 0.0, -0.2 / n)
        val src = buildRamp(mem(s"bngfill_$assignment$coverage$masked.tif"), n, n, 1, gdalconstConstants.GDT_UInt16, gt)
        src.GetRasterBand(1).WriteRaster(0, 0, n, n, Array.tabulate(n * n)(p => (p + 1).toDouble))
        if (masked) {
            src.CreateMaskBand(gdalconstConstants.GMF_PER_DATASET)
            val srcMask = Array.tabulate[Byte](n * n)(i => if (i / n == 10 || i / n == 11) 0.toByte else 255.toByte)
            src.GetRasterBand(1).GetMaskBand().WriteRaster(0, 0, n, n, srcMask)
            src.FlushCache()
            assert(src.GetRasterBand(1).GetMaskFlags() == gdalconstConstants.GMF_PER_DATASET, "fixture needs a per-dataset mask")
        }
        fillRect(src, 1, 0, 3, n, 1, 65535.0)
        declared.foreach(src.GetRasterBand(1).SetNoDataValue)
        assert(noDataOf(src, 1) == declared, s"fixture NoData ${noDataOf(src, 1)}, expected $declared")
        val inv = invert(gt)
        val (bng, wgs) = (epsgSR(27700), epsgSR(4326))
        val toLonLat = new CoordinateTransformation(bng, wgs)
        val chipNoData = mutable.Set.empty[Option[Double]]
        var (valid, invented, fallbackValid, maskedValid, declaredValid) = (0L, 0L, 0L, 0L, 0L)
        val it = RasterTessellate.tessellateBngIter(handOff(src), Map.empty[String, String], BngRes, assignment, coverage)
        try {
            while (it.hasNext) {
                val (_, chip, _) = it.next()
                try {
                    chipNoData += noDataOf(chip, 1)
                    val cgt = chip.GetGeoTransform
                    val w = chip.getRasterXSize
                    val (vals, mask) = readBand(chip, 1)
                    var k = 0
                    while (k < vals.length) {
                        if (mask(k) != 0) {
                            valid += 1
                            val (i, j) = (k % w, k / w)
                            val e = cgt(0) + (i + 0.5) * cgt(1) + (j + 0.5) * cgt(2)
                            val nn = cgt(3) + (i + 0.5) * cgt(4) + (j + 0.5) * cgt(5)
                            val ll = toLonLat.TransformPoint(e, nn)
                            val sc = inv(0) + ll(0) * inv(1) + ll(1) * inv(2)
                            val sr = inv(3) + ll(0) * inv(4) + ll(1) * inv(5)
                            val v = vals(k).toLong
                            if (v == 65535L || v == 65534L) fallbackValid += 1
                            else if (v < 1 || v > n * n) invented += 1
                            else {
                                // the value names its source pixel; the chip pixel centre must fall on it
                                // (1.5 px of slack for gdalwarp's approximate transformer at the edge)
                                val (er, ec) = (((v - 1) / n).toInt, ((v - 1) % n).toInt)
                                if (math.abs(sc - (ec + 0.5)) > 1.5 || math.abs(sr - (er + 0.5)) > 1.5) invented += 1
                                else if (masked && (er == 10 || er == 11)) maskedValid += 1
                                if (declared.contains(vals(k))) declaredValid += 1
                            }
                        }
                        k += 1
                    }
                } finally RasterDriver.releaseDataset(chip)
            }
        } finally {
            it.asInstanceOf[AutoCloseable].close()
            toLonLat.delete(); bng.delete(); wgs.delete()
            RasterDriver.releaseDataset(src)
        }
        (valid, invented, fallbackValid, maskedValid, chipNoData.toSet, declaredValid)
    }

    for ((assignment, coverage) <- Seq(("centroid", "sparse"), ("centroid", "complete"), ("covering", "sparse"))) {
        test(s"(s) BNG $assignment+$coverage on an EPSG:4326 source without NoData: the warp fill is NoData, not valid zeros") {
            val (valid, invented, fallbackValid, _, chipNoData, _) = bngWarpFill(assignment, coverage)
            assert(valid > 0, "the chips hold no valid pixel")
            assert(invented == 0, s"$invented of $valid valid chip pixels are warp fill (0) or off the source pixel they name")
            assert(fallbackValid == 0, s"$fallbackValid source pixels equal to 65535 stayed valid")
            assert(chipNoData == Set(Some(65535.0)), s"chip NoData values $chipNoData, expected 65535")
        }
    }

    for (assignment <- Seq("centroid", "covering")) {
        test(s"(t) BNG $assignment on a source without NoData but with its own mask: masked pixels, the warp fill and real 65535s are NoData") {
            val (valid, invented, fallbackValid, maskedValid, chipNoData, _) = bngWarpFill(assignment, "sparse", masked = true)
            assert(valid > 0, "the chips hold no valid pixel")
            assert(invented == 0, s"$invented of $valid valid chip pixels are warp fill (0) or off the source pixel they name")
            assert(maskedValid == 0, s"$maskedValid masked source pixels became valid in the chips")
            assert(fallbackValid == 0, s"$fallbackValid source pixels equal to 65535 stayed valid (or moved to 65534)")
            assert(chipNoData == Set(Some(65535.0)), s"chip NoData values $chipNoData, expected 65535")
        }

        test(s"(t2) BNG $assignment on a source with NoData 7 and its own mask: masked pixels and the 7s are NoData, 65535 stays real") {
            val (valid, invented, fallbackValid, maskedValid, chipNoData, declaredValid) =
                bngWarpFill(assignment, "sparse", masked = true, declared = Some(7.0))
            assert(valid > 0, "the chips hold no valid pixel")
            assert(invented == 0, s"$invented of $valid valid chip pixels are warp fill (0) or off the source pixel they name")
            assert(maskedValid == 0, s"$maskedValid masked source pixels became valid in the chips")
            assert(declaredValid == 0, s"$declaredValid source pixels equal to the NoData 7 are valid")
            assert(fallbackValid > 0, "65535 is a real value when the source NoData is 7: row 3 must stay valid")
            assert(chipNoData == Set(Some(7.0)), s"chip NoData values $chipNoData, expected the source's 7")
        }
    }

    test("(u) BNG on a source with its own NoData: the warp keeps it, so 65535 stays a real value") {
        val (valid, invented, fallbackValid, _, chipNoData, _) = bngWarpFill("centroid", "sparse", declared = Some(0.0))
        assert(valid > 0, "the chips hold no valid pixel")
        assert(invented == 0, s"$invented of $valid valid chip pixels are warp fill (0) or off the source pixel they name")
        assert(fallbackValid > 0, "65535 is a real value when the source NoData is 0: row 3 must stay valid")
        assert(chipNoData == Set(Some(0.0)), s"chip NoData values $chipNoData, expected the source's 0")
    }

    /** A /vsimem 40 x 40 uint8 RGBA EPSG:4326 GTiff over Norfolk, no NoData: non-zero RGB and an opaque alpha,
      * except rows 4-8 (alpha 0, RGB still non-zero) and row 20 (real R = G = B = 0 under an opaque alpha).
      * Returns the dataset, its size and its geotransform. */
    private def rgbaSource(name: String): (Dataset, Int, Array[Double]) = {
        val n = 40
        val gt = Array(1.00, 0.2 / n, 0.0, 52.60, 0.0, -0.2 / n)
        val drv = GDALManager.gtiffDriver()
        val src = drv.Create(mem(name), n, n, 4, gdalconstConstants.GDT_Byte, Array("PHOTOMETRIC=RGB", "ALPHA=YES"))
        src.SetGeoTransform(gt)
        val srs = epsgSR(4326)
        src.SetProjection(srs.ExportToWkt())
        srs.delete()
        (1 to 4).foreach { b =>
            val arr = Array.tabulate(n * n) { p =>
                val (r, mult) = (p / n, Seq(1, 3, 7)(math.min(b, 3) - 1))
                if (b == 4) (if (r >= 4 && r <= 8) 0.0 else 255.0)
                else if (r == 20) 0.0
                else (p * mult % 199 + 1).toDouble
            }
            src.GetRasterBand(b).WriteRaster(0, 0, n, n, arr)
        }
        src.FlushCache()
        assert(src.GetRasterBand(4).GetColorInterpretation == gdalconstConstants.GCI_AlphaBand, "fixture needs an alpha band")
        assert((src.GetRasterBand(1).GetMaskFlags() & gdalconstConstants.GMF_ALPHA) != 0, "fixture needs an alpha mask")
        (src, n, gt)
    }

    /** Valid chip pixels (valid in any band) of a BNG tessellation of an RGBA uint8 EPSG:4326 source, as
      * `(chips, valid, outside, transparent, zeroRgb, zeroRgbOff)`: `outside` lies off the source footprint
      * (warp fill), `transparent` sits on the source's transparent rows, `zeroRgb` has R = G = B = 0 and
      * `zeroRgbOff` is one of those that is not on the source's opaque all-zero row. Source pixel `p` has
      * non-zero RGB and an opaque alpha, except rows 4-8 (alpha 0, RGB still non-zero) and row 20 (real
      * R = G = B = 0 under an opaque alpha). Positions are judged on the fractional source pixel, with the
      * edges of a row left out (gdalwarp's approximate transformer moves a centre by a fraction of a pixel). */
    private def bngRgbaChips(assignment: String): (Int, Long, Long, Long, Long, Long) = {
        val (src, n, gt) = rgbaSource(s"bngrgba_$assignment.tif")
        val inv = invert(gt)
        val (bng, wgs) = (epsgSR(27700), epsgSR(4326))
        val toLonLat = new CoordinateTransformation(bng, wgs)
        var (chips, valid, outside, transparent, zeroRgb, zeroRgbOff) = (0, 0L, 0L, 0L, 0L, 0L)
        val it = RasterTessellate.tessellateBngIter(handOff(src), Map.empty[String, String], BngRes, assignment, "sparse")
        try {
            while (it.hasNext) {
                val (_, chip, _) = it.next()
                try {
                    chips += 1
                    val cgt = chip.GetGeoTransform
                    val w = chip.getRasterXSize
                    val bands = (1 to chip.getRasterCount).map(readBand(chip, _))
                    // Valid in any band, but an alpha band that declares no NoData is masked by nothing (its own
                    // mask is all valid) and marks the fill through its values, which the other bands' alpha mask reads.
                    val judged = bands.indices.filterNot { i =>
                        val band = chip.GetRasterBand(i + 1)
                        band.GetColorInterpretation == gdalconstConstants.GCI_AlphaBand &&
                            (band.GetMaskFlags() & gdalconstConstants.GMF_ALL_VALID) != 0
                    }
                    var k = 0
                    while (k < w * chip.getRasterYSize) {
                        if (judged.exists(i => bands(i)._2(k) != 0)) {
                            valid += 1
                            val (i, j) = (k % w, k / w)
                            val e = cgt(0) + (i + 0.5) * cgt(1) + (j + 0.5) * cgt(2)
                            val nn = cgt(3) + (i + 0.5) * cgt(4) + (j + 0.5) * cgt(5)
                            val ll = toLonLat.TransformPoint(e, nn)
                            val sc = inv(0) + ll(0) * inv(1) + ll(1) * inv(2)
                            val sr = inv(3) + ll(0) * inv(4) + ll(1) * inv(5)
                            if (sc < -1.5 || sc > n + 1.5 || sr < -1.5 || sr > n + 1.5) outside += 1
                            else if (sr >= 4.75 && sr <= 8.25) transparent += 1
                            if ((0 until 3).forall(b => bands(b)._1(k) == 0.0)) {
                                zeroRgb += 1
                                if (sr < 19.25 || sr > 21.75) zeroRgbOff += 1
                            }
                        }
                        k += 1
                    }
                } finally RasterDriver.releaseDataset(chip)
            }
        } finally {
            it.asInstanceOf[AutoCloseable].close()
            toLonLat.delete(); bng.delete(); wgs.delete()
            RasterDriver.releaseDataset(src)
        }
        (chips, valid, outside, transparent, zeroRgb, zeroRgbOff)
    }

    for (assignment <- Seq("centroid", "covering")) {
        test(s"(v) BNG $assignment on an RGBA source: its alpha band marks the warp fill, real 0 pixels stay valid") {
            val (chips, valid, outside, transparent, zeroRgb, zeroRgbOff) = bngRgbaChips(assignment)
            assert(chips > 0 && valid > 0, "the chips hold no valid pixel")
            assert(outside == 0, s"$outside valid chip pixels lie off the source footprint (warp fill read as data)")
            assert(transparent == 0, s"$transparent transparent source pixels are valid in the chips")
            assert(zeroRgb > 0, "the opaque all-zero pixels of the source must stay valid")
            assert(zeroRgbOff == 0, s"$zeroRgbOff valid all-zero chip pixels are not the source's opaque zero row")
        }
    }

    test("(y) H3 covering on an RGBA source: no NoData over the alpha band, real 0 pixels stay valid") {
        val (src, n, gt) = rgbaSource("h3rgba_covering.tif")
        val inv = invert(gt)
        var (chips, valid, outside, transparent, zeroRgb, zeroRgbOff) = (0, 0L, 0L, 0L, 0L, 0L)
        val declared = mutable.Set.empty[Option[Double]]
        var alphaMasked = true
        val it = RasterTessellate.tessellateH3Iter(handOff(src), Map.empty[String, String], Res, "covering", "sparse")
        try {
            while (it.hasNext) {
                val (_, chip, _) = it.next()
                try {
                    chips += 1
                    (1 to chip.getRasterCount).foreach(b => declared += noDataOf(chip, b))
                    alphaMasked &&= (chip.GetRasterBand(1).GetMaskFlags() & gdalconstConstants.GMF_ALPHA) != 0
                    val cgt = chip.GetGeoTransform
                    val w = chip.getRasterXSize
                    val mask = readBand(chip, 1)._2
                    val bands = (1 to 3).map(readBand(chip, _)._1)
                    var k = 0
                    while (k < w * chip.getRasterYSize) {
                        // Valid as GDAL reads it: through band 1's mask, which is the alpha band's.
                        if (mask(k) != 0) {
                            valid += 1
                            val (i, j) = (k % w, k / w)
                            val x = cgt(0) + (i + 0.5) * cgt(1) + (j + 0.5) * cgt(2)
                            val y = cgt(3) + (i + 0.5) * cgt(4) + (j + 0.5) * cgt(5)
                            val sc = inv(0) + x * inv(1) + y * inv(2)
                            val sr = inv(3) + x * inv(4) + y * inv(5)
                            if (sc < -1.5 || sc > n + 1.5 || sr < -1.5 || sr > n + 1.5) outside += 1
                            else if (sr >= 4.75 && sr <= 8.25) transparent += 1
                            if (bands.forall(_(k) == 0.0)) {
                                zeroRgb += 1
                                if (sr < 19.25 || sr > 21.75) zeroRgbOff += 1
                            }
                        }
                        k += 1
                    }
                } finally RasterDriver.releaseDataset(chip)
            }
        } finally {
            it.asInstanceOf[AutoCloseable].close()
            RasterDriver.releaseDataset(src)
        }
        assert(chips > 0 && valid > 0, "the chips hold no valid pixel")
        assert(declared == Set(None), s"the chips declare NoData $declared over the source's alpha band")
        assert(alphaMasked, "band 1 of a chip is not masked by its alpha band")
        assert(outside == 0, s"$outside valid chip pixels lie off the source footprint (clip fill read as data)")
        assert(transparent == 0, s"$transparent transparent source pixels are valid in the chips")
        assert(zeroRgb > 0, "the opaque all-zero pixels of the source must stay valid")
        assert(zeroRgbOff == 0, s"$zeroRgbOff valid all-zero chip pixels are not the source's opaque zero row")
    }

    test("(z) H3 covering+sparse on an RGBA source drops the chips its alpha band leaves fully transparent") {
        val (src, _, _) = rgbaSource("h3rgba_sparse.tif")
        fillRect(src, 4, 0, 0, 20, 40, 0.0) // the left half is transparent too (alpha 0, RGB non-zero)
        def validPerCell(coverage: String): Map[Long, Long] = {
            val it = RasterTessellate.tessellateH3Iter(handOff(src), Map.empty[String, String], Res, "covering", coverage)
            val out = mutable.Map.empty[Long, Long]
            try {
                while (it.hasNext) {
                    val (cell, chip, _) = it.next()
                    try out(cell) = validCount(chip, 1)
                    finally RasterDriver.releaseDataset(chip)
                }
            } finally it.asInstanceOf[AutoCloseable].close()
            out.toMap
        }
        try {
            val (complete, sparse) = (validPerCell("complete"), validPerCell("sparse"))
            val blank = complete.collect { case (cell, 0L) => cell }.toSet
            assert(blank.nonEmpty, "the fixture has no covering cell over the transparent half")
            assert(sparse.keySet == complete.keySet -- blank,
              s"sparse kept ${sparse.keySet & blank} (fully transparent) or dropped ${(complete.keySet -- blank) -- sparse.keySet}")
            assert(sparse.values.forall(_ > 0), "a sparse chip holds no valid pixel")
        } finally RasterDriver.releaseDataset(src)
    }
}
