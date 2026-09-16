package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.util.V2Tile
import com.databricks.labs.gbx.util.NodeFilePathUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files
import java.util.UUID

/**
  * Direct-execute unit tests for [[RST_BinPointsAgg]] and [[BinPointsAcc]].
  *
  * Tests operate at the accumulator + eval level — no Spark session or JAR needed.
  * GDAL is initialised once in [[beforeAll]].
  *
  * Row layout for the shared 2×2 fixture mirrors [[RST_BinPointsTest]]:
  *   Grid: [0,2]×[0,2], 2 pixels wide × 2 pixels tall, EPSG:4326.
  *   (0.5, 1.5, 10.0) → row=0, col=0 (top-left,    index 0) ←  two-point cell with (0.5,1.5,25.0)
  *   (0.5, 1.5, 25.0) → row=0, col=0 (top-left,    index 0)
  *   (1.5, 0.5,  7.0) → row=1, col=1 (bottom-right, index 3)
  *   Indices 1 (top-right) and 2 (bottom-left) are empty → NoData (-9999).
  */
class RST_BinPointsAggTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        // Mark GDALManager.isEnabled so eval()'s guard skips ExpressionConfigExpr
        // resolution (which requires SparkEnv, unavailable in this bare unit test).
        GDALManager.isEnabled = true
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    // --- shared 2×2 fixture (mirrors RST_BinPointsTest) ---
    private val x2   = Array(0.5, 0.5, 1.5)
    private val y2   = Array(1.5, 1.5, 0.5)
    private val z2   = Array(10.0, 25.0, 7.0)
    private val xmin = 0.0; private val ymin = 0.0
    private val xmax = 2.0; private val ymax = 2.0
    private val w    = 2;   private val h    = 2
    private val epsg = 4326

    /** Populate a [[BinPointsAcc]] from parallel arrays. */
    private def fillBuf(x: Array[Double], y: Array[Double], z: Array[Double]): BinPointsAcc = {
        val buf = BinPointsAcc.empty
        for (i <- x.indices) buf.add(x(i), y(i), z(i))
        buf
    }

    /** Build an agg expression with Literal constant args; xExpr/yExpr/zExpr are not used
      * by eval (they're only needed by update, which is not exercised here). */
    private def makeAgg(stat: String): RST_BinPointsAgg = RST_BinPointsAgg(
        xExpr = Literal(0.0), yExpr = Literal(0.0), zExpr = Literal(0.0),
        xminExpr     = Literal(xmin),
        yminExpr     = Literal(ymin),
        xmaxExpr     = Literal(xmax),
        ymaxExpr     = Literal(ymax),
        widthPxExpr  = Literal(w),
        heightPxExpr = Literal(h),
        sridExpr     = Literal(epsg),
        statisticExpr = Literal(UTF8String.fromString(stat), StringType)
    )

    /** Read all Float32 band-1 values (row-major) from the tile row's raster field. */
    private def readBandFromRow(row: InternalRow): Array[Float] = {
        val bytes = V2Tile.getRaster(row)
        bytes should not be null
        val path = s"/vsimem/tbpa_${UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(path, bytes)
        val ds = gdal.Open(path)
        try {
            val wi  = ds.GetRasterXSize
            val hi  = ds.GetRasterYSize
            val buf = new Array[Float](wi * hi)
            ds.GetRasterBand(1).ReadRaster(0, 0, wi, hi, buf)
            buf
        } finally {
            ds.delete()
            gdal.Unlink(path)
        }
    }

    // ---------------------------------------------------------------
    // BinPointsAcc: round-trip + size tracking
    // ---------------------------------------------------------------

    test("BinPointsAcc: add/merge/serialize/deserialize round-trip preserves points") {
        val a = BinPointsAcc.empty
        a.add(1.0, 2.0, 3.0)
        a.add(4.0, 5.0, 6.0)
        val b = BinPointsAcc.empty
        b.add(7.0, 8.0, 9.0)
        a.merge(b)
        a.points.length shouldBe 3

        val restored = BinPointsAcc.deserialize(a.serialize)
        restored.points.length shouldBe 3
        restored.points(0) shouldBe ((1.0, 2.0, 3.0))
        restored.points(1) shouldBe ((4.0, 5.0, 6.0))
        restored.points(2) shouldBe ((7.0, 8.0, 9.0))
        restored.approxByteSize shouldBe 72L  // 3 × 24 bytes
    }

    test("BinPointsAcc: approxByteSize tracks 24 bytes per point") {
        val buf = BinPointsAcc.empty
        buf.approxByteSize shouldBe 0L
        buf.add(1.0, 2.0, 3.0)
        buf.approxByteSize shouldBe 24L
        buf.add(4.0, 5.0, 6.0)
        buf.approxByteSize shouldBe 48L
    }

    test("BinPointsAcc: guardSize throws when exceeded") {
        an[IllegalStateException] should be thrownBy {
            BinPointsAcc.guardSize(BinPointsAcc.MAX_BUFFER_BYTES + 1L)
        }
    }

    test("BinPointsAcc: guardSize does not throw at the cap boundary") {
        // Exactly at the cap must not throw (> not >=)
        BinPointsAcc.guardSize(BinPointsAcc.MAX_BUFFER_BYTES)
    }

    // ---------------------------------------------------------------
    // RST_BinPointsAgg eval: empty buffer → null
    // ---------------------------------------------------------------

    test("RST_BinPointsAgg: empty buffer → eval returns null") {
        val result = makeAgg("max").eval(BinPointsAcc.empty)
        assert(result == null)
    }

    // ---------------------------------------------------------------
    // RST_BinPointsAgg eval: statistics match RST_BinPoints.execute
    // ---------------------------------------------------------------

    test("RST_BinPointsAgg: max — two-point cell → max(10,25)=25; single-point → 7; empties → -9999") {
        val row = makeAgg("max").eval(fillBuf(x2, y2, z2)).asInstanceOf[InternalRow]
        row should not be null
        val band = readBandFromRow(row)
        band(0) shouldBe 25.0f     // top-left:     max(10,25)
        band(3) shouldBe 7.0f      // bottom-right: 7
        band(1) shouldBe -9999.0f  // top-right:    empty
        band(2) shouldBe -9999.0f  // bottom-left:  empty
    }

    test("RST_BinPointsAgg: min — two-point cell → min(10,25)=10") {
        val row = makeAgg("min").eval(fillBuf(x2, y2, z2)).asInstanceOf[InternalRow]
        row should not be null
        val band = readBandFromRow(row)
        band(0) shouldBe 10.0f
        band(3) shouldBe 7.0f
        band(1) shouldBe -9999.0f
        band(2) shouldBe -9999.0f
    }

    test("RST_BinPointsAgg: mean — two-point cell → mean(10,25)=17.5") {
        val row = makeAgg("mean").eval(fillBuf(x2, y2, z2)).asInstanceOf[InternalRow]
        row should not be null
        val band = readBandFromRow(row)
        band(0) shouldBe 17.5f
        band(3) shouldBe 7.0f
    }

    test("RST_BinPointsAgg: count — two-point cell → 2.0; single-point → 1.0; empty → -9999") {
        val row = makeAgg("count").eval(fillBuf(x2, y2, z2)).asInstanceOf[InternalRow]
        row should not be null
        val band = readBandFromRow(row)
        band(0) shouldBe 2.0f
        band(3) shouldBe 1.0f
        band(1) shouldBe -9999.0f
        band(2) shouldBe -9999.0f
    }

    test("RST_BinPointsAgg: median — 2 points (z=10,25) → numpy linear-interp at p=50 = 17.5") {
        // numpy.percentile([10,25], 50): idx = 0.5*(2-1) = 0.5, lo=0, hi=1, frac=0.5 → 10 + 0.5*15 = 17.5
        val row = makeAgg("median").eval(fillBuf(x2, y2, z2)).asInstanceOf[InternalRow]
        row should not be null
        val band = readBandFromRow(row)
        band(0) shouldBe (17.5f +- 0.001f)
        band(3) shouldBe (7.0f  +- 0.001f)
    }

}
