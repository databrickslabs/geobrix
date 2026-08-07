package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for [[RST_RasterizeAgg]].
 *
 *  We construct the aggregator with Literal constant children (same approach as
 *  RST_DTMFromGeomsAgg) and drive `update`/`merge`/`eval` directly -- no Spark
 *  session required.
 *
 *  Extent: (0,0) -> (100,100), 100x100 px, EPSG:32633.
 *  Polygon A: (0,50)->(50,100) -- top-left quadrant, burn value 10.0.
 *  Polygon B: (50,0)->(100,50) -- bottom-right quadrant, burn value 20.0.
 *  Pixel A sample (col=25, row=25):  inside A -> 10.0.
 *  Pixel B sample (col=75, row=75):  inside B -> 20.0.
 *  Pixel O sample (col=75, row=25):  outside both -> -9999.0 (nodata).
 *  (GDAL row 0 is at ymax=100; row 25 is y in [75,100); row 75 is y in [25,0).)
 */
class RST_RasterizeAggTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    // ---- geometry helpers ---------------------------------------------------

    private val gf = new GeometryFactory()

    /** Rectangle WKB from two corners. */
    private def rectWkb(x0: Double, y0: Double, x1: Double, y1: Double): Array[Byte] = {
        val poly = gf.createPolygon(Array(
            new Coordinate(x0, y0),
            new Coordinate(x1, y0),
            new Coordinate(x1, y1),
            new Coordinate(x0, y1),
            new Coordinate(x0, y0)
        ))
        JTS.toWKB(poly)
    }

    // ---- ExpressionConfig helper --------------------------------------------

    private def encodedEmpty(): UTF8String = {
        val cfg = new ExpressionConfig(
            Map.empty[String, String],
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration()))
        val baos = new java.io.ByteArrayOutputStream()
        val oos  = new java.io.ObjectOutputStream(baos)
        oos.writeObject(cfg); oos.close()
        UTF8String.fromString(java.util.Base64.getEncoder.encodeToString(baos.toByteArray))
    }

    // ---- agg factory --------------------------------------------------------

    /** Build an RST_RasterizeAgg with all constant children as Literals.
     *  geomWkbExpr and valueExpr are null literals -- not used in eval (only in update),
     *  so they do not need to produce real values here.
     */
    private def makeAgg(): RST_RasterizeAgg =
        RST_RasterizeAgg(
            geomWkbExpr  = Literal.create(null, org.apache.spark.sql.types.BinaryType),
            valueExpr    = Literal(0.0),
            xminExpr     = Literal(0.0),
            yminExpr     = Literal(0.0),
            xmaxExpr     = Literal(100.0),
            ymaxExpr     = Literal(100.0),
            widthPxExpr  = Literal(100),
            heightPxExpr = Literal(100),
            outSridExpr  = Literal(32633),
            exprConfExpr = Literal.create(encodedEmpty(), StringType)
        )

    // ---- pixel readback helper ----------------------------------------------

    private def readPixel(tileRow: Any, col: Int, row: Int): Double = {
        val ir    = tileRow.asInstanceOf[InternalRow]
        val bytes = ir.getBinary(1)
        bytes should not be null
        val tmp = s"/vsimem/ragg_test_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(tmp, bytes)
        val ds = gdal.Open(tmp)
        try {
            val buf = new Array[Double](1)
            ds.GetRasterBand(1).ReadRaster(col, row, 1, 1, buf)
            buf(0)
        } finally {
            ds.delete()
            gdal.Unlink(tmp)
        }
    }

    // ---- tests --------------------------------------------------------------

    test("multi-feature burn: two non-overlapping polygons burn distinct values; outside is nodata") {
        val wkbA = rectWkb(0.0, 50.0, 50.0, 100.0)  // top-left quadrant
        val wkbB = rectWkb(50.0, 0.0, 100.0, 50.0)  // bottom-right quadrant

        val agg = makeAgg()
        val buf = agg.createAggregationBuffer()
        agg.update(buf, wkbA, 10.0)
        agg.update(buf, wkbB, 20.0)

        val result: AnyRef = agg.eval(buf).asInstanceOf[AnyRef]
        result should not be null

        // GDAL pixel layout: row 0 is at ymax (y=100), row 99 is at ymin (y=0).
        // Polygon A spans y in [50,100): GDAL rows 0..49.  col 25 is inside.
        // Polygon B spans y in [0,50):  GDAL rows 50..99. col 75 is inside.
        // Pixel at (col=25, row=25):  inside A -> 10.0
        readPixel(result, 25, 25) shouldBe 10.0 +- 1e-6
        // Pixel at (col=75, row=75):  inside B -> 20.0
        readPixel(result, 75, 75) shouldBe 20.0 +- 1e-6
        // Pixel at (col=75, row=25):  outside both -> -9999.0 (nodata)
        readPixel(result, 75, 25) shouldBe -9999.0 +- 1e-6
    }

    test("overlap winner is deterministic regardless of update order (canonical fold)") {
        // Two polygons overlapping in the top-left quadrant; burned in both orders.
        // The aggregator sorts features by (geom_wkb, value) before burning, so the
        // overlap pixel must be identical regardless of the order rows arrived.
        val wkbA = rectWkb(0.0, 0.0, 60.0, 100.0)   // left band, value 10.0
        val wkbB = rectWkb(40.0, 0.0, 100.0, 100.0) // right band, value 20.0 (overlap x in [40,60))

        val aggAB = makeAgg()
        val bufAB = aggAB.createAggregationBuffer()
        aggAB.update(bufAB, wkbA, 10.0)
        aggAB.update(bufAB, wkbB, 20.0)
        val resAB: AnyRef = aggAB.eval(bufAB).asInstanceOf[AnyRef]

        val aggBA = makeAgg()
        val bufBA = aggBA.createAggregationBuffer()
        aggBA.update(bufBA, wkbB, 20.0)   // reversed arrival order
        aggBA.update(bufBA, wkbA, 10.0)
        val resBA: AnyRef = aggBA.eval(bufBA).asInstanceOf[AnyRef]

        // Overlap column 50 (x in [40,60)) must resolve to the SAME winner both ways.
        val winAB = readPixel(resAB, 50, 50)
        val winBA = readPixel(resBA, 50, 50)
        winAB shouldBe winBA +- 1e-9
        // Non-overlap sanity: col 10 only A (10.0), col 90 only B (20.0).
        readPixel(resAB, 10, 50) shouldBe 10.0 +- 1e-6
        readPixel(resAB, 90, 50) shouldBe 20.0 +- 1e-6
    }

    test("buffer serde roundtrip preserves features") {
        val wkbA = rectWkb(0.0, 50.0, 50.0, 100.0)
        val wkbB = rectWkb(50.0, 0.0, 100.0, 50.0)

        val agg = makeAgg()
        val buf = agg.createAggregationBuffer()
        agg.update(buf, wkbA, 10.0)
        agg.update(buf, wkbB, 20.0)

        val serialized   = agg.serialize(buf)
        val deserialized = agg.deserialize(serialized)

        deserialized.features.length shouldBe 2
        deserialized.features(0)._2 shouldBe 10.0 +- 1e-12
        deserialized.features(1)._2 shouldBe 20.0 +- 1e-12
        java.util.Arrays.equals(deserialized.features(0)._1, wkbA) shouldBe true
        java.util.Arrays.equals(deserialized.features(1)._1, wkbB) shouldBe true
    }

    test("merge then eval: two separate buffers produce a raster with both burns") {
        val wkbA = rectWkb(0.0, 50.0, 50.0, 100.0)  // top-left quadrant, value 10.0
        val wkbB = rectWkb(50.0, 0.0, 100.0, 50.0)  // bottom-right quadrant, value 20.0

        val agg = makeAgg()

        val buf1 = agg.createAggregationBuffer()
        agg.update(buf1, wkbA, 10.0)

        val buf2 = agg.createAggregationBuffer()
        agg.update(buf2, wkbB, 20.0)

        val merged = agg.merge(buf1, buf2)
        merged.features.length shouldBe 2

        val result: AnyRef = agg.eval(merged).asInstanceOf[AnyRef]
        result should not be null

        // Polygon A top-left: GDAL row 0 is ymax=100; row 25 is in [75,100) => inside A
        readPixel(result, 25, 25) shouldBe 10.0 +- 1e-6
        // Polygon B bottom-right: row 75 is in [25,0) => inside B
        readPixel(result, 75, 75) shouldBe 20.0 +- 1e-6
        // Outside both polygons -> nodata
        readPixel(result, 75, 25) shouldBe -9999.0 +- 1e-6
    }

}
