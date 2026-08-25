package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.gdal.gdal.gdal
import org.locationtech.jts.geom.{Coordinate, Geometry, LineString}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

class RST_DTMFromGeomsTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    /** z = 2*x + 3*y + 5 sampled at the 4 corners of a 100x100 extent (EPSG:32633). */
    private def planePoints(): Seq[Geometry] = Seq(
        JTS.point(new Coordinate(0.0,   0.0,   5.0)),
        JTS.point(new Coordinate(100.0, 0.0,   205.0)),
        JTS.point(new Coordinate(0.0,   100.0, 305.0)),
        JTS.point(new Coordinate(100.0, 100.0, 505.0))
    )

    /** Read a single pixel value (col,row) from the GTiff bytes in a tile row. */
    private def pixel(row: InternalRow, col: Int, r: Int): Double = {
        val bytes = row.getBinary(1)
        bytes should not be null
        val tmp = s"/vsimem/dtm_readback_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(tmp, bytes)
        val ds = gdal.Open(tmp)
        try {
            val buf = new Array[Double](1)
            ds.GetRasterBand(1).ReadRaster(col, r, 1, 1, buf)
            buf(0)
        } finally { ds.delete(); gdal.Unlink(tmp) }
    }

    test("execute reproduces the planar surface at cell centers") {
        val row = RST_DTMFromGeoms.execute(
            planePoints(), Seq.empty[LineString],
            mergeTolerance = 0.0, snapTolerance = 0.0,
            xmin = 0.0, ymin = 0.0, xmax = 100.0, ymax = 100.0,
            widthPx = 10, heightPx = 10, out_srid = 32633, noData = -9999.0
        )
        row should not be null
        pixel(row, 0, 0) shouldBe 300.0 +- 1e-3
        pixel(row, 9, 9) shouldBe 210.0 +- 1e-3
    }

    test("execute writes no_data for cells outside the point hull") {
        val row = RST_DTMFromGeoms.execute(
            planePoints(), Seq.empty[LineString],
            0.0, 0.0,
            xmin = -100.0, ymin = -100.0, xmax = 200.0, ymax = 200.0,
            widthPx = 30, heightPx = 30, out_srid = 32633, noData = -9999.0
        )
        pixel(row, 0, 0) shouldBe -9999.0 +- 1e-6
    }

    test("execute honors a breakline without throwing") {
        val bl = JTS.fromWKT("LINESTRING (0 50, 100 50)").asInstanceOf[LineString]
        noException should be thrownBy {
            RST_DTMFromGeoms.execute(
                planePoints(), Seq(bl), 0.0, 0.01,
                0.0, 0.0, 100.0, 100.0, 10, 10, 32633, -9999.0)
        }
    }

    test("execute rejects degenerate extents and non-positive dims") {
        an[IllegalArgumentException] should be thrownBy {
            RST_DTMFromGeoms.execute(planePoints(), Seq.empty, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0, 10, 10, 32633, -9999.0)
        }
        an[IllegalArgumentException] should be thrownBy {
            RST_DTMFromGeoms.execute(planePoints(), Seq.empty, 0.0, 0.0, 0.0, 0.0, 100.0, 100.0, 0, 10, 32633, -9999.0)
        }
    }

    test("builder accepts 11 args (no_data defaulted) and 12 args") {
        val lit = (v: Any) => org.apache.spark.sql.catalyst.expressions.Literal(v)
        val base = Seq[org.apache.spark.sql.catalyst.expressions.Expression](
            lit(null), lit(null), lit(0.0), lit(0.0),
            lit(0.0), lit(0.0), lit(100.0), lit(100.0),
            lit(10), lit(10), lit(32633)
        )
        RST_DTMFromGeoms.builder()(base) shouldBe a[RST_DTMFromGeoms]
        RST_DTMFromGeoms.builder()(base :+ lit(-1.0)) shouldBe a[RST_DTMFromGeoms]
        an[IllegalArgumentException] should be thrownBy { RST_DTMFromGeoms.builder()(base.take(5)) }
    }

    test("DTMFromGeomsAcc serialize/deserialize roundtrips point WKBs") {
        val buf = DTMFromGeomsAcc.empty
        planePoints().foreach(p => buf.add(JTS.toWKB3(p)))
        val restored = DTMFromGeomsAcc.deserialize(buf.serialize)
        restored.points.length shouldBe 4
        restored.points.zip(buf.points).foreach { case (a, b) => a shouldBe b }
    }

    test("RST_DTMFromGeomsAgg produces the same raster as the non-agg execute") {
        val lit = (v: Any) => org.apache.spark.sql.catalyst.expressions.Literal(v)
        val buf = DTMFromGeomsAcc.empty
        planePoints().foreach(p => buf.add(JTS.toWKB3(p)))
        val agg = RST_DTMFromGeomsAgg(
            pointExpr = null,
            breaklinesExpr = lit(null),
            mergeToleranceExpr = lit(0.0), snapToleranceExpr = lit(0.0),
            xminExpr = lit(0.0), yminExpr = lit(0.0), xmaxExpr = lit(100.0), ymaxExpr = lit(100.0),
            widthPxExpr = lit(10), heightPxExpr = lit(10), outSridExpr = lit(32633),
            noDataExpr = lit(-9999.0)
        )
        val aggRow = agg.eval(buf).asInstanceOf[InternalRow]
        val nonAggRow = RST_DTMFromGeoms.execute(
            planePoints(), Seq.empty[LineString], 0.0, 0.0,
            0.0, 0.0, 100.0, 100.0, 10, 10, 32633, -9999.0)
        pixel(aggRow, 0, 0) shouldBe pixel(nonAggRow, 0, 0) +- 1e-9
        pixel(aggRow, 9, 9) shouldBe pixel(nonAggRow, 9, 9) +- 1e-9
    }

    test("RST_DTMFromGeomsAgg.update rejects a 2D-WKB point (Z stripped)") {
        val lit = (v: Any) => org.apache.spark.sql.catalyst.expressions.Literal(v)
        // JTS.toWKB is the 2D writer -> strips Z, simulating a user passing 2D WKB.
        val twoDWkb = JTS.toWKB(planePoints().head)
        val agg = RST_DTMFromGeomsAgg(
            pointExpr = lit(twoDWkb),
            breaklinesExpr = lit(null),
            mergeToleranceExpr = lit(0.0), snapToleranceExpr = lit(0.0),
            xminExpr = lit(0.0), yminExpr = lit(0.0), xmaxExpr = lit(100.0), ymaxExpr = lit(100.0),
            widthPxExpr = lit(10), heightPxExpr = lit(10), outSridExpr = lit(32633),
            noDataExpr = lit(-9999.0)
        )
        an[IllegalArgumentException] should be thrownBy {
            agg.update(DTMFromGeomsAcc.empty, org.apache.spark.sql.catalyst.InternalRow.empty)
        }
    }
}
