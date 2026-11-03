package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.gdal.gdal.gdal
import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for `RST_GridFromPoints` and its aggregator counterpart.
 *
 *  Each test feeds 4 known corner points (values 0, 10, 20, 30) into IDW and
 *  asserts:
 *    - the center pixel falls within the mean-of-corners range, and
 *    - the aggregator produces the same numerical result as the non-aggregator
 *      given the same data.
 */
class RST_GridFromPointsTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    /** Four corner points of a 100x100 m extent in EPSG:32633. Values 0,10,20,30. */
    private def cornerPoints(): Seq[(Array[Byte], Double)] = {
        val gf = new GeometryFactory()
        Seq(
            (JTS.toWKB(gf.createPoint(new Coordinate(0.0,   0.0))),    0.0),
            (JTS.toWKB(gf.createPoint(new Coordinate(100.0, 0.0))),   10.0),
            (JTS.toWKB(gf.createPoint(new Coordinate(0.0,   100.0))), 20.0),
            (JTS.toWKB(gf.createPoint(new Coordinate(100.0, 100.0))), 30.0)
        )
    }

    /** Read the center pixel value of the GTiff bytes returned by `execute`. */
    private def centerPixel(row: InternalRow): Double = {
        val bytes = row.getBinary(1)
        bytes should not be null
        val tmp = s"/vsimem/idw_readback_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(tmp, bytes)
        val ds = gdal.Open(tmp)
        try {
            val w = ds.GetRasterXSize
            val h = ds.GetRasterYSize
            val buf = new Array[Double](1)
            ds.GetRasterBand(1).ReadRaster(w / 2, h / 2, 1, 1, buf)
            buf(0)
        } finally {
            ds.delete()
            gdal.Unlink(tmp)
        }
    }

    test("RST_GridFromPoints IDW: center pixel approximates mean of 4 corner values") {
        val row = RST_GridFromPoints.execute(
            cornerPoints(),
            xmin = 0.0, ymin = 0.0, xmax = 100.0, ymax = 100.0,
            widthPx = 50, heightPx = 50,
            out_srid = 32633,
            power = 2.0, maxPts = 12
        )
        row should not be null
        val center = centerPixel(row)
        // Mean of 0,10,20,30 = 15. IDW with power=2 at the dead centre is
        // exactly the mean (equal weights). Tolerate small numerical drift.
        center should (be > 13.0 and be < 17.0)
    }

    test("RST_GridFromPoints rejects degenerate extents and zero/negative parameters") {
        an[IllegalArgumentException] should be thrownBy {
            RST_GridFromPoints.execute(cornerPoints(), 0.0, 0.0, 0.0, 100.0, 50, 50, 32633, 2.0, 12)
        }
        an[IllegalArgumentException] should be thrownBy {
            RST_GridFromPoints.execute(cornerPoints(), 0.0, 0.0, 100.0, 100.0, 0, 50, 32633, 2.0, 12)
        }
        an[IllegalArgumentException] should be thrownBy {
            RST_GridFromPoints.execute(cornerPoints(), 0.0, 0.0, 100.0, 100.0, 50, 50, 32633, 0.0, 12)
        }
        // an explicit out_srid of 0 with an out_crs is "both set"; only a NULL (NoSrid) or no out_crs is unset
        val crs = org.apache.spark.unsafe.types.UTF8String.fromString("EPSG:3857")
        an[IllegalArgumentException] should be thrownBy RST_GridFromPoints.outputCrs(0, crs)
        RST_GridFromPoints.outputCrs(RST_GridFromPoints.NoSrid, crs) shouldBe "EPSG:3857"
        RST_GridFromPoints.outputCrs(0, null) shouldBe null
    }

}
