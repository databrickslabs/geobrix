package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.{BNG, Quadbin}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Parity and burn-content tests for all three per-grid `RST_*_RasterizeAgg` UDAFs.
 *
 *  Name/arity checks require no GDAL. Round-trip burn tests drive `update`/`eval`
 *  directly (no Spark session) and verify that every covered pixel maps back into
 *  the input cell set via the grid's own `pointToCellID`, mirroring the H3 test in
 *  [[com.databricks.labs.gbx.rasterx.RST_H3_RasterizeAggTest]].
 */
class RST_GridRasterizeAggTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    // ---- helpers -----------------------------------------------------------

    private def encodedEmpty(): UTF8String = {
        val cfg = new ExpressionConfig(
            Map.empty[String, String],
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration()))
        val baos = new java.io.ByteArrayOutputStream()
        val oos  = new java.io.ObjectOutputStream(baos)
        oos.writeObject(cfg); oos.close()
        UTF8String.fromString(java.util.Base64.getEncoder.encodeToString(baos.toByteArray))
    }

    private case class RasterRead(gt: Array[Double], width: Int, height: Int, data: Array[Double])

    private def readRaster(tileRow: Any): RasterRead = {
        val bytes = tileRow.asInstanceOf[Array[Byte]]  // the aggregate returns the GeoTIFF bytes
        bytes should not be null
        val tmp = s"/vsimem/grid_ragg_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(tmp, bytes)
        val ds = gdal.Open(tmp)
        try {
            val w = ds.getRasterXSize
            val h = ds.getRasterYSize
            val buf = new Array[Double](w * h)
            ds.GetRasterBand(1).ReadRaster(0, 0, w, h, buf)
            RasterRead(ds.GetGeoTransform, w, h, buf)
        } finally {
            ds.delete()
            gdal.Unlink(tmp)
        }
    }

    // ---- name + arity checks (no GDAL needed) ------------------------------

    test("h3 rasterize_agg canonical name + 12-arg builder") {
        assert(RST_H3_RasterizeAgg.name == "gbx_rst_h3_rasterize_agg")
        val args = (0 until 12).map(i => Literal(i)).toSeq
        assert(RST_H3_RasterizeAgg.builder()(args).isInstanceOf[RST_H3_RasterizeAgg])
    }

    test("quadbin rasterize_agg canonical name + 12-arg builder") {
        assert(RST_Quadbin_RasterizeAgg.name == "gbx_rst_quadbin_rasterize_agg")
        val args = (0 until 12).map(i => Literal(i)).toSeq
        assert(RST_Quadbin_RasterizeAgg.builder()(args).isInstanceOf[RST_Quadbin_RasterizeAgg])
    }

    test("bng rasterize_agg canonical name + 12-arg builder") {
        assert(RST_BNG_RasterizeAgg.name == "gbx_rst_bng_rasterize_agg")
        val args = (0 until 12).map(i => Literal(i)).toSeq
        assert(RST_BNG_RasterizeAgg.builder()(args).isInstanceOf[RST_BNG_RasterizeAgg])
    }

    // ---- BNG burn round-trip -----------------------------------------------

    // London area BNG points (EPSG:27700 eastings/northings), resolution 3 (1 km cells).
    private val bngRes = 3
    private val bngPoints = Seq(
        (530000.0, 180000.0),  // Lambeth
        (532000.0, 182000.0),  // Southwark
        (528000.0, 178000.0)   // Wandsworth
    )

    private def makeBNGAutoAgg(): RST_BNG_RasterizeAgg =
        RST_BNG_RasterizeAgg(
            cellidExpr    = Literal.create(null, StringType),
            valueExpr     = Literal(0.0),
            outSridExpr   = Literal(27700),
            pixelSizeExpr = Literal.create(null, DoubleType),
            xminExpr      = Literal.create(null, DoubleType),
            yminExpr      = Literal.create(null, DoubleType),
            xmaxExpr      = Literal.create(null, DoubleType),
            ymaxExpr      = Literal.create(null, DoubleType),
            widthExpr     = Literal.create(null, IntegerType),
            heightExpr    = Literal.create(null, IntegerType),
            modeExpr      = Literal("centroids"),
            kringPadExpr  = Literal(1),
            exprConfExpr  = Literal.create(encodedEmpty(), StringType)
        )

    test("BNG auto-grid burn: every covered pixel maps back into the input cell set") {
        val cellIds = bngPoints.map { case (e, n) => BNG.pointToCellID(e, n, bngRes) }
        val cellSet = cellIds.toSet

        val agg = makeBNGAutoAgg()
        val buf = agg.createAggregationBuffer()
        // Direct typed update: cellId already parsed to Long.
        cellIds.zipWithIndex.foreach { case (c, i) => agg.update(buf, c, (i + 1).toDouble) }

        val result: AnyRef = agg.eval(buf).asInstanceOf[AnyRef]
        result should not be null

        val r = readRaster(result)
        var covered = 0
        var py = 0
        while (py < r.height) {
            var px = 0
            while (px < r.width) {
                val v = r.data(py * r.width + px)
                if (v != -9999.0) {
                    covered += 1
                    // Pixel-centroid in EPSG:27700 (BNG raster is 27700-native).
                    val xOffset = 0.5 + px
                    val yOffset = 0.5 + py
                    val e = r.gt(0) + xOffset * r.gt(1) + yOffset * r.gt(2)
                    val n = r.gt(3) + xOffset * r.gt(4) + yOffset * r.gt(5)
                    val back = BNG.pointToCellID(e, n, bngRes)
                    cellSet should contain(back)
                }
                px += 1
            }
            py += 1
        }
        covered should be >= bngPoints.length
    }

    test("BNG absent cells are written as -9999.0 NoData") {
        val cellId = BNG.pointToCellID(530000.0, 180000.0, bngRes)
        val agg    = makeBNGAutoAgg()
        val buf    = agg.createAggregationBuffer()
        agg.update(buf, cellId, 42.0)

        val r = readRaster(agg.eval(buf).asInstanceOf[AnyRef])
        // The kring-padded grid contains the cell and its neighbours. Pixels outside
        // all padded cells must be NoData.
        val noDataPixels = r.data.count(_ == -9999.0)
        noDataPixels should be > 0
    }

    // ---- Quadbin burn round-trip --------------------------------------------

    // London area lon/lat, zoom 12 (~9.7 km cells).
    private val qbZoom = 12
    private val qbPoints = Seq(
        (-0.1276, 51.5074),  // Westminster
        (-0.1419, 51.5014),  // Buckingham Palace area
        (-0.0550, 51.5100)   // Canary Wharf area (different zoom-12 cell)
    )

    private def makeQuadbinAutoAgg(): RST_Quadbin_RasterizeAgg =
        RST_Quadbin_RasterizeAgg(
            cellidExpr    = Literal.create(null, LongType),
            valueExpr     = Literal(0.0),
            outSridExpr   = Literal(4326),
            pixelSizeExpr = Literal.create(null, DoubleType),
            xminExpr      = Literal.create(null, DoubleType),
            yminExpr      = Literal.create(null, DoubleType),
            xmaxExpr      = Literal.create(null, DoubleType),
            ymaxExpr      = Literal.create(null, DoubleType),
            widthExpr     = Literal.create(null, IntegerType),
            heightExpr    = Literal.create(null, IntegerType),
            modeExpr      = Literal("centroids"),
            kringPadExpr  = Literal(1),
            exprConfExpr  = Literal.create(encodedEmpty(), StringType)
        )

    test("Quadbin auto-grid burn: every covered pixel maps back into the input cell set") {
        val cellIds = qbPoints.map { case (lon, lat) => Quadbin.pointToCell(lon, lat, qbZoom) }
        val cellSet = cellIds.toSet

        val agg = makeQuadbinAutoAgg()
        val buf = agg.createAggregationBuffer()
        cellIds.zipWithIndex.foreach { case (c, i) => agg.update(buf, c, (i + 1).toDouble) }

        val result: AnyRef = agg.eval(buf).asInstanceOf[AnyRef]
        result should not be null

        val r = readRaster(result)
        var covered = 0
        var py = 0
        while (py < r.height) {
            var px = 0
            while (px < r.width) {
                val v = r.data(py * r.width + px)
                if (v != -9999.0) {
                    covered += 1
                    // Pixel-centroid in EPSG:4326 (Quadbin raster is 4326-native).
                    val xOffset = 0.5 + px
                    val yOffset = 0.5 + py
                    val lon = r.gt(0) + xOffset * r.gt(1) + yOffset * r.gt(2)
                    val lat = r.gt(3) + xOffset * r.gt(4) + yOffset * r.gt(5)
                    val back = Quadbin.pointToCell(lon, lat, qbZoom)
                    cellSet should contain(back)
                }
                px += 1
            }
            py += 1
        }
        // Assert at least as many covered pixels as distinct cells burned.
        covered should be >= cellSet.size
    }

    test("Quadbin absent cells are written as -9999.0 NoData") {
        val cellId = Quadbin.pointToCell(-0.1276, 51.5074, qbZoom)
        val agg    = makeQuadbinAutoAgg()
        val buf    = agg.createAggregationBuffer()
        agg.update(buf, cellId, 7.0)

        val r = readRaster(agg.eval(buf).asInstanceOf[AnyRef])
        val noDataPixels = r.data.count(_ == -9999.0)
        noDataPixels should be > 0
    }
}
