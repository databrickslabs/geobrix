package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.custom.Custom_Grid
import com.databricks.labs.gbx.gridx.grid.{CustomGridSystem, GridConf}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Name/arity + burn round-trip tests for the custom-grid `RST_Custom_RasterizeAgg` UDAF.
 *  Mirrors [[RST_GridRasterizeAggTest]]; drives `update`/`eval` directly (no Spark session).
 */
class RST_Custom_RasterizeAggTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

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
        val tmp = s"/vsimem/custom_ragg_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
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

    // Custom grid [0, 1_000_000] x [0, 1_000_000] in EPSG:27700; res 3 => 12_500 m cells.
    private val conf = GridConf(0, 1000000, 0, 1000000, 2, 100000, 100000, Some(27700))
    private val grid = CustomGridSystem(conf)
    private val res  = 3

    private def gridExpr: Expression =
        Custom_Grid(Literal(0L), Literal(1000000L), Literal(0L), Literal(1000000L),
            Literal(2), Literal(100000), Literal(100000), Literal(27700))

    // Three well-separated EPSG:27700 points -> distinct res-3 cells.
    private val points = Seq((500000.0, 500000.0), (525000.0, 525000.0), (475000.0, 475000.0))

    /** Explicit-extent aggregator (xmin..height supplied) so the burn grid is deterministic. */
    private def makeCustomAgg(): RST_Custom_RasterizeAgg =
        RST_Custom_RasterizeAgg(
            cellidExpr    = Literal.create(null, LongType),
            valueExpr     = Literal(0.0),
            gridExpr      = gridExpr,
            outSridExpr   = Literal(27700),
            pixelSizeExpr = Literal.create(null, DoubleType),
            xminExpr      = Literal(460000.0),
            yminExpr      = Literal(460000.0),
            xmaxExpr      = Literal(540000.0),
            ymaxExpr      = Literal(540000.0),
            widthExpr     = Literal(80),
            heightExpr    = Literal(80),
            modeExpr      = Literal("centroids"),
            kringPadExpr  = Literal(1),
            exprConfExpr  = Literal.create(encodedEmpty(), StringType)
        )

    test("custom rasterize_agg canonical name + 13-arg builder") {
        assert(RST_Custom_RasterizeAgg.name == "gbx_rst_custom_rasterize_agg")
        val args = (0 until 13).map(i => Literal(i)).toSeq
        assert(RST_Custom_RasterizeAgg.builder()(args).isInstanceOf[RST_Custom_RasterizeAgg])
    }

    test("custom burn: every covered pixel maps back into the input cell set") {
        val cellIds = points.map { case (x, y) => grid.pointToCellID(x, y, res) }
        val cellSet = cellIds.toSet

        val agg = makeCustomAgg()
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
                    val xOffset = 0.5 + px
                    val yOffset = 0.5 + py
                    val cx = r.gt(0) + xOffset * r.gt(1) + yOffset * r.gt(2)
                    val cy = r.gt(3) + xOffset * r.gt(4) + yOffset * r.gt(5)
                    cellSet should contain(grid.pointToCellID(cx, cy, res))
                }
                px += 1
            }
            py += 1
        }
        covered should be >= cellSet.size
    }

    test("custom burn: absent cells are written as -9999.0 NoData") {
        val cellId = grid.pointToCellID(500000.0, 500000.0, res)
        val agg    = makeCustomAgg()
        val buf    = agg.createAggregationBuffer()
        agg.update(buf, cellId, 42.0)

        val r = readRaster(agg.eval(buf).asInstanceOf[AnyRef])
        r.data.count(_ == -9999.0) should be > 0
    }

    // -------------------------------------------------------------------------------------------
    // Auto-grid path (extent OMITTED -> computeGridspec): exercises grid-CRS sampling, the
    // srid == gridCrs short-circuit, and the reproject branch (out_srid != gridCrs).
    // -------------------------------------------------------------------------------------------

    /** Extent-OMITTED aggregator (xmin..height null) at `outSrid`; pixel_size omitted -> default. */
    private def makeCustomAutoAgg(outSrid: Int): RST_Custom_RasterizeAgg =
        RST_Custom_RasterizeAgg(
            cellidExpr    = Literal.create(null, LongType),
            valueExpr     = Literal(0.0),
            gridExpr      = gridExpr,
            outSridExpr   = Literal(outSrid),
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

    // A derived canvas follows the grid's own cell edges. These grids start at 529000 / 179000, which is no multiple
    // of the 2000 m cell: a canvas snapped from the coordinate origin put every pixel centre on a cell edge.
    test("derived canvas follows the cell edges of a grid that does not start at the origin") {
        val offset = CustomGridSystem(GridConf(529000, 533000, 179000, 183000, 2, 4000, 4000, Some(27700)))
        val cells  = Seq((530000.0, 180000.0), (532000.0, 180000.0), (530000.0, 182000.0))
            .map { case (x, y) => offset.pointToCellID(x, y, 1) }
        val g = RST_Custom_RasterizeAgg.computeGridspec(offset, cells.iterator, 27700, None, "centroids", 0, 1)
        g shouldBe ((529000.0, 179000.0, 533000.0, 183000.0, 2, 2))
        val env = RST_Custom_RasterizeAgg.computeGridspec(offset, cells.iterator, 27700, None, "spatial_envelope", 0, 1)
        env shouldBe ((529000.0, 179000.0, 533000.0, 183000.0, 2, 2))
    }

    test("derived canvas pads interior cells by kring_pad on the cell lattice") {
        val offset = CustomGridSystem(GridConf(529000, 537000, 179000, 187000, 2, 8000, 8000, Some(27700)))
        // res 2 = 2000 m cells, 4 x 4; the cells (1,1), (2,1), (1,2) have every ring-1 neighbour in the grid.
        val cells = Seq((532000.0, 182000.0), (534000.0, 182000.0), (532000.0, 184000.0))
            .map { case (x, y) => offset.pointToCellID(x, y, 2) }
        val g = RST_Custom_RasterizeAgg.computeGridspec(offset, cells.iterator, 27700, None, "centroids", 1, 2)
        g shouldBe ((529000.0, 179000.0, 537000.0, 187000.0, 4, 4))
    }

    test("derived canvas at the grid edge pads with the cells that exist") {
        val offset = CustomGridSystem(GridConf(529000, 533000, 179000, 183000, 2, 4000, 4000, Some(27700)))
        val cells  = Seq((530000.0, 180000.0), (532000.0, 180000.0), (530000.0, 182000.0))
            .map { case (x, y) => offset.pointToCellID(x, y, 1) }
        // kRing runs one past the last cell; kRingInGrid stops at it
        offset.kRingInGrid(cells(1), 1).toSet shouldBe offset.kRingInGrid(cells(1), 1).toSet.filter(c =>
            offset.getCellPositionX(offset.getCellPosition(c), 1) < 2 && offset.getCellPositionY(offset.getCellPosition(c), 1) < 2)
        offset.kRingInGrid(cells(1), 1).size shouldBe 4
        val g = RST_Custom_RasterizeAgg.computeGridspec(offset, cells.iterator, 27700, None, "centroids", 1, 1)
        g shouldBe ((529000.0, 179000.0, 533000.0, 183000.0, 2, 2))
    }

    test("auto-grid burn on a grid that does not start at the origin fills the cells, not NoData") {
        val offset = CustomGridSystem(GridConf(529000, 533000, 179000, 183000, 2, 4000, 4000, Some(27700)))
        val pts    = Seq((530000.0, 180000.0), (532000.0, 180000.0), (530000.0, 182000.0))
        val cells  = pts.map { case (x, y) => offset.pointToCellID(x, y, 1) }
        val agg = RST_Custom_RasterizeAgg(
            cellidExpr    = Literal.create(null, LongType),
            valueExpr     = Literal(0.0),
            gridExpr      = Custom_Grid(Literal(529000L), Literal(533000L), Literal(179000L), Literal(183000L),
                Literal(2), Literal(4000), Literal(4000), Literal(27700)),
            outSridExpr   = Literal(27700),
            pixelSizeExpr = Literal.create(null, DoubleType),
            xminExpr      = Literal.create(null, DoubleType),
            yminExpr      = Literal.create(null, DoubleType),
            xmaxExpr      = Literal.create(null, DoubleType),
            ymaxExpr      = Literal.create(null, DoubleType),
            widthExpr     = Literal.create(null, IntegerType),
            heightExpr    = Literal.create(null, IntegerType),
            modeExpr      = Literal("centroids"),
            kringPadExpr  = Literal(0),
            exprConfExpr  = Literal.create(encodedEmpty(), StringType)
        )
        val buf = agg.createAggregationBuffer()
        cells.zipWithIndex.foreach { case (c, i) => agg.update(buf, c, (i + 1).toDouble) }
        val r = readRaster(agg.eval(buf).asInstanceOf[AnyRef])
        (r.width, r.height) shouldBe ((2, 2))
        r.gt(0) shouldBe 529000.0
        r.gt(3) shouldBe 183000.0
        r.data.toSeq shouldBe Seq(3.0, -9999.0, 1.0, 2.0) // north row first
    }

    test("auto-grid (extent omitted) at out_srid == grid CRS round-trips into the input cell set") {
        val cellIds = points.map { case (x, y) => grid.pointToCellID(x, y, res) }
        val cellSet = cellIds.toSet

        val agg = makeCustomAutoAgg(27700)
        val buf = agg.createAggregationBuffer()
        cellIds.zipWithIndex.foreach { case (c, i) => agg.update(buf, c, (i + 1).toDouble) }

        val r = readRaster(agg.eval(buf).asInstanceOf[AnyRef])
        var covered = 0
        var py = 0
        while (py < r.height) {
            var px = 0
            while (px < r.width) {
                if (r.data(py * r.width + px) != -9999.0) {
                    covered += 1
                    val cx = r.gt(0) + (0.5 + px) * r.gt(1) + (0.5 + py) * r.gt(2)
                    val cy = r.gt(3) + (0.5 + px) * r.gt(4) + (0.5 + py) * r.gt(5)
                    cellSet should contain(grid.pointToCellID(cx, cy, res))
                }
                px += 1
            }
            py += 1
        }
        covered should be >= cellSet.size
    }

    test("auto-grid (extent omitted) with out_srid != grid CRS exercises the reproject branch and round-trips") {
        val cellIds = points.map { case (x, y) => grid.pointToCellID(x, y, res) }
        val cellSet = cellIds.toSet

        val agg = makeCustomAutoAgg(3857) // grid CRS is 27700 -> computeGridspec reprojects samples to 3857
        val buf = agg.createAggregationBuffer()
        cellIds.zipWithIndex.foreach { case (c, i) => agg.update(buf, c, (i + 1).toDouble) }

        val r = readRaster(agg.eval(buf).asInstanceOf[AnyRef])

        // Raster is now in EPSG:3857; back-transform covered pixel centroids to the grid CRS (27700).
        val sr3857 = new org.gdal.osr.SpatialReference(); sr3857.ImportFromEPSG(3857)
        sr3857.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        val sr27700 = new org.gdal.osr.SpatialReference(); sr27700.ImportFromEPSG(27700)
        sr27700.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        val back = new org.gdal.osr.CoordinateTransformation(sr3857, sr27700)
        try {
            var covered = 0
            var py = 0
            while (py < r.height) {
                var px = 0
                while (px < r.width) {
                    if (r.data(py * r.width + px) != -9999.0) {
                        covered += 1
                        val x3857 = r.gt(0) + (0.5 + px) * r.gt(1) + (0.5 + py) * r.gt(2)
                        val y3857 = r.gt(3) + (0.5 + px) * r.gt(4) + (0.5 + py) * r.gt(5)
                        val p = back.TransformPoint(x3857, y3857)
                        cellSet should contain(grid.pointToCellID(p(0), p(1), res))
                    }
                    px += 1
                }
                py += 1
            }
            covered should be >= cellSet.size
        } finally {
            back.delete(); sr3857.delete(); sr27700.delete()
        }
    }
}
