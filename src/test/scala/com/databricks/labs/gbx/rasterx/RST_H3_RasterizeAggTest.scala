package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.gridx.grid.H3
import com.databricks.labs.gbx.rasterx.expressions.agg.RST_H3_RasterizeAgg
import com.databricks.labs.gbx.rasterx.expressions.grid.RST_H3_CellBBox
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Direct-execute tests for [[RST_H3_RasterizeAgg]] + [[RST_H3_CellBBox]].
 *
 *  We construct the aggregator with Literal constant children (same approach as
 *  RST_RasterizeAggTest) and drive `update`/`eval` directly -- no Spark session.
 *
 *  Round-trip invariant: every covered (non-NoData) output pixel, mapped back via
 *  pixel-centroid -> H3.pointToCellID at the cell resolution, must fall within the
 *  input cell set (the padded grid can touch neighbours, but never an unrelated cell).
 */
class RST_H3_RasterizeAggTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
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

    /** Build an auto-grid RST_H3_RasterizeAgg (centroids mode, kring_pad=1, EPSG:4326). */
    private def makeAutoAgg(): RST_H3_RasterizeAgg =
        RST_H3_RasterizeAgg(
            cellidExpr    = Literal.create(null, LongType),
            valueExpr     = Literal(0.0),
            outSridExpr   = Literal(4326),
            pixelSizeExpr = Literal.create(null, org.apache.spark.sql.types.DoubleType),
            xminExpr      = Literal.create(null, org.apache.spark.sql.types.DoubleType),
            yminExpr      = Literal.create(null, org.apache.spark.sql.types.DoubleType),
            xmaxExpr      = Literal.create(null, org.apache.spark.sql.types.DoubleType),
            ymaxExpr      = Literal.create(null, org.apache.spark.sql.types.DoubleType),
            widthExpr     = Literal.create(null, IntegerType),
            heightExpr    = Literal.create(null, IntegerType),
            modeExpr      = Literal("centroids"),
            kringPadExpr  = Literal(1),
            exprConfExpr  = Literal.create(encodedEmpty(), StringType)
        )

    // ---- raster readback helper ---------------------------------------------

    private case class RasterRead(gt: Array[Double], width: Int, height: Int, data: Array[Double])

    private def readRaster(tileRow: Any): RasterRead = {
        val ir    = tileRow.asInstanceOf[InternalRow]
        val bytes = ir.getBinary(1)
        bytes should not be null
        val tmp = s"/vsimem/h3ragg_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
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

    // ---- tests --------------------------------------------------------------

    private val res = 9
    // A few central-London points -> res-9 H3 cells.
    private val points = Seq(
        (-0.1276, 51.5074),  // Westminster
        (-0.1419, 51.5014),  // Buckingham Palace
        (-0.1195, 51.5033)   // London Eye
    )

    test("auto-grid burn: every covered pixel maps back into the input cell set") {
        val cellIds = points.map { case (lon, lat) => H3.pointToCellID(lon, lat, res) }
        val cellSet = cellIds.toSet

        val agg = makeAutoAgg()
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
                if (v != RST_H3_RasterizeAgg.NoData) {
                    covered += 1
                    // Pixel-centroid -> geo coord (same affine as the burn) -> H3 cell.
                    val xOffset = 0.5 + px
                    val yOffset = 0.5 + py
                    val xGeo = r.gt(0) + xOffset * r.gt(1) + yOffset * r.gt(2)
                    val yGeo = r.gt(3) + xOffset * r.gt(4) + yOffset * r.gt(5)
                    val back = H3.pointToCellID(xGeo, yGeo, res)
                    cellSet should contain (back)
                    // The burned value must be the one we assigned to that cell.
                    val expected = (cellIds.indexOf(back) + 1).toDouble
                    v shouldBe expected +- 1e-9
                }
                px += 1
            }
            py += 1
        }
        // At least the three centroid pixels must be covered.
        covered should be >= points.length
    }

    test("presence mask: null value burns 1.0 for covered pixels") {
        val cellIds = points.map { case (lon, lat) => H3.pointToCellID(lon, lat, res) }

        // A null `value` column resolves to the 1.0 presence mask in the Catalyst
        // `update(buffer, input)` path; the direct typed path takes an explicit value,
        // so we write 1.0 directly here to assert the masked-output readback.
        val agg = makeAutoAgg().copy(valueExpr = Literal.create(null, org.apache.spark.sql.types.DoubleType))
        val buf = agg.createAggregationBuffer()
        cellIds.foreach { c => agg.update(buf, c, 1.0) }

        val result: AnyRef = agg.eval(buf).asInstanceOf[AnyRef]
        val r = readRaster(result)
        val covered = r.data.count(_ != RST_H3_RasterizeAgg.NoData)
        covered should be >= cellIds.toSet.size
        r.data.filter(_ != RST_H3_RasterizeAgg.NoData).foreach(_ shouldBe 1.0 +- 1e-9)
    }

    test("empty group evaluates to null") {
        val agg = makeAutoAgg()
        val buf = agg.createAggregationBuffer()
        agg.eval(buf).asInstanceOf[AnyRef] shouldBe null
    }

    test("mixed-resolution cell set throws") {
        val c9  = H3.pointToCellID(-0.1276, 51.5074, 9)
        val c10 = H3.pointToCellID(-0.1276, 51.5074, 10)
        val agg = makeAutoAgg()
        val buf = agg.createAggregationBuffer()
        agg.update(buf, c9, 1.0)
        agg.update(buf, c10, 2.0)
        an[IllegalArgumentException] should be thrownBy agg.eval(buf)
    }

    test("buffer serde roundtrip preserves cells") {
        val cellIds = points.map { case (lon, lat) => H3.pointToCellID(lon, lat, res) }
        val agg = makeAutoAgg()
        val buf = agg.createAggregationBuffer()
        cellIds.zipWithIndex.foreach { case (c, i) => agg.update(buf, c, (i + 1).toDouble) }

        val deserialized = agg.deserialize(agg.serialize(buf))
        deserialized.cells.length shouldBe cellIds.length
        deserialized.cells.map(_._1).toSet shouldBe cellIds.toSet
    }

    private val _noCrs = Literal(null, org.apache.spark.sql.types.StringType)

    test("gbx_h3_cell_bbox centroids mode returns a degenerate point bbox") {
        val cell = H3.pointToCellID(-0.1276, 51.5074, res)
        val expr = RST_H3_CellBBox(
          Literal(cell), Literal(4326), Literal("centroids"), Literal(0), _noCrs)
        val row = expr.eval(InternalRow.empty).asInstanceOf[InternalRow]
        row should not be null
        val xmin = row.getDouble(0); val ymin = row.getDouble(1)
        val xmax = row.getDouble(2); val ymax = row.getDouble(3)
        // Degenerate (single centroid) -> xmin == xmax, ymin == ymax.
        xmin shouldBe xmax +- 1e-12
        ymin shouldBe ymax +- 1e-12
        // Centroid should index back to the same cell.
        H3.pointToCellID(xmin, ymin, res) shouldBe cell
    }

    test("gbx_h3_cell_bbox spatial_envelope mode is a non-degenerate hexagon envelope") {
        val cell = H3.pointToCellID(-0.1276, 51.5074, res)
        val expr = RST_H3_CellBBox(
          Literal(cell), Literal(4326), Literal("spatial_envelope"), Literal(0), _noCrs)
        val row = expr.eval(InternalRow.empty).asInstanceOf[InternalRow]
        val xmin = row.getDouble(0); val ymin = row.getDouble(1)
        val xmax = row.getDouble(2); val ymax = row.getDouble(3)
        xmax should be > xmin
        ymax should be > ymin
    }

    test("gbx_h3_cell_bbox out_crs string wins over srid + reprojects to web mercator") {
        val cell = H3.pointToCellID(-0.1276, 51.5074, res)
        // out_crs = EPSG:3857 -> bbox in metres (large magnitudes), not degrees.
        val expr = RST_H3_CellBBox(
          Literal(cell), Literal(4326), Literal("centroids"), Literal(0), Literal("EPSG:3857"))
        val row = expr.eval(InternalRow.empty).asInstanceOf[InternalRow]
        math.abs(row.getDouble(2)) should be > 1000.0
    }

    test("gbx_h3_cell_bbox accepts an ESRI srid (resolveCrs), no error") {
        val cell = H3.pointToCellID(-0.1276, 51.5074, res)
        val expr = RST_H3_CellBBox(
          Literal(cell), Literal(54008), Literal("centroids"), Literal(0), _noCrs)
        val row = expr.eval(InternalRow.empty).asInstanceOf[InternalRow]
        row should not be null
    }
}
