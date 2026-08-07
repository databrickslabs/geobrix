package com.databricks.labs.gbx.rasterx.expressions.vector

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.ogr.{FieldDefn, ogr}
import org.gdal.ogr.ogrConstants.{OFTReal, wkbPolygon}
import org.gdal.osr.SpatialReference

import java.util.{Vector => JVector}
import scala.collection.mutable.ArrayBuffer

/** Extract vector polygons from a raster tile's contiguous value regions.
 *
 *  Returns `ARRAY<struct(geom_wkb BINARY, value DOUBLE)>`, one entry per
 *  connected component of equal pixel values. NoData pixels are excluded via
 *  the band's mask.
 *
 *  Optional arguments:
 *    - `band` (default 1) - 1-based raster band index to polygonize.
 *    - `connectedness` (default 4) - either 4 or 8; GDAL `8CONNECTED` option.
 */
case class RST_Polygonize(
    tile: Expression,
    bandExpr: Expression,
    connectednessExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] =
        Seq(tile, bandExpr, connectednessExpr, ExpressionConfigExpr())
    override def dataType: DataType = ArrayType(
        StructType(Seq(
            StructField("geom_wkb", BinaryType),
            StructField("value", DoubleType)
        ))
    )
    override def nullable: Boolean = true
    override def prettyName: String = RST_Polygonize.name
    override def replacement: Expression = invoke(RST_Polygonize)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2))

}

/** Companion: SQL name, builder, and dispatch entry points.
 *
 *  PySpark sends Python ints as `LongType` - we expose both Int and Long
 *  overloads for the `band` / `connectedness` args. Per Wave 3 finding.
 */
object RST_Polygonize extends WithExpressionInfo {

    def eval(row: InternalRow, band: Int, connectedness: Int, conf: UTF8String): ArrayData =
        doInvoke(row, band, connectedness, conf, BinaryType)
    def eval(row: InternalRow, band: Long, connectedness: Long, conf: UTF8String): ArrayData =
        doInvoke(row, band.toInt, connectedness.toInt, conf, BinaryType)

    private def doInvoke(
        row: InternalRow, band: Int, connectedness: Int,
        conf: UTF8String, rdt: DataType
    ): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(row, rdt)
                try execute(ds, band, connectedness)
                finally RasterDriver.releaseDataset(ds)
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    /** Pure compute path - extracted for direct unit-testing without Spark. */
    def execute(ds: Dataset, band: Int, connectedness: Int): ArrayData = {
        require(band >= 1 && band <= ds.GetRasterCount, s"rst_polygonize: band must be in [1, ${ds.GetRasterCount}]; got $band")
        require(connectedness == 4 || connectedness == 8,
            s"rst_polygonize: connectedness must be 4 or 8; got $connectedness")
        val srcBand = ds.GetRasterBand(band)
        val maskBand = srcBand.GetMaskBand()

        // Build an in-memory OGR layer to receive the output polygons.
        GDALManager.initOgr()
        val ogrDriver = ogr.GetDriverByName("Memory")
        val outDs = ogrDriver.CreateDataSource("rst_polygonize_out")
        val sr = new SpatialReference()
        // Inherit the raster's SRS if any; else leave it null (still valid for export).
        val srcSrs = ds.GetSpatialRef
        val outSr = if (srcSrs != null) srcSrs else { sr.ImportFromEPSG(4326); sr }
        val outLayer = outDs.CreateLayer("polygons", outSr, wkbPolygon)
        val fd = new FieldDefn("value", OFTReal)
        outLayer.CreateField(fd); fd.delete()

        val options = new JVector[String]()
        if (connectedness == 8) options.add("8CONNECTED=8")

        try {
            // fieldIdx = 0 -> write pixel value into the "value" field we just created.
            gdal.Polygonize(srcBand, maskBand, outLayer, 0, options)
            outLayer.ResetReading()
            val rows = ArrayBuffer.empty[InternalRow]
            var feat = outLayer.GetNextFeature()
            while (feat != null) {
                val geom = feat.GetGeometryRef()
                if (geom != null) {
                    val wkb = geom.ExportToWkb()
                    val v = feat.GetFieldAsDouble(0)
                    rows += InternalRow.fromSeq(Seq(wkb, v))
                }
                feat.delete()
                feat = outLayer.GetNextFeature()
            }
            ArrayData.toArrayData(rows.toArray)
        } finally {
            outDs.delete()
            sr.delete()
        }
    }

    override def name: String = "gbx_rst_polygonize"

    /** Builder: 1 to 3 args (tile, [band, [connectedness]]). */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 1 => RST_Polygonize(c(0), Literal(1), Literal(4))
        case 2 => RST_Polygonize(c(0), c(1), Literal(4))
        case 3 => RST_Polygonize(c(0), c(1), c(2))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_polygonize takes 1 to 3 arguments (tile, [band, [connectedness]]); got $n"
        )
    }

}
