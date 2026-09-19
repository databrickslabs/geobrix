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
import org.gdal.gdalconst.gdalconstConstants
import org.gdal.ogr.{FieldDefn, ogr}
import org.gdal.ogr.ogrConstants.{OFTInteger, wkbPolygon}
import org.gdal.osr.SpatialReference

import java.util.{Vector => JVector}
import scala.collection.mutable.ArrayBuffer

/** Reclassify a raster band into half-open `[breaks[i], breaks[i+1])` bins and
 *  return one polygon per contiguous same-bin region (4-connectivity).
 *
 *  Output: `ARRAY<STRUCT<geom_wkb BINARY, band INT, lower DOUBLE, upper DOUBLE>>`.
 *  One entry per contiguous region at 4-connectivity, where `band` is the
 *  zero-based bin index, `lower = breaks[band]`, `upper = breaks[band+1]`.
 *
 *  NoData pixels and pixels outside `[breaks[0], breaks[-1])` are excluded.
 *  `breaks` must be strictly ascending (length ≥ 2).
 */
case class RST_Isoband(
    tile: Expression,
    breaksExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] =
        Seq(tile, breaksExpr, ExpressionConfigExpr())

    // Pin breaks to ARRAY<DOUBLE> so ImplicitCastInputTypes coerces SQL literals
    // (Spark 4.0 infers array(0.0, 50.0, …) as ARRAY<DECIMAL>, not ARRAY<DOUBLE>;
    // without this override, breaksData.toDoubleArray() throws ClassCastException at
    // runtime and RST_ErrorHandler silently returns null).  Mirrors RST_Contour.
    override def inputTypes: Seq[DataType] =
        Seq(tile.dataType, ArrayType(DoubleType), StringType)

    override def dataType: DataType = ArrayType(
        StructType(Seq(
            StructField("geom_wkb", BinaryType),
            StructField("band", IntegerType),
            StructField("lower", DoubleType),
            StructField("upper", DoubleType)
        ))
    )

    override def nullable: Boolean = true
    override def prettyName: String = RST_Isoband.name
    override def replacement: Expression = invoke(RST_Isoband)

    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and dispatch entry points. */
object RST_Isoband extends WithExpressionInfo {

    def eval(tileRow: InternalRow, breaksData: ArrayData, conf: UTF8String): ArrayData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                val ds = RasterSerializationUtil.rowToDS(tileRow, BinaryType)
                val breaks = if (breaksData == null) Array.empty[Double]
                             else breaksData.toDoubleArray()
                try execute(ds, breaks, 1)
                finally RasterDriver.releaseDataset(ds)
            },
            tileRow,
            BinaryType,
            conf
          )
        ).map(_.asInstanceOf[ArrayData]).orNull

    /** Pure compute path — extracted for direct unit-testing without Spark.
     *
     *  Reclassifies `band` (1-based) of `srcDs` into bins defined by `breaks`:
     *  a pixel value `v` falls in bin `i` when `breaks[i] <= v < breaks[i+1]`.
     *  NoData pixels and values outside `[breaks[0], breaks[-1])` are excluded.
     *  One WKB Polygon is emitted per contiguous same-bin region (4-connectivity).
     *
     *  @param srcDs  source GDAL Dataset; not released by this method.
     *  @param breaks strictly ascending array of at least 2 break values.
     *  @param band   1-based band index (default 1).
     */
    def execute(srcDs: Dataset, breaks: Array[Double], band: Int = 1): ArrayData = {
        require(
            breaks.length >= 2 &&
              (0 until breaks.length - 1).forall(i => breaks(i) < breaks(i + 1)),
            "breaks must be strictly ascending"
        )

        val w = srcDs.GetRasterXSize
        val h = srcDs.GetRasterYSize
        val gt = Array.ofDim[Double](6)
        srcDs.GetGeoTransform(gt)

        val srcBand = srcDs.GetRasterBand(band)

        // Read source values as Float64 (GDAL converts from any native type).
        val vals = new Array[Double](w * h)
        srcBand.ReadRaster(0, 0, w, h, vals)

        // Read mask band: 0 = NoData/masked, 255 = valid.
        val maskBuf = new Array[Byte](w * h)
        srcBand.GetMaskBand().ReadRaster(0, 0, w, h, maskBuf)

        // Compute per-pixel bin index; -1 for excluded pixels.
        // binIdx = (count of breaks <= v) - 1, half-open [breaks[i], breaks[i+1]).
        val nBreaks = breaks.length
        val idxArr = new Array[Int](w * h)
        var p = 0
        while (p < w * h) {
            val masked = (maskBuf(p) & 0xff) == 0
            if (masked) {
                idxArr(p) = -1
            } else {
                val v = vals(p)
                var count = 0
                var j = 0
                while (j < nBreaks) {
                    if (breaks(j) <= v) count += 1
                    j += 1
                }
                val binIdx = count - 1
                if (binIdx >= 0 && binIdx <= nBreaks - 2) idxArr(p) = binIdx
                else idxArr(p) = -1
            }
            p += 1
        }

        // Build in-memory Int32 GDAL raster carrying the bin index.
        // NoData = -1 so GetMaskBand() returns 0 for excluded pixels.
        // Nested try/finally ensures both idxDs and outDs are released on every path
        // (including errors during OGR layer setup or WriteRaster).
        val memDrv = gdal.GetDriverByName("MEM")
        val idxDs = memDrv.Create("", w, h, 1, gdalconstConstants.GDT_Int32)
        try {
            idxDs.SetGeoTransform(gt)
            val srcProj = srcDs.GetProjection()
            if (srcProj != null && srcProj.nonEmpty) idxDs.SetProjection(srcProj)
            val idxBand = idxDs.GetRasterBand(1)
            idxBand.SetNoDataValue(-1.0)
            idxBand.WriteRaster(0, 0, w, h, w, h, gdalconstConstants.GDT_Int32, idxArr)
            idxBand.FlushCache()

            // Build in-memory OGR output layer (wkbPolygon, integer "band" field).
            GDALManager.initOgr()
            val ogrDriver = ogr.GetDriverByName("Memory")
            val outDs = ogrDriver.CreateDataSource("rst_isoband_out")
            val sr = new SpatialReference()
            try {
                val srcSrs = srcDs.GetSpatialRef
                val outSr = if (srcSrs != null) srcSrs else { sr.ImportFromEPSG(4326); sr }
                val outLayer = outDs.CreateLayer("isobands", outSr, wkbPolygon)
                val fd = new FieldDefn("band", OFTInteger)
                outLayer.CreateField(fd); fd.delete()

                // 4-connectivity is the GDAL Polygonize default; omit "8CONNECTED=8".
                val options = new JVector[String]()
                val maskBand = idxBand.GetMaskBand()

                // fieldIdx = 0 -> write bin index into the "band" field.
                gdal.Polygonize(idxBand, maskBand, outLayer, 0, options)
                outLayer.ResetReading()
                val rows = ArrayBuffer.empty[InternalRow]
                var feat = outLayer.GetNextFeature()
                while (feat != null) {
                    val geom = feat.GetGeometryRef()
                    if (geom != null) {
                        val wkb = geom.ExportToWkb()
                        val v = feat.GetFieldAsInteger(0)
                        // Guard: skip any stray feature with idx out of valid range.
                        if (v >= 0 && v <= nBreaks - 2) {
                            rows += InternalRow.fromSeq(Seq(wkb, v, breaks(v), breaks(v + 1)))
                        }
                    }
                    feat.delete()
                    feat = outLayer.GetNextFeature()
                }
                ArrayData.toArrayData(rows.toArray)
            } finally {
                outDs.delete()
                sr.delete()
            }
        } finally {
            idxDs.delete()
        }
    }

    override def name: String = "gbx_rst_isoband"

    /** Builder: exactly 2 arguments (tile, breaks). */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 2 => RST_Isoband(c(0), c(1))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_isoband takes 2 arguments (tile, breaks); got $n"
        )
    }

}
