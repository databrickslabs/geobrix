package com.databricks.labs.gbx.rasterx.expressions

/** DTM from points and breaklines (Delaunay interpolation + rasterize).
 *
 * Registered as `gbx_rst_dtmfromgeoms(points, breaklines, merge_tolerance,
 * snap_tolerance, xmin, ymin, xmax, ymax, width_px, height_px, srid [, no_data])`.
 * The 12-arg form accepts an explicit no_data sentinel; the 11-arg form defaults
 * to -9999.0. Output is a single-band Float64 GTiff tile.
 */
import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.vectorx.jts.InterpolateElevation
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.locationtech.jts.geom.{Geometry, LineString}

case class RST_DTMFromGeoms(
    pointsArray: Expression,
    breaklinesArray: Expression,
    mergeTolerance: Expression,
    snapTolerance: Expression,
    xminExpr: Expression,
    yminExpr: Expression,
    xmaxExpr: Expression,
    ymaxExpr: Expression,
    widthPxExpr: Expression,
    heightPxExpr: Expression,
    outSridExpr: Expression,
    noDataExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        pointsArray, breaklinesArray, mergeTolerance, snapTolerance,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, outSridExpr, noDataExpr,
        ExpressionConfigExpr()
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_DTMFromGeoms.name
    override def replacement: Expression = invoke(RST_DTMFromGeoms)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10), nc(11))

}

object RST_DTMFromGeoms extends WithExpressionInfo {

    /** Default no-data sentinel (matches RST_GridFromPoints). */
    val DefaultNoData: Double = -9999.0

    // Int-args entry (Catalyst / SQL literals).
    def eval(
        pointsArray: ArrayData, breaklinesArray: ArrayData,
        mergeTolerance: Double, snapTolerance: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, out_srid: Int, noData: Double,
        conf: UTF8String
    ): InternalRow = doInvoke(
        pointsArray, breaklinesArray, mergeTolerance, snapTolerance,
        xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid, noData, conf)

    // Long-args entry (PySpark passes Python ints as Long).
    def eval(
        pointsArray: ArrayData, breaklinesArray: ArrayData,
        mergeTolerance: Double, snapTolerance: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Long, heightPx: Long, out_srid: Long, noData: Double,
        conf: UTF8String
    ): InternalRow = doInvoke(
        pointsArray, breaklinesArray, mergeTolerance, snapTolerance,
        xmin, ymin, xmax, ymax, widthPx.toInt, heightPx.toInt, out_srid.toInt, noData, conf)

    private def doInvoke(
        pointsArray: ArrayData, breaklinesArray: ArrayData,
        mergeTolerance: Double, snapTolerance: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, out_srid: Int, noData: Double,
        conf: UTF8String
    ): InternalRow =
        Option(
            RST_ErrorHandler.safeEval(
                () => {
                    val exprConf = ExpressionConfig.fromB64(conf.toString)
                    RST_ExpressionUtil.init(exprConf)
                    if (pointsArray == null) return null
                    val pts = geomsFromArrayData(pointsArray).toSeq
                    val lines = (if (breaklinesArray == null) Seq.empty[Geometry]
                                 else geomsFromArrayData(breaklinesArray).toSeq)
                        .map(_.asInstanceOf[LineString])
                    execute(pts, lines, mergeTolerance, snapTolerance,
                        xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid, noData)
                },
                null, BinaryType, conf
            )
        ).map(_.asInstanceOf[InternalRow]).orNull

    /** Decode an ARRAY of geometries; element may be BINARY (WKB) or STRING (WKT). */
    private def geomsFromArrayData(data: ArrayData): Array[Geometry] = {
        val n = data.numElements()
        val out = new Array[Geometry](n)
        var i = 0
        while (i < n) {
            if (!data.isNullAt(i)) {
                out(i) = data.get(i, null) match {
                    case b: Array[Byte] => JTS.fromWKB(b)
                    case s: UTF8String  => JTS.fromWKT(s.toString)
                    case other          => throw new IllegalArgumentException(
                        "rst_dtmfromgeoms: geometry array element must be BINARY (WKB) or STRING (WKT); " +
                        s"got ${if (other == null) "null" else other.getClass.getName}")
                }
            }
            i += 1
        }
        out.filter(_ != null)
    }

    /** Pure compute path shared by the non-agg expression and the aggregator.
     *  Builds a constrained-Delaunay TIN from `points` (+ optional `breaklines`),
     *  interpolates Z at the bbox cell centers, and writes a single-band Float64
     *  GTiff tile. Cells outside the triangulated hull are `noData`.
     */
    def execute(
        points: Seq[Geometry],
        breaklines: Seq[LineString],
        mergeTolerance: Double,
        snapTolerance: Double,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, out_srid: Int,
        noData: Double
    ): InternalRow = {
        // Materialize rootPath defensively
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        java.nio.file.Files.createDirectories(NodeFilePathUtil.rootPath)
        require(widthPx > 0,  s"rst_dtmfromgeoms: width_px must be positive; got $widthPx")
        require(heightPx > 0, s"rst_dtmfromgeoms: height_px must be positive; got $heightPx")
        require(xmax > xmin,  s"rst_dtmfromgeoms: xmax ($xmax) must be > xmin ($xmin)")
        require(ymax > ymin,  s"rst_dtmfromgeoms: ymax ($ymax) must be > ymin ($ymin)")
        require(points.nonEmpty, "rst_dtmfromgeoms: at least one point is required")

        val mp = JTS.multiPoint(points.toArray)
        mp.setSRID(out_srid)
        val grid = InterpolateElevation.pointGridBBox(xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid)
        // Pin to the conforming (Steiner) path: rst_dtmfromgeoms keeps its established behavior;
        // the constrained/conforming mode switch is scoped to the VectorX TIN expressions.
        val interpolated = InterpolateElevation.interpolate(
            mp, breaklines, grid, mergeTolerance, snapTolerance, None, "conforming")

        val ds = VectorRasterBridge.buildEmptyRaster(xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid, noData)
        try {
            val xRes = (xmax - xmin) / widthPx
            val yRes = (ymax - ymin) / heightPx
            val arr = Array.fill[Double](widthPx * heightPx)(noData)
            interpolated.foreach { p =>
                val col = math.floor((p.getX - xmin) / xRes).toInt
                val r   = math.floor((ymax - p.getY) / yRes).toInt
                if (col >= 0 && col < widthPx && r >= 0 && r < heightPx) {
                    arr(r * widthPx + col) = p.getCoordinate.getZ
                }
            }
            ds.GetRasterBand(1).WriteRaster(0, 0, widthPx, heightPx, arr)
            ds.FlushCache()
            tileRow(VectorRasterBridge.toGTiffBytes(ds))
        } finally {
            ds.delete()
        }
    }

    /** Build the (index_id, raster, metadata) tile row downstream serializers expect. */
    def tileRow(bytes: Array[Byte]): InternalRow = {
        val mtd = Map(
            "driver" -> "GTiff",
            "extension" -> "tif",
            "size" -> bytes.length.toString,
            "parentPath" -> "",
            "all_parents" -> "",
            "last_command" -> "gbx_rst_dtmfromgeoms"
        )
        // v2 8-field tile: cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata
        InternalRow.fromSeq(Seq(0L, bytes, null, null, null, null, null, SerializationUtil.toMapData[String, String](mtd)))
    }

    override def name: String = "gbx_rst_dtmfromgeoms"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        case 11 => RST_DTMFromGeoms(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10),
            Literal(DefaultNoData))
        case 12 => RST_DTMFromGeoms(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8), c(9), c(10), c(11))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_dtmfromgeoms takes 11 or 12 arguments (points_array, breaklines_array, merge_tolerance, " +
            s"snap_tolerance, xmin, ymin, xmax, ymax, width_px, height_px, out_srid, [no_data]); got $n")
    }

}
