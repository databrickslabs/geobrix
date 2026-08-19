package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, V2Tile, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, GridOptions, gdal}

import java.util.{Vector => JVector}

/**
  * Inverse-Distance-Weighted (IDW) interpolation of point samples to a raster
  * tile. Non-aggregator form - points are passed as arrays in a single row.
  *
  * The output is a single-band Float64 GTiff tile of shape `width_px x height_px`
  * covering the bounding box `(xmin, ymin) -> (xmax, ymax)` in the given SRID.
  * Points are interpolated via `gdal.Grid` using the
  * `invdist:power=<p>:max_points=<m>` algorithm; NoData = `-9999.0`.
  *
  * For the aggregator form (one point per row, grouped by extent) use
  * [[RST_GridFromPointsAgg]].
  */
case class RST_GridFromPoints(
    pointsArray: Expression,
    valuesArray: Expression,
    xminExpr: Expression,
    yminExpr: Expression,
    xmaxExpr: Expression,
    ymaxExpr: Expression,
    widthPxExpr: Expression,
    heightPxExpr: Expression,
    outSridExpr: Expression,
    powerExpr: Expression,
    maxPtsExpr: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        pointsArray, valuesArray,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, outSridExpr,
        powerExpr, maxPtsExpr,
        ExpressionConfigExpr()
    )
    override def inputTypes: Seq[DataType] = Seq(
        pointsArray.dataType, ArrayType(DoubleType, containsNull = false),
        DoubleType, DoubleType, DoubleType, DoubleType,
        IntegerType, IntegerType, IntegerType,
        DoubleType, IntegerType,
        StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_GridFromPoints.name
    override def replacement: Expression = invoke(RST_GridFromPoints)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10))

}

object RST_GridFromPoints extends WithExpressionInfo {

    /** Default IDW exponent - same default as the gdal_grid CLI. */
    val DefaultPower: Double = 2.0
    /** Default neighbours considered per output cell. */
    val DefaultMaxPoints: Int = 12

    // Int-args entry point used by Catalyst for non-PySpark callers.
    def eval (
        pointsArray: ArrayData, valuesArray: ArrayData,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, out_srid: Int,
        power: Double, maxPts: Int,
        conf: UTF8String
    ): InternalRow = doInvoke(
        pointsArray, valuesArray,
        xmin, ymin, xmax, ymax,
        widthPx, heightPx, out_srid,
        power, maxPts,
        conf
    )

    // Long-args entry point used by PySpark (Python ints arrive as Long).
    def eval (
        pointsArray: ArrayData, valuesArray: ArrayData,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Long, heightPx: Long, out_srid: Long,
        power: Double, maxPts: Long,
        conf: UTF8String
    ): InternalRow = doInvoke(
        pointsArray, valuesArray,
        xmin, ymin, xmax, ymax,
        widthPx.toInt, heightPx.toInt, out_srid.toInt,
        power, maxPts.toInt,
        conf
    )

    private def doInvoke(
        pointsArray: ArrayData, valuesArray: ArrayData,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, out_srid: Int,
        power: Double, maxPts: Int,
        conf: UTF8String
    ): InternalRow =
        Option(
            RST_ErrorHandler.safeEval(
                () => {
                    val exprConf = ExpressionConfig.fromB64(conf.toString)
                    RST_ExpressionUtil.init(exprConf)
                    if (pointsArray == null || valuesArray == null) return null
                    val geoms = geomsFromArrayData(pointsArray)
                    val values = valuesArray.toDoubleArray()
                    val features = featuresFromGeomsAndValues(geoms, values)
                    execute(features, xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid, power, maxPts)
                },
                null,
                BinaryType,
                conf
            )
        ).map(_.asInstanceOf[InternalRow]).orNull

    /** Walk ArrayData; first non-null element determines WKB vs WKT encoding. */
    private[grid] def geomsFromArrayData(data: ArrayData): Array[org.locationtech.jts.geom.Geometry] = {
        val n = data.numElements()
        val out = new Array[org.locationtech.jts.geom.Geometry](n)
        var i = 0
        while (i < n) {
            if (!data.isNullAt(i)) {
                val elem = data.get(i, null) // get with null DataType pulls raw object
                out(i) = elem match {
                    case b: Array[Byte] => JTS.fromWKB(b)
                    case s: UTF8String  => JTS.fromWKT(s.toString)
                    case other          => throw new IllegalArgumentException(
                        s"rst_gridfrompoints: point array element must be BINARY (WKB) or STRING (WKT); " +
                            s"got ${if (other == null) "null" else other.getClass.getName}")
                }
            }
            i += 1
        }
        out
    }

    /** Convert parallel arrays into the (wkb, value) tuples consumed by `VectorRasterBridge.buildOgrLayer`. */
    def featuresFromGeomsAndValues(
        geoms: Array[org.locationtech.jts.geom.Geometry], values: Array[Double]
    ): Seq[(Array[Byte], Double)] = {
        require(geoms.length == values.length,
            s"rst_gridfrompoints: points (${geoms.length}) and values (${values.length}) length mismatch")
        val out = scala.collection.mutable.ArrayBuffer.empty[(Array[Byte], Double)]
        var i = 0
        while (i < geoms.length) {
            val g = geoms(i)
            if (g != null && !g.isEmpty) {
                out += ((JTS.toWKB(g), values(i)))
            }
            i += 1
        }
        out.toSeq
    }

    /** Pure compute path - direct-execute-friendly. Returns a tile InternalRow (cellid, bytes, metadata). */
    def execute(
        features: Seq[(Array[Byte], Double)],
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, out_srid: Int,
        power: Double, maxPts: Int
    ): InternalRow = {
        // Materialize rootPath defensively - same /vsimem prep pattern as RST_Rasterize.
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        java.nio.file.Files.createDirectories(NodeFilePathUtil.rootPath)

        require(widthPx > 0, s"rst_gridfrompoints: width_px must be positive; got $widthPx")
        require(heightPx > 0, s"rst_gridfrompoints: height_px must be positive; got $heightPx")
        require(xmax > xmin, s"rst_gridfrompoints: xmax ($xmax) must be > xmin ($xmin)")
        require(ymax > ymin, s"rst_gridfrompoints: ymax ($ymax) must be > ymin ($ymin)")
        require(power > 0.0, s"rst_gridfrompoints: power must be positive; got $power")
        require(maxPts > 0, s"rst_gridfrompoints: max_pts must be positive; got $maxPts")

        if (features.isEmpty) {
            // No points -> return an empty NoData raster of the requested shape.
            val empty = VectorRasterBridge.buildEmptyRaster(
                xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid)
            empty.FlushCache()
            val bytes = VectorRasterBridge.toGTiffBytes(empty)
            empty.delete()
            return tileRow(bytes)
        }

        // gdal.Grid expects a raster-Dataset-typed handle on its source even though
        // gdal_grid is fundamentally a vector-to-raster operation. The Memory OGR
        // driver doesn't roundtrip back through `gdal.OpenEx(..., GDAL_OF_VECTOR)`,
        // so materialize the features as a /vsimem GeoJSON, then re-open as a
        // vector Dataset for the Grid call. Cheap for the per-tile point counts
        // IDW is practical for (~thousands of points).
        val uid = java.util.UUID.randomUUID().toString.replace("-", "")
        val srcPath = s"/vsimem/gbx_idw_src_$uid.geojson"
        writeGeoJson(srcPath, features, out_srid)
        val outPath = s"/vsimem/gbx_idw_$uid.tif"
        try {
            // GDAL_OF_VECTOR = 0x04 in gdal.h; the Java binding exposes it via gdalconst.
            val srcDs: Dataset = gdal.OpenEx(srcPath, org.gdal.gdalconst.gdalconstConstants.OF_VECTOR.toLong)
            if (srcDs == null) {
                throw new RuntimeException(
                    s"rst_gridfrompoints: failed to open temp GeoJSON source: ${gdal.GetLastErrorMsg()}")
            }
            try {
                val opts = new JVector[String]()
                opts.add("-of"); opts.add("GTiff")
                opts.add("-a"); opts.add(s"invdist:power=$power:max_points=$maxPts:nodata=-9999.0")
                opts.add("-zfield"); opts.add(VectorRasterBridge.ValueFieldName)
                opts.add("-txe"); opts.add(xmin.toString); opts.add(xmax.toString)
                opts.add("-tye"); opts.add(ymin.toString); opts.add(ymax.toString)
                opts.add("-outsize"); opts.add(widthPx.toString); opts.add(heightPx.toString)
                opts.add("-ot"); opts.add("Float64")
                val gridOpts = new GridOptions(opts)
                val result: Dataset =
                    try {
                        gdal.Grid(outPath, srcDs, gridOpts)
                    } finally {
                        gridOpts.delete()
                    }
                val errMsg = gdal.GetLastErrorMsg()
                if (result == null) {
                    throw new RuntimeException(
                        s"gdal.Grid(invdist) failed: " +
                            (if (errMsg == null || errMsg.isEmpty) "<no error>" else errMsg))
                }
                try {
                    result.FlushCache()
                    val bytes = VectorRasterBridge.toGTiffBytes(result)
                    tileRow(bytes)
                } finally {
                    result.delete()
                }
            } finally {
                srcDs.delete()
            }
        } finally {
            gdal.Unlink(srcPath)
            gdal.Unlink(outPath)
        }
    }

    /** Write (geom_wkb, value) tuples to a /vsimem GeoJSON file via the OGR GeoJSON driver. */
    private def writeGeoJson(
        path: String, features: Seq[(Array[Byte], Double)], srid: Int
    ): Unit = {
        import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
        import org.gdal.ogr.{Feature, FieldDefn, Geometry => OgrGeom, ogr}
        import org.gdal.ogr.ogrConstants.{OFTReal, wkbPoint}
        GDALManager.initOgr()
        val driver = ogr.GetDriverByName("GeoJSON")
        val ds = driver.CreateDataSource(path)
        // Resolve via the shared rule (ESRI codes classify correctly, not just EPSG).
        val sr = SpatialRefOps.resolveCrs(srid.toString)
        val layer = ds.CreateLayer("features", sr, wkbPoint)
        val fd = new FieldDefn(VectorRasterBridge.ValueFieldName, OFTReal)
        layer.CreateField(fd); fd.delete()
        val defn = layer.GetLayerDefn()
        features.foreach { case (wkb, v) =>
            val feat = new Feature(defn)
            val g = OgrGeom.CreateFromWkb(wkb)
            if (g != null) {
                feat.SetGeometry(g)
                feat.SetField(VectorRasterBridge.ValueFieldName, v)
                layer.CreateFeature(feat)
                g.delete()
            }
            feat.delete()
        }
        sr.delete()
        ds.FlushCache()
        ds.delete()
    }

    /** Build the (cellid, bytes, metadata) InternalRow that downstream serializers expect. */
    def tileRow(bytes: Array[Byte]): InternalRow = {
        val mtd = Map(
            "driver" -> "GTiff",
            "extension" -> "tif",
            "size" -> bytes.length.toString,
            "parentPath" -> "",
            "all_parents" -> "",
            "last_command" -> "gdal.Grid(invdist)"
        )
        val mapData = SerializationUtil.toMapData[String, String](mtd)
        V2Tile.row(cellid = 0L, raster = bytes, metadata = mapData)
    }

    override def name: String = "gbx_rst_gridfrompoints"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => c.length match {
        // 9-arg form: defaults for power=2.0, max_pts=12.
        case 9 => RST_GridFromPoints(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8),
            Literal(DefaultPower), Literal(DefaultMaxPoints))
        case 10 => RST_GridFromPoints(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8),
            c(9), Literal(DefaultMaxPoints))
        case 11 => RST_GridFromPoints(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7), c(8),
            c(9), c(10))
        case n => throw new IllegalArgumentException(
            s"gbx_rst_gridfrompoints takes 9 to 11 arguments " +
            s"(points_array, values_array, xmin, ymin, xmax, ymax, width_px, height_px, out_srid, [power, [max_pts]]); got $n"
        )
    }
}
