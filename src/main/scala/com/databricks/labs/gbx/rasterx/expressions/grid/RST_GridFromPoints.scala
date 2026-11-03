package com.databricks.labs.gbx.rasterx.expressions.grid

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.rasterx.operations.SpatialRefOps
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, V2Tile, VectorRasterBridge}
import com.databricks.labs.gbx.util.SerializationUtil
import com.databricks.labs.gbx.vectorx.jts.JTS
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Coalesce, Expression, If, IsNull, Literal, Or}
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
    maxPtsExpr: Expression,
    outCrsExpr: Expression = Literal.create(null, StringType)
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(
        pointsArray, valuesArray,
        xminExpr, yminExpr, xmaxExpr, ymaxExpr,
        widthPxExpr, heightPxExpr, outSridExpr,
        powerExpr, maxPtsExpr, outCrsExpr,
        ExpressionConfigExpr()
    )
    override def inputTypes: Seq[DataType] = Seq(
        pointsArray.dataType, ArrayType(DoubleType, containsNull = false),
        DoubleType, DoubleType, DoubleType, DoubleType,
        IntegerType, IntegerType, IntegerType,
        DoubleType, IntegerType, StringType,
        StringType
    )
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(BinaryType)
    override def nullable: Boolean = true
    override def prettyName: String = RST_GridFromPoints.name
    // optional NULL arguments (out_srid, out_crs, power, max_pts) mean unset: a NULL must reach eval, which guards the arrays.
    // A NULL required scalar (extent, size) is NULL: propagateNull = false would hand a primitive parameter 0 instead,
    // so it is tested here.
    override def replacement: Expression = {
        val required = Seq(xminExpr, yminExpr, xmaxExpr, ymaxExpr, widthPxExpr, heightPxExpr)
        If(required.map(IsNull(_): Expression).reduce(Or(_, _)), Literal.create(null, dataType),
           invoke(RST_GridFromPoints, propagateNull = false))
    }
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7), nc(8), nc(9), nc(10), nc(11))

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
        power: Double, maxPts: Int, outCrs: UTF8String,
        conf: UTF8String
    ): InternalRow = doInvoke(
        pointsArray, valuesArray,
        xmin, ymin, xmax, ymax,
        widthPx, heightPx, out_srid,
        power, maxPts, outCrs,
        conf
    )

    // Long-args entry point used by PySpark (Python ints arrive as Long).
    def eval (
        pointsArray: ArrayData, valuesArray: ArrayData,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Long, heightPx: Long, out_srid: Long,
        power: Double, maxPts: Long, outCrs: UTF8String,
        conf: UTF8String
    ): InternalRow = doInvoke(
        pointsArray, valuesArray,
        xmin, ymin, xmax, ymax,
        widthPx.toInt, heightPx.toInt, out_srid.toInt,
        power, maxPts.toInt, outCrs,
        conf
    )

    /** What the builder passes for an `out_srid` that is NULL: no EPSG code, so it never equals a user's 0. */
    val NoSrid: Int = Int.MinValue

    /** The output CRS as text: `out_crs` wins over `out_srid`; both set (an `out_srid` of 0 counts) raises; neither (a
      * NULL or 0 `out_srid`) is no CRS (null). The two never reach `safeEval`, which would turn the refusal into an
      * error tile. */
    def outputCrs(outSrid: Int, outCrs: UTF8String): String =
        if (outCrs != null && outSrid != NoSrid) {
            throw new IllegalArgumentException("rst_gridfrompoints: provide out_srid OR out_crs, not both")
        } else if (outCrs != null) outCrs.toString
        else if (outSrid != 0 && outSrid != NoSrid) outSrid.toString
        else null

    /** The arguments that need no raster, checked before `safeEval` so that a wrong one names its value. */
    private def requireArguments(
        points: ArrayData, values: ArrayData,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, power: Double, maxPts: Int
    ): Unit = {
        if (widthPx <= 0 || heightPx <= 0) {
            throw new IllegalArgumentException(
                s"rst_gridfrompoints: width_px and height_px must be above 0 (got $widthPx x $heightPx)")
        }
        if (!(xmax > xmin) || !(ymax > ymin)) {
            throw new IllegalArgumentException(
                s"rst_gridfrompoints: the extent must have xmax above xmin and ymax above ymin (got x $xmin..$xmax, y $ymin..$ymax)")
        }
        if (points.numElements() != values.numElements()) {
            throw new IllegalArgumentException(
                s"rst_gridfrompoints: points (${points.numElements()}) and values (${values.numElements()}) length mismatch")
        }
        if (!(power > 0.0)) throw new IllegalArgumentException(s"rst_gridfrompoints: power must be positive; got $power")
        if (maxPts <= 0) throw new IllegalArgumentException(s"rst_gridfrompoints: max_pts must be positive; got $maxPts")
    }

    private def doInvoke(
        pointsArray: ArrayData, valuesArray: ArrayData,
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, out_srid: Int,
        power: Double, maxPts: Int, outCrs: UTF8String,
        conf: UTF8String
    ): InternalRow =
        if (pointsArray == null || valuesArray == null) null
        else {
            requireArguments(pointsArray, valuesArray, xmin, ymin, xmax, ymax, widthPx, heightPx, power, maxPts)
            val crs = outputCrs(out_srid, outCrs)
            RST_ExpressionUtil.init(ExpressionConfig.fromB64(conf.toString))
            if (crs != null) SpatialRefOps.resolveCrs(crs).delete() // an unknown CRS raises here, naming the value
            Option(
                RST_ErrorHandler.safeEval(
                    () => {
                        val geoms = geomsFromArrayData(pointsArray)
                        val values = valuesArray.toDoubleArray()
                        val features = featuresFromGeomsAndValues(geoms, values)
                        execute(features, xmin, ymin, xmax, ymax, widthPx, heightPx, crs, power, maxPts)
                    },
                    null,
                    BinaryType,
                    conf
                )
            ).map(_.asInstanceOf[InternalRow]).orNull
        }

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

    /** [[execute]] with the output CRS as an EPSG or ESRI code. */
    def execute(
        features: Seq[(Array[Byte], Double)],
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, out_srid: Int,
        power: Double, maxPts: Int
    ): InternalRow = execute(features, xmin, ymin, xmax, ymax, widthPx, heightPx, out_srid.toString, power, maxPts)

    /** Pure compute path - direct-execute-friendly. Returns a tile InternalRow (cellid, bytes, metadata). */
    def execute(
        features: Seq[(Array[Byte], Double)],
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, crs: String,
        power: Double, maxPts: Int
    ): InternalRow = {
        // Materialize rootPath defensively - same /vsimem prep pattern as the vector bridge.
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
            val empty = VectorRasterBridge.buildEmptyRasterCrs(
                xmin, ymin, xmax, ymax, widthPx, heightPx, crs, -9999.0)
            empty.FlushCache()
            val bytes = VectorRasterBridge.toGTiffBytes(empty)
            empty.delete()
            return tileRow(bytes)
        }

        // gdal.Grid's invdist reads max_points only inside a search ellipse, and then takes the first points in input
        // order, not the nearest: the contract (a pixel is the weighted mean of its `max_pts` NEAREST points) is
        // computed here, from a nearest-neighbour index, whenever the limit cuts the points.
        if (maxPts < features.size) return nearestIdw(features, xmin, ymin, xmax, ymax, widthPx, heightPx, crs, power, maxPts)

        // gdal.Grid expects a raster-Dataset-typed handle on its source even though
        // gdal_grid is fundamentally a vector-to-raster operation. The Memory OGR
        // driver doesn't roundtrip back through `gdal.OpenEx(..., GDAL_OF_VECTOR)`,
        // so materialize the features as a /vsimem GeoJSON, then re-open as a
        // vector Dataset for the Grid call. Cheap for the per-tile point counts
        // IDW is practical for (~thousands of points).
        val uid = java.util.UUID.randomUUID().toString.replace("-", "")
        val srcPath = s"/vsimem/gbx_idw_src_$uid.geojson"
        writeGeoJson(srcPath, features, crs)
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
                    // the GeoJSON source reads as WGS84 when it names no CRS, and gdal.Grid copies that onto the grid
                    if (crs == null) result.SetProjection("")
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

    /** Inverse-distance weighting over each pixel centre's `k` nearest points (weight 1 / distance ** power; a point on
      * the centre gives its value). Float64, NoData -9999, on the same grid as the `gdal.Grid` path. */
    private def nearestIdw(
        features: Seq[(Array[Byte], Double)],
        xmin: Double, ymin: Double, xmax: Double, ymax: Double,
        widthPx: Int, heightPx: Int, crs: String,
        power: Double, k: Int
    ): InternalRow = {
        import org.locationtech.jts.geom.Envelope
        import org.locationtech.jts.index.strtree.{ItemBoundable, ItemDistance, STRtree}
        val tree = new STRtree(10)
        features.foreach { case (wkb, v) =>
            val c = JTS.fromWKB(wkb).getCoordinate
            tree.insert(new Envelope(c), (c.x, c.y, v))
        }
        tree.build()
        val metric = new ItemDistance {
            override def distance(a: ItemBoundable, b: ItemBoundable): Double = {
                val p = a.getItem.asInstanceOf[(Double, Double, Double)]
                val q = b.getItem.asInstanceOf[(Double, Double, Double)]
                math.hypot(p._1 - q._1, p._2 - q._2)
            }
        }
        val xRes = (xmax - xmin) / widthPx
        val yRes = (ymax - ymin) / heightPx
        val out = Array.fill[Double](widthPx * heightPx)(-9999.0)
        var row = 0
        while (row < heightPx) {
            val cy = ymax - (row + 0.5) * yRes
            var col = 0
            while (col < widthPx) {
                val cx = xmin + (col + 0.5) * xRes
                val near = tree.nearestNeighbour(new Envelope(cx, cx, cy, cy), (cx, cy, 0.0), metric, k)
                var sumW = 0.0
                var sumV = 0.0
                var exact = Double.NaN
                near.foreach { item =>
                    val (px, py, v) = item.asInstanceOf[(Double, Double, Double)]
                    val d = math.hypot(px - cx, py - cy)
                    if (d == 0.0) exact = v
                    else {
                        val w = 1.0 / math.pow(d, power)
                        sumW += w
                        sumV += w * v
                    }
                }
                out(row * widthPx + col) = if (!exact.isNaN) exact else if (sumW > 0.0) sumV / sumW else -9999.0
                col += 1
            }
            row += 1
        }
        val ds = VectorRasterBridge.buildEmptyRasterCrs(xmin, ymin, xmax, ymax, widthPx, heightPx, crs, -9999.0)
        try {
            ds.GetRasterBand(1).WriteRaster(0, 0, widthPx, heightPx, out)
            ds.FlushCache()
            tileRow(VectorRasterBridge.toGTiffBytes(ds))
        } finally {
            ds.delete()
        }
    }

    /** Write (geom_wkb, value) tuples to a /vsimem GeoJSON file via the OGR GeoJSON driver. */
    private def writeGeoJson(
        path: String, features: Seq[(Array[Byte], Double)], crs: String
    ): Unit = {
        import org.gdal.ogr.{Feature, FieldDefn, Geometry => OgrGeom, ogr}
        import org.gdal.ogr.ogrConstants.{OFTReal, wkbPoint}
        GDALManager.initOgr()
        val driver = ogr.GetDriverByName("GeoJSON")
        val ds = driver.CreateDataSource(path)
        // Resolve via the shared rule (ESRI codes classify correctly, not just EPSG).
        val sr = if (crs == null) null else SpatialRefOps.resolveCrs(crs)
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
        if (sr != null) sr.delete()
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

    /** 8 to 12 arguments; a NULL `out_srid`, `power` or `max_pts` is unset (no CRS, 2.0, 12): the SQL default NULL is
      * coalesced here because a NULL reaching a primitive parameter would otherwise arrive as 0 (`out_srid` becomes
      * [[NoSrid]], so that an explicit 0 stays distinct from unset). */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) =>
        if (c.length < 8 || c.length > 12) {
            throw new IllegalArgumentException(
                s"gbx_rst_gridfrompoints takes 8 to 12 arguments " +
                s"(points_array, values_array, xmin, ymin, xmax, ymax, width_px, height_px, [out_srid, [power, [max_pts, [out_crs]]]]); got ${c.length}")
        } else {
            def opt(i: Int, default: Expression): Expression = if (c.length > i) Coalesce(Seq(c(i), default)) else default
            RST_GridFromPoints(c(0), c(1), c(2), c(3), c(4), c(5), c(6), c(7),
                opt(8, Literal(NoSrid)), opt(9, Literal(DefaultPower)), opt(10, Literal(DefaultMaxPoints)),
                if (c.length > 11) c(11) else Literal.create(null, StringType))
        }
}
