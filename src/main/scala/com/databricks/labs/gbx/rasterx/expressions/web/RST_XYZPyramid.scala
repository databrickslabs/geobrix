package com.databricks.labs.gbx.rasterx.expressions.web

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDAL, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.BoundingBox
import com.databricks.labs.gbx.rasterx.tile.TileMath
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{CollectionGenerator, Expression, Literal}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

/** Generator: explode one source raster into one row per intersecting (z, x, y) tile across
 *  a zoom range.
 *
 *  Pattern-mirrors `RST_MakeTiles` -- extends `CollectionGenerator`, single-input row ->
 *  many output rows, codegen-fallback. Output schema is
 *  `STRUCT<z INT, x INT, y INT, bytes BINARY>`.
 *
 *  Internally calls `RST_TileXYZ.executeWithScale` per (z, x, y); the resulting bytes are
 *  PNG / JPEG / WEBP per the format argument. The intersection set is computed in WGS84 via
 *  [[BoundingBox.bbox]] -> [[TileMath.intersectingTiles]] (Y north-down).
 *
 *  The `rescale` parameter mirrors [[RST_TileXYZ.resolveScale]] semantics and is resolved
 *  ONCE from the source before the tile loop -- all tiles share one 8-bit mapping (no seams)
 *  and source stats are read a single time. Defaults to `"auto"`.
 *
 *  Guards:
 *  - `maxZ <= 20` (cell-count explodes beyond that).
 *  - Total tile-count across the zoom range <= 10^6, with a friendly error pointing at
 *    `maxZ` and at upstream resampling (`rst_to_webmercator`) for the typical fix.
 */
case class RST_XYZPyramid(
    tile: Expression,
    minZExpr: Expression,
    maxZExpr: Expression,
    formatExpr: Expression,
    sizeExpr: Expression,
    resamplingExpr: Expression,
    rescaleExpr: Expression,
    exprConfExpr: Expression = ExpressionConfigExpr()
) extends CollectionGenerator
      with Serializable
      with CodegenFallback {

    private def rasterType: DataType = RST_ExpressionUtil.rasterType(tile)
    /** Element schema is a single column "tile" wrapping the (z, x, y, bytes) struct --
     *  mirrors `RST_MakeTiles` so callers `select(rst_xyzpyramid(...).alias("t"))` and
     *  unpack via `t.tile.z`, `t.tile.bytes`, etc. */
    override def dataType: DataType = RST_XYZPyramid.tileStruct
    override def position: Boolean = false
    override def inline: Boolean = false
    override def elementSchema: StructType = RST_XYZPyramid.elementSchemaStatic
    override def children: Seq[Expression] =
        Seq(tile, minZExpr, maxZExpr, formatExpr, sizeExpr, resamplingExpr, rescaleExpr, exprConfExpr)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression =
        copy(nc(0), nc(1), nc(2), nc(3), nc(4), nc(5), nc(6), nc(7))

    override def eval(input: InternalRow): IterableOnce[InternalRow] =
        RST_ErrorHandler.safeEval(() => doEval(input), input, rasterType)

    private def doEval(input: InternalRow): IterableOnce[InternalRow] = {
        val exprConf = ExpressionConfig.fromExpr(exprConfExpr)
        RST_ExpressionUtil.init(exprConf)

        val rawTile = tile.eval(input).asInstanceOf[InternalRow]
        if (rawTile == null) return Iterator.empty

        val minZ = readInt(minZExpr.eval(input), "min_z")
        val maxZ = readInt(maxZExpr.eval(input), "max_z")
        require(minZ >= 0, s"rst_xyzpyramid: min_z must be >= 0; got $minZ")
        require(maxZ >= minZ, s"rst_xyzpyramid: max_z ($maxZ) must be >= min_z ($minZ)")
        require(
          maxZ <= TileMath.MAX_ZOOM,
          s"rst_xyzpyramid: max_z must be <= ${TileMath.MAX_ZOOM} (cell-count explosion at higher zooms); got $maxZ"
        )

        val format = Option(formatExpr.eval(input)).map(_.asInstanceOf[UTF8String].toString).getOrElse("PNG")
        val size = readInt(sizeExpr.eval(input), "size")
        val resampling = Option(resamplingExpr.eval(input)).map(_.asInstanceOf[UTF8String].toString).getOrElse("bilinear")
        val rescale = Option(rescaleExpr.eval(input)).map(_.asInstanceOf[UTF8String].toString).getOrElse("auto")

        val (_, ds, options) = RasterSerializationUtil.rowToTile(rawTile, rasterType)
        try {
            // Resolve the 8-bit rescale mapping ONCE from the source (stats read once; every
            // tile shares one mapping => no tile-to-tile seams). Mirrors the light tier.
            val scaleFlags = RST_TileXYZ.resolveScale(ds, rescale)

            // Compute source extent in WGS84 (lon/lat) once, then expand across zoom range.
            val bboxGeom = BoundingBox.bbox(ds, GDAL.WSG84)
            val env = bboxGeom.getEnvelopeInternal
            val lonMin = env.getMinX
            val lonMax = env.getMaxX
            val latMin = env.getMinY
            val latMax = env.getMaxY

            // Cell-count guard: sum intersecting tiles across [minZ, maxZ] without materializing.
            var total: Long = 0L
            var z = minZ
            while (z <= maxZ) {
                total += TileMath.intersectingTileCount(lonMin, latMin, lonMax, latMax, z)
                if (total > RST_XYZPyramid.MAX_TILE_COUNT) {
                    throw new IllegalArgumentException(
                      s"rst_xyzpyramid: tile-count across zoom range [$minZ, $maxZ] exceeds " +
                      s"${RST_XYZPyramid.MAX_TILE_COUNT} (raster extent is too large for that pyramid depth). " +
                      s"Lower max_z, or upstream-resample the raster before pyramidizing."
                    )
                }
                z += 1
            }

            // Emit (z, x, y, bytes) rows. We keep a single source `ds` open across all
            // tiles -- RST_TileXYZ.executeWithScale does not close it. The finally block
            // releases the source. The pre-resolved scaleFlags string is passed to every tile
            // so all tiles share one 8-bit mapping (no seams) and stats are read once.
            val rows = new ArrayBuffer[InternalRow](math.min(total, Int.MaxValue.toLong).toInt)
            var zi = minZ
            while (zi <= maxZ) {
                val tiles = TileMath.intersectingTiles(lonMin, latMin, lonMax, latMax, zi)
                var i = 0
                while (i < tiles.length) {
                    val (zz, xx, yy) = tiles(i)
                    val bytes = RST_TileXYZ.executeWithScale(ds, options, zz, xx, yy, format, size, resampling, scaleFlags)
                    val struct = InternalRow.fromSeq(Seq(zz, xx, yy, bytes))
                    rows += InternalRow.fromSeq(Seq(struct))
                    i += 1
                }
                zi += 1
            }
            rows.iterator
        } finally {
            RasterDriver.releaseDataset(ds)
        }
    }

    /** PySpark sends Python ints as LongType; SQL literals come in as IntegerType. Accept both. */
    private def readInt(v: Any, fieldName: String): Int = v match {
        case i: java.lang.Integer => i.intValue
        case l: java.lang.Long    => l.toInt
        case i: Int               => i
        case l: Long              => l.toInt
        case null                 => throw new IllegalArgumentException(s"rst_xyzpyramid: $fieldName is null")
        case other                => throw new IllegalArgumentException(s"rst_xyzpyramid: $fieldName must be Int/Long; got $other")
    }
}

/** Companion: SQL name, builder, output schema. */
object RST_XYZPyramid extends WithExpressionInfo {

    /** Maximum total candidate tiles across the requested zoom range. */
    val MAX_TILE_COUNT: Long = 1000000L

    /** The inner (z, x, y, bytes) struct produced per emitted tile. */
    val tileStruct: StructType = StructType(Seq(
        StructField("z", IntegerType, nullable = false),
        StructField("x", IntegerType, nullable = false),
        StructField("y", IntegerType, nullable = false),
        StructField("bytes", BinaryType, nullable = true)
    ))

    /** Generator element schema: a single column named "tile" wrapping the inner struct.
     *  Matches `RST_MakeTiles` so generator outputs are aliased once and unpacked via
     *  `t.tile.z`, `t.tile.bytes`, etc. */
    val elementSchemaStatic: StructType = StructType(Seq(
        StructField("tile", tileStruct, nullable = true)
    ))

    override def name: String = "gbx_rst_xyzpyramid"

    /** Builder: 3 to 7 args (tile, min_z, max_z, [format, [size, [resampling, [rescale]]]]). */
    override def builder(): FunctionBuilder = (c: Seq[Expression]) => {
        c.length match {
            case 3 => RST_XYZPyramid(c(0), c(1), c(2), Literal("PNG"), Literal(256), Literal("bilinear"), Literal("auto"))
            case 4 => RST_XYZPyramid(c(0), c(1), c(2), c(3), Literal(256), Literal("bilinear"), Literal("auto"))
            case 5 => RST_XYZPyramid(c(0), c(1), c(2), c(3), c(4), Literal("bilinear"), Literal("auto"))
            case 6 => RST_XYZPyramid(c(0), c(1), c(2), c(3), c(4), c(5), Literal("auto"))
            case 7 => RST_XYZPyramid(c(0), c(1), c(2), c(3), c(4), c(5), c(6))
            case n => throw new IllegalArgumentException(
                s"gbx_rst_xyzpyramid takes 3 to 7 arguments (tile, min_z, max_z, [format, [size, [resampling, [rescale]]]]); got $n"
            )
        }
    }
}
