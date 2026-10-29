package com.databricks.labs.gbx.kernels.spec

import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil, V2Tile, VirtualTileException}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.{Expression, ImplicitCastInputTypes}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/** The GDAL spec engine as one Catalyst expression: every scalar row with a `gdal` block, by its SQL name.
  *
  * The spec comes from the JAR's copy of the manifest ([[GdalSpecs]]); the children are the function's arguments,
  * cast to its session types. Each row runs [[GdalSpecEngine.run]] (a new algorithm per row; no state shared but
  * GDAL's per-thread last error), so the work is GDAL-bound and codegen buys nothing.
  *
  * The error map is the light tier's (`engine/l3.py`): a NULL input or a NULL-result argument gives NULL; an error
  * tile in comes out unchanged; a tile with neither raster nor path is NULL; GDAL's run failing, or a `bad: data`
  * value, gives an error tile (`last_error` = `error_message` = the reason); a refused value raises. A virtual tile
  * never reaches here through the manager's wrapper; called directly, it is refused as every heavy function
  * refuses one.
  */
case class GdalSpecExpr(sqlName: String, children: Seq[Expression]) extends Expression with ImplicitCastInputTypes
    with CodegenFallback {

    @transient private lazy val fn: SpecFunction = GdalSpecs.function(sqlName)
    private lazy val tiles: Boolean = GdalSpecs.function(sqlName).gdal.inputs.head._2 == "rasters"

    override def inputTypes: Seq[DataType] =
        children.head.dataType +: GdalSpecs.function(sqlName).params.tail.take(children.length - 1).map(p =>
            p.sessionType match {
                case "STRING"  => StringType
                case "DOUBLE"  => DoubleType
                case "INT"     => IntegerType
                case "BOOLEAN" => BooleanType
                case other     => throw new IllegalArgumentException(s"$sqlName: session type $other is not bound")
            }
        )

    override def dataType: DataType = RST_ExpressionUtil.v2TileType
    override def nullable: Boolean = true
    override def prettyName: String = sqlName
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(children = nc)

    private def scalaValue(v: Any): Any = v match {
        case s: UTF8String => s.toString
        case other         => other
    }

    private def tile(cellid: Long, res: SpecResult): InternalRow = {
        val md =
            if (res.error != null) Map("last_error" -> res.error, "error_message" -> res.error)
            else Map("driver" -> "GTiff", "compression" -> "ZSTD", "last_command" -> res.command)
        V2Tile.row(cellid = cellid, raster = res.raster, metadata = SerializationUtil.toMapData[String, String](md))
    }

    private def cellIdOf(row: InternalRow): Long = {
        val i = V2Tile.idx("cellid")
        if (row.isNullAt(i)) 0L else row.getLong(i)
    }

    /** (the rasters, the first tile's cell id), or Left(what to return as it is) for an error tile (unchanged),
      * a NULL or empty tile (NULL). */
    private def inputs(first: Any): Either[Any, (Seq[Array[Byte]], Long)] = {
        val rows: Seq[InternalRow] =
            if (!tiles) Seq(first.asInstanceOf[InternalRow])
            else {
                val a = first.asInstanceOf[ArrayData]
                (0 until a.numElements()).map(i => if (a.isNullAt(i)) null else a.getStruct(i, V2Tile.schema.length))
            }
        if (rows.isEmpty) return Left(null)
        val rasterIdx = V2Tile.idx("raster")
        val pathIdx = V2Tile.idx("path")
        for (r <- rows) {
            if (r == null) return Left(null)
            if (RasterSerializationUtil.isErrorTile(r)) return Left(r.copy())
            if (r.isNullAt(rasterIdx)) {
                if (r.isNullAt(pathIdx)) return Left(null)
                throw new VirtualTileException(
                  s"$sqlName received a virtual tile (raster is null, path=${r.getUTF8String(pathIdx)}). The heavyweight " +
                      "tier operates only on materialized tiles: register the function through databricks.labs.gbx.register, " +
                      "whose wrapper sends virtual tiles to the light tier."
                )
            }
        }
        Right((rows.map(_.getBinary(rasterIdx)), cellIdOf(rows.head)))
    }

    override def eval(input: InternalRow): Any = {
        val first = children.head.eval(input)
        if (first == null) return null
        inputs(first) match {
            case Left(passthrough) => passthrough
            case Right((rasters, cellid)) =>
                val values = fn.params.tail.zip(children.tail).map { case (p, c) => p.name -> scalaValue(c.eval(input)) }.toMap
                GdalSpecEngine.run(fn, rasters, values).map(tile(cellid, _)).orNull
        }
    }

}
