package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.operations.RasterAccessors
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterSerializationUtil}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.Dataset

/** Returns the subdatasets of the raster. */
case class RST_Subdatasets(
    tile: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, ExpressionConfigExpr())
    override def nullable: Boolean = true
    override def prettyName: String = RST_Subdatasets.name
    override def replacement: Expression = invoke(RST_Subdatasets)
    override def dataType: DataType = MapType(StringType, StringType)
    override def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_Subdatasets extends WithExpressionInfo {

    def eval(row: InternalRow, conf: UTF8String): MapData = eval(row, conf, BinaryType)

    def eval(row: InternalRow, conf: UTF8String, rdt: DataType): MapData =
        Option(
          RST_ErrorHandler.safeEval(
            () => {
                val exprConf = ExpressionConfig.fromB64(conf.toString)
                RST_ExpressionUtil.init(exprConf)
                // the tile's own dataset, not its GeoTIFF translation, which keeps no subdataset
                val (_, ds, _) = RasterSerializationUtil.rowToSourceTile(row, rdt)
                // a netCDF / HDF dataset is read and closed one thread at a time (GDALManager.withHdfLock)
                val res = GDALManager.withHdfLockIf(GDALManager.isHdfFamily(ds)) {
                    try execute(ds) finally RasterDriver.releaseDataset(ds)
                }
                SerializationUtil.toMapData[String, String](res)
            },
            row,
            rdt,
            conf
          )
        ).map(_.asInstanceOf[MapData]).orNull

    def execute(ds: Dataset): Map[String, String] = RasterAccessors.subdatasetsMap(ds)

    override def name: String = "gbx_rst_subdatasets"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_Subdatasets(c(0))

}
