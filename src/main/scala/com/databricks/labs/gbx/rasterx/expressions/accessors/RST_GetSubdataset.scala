package com.databricks.labs.gbx.rasterx.expressions.accessors

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, RasterArgumentException, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants.GA_ReadOnly

import scala.jdk.CollectionConverters._

/** Returns the subdatasets of the raster. */
case class RST_GetSubdataset(
    tile: Expression,
    subsetName: Expression
) extends InvokedExpression {

    override def children: Seq[Expression] = Seq(tile, subsetName, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_GetSubdataset.name
    override def replacement: Expression = invoke(RST_GetSubdataset)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0), nc(1))

}

/** Companion: SQL name, builder, and eval entry points for path/binary tile. */
object RST_GetSubdataset extends WithExpressionInfo {

    def eval(row: InternalRow, subsetName: UTF8String, conf: UTF8String): InternalRow = eval(row, subsetName, conf, BinaryType)

    def eval(row: InternalRow, subsetName: UTF8String, conf: UTF8String, rdt: DataType): InternalRow =
        RST_ErrorHandler.safeEval(
          () => {
              val exprConf = ExpressionConfig.fromB64(conf.toString)
              RST_ExpressionUtil.init(exprConf)
              val (cell, ds, mtd) = RasterSerializationUtil.rowToSourceTile(row, rdt)
              var subdataset: Dataset = null
              var containerFiles = Set.empty[String]
              try {
                  // A netCDF / HDF container and its subdataset are opened, cut to GeoTIFF bytes and closed one thread at
                  // a time (GDALManager.withHdfLock); nothing in this block waits on a Spark task.
                  GDALManager.withHdfLockIf(GDALManager.isHdfFamily(ds)) {
                      containerFiles = ds.GetFileList().asScala.map(_.toString).toSet
                      subdataset = execute(ds, subsetName.toString)
                      // the subdataset is stored as the GeoTIFF the bytes rule reads, not as its container's driver
                      // and extension
                      RasterSerializationUtil.tileToRow(
                        (cell, subdataset, mtd + ("driver" -> "GTiff") + ("extension" -> "tif")),
                        rdt,
                        exprConf.hConf
                      )
                  }
              } finally {
                  GDALManager.withHdfLockIf(GDALManager.isHdfFamily(ds)) {
                      // before ds: it is open on ds's in-memory file, which it lists but does not own: only ds
                      // unlinks the container's files
                      RasterDriver.releaseDataset(subdataset, keep = containerFiles)
                      RasterDriver.releaseDataset(ds)
                  }
              }
          },
          row,
          rdt
        )

    /** The names of the subdatasets of `ds` (the part of each GDAL name after the container's). */
    private def subdatasetNames(ds: Dataset): Seq[String] =
        Option(ds.GetMetadata_List("SUBDATASETS")).map(_.asScala.map(_.toString).toSeq).getOrElse(Seq.empty)
            .filter(_.contains("_NAME="))
            .map(_.split("_NAME=", 2)(1))

    def execute(ds: Dataset, name: String): Dataset = {
        val path = ds.GetDescription
        val driver = ds.GetDriver.getShortName
        val subdatasetPath = s"$driver:$path:$name"
        val sub = GDALManager.withHdfLockIf(GDALManager.isHdfFamily(ds))(scala.util.Try(gdal.Open(subdatasetPath, GA_ReadOnly)).getOrElse(null))
        // A tile that holds no subdataset of that name: the argument names none, which fails the call naming it
        if (sub == null)
            throw new RasterArgumentException(
              s"gbx_rst_getsubdataset: no subdataset named '$name' (available: ${subdatasetNames(ds).mkString(", ")})"
            )
        sub
    }

    override def name: String = "gbx_rst_getsubdataset"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => new RST_GetSubdataset(c(0), c(1))

}
