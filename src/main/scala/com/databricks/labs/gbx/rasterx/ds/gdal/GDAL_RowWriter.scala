package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.RasterSerializationUtil
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFilePathUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.gdal.gdal.gdal

import java.nio.file.{Files, Paths}
import java.util.UUID
import scala.util.Try
import scala.util.hashing.MurmurHash3

/** Writes each InternalRow's tile to a raster file under root (name from column or MurmurHash3), via GDALTranslate. */
class GDAL_RowWriter(
    schema: StructType,
    root: String,
    nameCol: Option[String],
    ext: String,
    pid: Int,
    tid: Long,
    expressionConfig: ExpressionConfig
) extends DataWriter[InternalRow] {

    private var errors = Array.empty[String]
    private val tileIdx = schema.fieldIndex("tile")
    private val nameIdx = nameCol.flatMap(n => Try(schema.fieldIndex(n)).toOption)

    /** Serializes row's tile, translates to local file, then copies to root; releases GDAL datasets. */
    override def write(row: InternalRow): Unit = {
        val tileDt = schema.fields(tileIdx).dataType.asInstanceOf[StructType]
        val tile = row.getStruct(tileIdx, tileDt.length)
        // name or Murmur3
        val name = nameIdx match {
            case Some(idx) => row.getString(idx)
            case None      =>
                val mm3 = MurmurHash3.seqHash(tile.toSeq(tileDt)).toString.replace("-", "_")
                s"${mm3}_${pid}_$tid"
        }
        val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(tile, tileDt.fields(1).dataType)
        val path = s"$root/$name.$ext"
        val uuid = UUID.randomUUID().toString.replace("-", "_")
        val localPath = s"${NodeFilePathUtil.rootPath}/$uuid/$name.$ext"
        Files.createDirectories(Paths.get(localPath).getParent)
        // Create a copy via gdal_translate to ensure proper format, compression, etc.
        val (res, resMtd) = GDALTranslate.executeTranslate(localPath, ds, "gdal_translate", mtd)
        errors ++= Seq(gdal.GetLastErrorMsg())
        resMtd.foreach { case (k, v) => res.SetMetadataItem(s"RASTERX_$k", v) }
        res.SetMetadataItem("RASTERX_CELL", cell.toString)
        res.FlushCache()
        RasterDriver.releaseDataset(ds)
        HadoopUtils.copyToPath(localPath, path, expressionConfig.hConf)
        RasterDriver.releaseDataset(res)
        Files.deleteIfExists(Paths.get(localPath))
        Files.deleteIfExists(Paths.get(localPath).getParent)
    }

    /** Returns a commit message carrying any GDAL error strings collected during write. */
    /** Overrides DataWriter.commit: returns GDAL_WriterMsg with any collected error strings. */
    override def commit(): WriterCommitMessage = GDAL_WriterMsg(errors)
    /** Overrides DataWriter.abort: no-op. */
    override def abort(): Unit = {}
    /** Overrides DataWriter.close: no-op (resources released per row). */
    override def close(): Unit = {}

}
