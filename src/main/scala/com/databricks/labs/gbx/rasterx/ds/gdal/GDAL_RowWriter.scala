package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operator.GDALTranslate
import com.databricks.labs.gbx.rasterx.util.RasterSerializationUtil
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFilePathUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import org.gdal.gdal.{Dataset, gdal}

import java.io.IOException
import java.nio.file.{Files, Path, Paths}
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

    /** Serializes row's tile, translates it to a local file, then copies that to root, replacing an earlier
      * output of the same name. Both GDAL Datasets and the local copy are released on every exit, a failed
      * translate or copy included. */
    override def write(row: InternalRow): Unit = {
        val tileDt = schema.fields(tileIdx).dataType.asInstanceOf[StructType]
        val tile = row.getStruct(tileIdx, tileDt.length)
        // name or Murmur3
        val name = nameIdx match {
            case Some(idx) => row.getString(idx)
            case None      =>
                // Unsigned decimal keeps the tile name digit-leading (never "_", "-", or "."),
                // so stock Spark file readers (binaryFile, etc.) do not silently skip it via
                // Hadoop's hidden-file filter. MurmurHash3.seqHash is a signed Int, so the old
                // .toString.replace("-", "_") produced "_"-leading names for ~half of all tiles.
                val mm3 = java.lang.Integer.toUnsignedString(MurmurHash3.seqHash(tile.toSeq(tileDt)))
                s"${mm3}_${pid}_$tid"
        }
        val (cell, ds, mtd) = RasterSerializationUtil.rowToTile(tile, tileDt.fields(1).dataType)
        val path = s"$root/$name.$ext"
        val uuid = UUID.randomUUID().toString.replace("-", "_")
        val localDir = NodeFilePathUtil.rootPath.resolve(uuid)
        val localPath = localDir.resolve(s"$name.$ext").toString
        try {
            var res: Dataset = null
            try {
                Files.createDirectories(Paths.get(localPath).getParent)
                // Create a copy via gdal_translate to ensure proper format, compression, etc.
                val (out, resMtd) = GDALTranslate.executeTranslate(localPath, ds, "gdal_translate", mtd)
                res = out
                errors ++= Seq(gdal.GetLastErrorMsg())
                if (res == null) throw new IOException(s"gdal_translate wrote no output to $localPath: ${errors.last}")
                resMtd.foreach { case (k, v) => res.SetMetadataItem(s"RASTERX_$k", v) }
                res.SetMetadataItem("RASTERX_CELL", cell.toString)
                res.FlushCache()
            } finally {
                try RasterDriver.releaseDataset(ds)
                finally RasterDriver.releaseDataset(res)
            }
            // Copied once closed: a directory format such as Zarr finishes writing its metadata on close.
            HadoopUtils.copyToPath(localPath, path, expressionConfig.hConf)
        } finally GDAL_RowWriter.deleteLocal(localDir)
    }

    /** Returns a commit message carrying any GDAL error strings collected during write. */
    /** Overrides DataWriter.commit: returns GDAL_WriterMsg with any collected error strings. */
    override def commit(): WriterCommitMessage = GDAL_WriterMsg(errors)
    /** Overrides DataWriter.abort: no-op. */
    override def abort(): Unit = {}
    /** Overrides DataWriter.close: no-op (resources released per row). */
    override def close(): Unit = {}

}

object GDAL_RowWriter {

    /** Deletes one write's local staging dir and the output in it, a directory format such as Zarr
      * included; never throws (a failure to delete is logged). */
    private[ds] def deleteLocal(dir: Path): Unit = NodeFilePathUtil.deleteRecursively(dir)

}
