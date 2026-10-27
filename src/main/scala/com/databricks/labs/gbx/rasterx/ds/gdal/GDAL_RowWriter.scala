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
import java.nio.file.{Files, Path}
import java.util.UUID
import scala.util.Try
import scala.util.control.NonFatal
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
      * translate or copy, or a name the local disk cannot hold, included. */
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
        val localDir = NodeFilePathUtil.rootPath.resolve(UUID.randomUUID().toString.replace("-", "_"))
        try {
            val path = GDAL_RowWriter.outputPath(root, name, ext)
            val localPath = localDir.resolve(s"$name.$ext") // throws for a name a local path cannot hold
            translateTo(localPath, tile, tileDt)
            // Copied once closed: a directory format such as Zarr finishes writing its metadata on close.
            HadoopUtils.copyToPath(localPath.toString, path, expressionConfig.hConf)
        } finally GDAL_RowWriter.deleteLocal(localDir)
    }

    /** Opens the tile and translates it to `localPath`, then closes the output. Both Datasets are released on
      * every exit; a failure to release is attached to the error being thrown, never put in its place. */
    private def translateTo(localPath: Path, tile: InternalRow, tileDt: StructType): Unit = {
        var ds: Dataset = null
        var res: Dataset = null
        var failure: Throwable = null
        try {
            val (cell, in, mtd) = RasterSerializationUtil.rowToTile(tile, tileDt.fields(1).dataType)
            ds = in
            Files.createDirectories(localPath.getParent)
            // Create a copy via gdal_translate to ensure proper format, compression, etc.
            val (out, resMtd) = GDALTranslate.executeTranslate(localPath.toString, ds, "gdal_translate", mtd)
            res = out
            errors ++= Seq(gdal.GetLastErrorMsg())
            if (res == null) throw new IOException(s"gdal_translate wrote no output to $localPath: ${errors.last}")
            resMtd.foreach { case (k, v) => res.SetMetadataItem(s"RASTERX_$k", v) }
            res.SetMetadataItem("RASTERX_CELL", cell.toString)
            res.FlushCache()
        } catch {
            case t: Throwable =>
                failure = t
                throw t
        } finally {
            GDAL_RowWriter.releaseAll(failure, () => releaseDataset(ds), () => releaseDataset(res))
        }
    }

    /** Releases one Dataset of [[translateTo]]; a null one is a no-op. Overridable so a test can make a release fail. */
    protected[ds] def releaseDataset(dataset: Dataset): Unit = RasterDriver.releaseDataset(dataset)

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

    /** `<root>/<name>.<ext>`, refusing a name that would write outside `root`: one with a `..` path segment (the
      * path is normalized) and one that starts with `/` (resolving it against the local staging dir gives that
      * absolute path, so GDAL writes there and the cleanup of the staging dir never sees the file). */
    private[ds] def outputPath(root: String, name: String, ext: String): String = {
        if (name.startsWith("/")) {
            throw new IllegalArgumentException(
              s"The output name '$name' starts with '/', so it would be written outside $root; use a relative name"
            )
        }
        if (s"$name.$ext".split('/').contains("..")) {
            throw new IllegalArgumentException(
              s"The output name '$name' has a '..' path segment, so it would be written outside $root; " +
                  "use a name without '..' segments"
            )
        }
        s"$root/$name.$ext"
    }

    /** Runs every release, in order, even when one throws. `primary` is the error the caller is throwing (or
      * null): a release error is attached to it as suppressed, so it stays the error reported. With no
      * `primary`, the first release error is thrown, the later ones attached to it. */
    private[ds] def releaseAll(primary: Throwable, releases: (() => Unit)*): Unit = {
        var first: Throwable = null
        releases.foreach { release =>
            try release()
            catch {
                case NonFatal(e) =>
                    if (primary != null) primary.addSuppressed(e)
                    else if (first == null) first = e
                    else first.addSuppressed(e)
            }
        }
        if (first != null) throw first
    }

}
