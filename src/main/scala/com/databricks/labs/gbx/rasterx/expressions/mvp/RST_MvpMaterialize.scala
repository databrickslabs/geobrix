package com.databricks.labs.gbx.rasterx.expressions.mvp

import com.databricks.labs.gbx.expressions.{ExpressionConfig, ExpressionConfigExpr, InvokedExpression, WithExpressionInfo}
import com.databricks.labs.gbx.kernels.spec.GdalSpecs
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.ReTile
import com.databricks.labs.gbx.rasterx.util.{RST_ErrorHandler, RST_ExpressionUtil, V2Tile}
import com.databricks.labs.gbx.util.{NodeFileManager, SerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry.FunctionBuilder
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.{Dataset, gdal}
import org.gdal.gdalconst.gdalconstConstants

import scala.jdk.CollectionConverters.SeqHasAsJava

/** MVP-IO probe gbx_mvp_rst_materialize: the JVM decode for the virtual tiles of a bytes-free reader.
  *
  * A tile that already has bytes (or has no path: an error tile) comes back as it is. A virtual tile is opened from its
  * `path` (the same open as the Scala readers: [[RasterDriver.read]]'s node-file copy for a Volume or remote path), its
  * `window` cut, and the pixels encoded as a ZSTD GeoTIFF with the predictor of their data type; `cellid` and `window`
  * stay and `path` is cleared.
  *
  * The path is data, so the open is held to what the lightweight tier's `open_data` holds it to: a remote location is
  * refused ([[RST_MvpMaterialize.remoteLocation]]), and the dataset is opened with the manifest's allowed raster drivers
  * as GDAL's driver list, never the whole registry. A tile carrying a clip or a pending instruction is refused (this
  * probe applies neither; an error tile says so).
  */
case class RST_MvpMaterialize(tile: Expression) extends InvokedExpression {
    override def children: Seq[Expression] = Seq(tile, ExpressionConfigExpr())
    override def dataType: DataType = RST_ExpressionUtil.tileDataType(tile)
    override def nullable: Boolean = true
    override def prettyName: String = RST_MvpMaterialize.name
    override def replacement: Expression = invoke(RST_MvpMaterialize, nonFoldable = true)
    override protected def withNewChildrenInternal(nc: IndexedSeq[Expression]): Expression = copy(nc(0))
}

object RST_MvpMaterialize extends WithExpressionInfo {

    // the keys of a virtual tile's metadata that mean "apply this on materialize" (the light tier's pending instructions)
    private val PendingKeys = Seq("pending_nodata", "pending_srid", "pending_bands", "pending_crs", "zarr_array")

    // The remote-path gate of the lightweight tier (`_gdal_policy.remote_location`): a GDAL virtual file system outside the
    // data set, or a URL scheme outside it, names a remote location. No switch here: the JVM is strict.
    private val DataVsi = Set("s3", "s3_streaming", "az", "az_streaming", "adls", "gs", "gs_streaming", "mem", "zip", "tar", "gzip")
    private val DataSchemes = Set("s3", "s3a", "abfss", "abfs", "az", "gs", "wasbs", "zip", "tar", "gzip", "file", "dbfs")
    private val Vsi = java.util.regex.Pattern.compile("""(?:^|(?<=[/{"':,\s]))/vsi([a-z0-9_]+)""", java.util.regex.Pattern.CASE_INSENSITIVE)
    private val Scheme = java.util.regex.Pattern.compile("""(?<![a-z0-9+.\-])([a-z][a-z0-9+.\-]*)://""", java.util.regex.Pattern.CASE_INSENSITIVE)

    /** The location `path` names that a path from data may not (`/vsicurl/`, `https://` ...), else None. */
    def remoteLocation(path: String): Option[String] = {
        val vsi = Vsi.matcher(path)
        while (vsi.find()) {
            if (!DataVsi(vsi.group(1).toLowerCase)) return Some(s"/vsi${vsi.group(1).toLowerCase}/")
        }
        val scheme = Scheme.matcher(path)
        while (scheme.find()) {
            val s = scheme.group(1).toLowerCase
            if (s.split("\\+").exists(p => !DataSchemes(p))) return Some(s"$s://")
        }
        None
    }

    private lazy val (cId, rId, pId, wId, clipId, mdId, crsId) = (
      V2Tile.idx("cellid"), V2Tile.idx("raster"), V2Tile.idx("path"), V2Tile.idx("window"),
      V2Tile.idx("clip_polygon"), V2Tile.idx("metadata"), V2Tile.idx("crs")
    )

    def eval(row: InternalRow, conf: UTF8String): InternalRow =
        if (row == null || !row.isNullAt(rId) || row.isNullAt(pId)) row // null, materialized, or an error tile
        else RST_ErrorHandler.safeEval(() => decode(row, conf), row, BinaryType)

    private def decode(row: InternalRow, conf: UTF8String): InternalRow = {
        RST_ExpressionUtil.init(ExpressionConfig.fromB64(conf.toString))
        val path = row.getUTF8String(pId).toString
        val inMeta = if (row.isNullAt(mdId)) Map.empty[String, String] else SerializationUtil.createMap[String, String](row.getMap(mdId))
        remoteLocation(path).foreach { where =>
            throw new IllegalArgumentException(
              s"gbx_mvp_rst_materialize: '$path' names a remote location ($where), which GeoBrix does not open for a path " +
                  "that comes from data. Fix: copy the data to a Volume or a cloud object store (s3://, abfss://, gs://)")
        }
        if (!row.isNullAt(clipId) || PendingKeys.exists(inMeta.contains)) {
            throw new IllegalArgumentException(
              "gbx_mvp_rst_materialize: the tile carries a clip or a pending instruction, which this function does not apply. " +
                  "Fix: materialize it in the lightweight tier")
        }
        // The Scala readers' open: a local file as it is, any other path through a node-file copy (one hold on it).
        val isLocal = RasterDriver.isLocal(path)
        val local = if (isLocal) path else NodeFileManager.readRemote(path)
        var ds: Dataset = null
        var sub: Dataset = null
        try {
            gdal.ErrorReset()
            ds = gdal.OpenEx(local, gdalconstConstants.OF_RASTER | gdalconstConstants.OF_READONLY,
              new java.util.Vector[String](GdalSpecs.section.allowedDrivers.asJava))
            if (ds == null) {
                throw new RuntimeException(s"Failed to open dataset at path: $path; Error: ${gdal.GetLastErrorMsg}")
            }
            var md = Map("driver" -> "GTiff")
            sub = if (row.isNullAt(wId)) ds else {
                val w = row.getStruct(wId, 4)
                val (c, r, wd, h) = (w.getInt(0), w.getInt(1), w.getInt(2), w.getInt(3))
                require(c >= 0 && r >= 0 && wd > 0 && h > 0 && c + wd <= ds.getRasterXSize && r + h <= ds.getRasterYSize,
                  s"the window ($c, $r, $wd, $h) is not inside the ${ds.getRasterXSize} x ${ds.getRasterYSize} raster")
                val (s, m) = ReTile.getTile(ds, md, c, r, wd, h)
                md = m
                s
            }
            val bytes = RasterDriver.writeToBytes(sub, md ++ Map("driver" -> "GTiff", "compression" -> "ZSTD"))
            V2Tile.row(
              cellid = row.getLong(cId),
              raster = bytes,
              window = if (row.isNullAt(wId)) null else new GenericInternalRow(Array[Any](
                  row.getStruct(wId, 4).getInt(0), row.getStruct(wId, 4).getInt(1),
                  row.getStruct(wId, 4).getInt(2), row.getStruct(wId, 4).getInt(3))),
              crs = if (row.isNullAt(crsId)) null else row.getUTF8String(crsId).clone(),
              metadata = SerializationUtil.toMapData[String, String](inMeta + ("driver" -> "GTiff"))
            )
        } finally {
            // sub is a /vsimem/ dataset of its own (unlinked here); ds is closed and gives its hold on the node-file copy back
            // once, below (releaseDataset would give the hold back too, so it is not used on ds)
            if (sub != null && (sub ne ds)) RasterDriver.releaseDataset(sub)
            if (ds != null) ds.delete()
            if (!isLocal) NodeFileManager.releaseLocal(local)
        }
    }

    override def name: String = "gbx_mvp_rst_materialize"

    override def builder(): FunctionBuilder = (c: Seq[Expression]) => RST_MvpMaterialize(c(0))
}
