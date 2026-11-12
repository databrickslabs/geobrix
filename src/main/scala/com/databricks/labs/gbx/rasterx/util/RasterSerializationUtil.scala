package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver, TileBytes}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.Dataset

import scala.util.Try

/** Raised by [[RasterSerializationUtil.guardMaterialized]] when a virtual (non-materialized)
  * tile is passed to a heavyweight rst_* function. Extends IllegalArgumentException so it
  * remains an IAE for callers that match on that type, but is distinct from ordinary per-row
  * IAEs (e.g. bad EPSG code, non-Point geometry) that should be handled null-tolerantly.
  */
final class VirtualTileException(msg: String) extends IllegalArgumentException(msg)

/**
  * Converts between Spark InternalRow (tile struct) and GDAL Dataset for raster expressions.
  *
  * Supports two tile layouts:
  *  - v1 (3 fields): (cellid, raster, metadata)
  *  - v2 (9 fields): (cellid, raster, path, path_mode, window, clip_polygon, clip_crs, crs, metadata)
  *
  * Raster is binary content (BinaryType only). A v2 tile with raster=null and path set is a
  * virtual tile; the heavy tier cannot process it — callers must materialize first.
  *
  * Callers must call [[RasterDriver.releaseDataset]] on any returned Dataset when done.
  */
object RasterSerializationUtil {

    private case class TileLayout(cellid: Int, raster: Int, metadata: Int, path: Option[Int], isV2: Boolean)

    private def tileLayout(row: InternalRow): TileLayout = row.numFields match {
        case 3 => TileLayout(cellid = 0, raster = 1, metadata = 2, path = None, isV2 = false)
        case 9 => TileLayout(
            cellid   = V2Tile.idx("cellid"),
            raster   = V2Tile.idx("raster"),
            metadata = V2Tile.idx("metadata"),
            path     = Some(V2Tile.idx("path")),
            isV2     = true
        )
        case n => throw new IllegalArgumentException(
            s"Unrecognized raster tile struct: expected a v1 (3-field) or v2 (9-field) tile, got $n fields.")
    }

    private def guardMaterialized(row: InternalRow, lyt: TileLayout): Unit =
        if (lyt.isV2 && row.isNullAt(lyt.raster) && lyt.path.exists(p => !row.isNullAt(p))) {
            val path = lyt.path.map(p => row.getUTF8String(p).toString).getOrElse("<unknown>")
            throw new VirtualTileException(
                s"A JVM-level rst_* received a virtual tile (raster is null, path=$path). The " +
                "JVM level reads materialized (binary) tiles only. Materialize it at the Python level " +
                "first: call the Python rst_* with materialize=True, or write it out and read it back. " +
                "Then pass the result to the JVM-level function.")
        }

    /** Cell id of a tile row (v1 or v2 layout), read without opening the raster; -1 when the row is
      * null, is not a tile, or has a NULL cell id. */
    def cellIdOf(row: InternalRow): Long =
        Try {
            val lyt = tileLayout(row) // a null row throws here and falls through to -1
            if (row.isNullAt(lyt.cellid)) -1L else row.getLong(lyt.cellid)
        }.getOrElse(-1L)

    /** Metadata of a tile row, read without opening the raster; empty when the row is null, is not a tile or
      * has no metadata. */
    def metadataOf(row: InternalRow): Map[String, String] =
        Try {
            val lyt = tileLayout(row)
            if (row.isNullAt(lyt.metadata)) Map.empty[String, String]
            else SerializationUtil.createMap[String, String](row.getMap(lyt.metadata))
        }.getOrElse(Map.empty[String, String])

    /** True for an error tile, found structurally and without GDAL: no pixel data (a NULL raster, or the zero-length
      * one earlier releases stored), no path, and a diagnostic under `error_message` or `last_error`. The test is
      * structural because `last_error` alone is also stamped as a warning on good tiles by the merge aggregates. */
    def isErrorTile(row: InternalRow): Boolean =
        Try {
            val lyt = tileLayout(row)
            val noPixels = row.isNullAt(lyt.raster) || row.getBinary(lyt.raster).isEmpty
            val metadata = metadataOf(row)
            noPixels && lyt.path.forall(row.isNullAt) && (metadata.contains("error_message") || metadata.contains("last_error"))
        }.getOrElse(false)

    /** Deserialize a tile row to (cellId, Dataset, metadata); opens the raster via RasterDriver.
      * A tile with no raster and no path (an error tile) has nothing to open: the Dataset is null and GDAL is
      * not called. `gated = false` opens it without the decoded-size gate ([[RasterDriver.readFromBytesUngated]]):
      * for the retile family, which cuts the tile into windows and never decodes it whole. */
    def rowToTile(row: InternalRow, rasterDT: DataType, gated: Boolean = true): (Long, Dataset, Map[String, String]) = {
        val lyt = tileLayout(row)
        guardMaterialized(row, lyt)
        if (row.isNullAt(lyt.raster)) {
            (cellIdOf(row), null, metadataOf(row))
        } else {
            val cellID = row.getLong(lyt.cellid)
            val metadataRow = row.getMap(lyt.metadata)
            val metadata = SerializationUtil.createMap[String, String](metadataRow)
            val buffer = row.getBinary(lyt.raster)
            val ds = RasterDriver.readBytes(buffer, metadata, gated)
            (cellID, ds, TileBytes.metadataAfterRead(metadata, ds))
        }
    }

    /** Extract and open the raster Dataset from a tile row; caller must release the Dataset. Null for a tile with
      * no raster and no path (an error tile). `gated = false` opens it without the decoded-size gate: for the
      * header-only accessors, which decode no pixel ([[RasterDriver.readFromBytesUngated]]). */
    def rowToDS(row: InternalRow, rasterDT: DataType, shared: Boolean = false, gated: Boolean = true): Dataset = {
        val lyt = tileLayout(row)
        guardMaterialized(row, lyt)
        if (row.isNullAt(lyt.raster)) {
            null
        } else {
            val metadataRow = row.getMap(lyt.metadata)
            val metadata = SerializationUtil.createMap[String, String](metadataRow)
            val buffer = row.getBinary(lyt.raster)
            // an ungated read is a header answer: the file itself, not its GeoTIFF translation (a netCDF / HDF file's
            // metadata domains and subdatasets, a container's size), as the light tier answers it
            if (gated) RasterDriver.readBytes(buffer, metadata, gated) else RasterDriver.readSourceFromBytes(buffer, metadata)
        }
    }

    /** True for a tile whose raster bytes are a netCDF / HDF file: its expression runs under
      * [[GDALManager.withHdfLock]] from the open to the release ([[RST_ErrorHandler.safeEval]]). */
    def isHdfTile(row: InternalRow): Boolean =
        Try {
            val lyt = tileLayout(row)
            !row.isNullAt(lyt.raster) && GDALManager.isHdfFamilyBytes(row.getBinary(lyt.raster))
        }.getOrElse(false)

    /** The driver a materialized tile's bytes were written in when [[rowToDS]] reads them as a GeoTIFF translation
      * (netCDF, HFA, JP2OpenJPEG, GPKG, ...), else None (a TIFF, a tile with no raster, or bytes that do not open). */
    def translatedFromDriver(row: InternalRow): Option[String] = {
        val lyt = tileLayout(row)
        if (row.isNullAt(lyt.raster)) None
        else {
            val buffer = row.getBinary(lyt.raster)
            if (TileBytes.isTiff(buffer)) None
            else {
                val metadata = SerializationUtil.createMap[String, String](row.getMap(lyt.metadata))
                RasterDriver.sourceDriverOf(buffer, metadata)
            }
        }
    }

    /** Serialize (cellId, Dataset, metadata) to an InternalRow; writes raster to bytes. */
    def tileToRow(tuple: (Long, Dataset, Map[String, String]), dataType: DataType, hconf: SerializableConfiguration): InternalRow = // retained for signature compat
        tileToRow(tuple, dataType, hconf, None)

    /** As above, plus an optional ``clipCrs`` canonical CRS string stamped into the
      * v2 ``clip_crs`` field. Used by the GDAL/GTiff reader's clipCrs
      * option (parity with the light reader); ``None`` leaves it null. `writeExtra` are write options for this
      * encoding only, not stamped into the tile's metadata (`gbx_rst_cog_convert`'s block size). */
    def tileToRow(
        tuple: (Long, Dataset, Map[String, String]),
        dataType: DataType,
        hconf: SerializableConfiguration,
        clipCrs: Option[String],
        writeExtra: Map[String, String] = Map.empty
    ): InternalRow = {
        val metadata = SerializationUtil.toMapData[String, String](tuple._3)
        // A null Dataset is an error tile: its raster is NULL, the one form every tier uses.
        val bytes: Array[Byte] = if (tuple._2 == null) null else RasterDriver.writeToBytes(tuple._2, tuple._3, writeExtra)
        V2Tile.row(
            cellid  = tuple._1,
            raster  = bytes,
            clipCrs = clipCrs.map(org.apache.spark.unsafe.types.UTF8String.fromString).orNull,
            metadata = metadata
        )
    }

}
