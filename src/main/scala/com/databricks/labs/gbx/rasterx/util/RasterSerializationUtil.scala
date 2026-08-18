package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.Dataset

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
  *  - v2 (9 fields): (cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata, path_mode)
  *
  * Raster is binary content (BinaryType only). A v2 tile with raster=null and path set is a
  * virtual tile; the heavy tier cannot process it — callers must materialize first.
  *
  * Callers must call [[RasterDriver.releaseDataset]] on any returned Dataset when done.
  */
object RasterSerializationUtil {

    private case class TileLayout(cellid: Int, raster: Int, metadata: Int, path: Option[Int], isV2: Boolean)

    private def tileLayout(row: InternalRow): TileLayout = row.numFields match {
        case 3    => TileLayout(cellid = 0, raster = 1, metadata = 2, path = None, isV2 = false)
        case 8 | 9 => TileLayout(cellid = 0, raster = 1, metadata = 7, path = Some(2), isV2 = true)
        case n => throw new IllegalArgumentException(
            s"Unrecognized raster tile struct: expected a v1 (3-field) or v2 (8- or 9-field) tile, got $n fields.")
    }

    private def guardMaterialized(row: InternalRow, lyt: TileLayout): Unit =
        if (lyt.isV2 && row.isNullAt(lyt.raster) && lyt.path.exists(p => !row.isNullAt(p))) {
            val path = lyt.path.map(p => row.getUTF8String(p).toString).getOrElse("<unknown>")
            throw new VirtualTileException(
                s"Heavyweight rst_* received a virtual tile (raster is null, path=$path). The " +
                "heavyweight tier operates only on materialized (binary) tiles. Materialize it in the " +
                "lightweight tier first — call the lightweight rst_* with materialize=True, or write it " +
                "out and read it back — then pass the result to the heavyweight function.")
        }

    /** Deserialize a tile row to (cellId, Dataset, metadata); opens the raster via RasterDriver. */
    def rowToTile(row: InternalRow, rasterDT: DataType): (Long, Dataset, Map[String, String]) = {
        val lyt = tileLayout(row)
        guardMaterialized(row, lyt)
        val cellID = row.getLong(lyt.cellid)
        val metadataRow = row.getMap(lyt.metadata)
        val metadata = SerializationUtil.createMap[String, String](metadataRow)
        val buffer = row.getBinary(lyt.raster)
        val ds = RasterDriver.readFromBytes(buffer, metadata)
        (cellID, ds, metadata)
    }

    /** Extract and open the raster Dataset from a tile row; caller must release the Dataset. */
    def rowToDS(row: InternalRow, rasterDT: DataType, shared: Boolean = false): Dataset = {
        val lyt = tileLayout(row)
        guardMaterialized(row, lyt)
        val metadataRow = row.getMap(lyt.metadata)
        val metadata = SerializationUtil.createMap[String, String](metadataRow)
        val buffer = row.getBinary(lyt.raster)
        RasterDriver.readFromBytes(buffer, metadata)
    }

    /** Serialize (cellId, Dataset, metadata) to an InternalRow; writes raster to bytes. */
    def tileToRow(tuple: (Long, Dataset, Map[String, String]), dataType: DataType, hconf: SerializableConfiguration): InternalRow = // retained for signature compat
        tileToRow(tuple, dataType, hconf, None)

    /** As above, plus an optional ``clipCrs`` canonical CRS string stamped into the
      * v2 ``clip_crs`` field (position 5). Used by the GDAL/GTiff reader's clipCrs
      * option (parity with the light reader); ``None`` leaves it null. */
    def tileToRow(
        tuple: (Long, Dataset, Map[String, String]),
        dataType: DataType,
        hconf: SerializableConfiguration,
        clipCrs: Option[String]
    ): InternalRow = {
        val metadata = SerializationUtil.toMapData[String, String](tuple._3)
        val bytes =
            if (tuple._2 == null) {
                Array.emptyByteArray
            } else {
                RasterDriver.writeToBytes(tuple._2, tuple._3)
            }
        InternalRow.fromSeq(
          Seq(
            tuple._1, // cellid
            bytes,    // raster (binary)
            null,     // path (null — bytes are self-describing)
            null,     // window
            null,     // clip_polygon
            clipCrs.map(org.apache.spark.unsafe.types.UTF8String.fromString).orNull, // clip_crs
            null,     // crs
            metadata, // metadata (position 7)
            null      // path_mode (position 8, null for materialized tiles)
          )
        )
    }

    /** Reshape a v1 (3-field) OR v2 (8- or 9-field) tile InternalRow to the canonical v2 9-field
      * layout WITHOUT opening / re-encoding the raster. v2 rows are widened from 8 to 9 if
      * needed; v1 rows are widened: (cellid, raster, metadata) → (cellid, raster, null×5, metadata, null).
      *
      * Use this at aggregator update() time so every buffered row is 9-field, making the
      * size==1 fast-path passthrough and the serialize UnsafeProjection (which is created
      * over the 9-field dataType) both safe on v1 or 8-field v2 input.
      */
    def normalizeToV2Row(row: InternalRow): InternalRow = {
        val lyt = tileLayout(row)
        if (lyt.isV2 && row.numFields == 9) row
        else if (lyt.isV2) {
            // 8-field v2: widen by appending null path_mode at position 8
            val cellid      = row.getLong(0)
            val raster      = if (row.isNullAt(1)) null else row.getBinary(1)
            val path        = if (row.isNullAt(2)) null else row.getUTF8String(2)
            val window      = if (row.isNullAt(3)) null else row.getStruct(3, 4)
            val clipPolygon = if (row.isNullAt(4)) null else row.getBinary(4)
            val clipCrs     = if (row.isNullAt(5)) null else row.getUTF8String(5)
            val crs         = if (row.isNullAt(6)) null else row.getUTF8String(6)
            val metadata    = if (row.isNullAt(7)) null else row.getMap(7)
            InternalRow.fromSeq(Seq(cellid, raster, path, window, clipPolygon, clipCrs, crs, metadata, null))
        } else {
            // v1: cellid@0, raster@1, metadata@2 → v2: cellid@0, raster@1, [null×5], metadata@7, null
            val cellid   = row.getLong(lyt.cellid)
            val raster   = if (row.isNullAt(lyt.raster))   null else row.getBinary(lyt.raster)
            val metadata = if (row.isNullAt(lyt.metadata)) null else row.getMap(lyt.metadata)
            InternalRow.fromSeq(Seq(cellid, raster, null, null, null, null, null, metadata, null))
        }
    }

    /** Deserialize an array of tile structs to (cellId, Dataset, metadata); caller must release each Dataset.
      *
      * @param elementFieldCount the declared field count of the element StructType (3 for v1, 9 for v2).
      *                          Must come from the expression's declared input schema — never hardcoded.
      */
    def arrayToTiles(array: ArrayData, dataType: DataType, elementFieldCount: Int = 3): Seq[(Long, Dataset, Map[String, String])] = {
        val n = array.numElements()
        (0 until n).map { i =>
            val row = array.getStruct(i, elementFieldCount)
            rowToTile(row, dataType)
        }
    }

}
