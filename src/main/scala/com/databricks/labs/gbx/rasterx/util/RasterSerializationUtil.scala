package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.Dataset

/**
  * Converts between Spark InternalRow (tile struct) and GDAL Dataset for raster expressions.
  *
  * Supports two tile layouts:
  *  - v1 (3 fields): (cellid, raster, metadata)
  *  - v2 (8 fields): (cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata)
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
        case 8 => TileLayout(cellid = 0, raster = 1, metadata = 7, path = Some(2), isV2 = true)
        case n => throw new IllegalArgumentException(
            s"Unrecognized raster tile struct: expected a v1 (3-field) or v2 (8-field) tile, got $n fields.")
    }

    private def guardMaterialized(row: InternalRow, lyt: TileLayout): Unit =
        if (lyt.isV2 && row.isNullAt(lyt.raster) && lyt.path.exists(p => !row.isNullAt(p))) {
            val path = lyt.path.map(p => row.getUTF8String(p).toString).getOrElse("<unknown>")
            throw new IllegalArgumentException(
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
    def tileToRow(tuple: (Long, Dataset, Map[String, String]), dataType: DataType, hconf: SerializableConfiguration): InternalRow = { // retained for signature compat
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
            bytes, // binary
            metadata // metadata
          )
        )
    }

    /** Deserialize an array of tile structs to (cellId, Dataset, metadata); caller must release each Dataset. */
    def arrayToTiles(array: ArrayData, dataType: DataType): Seq[(Long, Dataset, Map[String, String])] = {
        val n = array.numElements()
        (0 until n).map { i =>
            val row = array.getStruct(i, 3)
            rowToTile(row, dataType)
        }
    }

}
