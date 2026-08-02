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
  * Tile rows have (cellid, raster, metadata): raster is binary content (BinaryType only).
  * Callers must call [[RasterDriver.releaseDataset]] on any returned Dataset when done.
  */
object RasterSerializationUtil {

    /** Deserialize a tile row to (cellId, Dataset, metadata); opens the raster via RasterDriver. */
    def rowToTile(row: InternalRow, rasterDT: DataType): (Long, Dataset, Map[String, String]) = {
        val cellID = row.getLong(0)
        val metadataRow = row.getMap(2)
        val metadata = SerializationUtil.createMap[String, String](metadataRow)
        val buffer = row.getBinary(1)
        val ds = RasterDriver.readFromBytes(buffer, metadata)
        (cellID, ds, metadata)
    }

    /** Extract and open the raster Dataset from a tile row; caller must release the Dataset. */
    def rowToDS(row: InternalRow, rasterDT: DataType, shared: Boolean = false): Dataset = {
        val metadataRow = row.getMap(2)
        val metadata = SerializationUtil.createMap[String, String](metadataRow)
        val buffer = row.getBinary(1)
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
