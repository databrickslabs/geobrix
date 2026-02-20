package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import com.databricks.labs.gbx.rasterx.operations.BalancedSubdivision
import com.databricks.labs.gbx.rasterx.util.{RST_ExpressionUtil, RasterSerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.BinaryType
import org.apache.spark.unsafe.types.UTF8String

/** Reads one partition of a GDAL source: splits the raster into tiles (BalancedSubdivision) and yields (source, tile) rows. */
class GDAL_Reader(partition: GDAL_Partition) extends PartitionReader[InternalRow] {

    RST_ExpressionUtil.init(partition.expressionConfig)

    // TODO: Options should come from the partition here
    private val ds = RasterDriver.read(partition.filePath, Map.empty)
    private val tilesIter = BalancedSubdivision.splitRasterIter(ds, Map.empty, partition.sizeInMB)
    RST_ExpressionUtil.addCleanupListener(tilesIter)
    private var counter = 0
    private val hconf = partition.expressionConfig.hConf

    /** Overrides PartitionReader.next: true while tilesIter has more tiles. */
    override def next(): Boolean = tilesIter.hasNext

    /** Overrides PartitionReader.get: (source path, tile row); releases each Dataset after serialization. */
    override def get(): InternalRow = {
        val tile = tilesIter.next()
        counter += 1
        val tileRow = RasterSerializationUtil.tileToRow((-1L, tile._1, tile._2), BinaryType, hconf)
        RasterDriver.releaseDataset(tile._1)
        InternalRow.fromSeq(
          Seq(
            UTF8String.fromString(partition.filePath),
            tileRow
          )
        )
    }

    /** Overrides PartitionReader.close: no-op (tiles released as we go via addCleanupListener). */
    override def close(): Unit = {
        // we close as we go, so nothing to do here
    }

}
