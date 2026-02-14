package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.{NodeFileManager, SerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader

/** Reads one partition of an OGR source: opens the layer and yields feature rows from start to end index. */
class OGR_Reader(partition: OGR_Partition) extends PartitionReader[InternalRow] {

    GDALManager.init(partition.expressionConfig)
    OGR_SchemaInference.enableOGRDrivers()
    NodeFileManager.init(partition.expressionConfig.hConf)

    private val tmpPath = NodeFileManager.readRemote(partition.filePath)
    private val dataset = OGR_Driver.open(tmpPath, partition.driver)
    private val layer = if (partition.layer.isEmpty) dataset.GetLayer(0) else dataset.GetLayer(partition.layer)
    layer.ResetReading()
    layer.SetNextByIndex(partition.start)
    private var counter = partition.start

    private var nextRow: InternalRow = _

    /** Overrides PartitionReader.next: fetches next feature into nextRow; returns false when at end or after close. */
    override def next(): Boolean = {
        nextRow = null
        val feature = layer.GetNextFeature()
        if (counter < partition.end && feature != null) {
            val row = OGR_SchemaInference.getFeatureFields(feature, partition.schema, partition.asWKB)
            nextRow = SerializationUtil.createRow(row)
            counter = counter + 1
            true
        } else {
            close()
            false
        }
    }

    /** Overrides PartitionReader.get: returns the row set by last next(). */
    override def get(): InternalRow = nextRow

    /** Overrides PartitionReader.close: releases remote file path via NodeFileManager. */
    override def close(): Unit = NodeFileManager.releaseRemote(partition.filePath)

}
