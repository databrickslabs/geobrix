package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.{NodeFileManager, SerializationUtil}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.gdal.ogr.{DataSource, Layer}

import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

/** Reads one partition of an OGR source: opens the layer and yields feature rows from start to end index. */
class OGR_Reader(partition: OGR_Partition) extends PartitionReader[InternalRow] {

    GDALManager.init(partition.expressionConfig)
    OGR_SchemaInference.enableOGRDrivers()
    NodeFileManager.init(partition.expressionConfig.hConf)

    // One hold on the node-file copy per reader. close() runs at the end of input (next) and again from
    // Spark's task-completion listener; the lease releases on the first close only.
    private val lease = NodeFileManager.acquire(partition.filePath)
    private val closed = new AtomicBoolean(false)
    private var dataset: DataSource = _
    private val layer: Layer =
        try {
            dataset = OGR_Driver.open(lease.localPath, partition.driver)
            val l = if (partition.layer.isEmpty) dataset.GetLayer(0) else dataset.GetLayer(partition.layer)
            if (l == null) {
                val which = if (partition.layer.isEmpty) "0" else s"'${partition.layer}'"
                throw new IllegalArgumentException(s"No layer $which in ${partition.filePath}")
            }
            l.ResetReading()
            l.SetNextByIndex(partition.start)
            l
        } catch {
            // Spark never calls close() on a reader whose constructor threw, so let go of everything here.
            case NonFatal(e) =>
                if (dataset != null) dataset.delete()
                lease.close()
                throw e
        }
    private var counter = partition.start

    private var nextRow: InternalRow = _

    /** Overrides PartitionReader.next: fetches next feature into nextRow; returns false when at end or after close. */
    override def next(): Boolean = {
        nextRow = null
        // After close the dataset is freed, so its layer must not be touched again.
        val feature = if (closed.get) null else layer.GetNextFeature()
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

    /** Overrides PartitionReader.close: closes the dataset, then releases the copy; idempotent. */
    override def close(): Unit = {
        if (closed.compareAndSet(false, true)) {
            try dataset.delete()
            finally lease.close()
        }
    }

}
