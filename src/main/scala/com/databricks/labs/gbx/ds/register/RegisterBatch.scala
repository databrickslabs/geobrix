package com.databricks.labs.gbx.ds.register

import com.databricks.labs.gbx
import com.databricks.labs.gbx.gridx
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.vectorx.jts
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

/**
  * A "batch" that performs no I/O but runs function registration when planned (e.g. gridx, rasterx, vectorx).
  * Used when the "register" data source is loaded so that registration happens as part of the query plan.
  */
class RegisterBatch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    /** Overrides Scan.readSchema: returns table schema. */
    override def readSchema(): StructType = schema

    /** Overrides Scan.toBatch: returns this batch. */
    override def toBatch: Batch = this

    /** Overrides Batch.planInputPartitions: runs registration (options "functions" = gridx.bng | vectorx.jts.legacy | rasterx | all); returns empty partitions. */
    override def planInputPartitions(): Array[InputPartition] = {
        val registerWhat = options.getOrElse("functions", "all")
        registerWhat match {
            case "gridx.bng"      => gridx.bng.functions.register(SparkSession.active)
            case "vectorx.jts.legacy" => jts.legacy.functions.register(SparkSession.active)
            case "rasterx"        => functions.register(SparkSession.active)
            case "all"            =>
                gridx.bng.functions.register(SparkSession.active)
                jts.legacy.functions.register(SparkSession.active)
                gbx.rasterx.functions.register(SparkSession.active)
        }
        Seq.empty[InputPartition].toArray // No data to read, just perform registration
    }

    /** Overrides Batch.createReaderFactory: returns null (no partitions, no data read). */
    override def createReaderFactory(): PartitionReaderFactory = (_: InputPartition) => null

}
