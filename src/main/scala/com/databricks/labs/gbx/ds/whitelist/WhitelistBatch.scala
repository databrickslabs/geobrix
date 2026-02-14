package com.databricks.labs.gbx.ds.whitelist

import com.databricks.labs.gbx.util.HadoopUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.SerializableConfiguration

import scala.util.Try

/** Scan/Batch that runs a single empty partition (e.g. to trigger path whitelist or side effects like registration). */
class WhitelistBatch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    /** Overrides Scan.readSchema: returns table schema. */
    override def readSchema(): StructType = schema

    /** Overrides Scan.toBatch: returns this batch. */
    override def toBatch: Batch = this

    /** Overrides Batch.planInputPartitions: single WhitelistPartition (no per-file splitting); lists path for side effect. */
    override def planInputPartitions(): Array[InputPartition] = {
        val rootPath = options.getOrElse("path", throw new IllegalArgumentException("Option 'path' is required for Whitelist data source"))
        val hconf = new SerializableConfiguration(SparkSession.builder().getOrCreate().sessionState.newHadoopConf())
        Try(HadoopUtils.listAllHadoopFiles(rootPath, hconf, "", dropEmpty = true))
        Array(new WhitelistPartition)
    }

    /** Overrides Batch.createReaderFactory: always builds WhitelistReader (partition ignored). */
    override def createReaderFactory(): PartitionReaderFactory = (_: InputPartition) => new WhitelistReader()

}

