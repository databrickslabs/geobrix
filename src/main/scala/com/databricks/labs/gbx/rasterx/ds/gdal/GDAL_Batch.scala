package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.util.{HadoopUtils, NodeFileManager}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.types.StructType

/** Scan/Batch for GDAL: plans one InputPartition per file and supplies GDAL_Reader as reader factory. */
class GDAL_Batch(schema: StructType, options: Map[String, String]) extends Scan with Batch {

    /** Overrides Scan.readSchema: returns table schema. */
    override def readSchema(): StructType = schema

    /** Overrides Scan.toBatch: returns this batch. */
    override def toBatch: Batch = this

    /** One partition per file under path (filtered by filterRegex), each with sizeInMB and expression config. */
    override def planInputPartitions(): Array[InputPartition] = {
        val inPath = options("path")
        val sizeInMB = options.getOrElse("sizeInMB", "16").toInt
        val filterRegex = options.getOrElse("filterRegex", ".*")

        val sparkSession = SparkSession.builder.getOrCreate
        val exprConfig = ExpressionConfig(sparkSession)
        NodeFileManager.init(exprConfig.hConf)

        val files = HadoopUtils.listAllHadoopFiles(inPath, exprConfig.hConf, filterRegex)
        val partitions = files.map { file => GDAL_Partition(file, sizeInMB, exprConfig) }
        partitions.toArray
    }

    /** Returns a factory that builds GDAL_Reader for each GDAL_Partition. */
    override def createReaderFactory(): PartitionReaderFactory =
        (partition: InputPartition) => {
            new GDAL_Reader(partition.asInstanceOf[GDAL_Partition])
        }



}
