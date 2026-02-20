package com.databricks.labs.gbx.rasterx.ds.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connector.write.{BatchWrite, Write, WriteBuilder}
import org.apache.spark.sql.types.StructType

/** WriteBuilder for GDAL: produces a Write whose toBatch is GDAL_BatchWrite. */
class GDAL_WriteBuilder(schema: StructType, options: Map[String, String]) extends WriteBuilder {
    /** Builds a Write that uses GDAL_BatchWrite with ExpressionConfig from the current SparkSession. */
    override def build(): Write = {
        val spark = SparkSession.builder().getOrCreate()
        val ec = ExpressionConfig(spark)
        new Write {
            /** Overrides Write.toBatch: returns GDAL_BatchWrite for this schema/options/config. */
            override def toBatch: BatchWrite = new GDAL_BatchWrite(schema, options, ec)
        }
    }
}
