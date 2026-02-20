package com.databricks.labs.gbx.rasterx.ds

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.ds.gdal._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class GDAL_ComponentsTest extends PlanTest with SilentSparkSession {

    // ========== GDAL_WriterMsg Tests ==========

    test("GDAL_WriterMsg should store error messages") {
        val errors = Array("Error 1", "Error 2", "Error 3")
        val msg = GDAL_WriterMsg(errors)
        
        msg.gdalErrors.length shouldBe 3
        msg.gdalErrors(0) shouldBe "Error 1"
        msg.gdalErrors(1) shouldBe "Error 2"
        msg.gdalErrors(2) shouldBe "Error 3"
    }

    test("GDAL_WriterMsg should handle empty error array") {
        val msg = GDAL_WriterMsg(Array.empty)
        
        msg.gdalErrors.length shouldBe 0
    }

    test("GDAL_WriterMsg should be a WriterCommitMessage") {
        val msg = GDAL_WriterMsg(Array("error"))
        
        msg shouldBe a [org.apache.spark.sql.connector.write.WriterCommitMessage]
    }

    // ========== GDAL_Partition Tests ==========

    test("GDAL_Partition should store file path and configuration") {
        val config = ExpressionConfig(spark)
        val partition = GDAL_Partition("/path/to/file.tif", 16, config)
        
        partition.filePath shouldBe "/path/to/file.tif"
        partition.sizeInMB shouldBe 16
        partition.expressionConfig should not be null
    }

    test("GDAL_Partition should be serializable") {
        val config = ExpressionConfig(Map.empty, new SerializableConfiguration(spark.sessionState.newHadoopConf()))
        val partition = GDAL_Partition("/path/to/file.tif", 32, config)
        
        partition shouldBe a [Serializable]
        partition shouldBe a [org.apache.spark.sql.connector.read.InputPartition]
    }

    test("GDAL_Partition should support different size configurations") {
        val config = ExpressionConfig(spark)
        
        val small = GDAL_Partition("/file1.tif", 4, config)
        val medium = GDAL_Partition("/file2.tif", 16, config)
        val large = GDAL_Partition("/file3.tif", 64, config)
        
        small.sizeInMB shouldBe 4
        medium.sizeInMB shouldBe 16
        large.sizeInMB shouldBe 64
    }

    // ========== GDAL_DataSource Tests ==========

    test("GDAL_DataSource should have correct short name") {
        val dataSource = new GDAL_DataSource()
        
        dataSource.shortName() shouldBe "gdal"
    }

    test("GDAL_DataSource should infer schema with source and tile columns") {
        val dataSource = new GDAL_DataSource()
        val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
        
        val schema = dataSource.inferSchema(options)
        
        schema.fields.length shouldBe 2
        schema.fields(0).name shouldBe "source"
        schema.fields(0).dataType shouldBe StringType
        schema.fields(1).name shouldBe "tile"
        // tile is a struct type containing raster data
    }

    test("GDAL_DataSource should be a TableProvider") {
        val dataSource = new GDAL_DataSource()
        
        dataSource shouldBe a [org.apache.spark.sql.connector.catalog.TableProvider]
    }

    test("GDAL_DataSource should be a DataSourceRegister") {
        val dataSource = new GDAL_DataSource()
        
        dataSource shouldBe a [org.apache.spark.sql.sources.DataSourceRegister]
    }

    test("GDAL_DataSource should create GDAL_Table") {
        val dataSource = new GDAL_DataSource()
        val schema = StructType(Array(
            StructField("source", StringType),
            StructField("tile", BinaryType)
        ))
        val properties = Map("path" -> "/test/path").asJava
        
        val table = dataSource.getTable(schema, Array.empty, properties)
        
        table should not be null
        table shouldBe a [GDAL_Table]
    }

    // ========== GDAL_Table Tests ==========

    test("GDAL_Table should have correct name") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new GDAL_Table(schema, Map.empty)
        
        table.name() shouldBe "gdal"
    }

    test("GDAL_Table should return provided schema") {
        val schema = StructType(Array(
            StructField("source", StringType),
            StructField("tile", BinaryType)
        ))
        val table = new GDAL_Table(schema, Map.empty)
        
        val returnedSchema = table.schema()
        returnedSchema.fields.length shouldBe 2
        returnedSchema.fields(0).name shouldBe "source"
        returnedSchema.fields(1).name shouldBe "tile"
    }

    test("GDAL_Table should convert schema to columns") {
        val schema = StructType(Array(
            StructField("col1", StringType, nullable = false),
            StructField("col2", BinaryType, nullable = true)
        ))
        val table = new GDAL_Table(schema, Map.empty)
        
        val columns = table.columns()
        columns.length shouldBe 2
        columns(0).name() shouldBe "col1"
        columns(0).dataType() shouldBe StringType
        columns(1).name() shouldBe "col2"
        columns(1).dataType() shouldBe BinaryType
    }

    test("GDAL_Table should support batch read capability") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new GDAL_Table(schema, Map.empty)
        
        val capabilities = table.capabilities()
        capabilities should contain(org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ)
    }

    test("GDAL_Table should support batch write capability") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new GDAL_Table(schema, Map.empty)
        
        val capabilities = table.capabilities()
        capabilities should contain(org.apache.spark.sql.connector.catalog.TableCapability.BATCH_WRITE)
    }

    test("GDAL_Table should be a SupportsRead") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new GDAL_Table(schema, Map("path" -> "/test"))
        
        table shouldBe a [org.apache.spark.sql.connector.catalog.SupportsRead]
    }

    test("GDAL_Table should be a SupportsWrite") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new GDAL_Table(schema, Map.empty)
        
        table shouldBe a [org.apache.spark.sql.connector.catalog.SupportsWrite]
    }

    // ========== GDAL_Batch Tests ==========
    // Note: Avoid tests that trigger planInputPartitions() or other file I/O operations

    test("GDAL_Batch should return provided schema") {
        val schema = StructType(Array(StructField("col", StringType)))
        val batch = new GDAL_Batch(schema, Map("path" -> "/test"))
        
        batch.readSchema() shouldBe schema
    }

    test("GDAL_Batch should return itself as Batch") {
        val schema = StructType(Array(StructField("col", StringType)))
        val batch = new GDAL_Batch(schema, Map("path" -> "/test"))
        
        batch.toBatch shouldBe batch
    }

    test("GDAL_Batch should be a Scan and Batch") {
        val schema = StructType(Array(StructField("col", StringType)))
        val batch = new GDAL_Batch(schema, Map("path" -> "/test"))
        
        batch shouldBe a [org.apache.spark.sql.connector.read.Scan]
        batch shouldBe a [org.apache.spark.sql.connector.read.Batch]
    }

}
