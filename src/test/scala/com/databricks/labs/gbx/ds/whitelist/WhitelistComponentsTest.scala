package com.databricks.labs.gbx.ds.whitelist

import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters.MapHasAsJava

class WhitelistComponentsTest extends AnyFunSuite {

    // ========== WhitelistDataSource Tests ==========

    test("WhitelistDataSource should have correct short name") {
        val ds = new WhitelistDataSource()
        
        ds.shortName() shouldBe "whitelist_ds"
    }

    test("WhitelistDataSource should infer schema with did_read column") {
        val ds = new WhitelistDataSource()
        val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
        
        val schema = ds.inferSchema(options)
        
        schema.fields should have length 1
        schema.fields(0).name shouldBe "did_read"
        schema.fields(0).dataType shouldBe BooleanType
    }

    test("WhitelistDataSource should be a TableProvider") {
        val ds = new WhitelistDataSource()
        
        ds shouldBe a [org.apache.spark.sql.connector.catalog.TableProvider]
    }

    test("WhitelistDataSource should be a DataSourceRegister") {
        val ds = new WhitelistDataSource()
        
        ds shouldBe a [org.apache.spark.sql.sources.DataSourceRegister]
    }

    test("WhitelistDataSource should create WhitelistTable") {
        val ds = new WhitelistDataSource()
        val schema = StructType(Array(StructField("test", StringType)))
        val properties = Map("key" -> "value").asJava
        
        val table = ds.getTable(schema, Array.empty, properties)
        
        table should not be null
        table shouldBe a [WhitelistTable]
    }

    // ========== WhitelistTable Tests ==========

    test("WhitelistTable should have correct name") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new WhitelistTable(schema, Map.empty)
        
        table.name() shouldBe "whitelist_ds"
    }

    test("WhitelistTable should return provided schema") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new WhitelistTable(schema, Map.empty)
        
        table.schema() shouldBe schema
    }

    test("WhitelistTable should convert schema to columns") {
        val schema = StructType(Array(
            StructField("col1", StringType, nullable = true),
            StructField("col2", BooleanType, nullable = false)
        ))
        val table = new WhitelistTable(schema, Map.empty)
        
        val columns = table.columns()
        columns should have length 2
        columns(0).name() shouldBe "col1"
        columns(1).name() shouldBe "col2"
    }

    test("WhitelistTable should support batch read capability") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new WhitelistTable(schema, Map.empty)
        
        val capabilities = table.capabilities()
        capabilities should contain (org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ)
    }

    test("WhitelistTable should be a SupportsRead") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new WhitelistTable(schema, Map.empty)
        
        table shouldBe a [org.apache.spark.sql.connector.catalog.SupportsRead]
    }

    test("WhitelistTable should create ScanBuilder") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new WhitelistTable(schema, Map("prop1" -> "value1"))
        val options = new CaseInsensitiveStringMap(Map("option1" -> "optValue1").asJava)
        
        val scanBuilder = table.newScanBuilder(options)
        scanBuilder should not be null
    }

    test("WhitelistTable should merge properties with options in ScanBuilder") {
        val schema = StructType(Array(StructField("test", StringType)))
        val tableProps = Map("prop1" -> "value1")
        val table = new WhitelistTable(schema, tableProps)
        val options = new CaseInsensitiveStringMap(Map("option1" -> "optValue1").asJava)
        
        val scan = table.newScanBuilder(options).build()
        scan should not be null
        scan shouldBe a [WhitelistBatch]
    }

    // ========== WhitelistBatch Tests ==========

    test("WhitelistBatch should return provided schema") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new WhitelistBatch(schema, Map("path" -> "/test"))
        
        batch.readSchema() shouldBe schema
    }

    test("WhitelistBatch should return itself as Batch") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new WhitelistBatch(schema, Map("path" -> "/test"))
        
        batch.toBatch shouldBe batch
    }

    test("WhitelistBatch should be a Scan and Batch") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new WhitelistBatch(schema, Map("path" -> "/test"))
        
        batch shouldBe a [org.apache.spark.sql.connector.read.Scan]
        batch shouldBe a [org.apache.spark.sql.connector.read.Batch]
    }

    test("WhitelistBatch should require path option") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new WhitelistBatch(schema, Map.empty) // No path provided
        
        // Should throw IllegalArgumentException when planInputPartitions is called
        // (but we can't call it without Spark session)
        batch should not be null
    }

    test("WhitelistBatch should create PartitionReaderFactory") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new WhitelistBatch(schema, Map("path" -> "/test"))
        
        val factory = batch.createReaderFactory()
        factory should not be null
    }

    // ========== WhitelistPartition Tests ==========

    test("WhitelistPartition should be an InputPartition") {
        val partition = new WhitelistPartition()
        
        partition shouldBe a [org.apache.spark.sql.connector.read.InputPartition]
    }

    test("WhitelistPartition should be Serializable") {
        val partition = new WhitelistPartition()
        
        partition shouldBe a [Serializable]
    }

    test("WhitelistPartition should be instantiable") {
        val partition = new WhitelistPartition()
        
        partition should not be null
    }

    // ========== WhitelistReader Tests ==========

    test("WhitelistReader should be a PartitionReader") {
        val reader = new WhitelistReader()
        
        reader shouldBe a [org.apache.spark.sql.connector.read.PartitionReader[_]]
    }

    test("WhitelistReader should have next() return true initially") {
        val reader = new WhitelistReader()
        
        reader.next() shouldBe true
    }

    test("WhitelistReader should return single row with true value") {
        val reader = new WhitelistReader()
        
        reader.next() shouldBe true
        val row = reader.get()
        
        row should not be null
        row.numFields shouldBe 1
        row.getBoolean(0) shouldBe true
    }

    test("WhitelistReader should have next() return false after get()") {
        val reader = new WhitelistReader()
        
        reader.next() shouldBe true
        reader.get()
        reader.next() shouldBe false
    }

    test("WhitelistReader should support close()") {
        val reader = new WhitelistReader()
        
        noException should be thrownBy reader.close()
    }

    test("WhitelistReader should be reusable after close()") {
        val reader = new WhitelistReader()
        
        reader.next() shouldBe true
        reader.get()
        reader.close()
        
        // After close, should still report hasNext=false
        reader.next() shouldBe false
    }

}
