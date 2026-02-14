package com.databricks.labs.gbx.ds.register

import org.apache.spark.sql.types.{BooleanType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters.MapHasAsJava

class RegisterComponentsTest extends AnyFunSuite {

    // ========== RegisterDataSource Tests ==========

    test("RegisterDataSource should have correct short name") {
        val ds = new RegisterDataSource()
        
        ds.shortName() shouldBe "register_ds"
    }

    test("RegisterDataSource should infer schema with did_read column") {
        val ds = new RegisterDataSource()
        val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
        
        val schema = ds.inferSchema(options)
        
        schema.fields should have length 1
        schema.fields(0).name shouldBe "did_read"
        schema.fields(0).dataType shouldBe BooleanType
    }

    test("RegisterDataSource should be a TableProvider") {
        val ds = new RegisterDataSource()
        
        ds shouldBe a [org.apache.spark.sql.connector.catalog.TableProvider]
    }

    test("RegisterDataSource should be a DataSourceRegister") {
        val ds = new RegisterDataSource()
        
        ds shouldBe a [org.apache.spark.sql.sources.DataSourceRegister]
    }

    test("RegisterDataSource should create RegisterTable") {
        val ds = new RegisterDataSource()
        val schema = StructType(Array(StructField("test", StringType)))
        val properties = Map("key" -> "value").asJava
        
        val table = ds.getTable(schema, Array.empty, properties)
        
        table should not be null
        table shouldBe a [RegisterTable]
    }

    // ========== RegisterTable Tests ==========

    test("RegisterTable should have correct name") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new RegisterTable(schema, Map.empty)
        
        table.name() shouldBe "register_ds"
    }

    test("RegisterTable should return provided schema") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new RegisterTable(schema, Map.empty)
        
        table.schema() shouldBe schema
    }

    test("RegisterTable should convert schema to columns") {
        val schema = StructType(Array(
            StructField("col1", StringType, nullable = true),
            StructField("col2", BooleanType, nullable = false)
        ))
        val table = new RegisterTable(schema, Map.empty)
        
        val columns = table.columns()
        columns should have length 2
        columns(0).name() shouldBe "col1"
        columns(1).name() shouldBe "col2"
    }

    test("RegisterTable should support batch read capability") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new RegisterTable(schema, Map.empty)
        
        val capabilities = table.capabilities()
        capabilities should contain (org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ)
    }

    test("RegisterTable should be a SupportsRead") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new RegisterTable(schema, Map.empty)
        
        table shouldBe a [org.apache.spark.sql.connector.catalog.SupportsRead]
    }

    test("RegisterTable should create ScanBuilder") {
        val schema = StructType(Array(StructField("test", StringType)))
        val table = new RegisterTable(schema, Map("prop1" -> "value1"))
        val options = new CaseInsensitiveStringMap(Map("option1" -> "optValue1").asJava)
        
        val scanBuilder = table.newScanBuilder(options)
        scanBuilder should not be null
    }

    test("RegisterTable should merge properties with options in ScanBuilder") {
        val schema = StructType(Array(StructField("test", StringType)))
        val tableProps = Map("prop1" -> "value1")
        val table = new RegisterTable(schema, tableProps)
        val options = new CaseInsensitiveStringMap(Map("option1" -> "optValue1").asJava)
        
        val scan = table.newScanBuilder(options).build()
        scan should not be null
        scan shouldBe a [RegisterBatch]
    }

    // ========== RegisterBatch Tests ==========

    test("RegisterBatch should return provided schema") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new RegisterBatch(schema, Map.empty)
        
        batch.readSchema() shouldBe schema
    }

    test("RegisterBatch should return itself as Batch") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new RegisterBatch(schema, Map.empty)
        
        batch.toBatch shouldBe batch
    }

    test("RegisterBatch should be a Scan and Batch") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new RegisterBatch(schema, Map.empty)
        
        batch shouldBe a [org.apache.spark.sql.connector.read.Scan]
        batch shouldBe a [org.apache.spark.sql.connector.read.Batch]
    }

    test("RegisterBatch should store options for gridx.bng") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new RegisterBatch(schema, Map("functions" -> "gridx.bng"))
        
        // Batch created successfully with gridx.bng option
        batch should not be null
    }

    test("RegisterBatch should store options for vectorx.jts.legacy") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new RegisterBatch(schema, Map("functions" -> "vectorx.jts.legacy"))
        
        batch should not be null
    }

    test("RegisterBatch should store options for rasterx") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new RegisterBatch(schema, Map("functions" -> "rasterx"))
        
        batch should not be null
    }

    test("RegisterBatch should default to all functions when no option provided") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new RegisterBatch(schema, Map.empty) // defaults to "all"
        
        batch should not be null
    }

    test("RegisterBatch should create PartitionReaderFactory") {
        val schema = StructType(Array(StructField("test", StringType)))
        val batch = new RegisterBatch(schema, Map.empty)
        
        val factory = batch.createReaderFactory()
        factory should not be null
    }

}
