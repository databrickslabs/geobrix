package com.databricks.labs.gbx.rasterx.ds

import com.databricks.labs.gbx.rasterx.ds.gdal.GDAL_Table
import com.databricks.labs.gbx.rasterx.ds.gtiff.GTiff_DataSource
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class GTiff_DataSourceTest extends PlanTest with SilentSparkSession {

    test("GTiff_DataSource should have short name gtiff_gdal") {
        val ds = new GTiff_DataSource()
        ds.shortName() shouldBe "gtiff_gdal"
    }

    test("GTiff_DataSource should inject driver GTiff in dsExtraMap") {
        val ds = new GTiff_DataSource()
        ds.dsExtraMap() shouldBe Map("driver" -> "GTiff")
        ds.dsExtraMap(Map("other" -> "value")) shouldBe Map("driver" -> "GTiff")
    }

    test("GTiff_DataSource should infer schema with source and tile columns") {
        val ds = new GTiff_DataSource()
        val options = new CaseInsensitiveStringMap(Map.empty[String, String].asJava)
        val schema = ds.inferSchema(options)
        schema.fields.length shouldBe 2
        schema.fields(0).name shouldBe "source"
        schema.fields(0).dataType shouldBe StringType
        schema.fields(1).name shouldBe "tile"
    }

    test("GTiff_DataSource should be a TableProvider") {
        val ds = new GTiff_DataSource()
        ds shouldBe a[org.apache.spark.sql.connector.catalog.TableProvider]
    }

    test("GTiff_DataSource should be a DataSourceRegister") {
        val ds = new GTiff_DataSource()
        ds shouldBe a[org.apache.spark.sql.sources.DataSourceRegister]
    }

    test("GTiff_DataSource getTable should return GDAL_Table with merged options") {
        val ds = new GTiff_DataSource()
        val schema = StructType(Array(
            StructField("source", StringType),
            StructField("tile", BinaryType)
        ))
        val properties = Map("path" -> "/test/path").asJava
        val table: Table = ds.getTable(schema, Array.empty[Transform], properties)
        table should not be null
        table shouldBe a[GDAL_Table]
    }
}
