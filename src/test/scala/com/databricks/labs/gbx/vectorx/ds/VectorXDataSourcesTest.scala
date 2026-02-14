package com.databricks.labs.gbx.vectorx.ds

import com.databricks.labs.gbx.vectorx.ds.gdb.FileGDB_DataSource
import com.databricks.labs.gbx.vectorx.ds.geojson.GeoJSON_DataSource
import com.databricks.labs.gbx.vectorx.ds.gpkg.GPKG_DataSource
import com.databricks.labs.gbx.vectorx.ds.ogr.{OGR_DataSource, OGR_Driver, OGR_Partition, OGR_Table}
import com.databricks.labs.gbx.vectorx.ds.shp.ShapeFile_DataSource
import org.apache.spark.sql.connector.catalog.{SupportsRead, TableProvider}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class VectorXDataSourcesTest extends AnyFunSuite {

    val testSchema: StructType = StructType(Array(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = true)
    ))

    // ====== FileGDB_DataSource ======

    test("FileGDB_DataSource should be a TableProvider") {
        val ds = new FileGDB_DataSource()
        ds shouldBe a[TableProvider]
    }

    test("FileGDB_DataSource should be a DataSourceRegister") {
        val ds = new FileGDB_DataSource()
        ds shouldBe a[DataSourceRegister]
    }

    test("FileGDB_DataSource should return correct shortName") {
        val ds = new FileGDB_DataSource()
        ds.shortName() shouldBe "file_gdb_ogr"
    }

    test("FileGDB_DataSource should return OpenFileGDB driver in extraMap") {
        val ds = new FileGDB_DataSource()
        val extras = ds.dsExtraMap()
        extras should contain key "driverName"
        extras("driverName") shouldBe "OpenFileGDB"
    }

    // ====== GeoJSON_DataSource ======

    test("GeoJSON_DataSource should be a TableProvider") {
        val ds = new GeoJSON_DataSource()
        ds shouldBe a[TableProvider]
    }

    test("GeoJSON_DataSource should be a DataSourceRegister") {
        val ds = new GeoJSON_DataSource()
        ds shouldBe a[DataSourceRegister]
    }

    test("GeoJSON_DataSource should return correct shortName") {
        val ds = new GeoJSON_DataSource()
        ds.shortName() shouldBe "geojson_ogr"
    }

    test("GeoJSON_DataSource should default to GeoJSONSeq for multi=true") {
        val ds = new GeoJSON_DataSource()
        val extras = ds.dsExtraMap(Map("multi" -> "true"))
        extras("driverName") shouldBe "GeoJSONSeq"
    }

    test("GeoJSON_DataSource should use GeoJSON for multi=false") {
        val ds = new GeoJSON_DataSource()
        val extras = ds.dsExtraMap(Map("multi" -> "false"))
        extras("driverName") shouldBe "GeoJSON"
    }

    test("GeoJSON_DataSource should default to GeoJSONSeq when multi not specified") {
        val ds = new GeoJSON_DataSource()
        val extras = ds.dsExtraMap()
        extras("driverName") shouldBe "GeoJSONSeq"
    }

    // ====== GPKG_DataSource ======

    test("GPKG_DataSource should be a TableProvider") {
        val ds = new GPKG_DataSource()
        ds shouldBe a[TableProvider]
    }

    test("GPKG_DataSource should be a DataSourceRegister") {
        val ds = new GPKG_DataSource()
        ds shouldBe a[DataSourceRegister]
    }

    test("GPKG_DataSource should return correct shortName") {
        val ds = new GPKG_DataSource()
        ds.shortName() shouldBe "gpkg_ogr"
    }

    test("GPKG_DataSource should return GPKG driver in extraMap") {
        val ds = new GPKG_DataSource()
        val extras = ds.dsExtraMap()
        extras should contain key "driverName"
        extras("driverName") shouldBe "GPKG"
    }

    // ====== ShapeFile_DataSource ======

    test("ShapeFile_DataSource should be a TableProvider") {
        val ds = new ShapeFile_DataSource()
        ds shouldBe a[TableProvider]
    }

    test("ShapeFile_DataSource should be a DataSourceRegister") {
        val ds = new ShapeFile_DataSource()
        ds shouldBe a[DataSourceRegister]
    }

    test("ShapeFile_DataSource should return correct shortName") {
        val ds = new ShapeFile_DataSource()
        ds.shortName() shouldBe "shapefile_ogr"
    }

    test("ShapeFile_DataSource should return ESRI Shapefile driver in extraMap") {
        val ds = new ShapeFile_DataSource()
        val extras = ds.dsExtraMap()
        extras should contain key "driverName"
        extras("driverName") shouldBe "ESRI Shapefile"
    }

    // ====== OGR_DataSource ======

    test("OGR_DataSource should be a TableProvider") {
        val ds = new OGR_DataSource()
        ds shouldBe a[TableProvider]
    }

    test("OGR_DataSource should be a DataSourceRegister") {
        val ds = new OGR_DataSource()
        ds shouldBe a[DataSourceRegister]
    }

    test("OGR_DataSource should return correct shortName") {
        val ds = new OGR_DataSource()
        ds.shortName() shouldBe "ogr"
    }

    // ====== OGR_Table ======

    test("OGR_Table should be a SupportsRead") {
        val table = new OGR_Table(testSchema, Map.empty)
        table shouldBe a[SupportsRead]
    }

    test("OGR_Table should return name") {
        val table = new OGR_Table(testSchema, Map.empty)
        table.name() shouldBe "ogr"
    }

    test("OGR_Table should return schema") {
        val table = new OGR_Table(testSchema, Map.empty)
        val schema = table.schema()
        schema should not be null
        schema.fields should have length 2
    }

    test("OGR_Table should return columns") {
        val table = new OGR_Table(testSchema, Map.empty)
        val columns = table.columns()
        columns should not be null
        columns should have length 2
        columns(0).name() shouldBe "id"
        columns(1).name() shouldBe "name"
    }

    test("OGR_Table should return capabilities with BATCH_READ") {
        val table = new OGR_Table(testSchema, Map.empty)
        val capabilities = table.capabilities()
        capabilities should not be null
        capabilities should contain(org.apache.spark.sql.connector.catalog.TableCapability.BATCH_READ)
    }

    test("OGR_Table should create ScanBuilder") {
        val table = new OGR_Table(testSchema, Map("path" -> "/test"))
        val builder = table.newScanBuilder(new org.apache.spark.sql.util.CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
        builder should not be null
    }

    // ====== OGR_Partition ======

    test("OGR_Partition should be an InputPartition") {
        val partition = OGR_Partition(
          filePath = "/test/file.shp",
          driver = "ESRI Shapefile",
          layer = "layer1",
          asWKB = true,
          schema = testSchema,
          start = 0,
          end = 100,
          expressionConfig = null
        )
        partition shouldBe a[org.apache.spark.sql.connector.read.InputPartition]
    }

    test("OGR_Partition should be Serializable") {
        val partition = OGR_Partition(
          filePath = "/test/file.shp",
          driver = "ESRI Shapefile",
          layer = "layer1",
          asWKB = false,
          schema = testSchema,
          start = 0,
          end = 100,
          expressionConfig = null
        )
        partition shouldBe a[Serializable]
    }

    test("OGR_Partition should store filePath") {
        val partition = OGR_Partition(
          filePath = "/test/file.shp",
          driver = "ESRI Shapefile",
          layer = "layer1",
          asWKB = true,
          schema = testSchema,
          start = 0,
          end = 100,
          expressionConfig = null
        )
        partition.filePath shouldBe "/test/file.shp"
    }

    test("OGR_Partition should store driver name") {
        val partition = OGR_Partition(
          filePath = "/test/file.shp",
          driver = "ESRI Shapefile",
          layer = "layer1",
          asWKB = true,
          schema = testSchema,
          start = 0,
          end = 100,
          expressionConfig = null
        )
        partition.driver shouldBe "ESRI Shapefile"
    }

    test("OGR_Partition should store layer name") {
        val partition = OGR_Partition(
          filePath = "/test/file.shp",
          driver = "ESRI Shapefile",
          layer = "test_layer",
          asWKB = true,
          schema = testSchema,
          start = 0,
          end = 100,
          expressionConfig = null
        )
        partition.layer shouldBe "test_layer"
    }

    test("OGR_Partition should store asWKB flag") {
        val partition = OGR_Partition(
          filePath = "/test/file.shp",
          driver = "ESRI Shapefile",
          layer = "layer1",
          asWKB = false,
          schema = testSchema,
          start = 0,
          end = 100,
          expressionConfig = null
        )
        partition.asWKB shouldBe false
    }

    test("OGR_Partition should store start and end") {
        val partition = OGR_Partition(
          filePath = "/test/file.shp",
          driver = "ESRI Shapefile",
          layer = "layer1",
          asWKB = true,
          schema = testSchema,
          start = 10,
          end = 50,
          expressionConfig = null
        )
        partition.start shouldBe 10
        partition.end shouldBe 50
    }

    // ====== OGR_Driver ======

    test("OGR_Driver handleZip should handle .zip extension") {
        val path = "/path/to/file.zip"
        val result = OGR_Driver.handleZip(path)
        result shouldBe "/vsizip//path/to/file.zip"
    }

    test("OGR_Driver handleZip should handle existing /vsizip// prefix") {
        val path = "/vsizip//path/to/file.zip"
        val result = OGR_Driver.handleZip(path)
        result shouldBe "/vsizip//path/to/file.zip"
    }

    test("OGR_Driver handleZip should fix /vsizip/ prefix (single slash)") {
        val path = "/vsizip/path/to/file.zip"
        val result = OGR_Driver.handleZip(path)
        result shouldBe "/vsizip//path/to/file.zip"
    }

    test("OGR_Driver handleZip should handle vsizip/ prefix (no leading slash)") {
        val path = "vsizip/path/to/file.zip"
        val result = OGR_Driver.handleZip(path)
        result shouldBe "vsizip//path/to/file.zip"
    }

    test("OGR_Driver handleZip should handle path without leading slash") {
        val path = "path/to/file.zip"
        val result = OGR_Driver.handleZip(path)
        result shouldBe "/vsizip//path/to/file.zip"
    }

    test("OGR_Driver handleZip should handle .zip/ in middle of path") {
        val path = "/path/file.zip/layer.shp"
        val result = OGR_Driver.handleZip(path)
        result shouldBe "/vsizip//path/file.zip/layer.shp"
    }

    test("OGR_Driver handleZip should not modify non-zip paths") {
        val path = "/path/to/file.shp"
        val result = OGR_Driver.handleZip(path)
        result shouldBe "/path/to/file.shp"
    }

    test("OGR_Driver handleZip should be case insensitive") {
        val path = "/path/to/file.ZIP"
        val result = OGR_Driver.handleZip(path)
        result shouldBe "/vsizip//path/to/file.ZIP"
    }

    test("OGR_Driver cleanPath should remove file: prefix") {
        val path = "file:/path/to/file.shp"
        val result = OGR_Driver.cleanPath(path)
        result shouldBe "/path/to/file.shp"
    }

    test("OGR_Driver cleanPath should handle zip and remove file: prefix") {
        val path = "file:/path/to/file.zip"
        val result = OGR_Driver.cleanPath(path)
        // file: prefix is removed, resulting in /path/to/file.zip which is then wrapped
        result shouldBe "/vsizip///path/to/file.zip"
    }

    test("OGR_Driver cleanPath should handle path without file: prefix") {
        val path = "/path/to/file.shp"
        val result = OGR_Driver.cleanPath(path)
        result shouldBe "/path/to/file.shp"
    }

}
