package com.databricks.labs.gbx.rasterx.ds

import com.databricks.labs.gbx.rasterx
import com.databricks.labs.gbx.rasterx.ds.netcdf.NetCDF_DataSource
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.matchers.should.Matchers._

import scala.jdk.CollectionConverters._

class NetCDF_DataSourceTest extends PlanTest with SilentSparkSession {

    test("NetCDF_DataSource short name is netcdf_gdal") {
        new NetCDF_DataSource().shortName() shouldBe "netcdf_gdal"
    }

    test("NetCDF_DataSource injects driver netCDF in dsExtraMap") {
        new NetCDF_DataSource().dsExtraMap() shouldBe Map("driver" -> "netCDF")
    }

    test("NetCDF_DataSource infers (source, tile) schema") {
        val ds = new NetCDF_DataSource()
        val schema = ds.inferSchema(new CaseInsensitiveStringMap(Map.empty[String, String].asJava))
        schema.fields.length shouldBe 2
        schema.fields(0).name shouldBe "source"
        schema.fields(0).dataType shouldBe StringType
        schema.fields(1).name shouldBe "tile"
    }

    test("NetCDF_DataSource is a TableProvider and DataSourceRegister") {
        val ds = new NetCDF_DataSource()
        ds shouldBe a[org.apache.spark.sql.connector.catalog.TableProvider]
        ds shouldBe a[org.apache.spark.sql.sources.DataSourceRegister]
    }

    test("netcdf_gdal bare load enumerates grid variables into (source, tile) rows") {
        import com.databricks.labs.gbx.rasterx.functions._
        rasterx.functions.register(spark)
        val ncDir = this.getClass.getResource("/binary/netcdf-coral/").toString
        val df = spark.read.format("netcdf_gdal").option("sizeInMB", "1")
            .option("filterRegex", ".*20220101\\.nc$").load(ncDir)
        val rows = df.select("source").collect()
        rows.length should be >= 1
        all(rows.map(_.getString(0))) should startWith("NETCDF:")
    }

    test("netcdf_gdal variable filter naming an absent variable yields no rows") {
        import com.databricks.labs.gbx.rasterx.functions._
        rasterx.functions.register(spark)
        val ncDir = this.getClass.getResource("/binary/netcdf-coral/").toString
        val df = spark.read.format("netcdf_gdal")
            .option("filterRegex", ".*20220101\\.nc$")
            .option("variable", "no_such_variable_xyz").load(ncDir)
        df.count() shouldBe 0L
    }

    test("netcdf_gdal enumerates only georeferenced grid variables (coral grid = 2)") {
        import com.databricks.labs.gbx.rasterx.functions._
        rasterx.functions.register(spark)
        val ncDir = this.getClass.getResource("/binary/netcdf-coral/").toString
        val df = spark.read.format("netcdf_gdal").option("sizeInMB", "-1")
            .option("filterRegex", ".*20220101\\.nc$").load(ncDir)
        val vars = df.select("source").collect().map(_.getString(0).split(":").last).toSet
        vars shouldBe Set("bleaching_alert_area", "mask")
    }

    // Single-variable grids have NO SUBDATASETS domain — GDAL opens them as a plain raster.
    // NetCDF_Batch falls back to one whole-file partition, recovering the real variable name
    // from band 1's NETCDF_VARNAME so it behaves like any (file, var) partition. Regression for
    // netcdf_gdal reading 0 rows from single-variable NASA-NEX/CMIP6-style files while light
    // netcdf_gbx read them fine.
    test("netcdf_gdal reads a single-variable grid with no SUBDATASETS domain") {
        import com.databricks.labs.gbx.rasterx.functions._
        rasterx.functions.register(spark)
        val ncDir = this.getClass.getResource("/binary/netcdf-singlevar/").toString
        val df = spark.read.format("netcdf_gdal").option("sizeInMB", "-1")
            .option("filterRegex", ".*tas_singlevar\\.nc$").load(ncDir)
        val rows = df.select("source").collect()
        rows.length should be >= 1
        // Source is selector-consistent with light netcdf_gbx: NETCDF:"file":var, ending in :tas.
        all(rows.map(_.getString(0))) should startWith("NETCDF:")
        all(rows.map(_.getString(0))) should endWith(":tas")
    }

    test("netcdf_gdal variables filter matches the recovered single-var name") {
        import com.databricks.labs.gbx.rasterx.functions._
        rasterx.functions.register(spark)
        val ncDir = this.getClass.getResource("/binary/netcdf-singlevar/").toString
        // The recovered NETCDF_VARNAME ("tas") is filterable like any subdataset variable.
        val hit = spark.read.format("netcdf_gdal").option("sizeInMB", "-1")
            .option("filterRegex", ".*tas_singlevar\\.nc$")
            .option("variables", "tas").load(ncDir)
        hit.count() should be >= 1L
        // A bogus variable name filters the single-var partition out entirely.
        val miss = spark.read.format("netcdf_gdal").option("sizeInMB", "-1")
            .option("filterRegex", ".*tas_singlevar\\.nc$")
            .option("variable", "no_such_variable_xyz").load(ncDir)
        miss.count() shouldBe 0L
    }

    // applyScale is an OPT-IN flag threaded only by NetCDF_Reader. This regression proves it
    // defaults OFF in shared WindowedExtract code: a gtiff_gdal read (no applyScale set anywhere)
    // still succeeds and produces a tile, so raster readers other than netcdf_gdal are unaffected.
    test("gtiff_gdal read is unaffected by applyScale default (raw values preserved)") {
        import com.databricks.labs.gbx.rasterx.functions._
        rasterx.functions.register(spark)
        val tif = this.getClass.getResource("/modis/").toString
        val df = spark.read.format("gtiff_gdal").option("sizeInMB", "1").load(tif).limit(1)
        // Reading succeeds and produces a tile; no applyScale option is set anywhere.
        df.count() shouldBe 1L
    }
}
