package com.databricks.labs.gbx.vectorx.ds

import com.databricks.labs.gbx.vectorx.ds.netcdf.NetCDF_OGR_DataSource
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class NetCDF_OGR_DataSourceTest extends PlanTest with SilentSparkSession {

    test("netcdf_ogr short name is netcdf_ogr") {
        new NetCDF_OGR_DataSource().shortName() shouldBe "netcdf_ogr"
    }

    test("netcdf_ogr injects driverName netCDF") {
        new NetCDF_OGR_DataSource().dsExtraMap() shouldBe Map("driverName" -> "netCDF")
    }

    test("netcdf_ogr is a TableProvider and DataSourceRegister") {
        val ds = new NetCDF_OGR_DataSource()
        ds shouldBe a[org.apache.spark.sql.connector.catalog.TableProvider]
        ds shouldBe a[org.apache.spark.sql.sources.DataSourceRegister]
    }

    // The container's OGR netCDF driver (GDAL 3.11.4) surfaces the CF-DSG "point" fixture as 5
    // native point features (verified via ogrinfo). If a future GDAL build stops exposing DSG
    // features this asserts them directly, so a regression surfaces here rather than silently.
    test("netcdf_ogr reads CF-DSG point features into the shared vector schema") {
        val dsgDir = this.getClass.getResource("/binary/netcdf-dsg/").toString
        val df = spark.read.format("netcdf_ogr").load(dsgDir)
        df.columns should contain allOf ("geom_0", "geom_0_srid", "geom_0_srid_proj")
        df.count() shouldBe 5L
    }

    // A grid-only netCDF (CMIP5/coral/ECMWF style) carries NO CF-DSG feature type, so the OGR
    // netCDF driver exposes zero vector layers. In GDAL 3.11.4 that means the driver cannot open
    // the file as a vector datasource at all: schema inference raises rather than returning an
    // empty frame. Either way netcdf_ogr surfaces no features from a grid — the raster side of
    // such files is the domain of the heavy netcdf_gdal reader (Task 2), not this vector reader.
    test("netcdf_ogr on a grid file surfaces no vector features (raises)") {
        val gridFile = this.getClass.getResource("/binary/netcdf-coral/ct5km_baa-max-7d_v3.1_20220101.nc").toString
        an[Exception] should be thrownBy {
            spark.read.format("netcdf_ogr").load(gridFile).count()
        }
    }
}
