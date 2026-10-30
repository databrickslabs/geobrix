package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.ogr.{FieldDefn, ogr, ogrConstants}
import org.gdal.osr.SpatialReference
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Contract tests for the OGR reader:
  *   - bare .shp file path reads correctly (sidecar-bundle staging fix)
  *   - multi-stem divergent-schema directory raises IllegalArgumentException
  *   - multi-stem same-schema directory reads without error
  *   - directory of unzipped shapefiles counts exactly (no sidecar over-count)
  *   - mixed unzipped + zipped directory counts exactly
  */
class OgrReaderContractTest extends PlanTest with SilentSparkSession {

    // ---------------------------------------------------------------------------
    // Helper: write a minimal ESRI Shapefile to a temp dir under a given stem.
    // Fields describe the schema; the file will have one POINT feature.
    // ---------------------------------------------------------------------------
    private def writeShapefile(
        dir: java.io.File,
        stem: String,
        fields: Seq[(String, Int)],
        name: String = ""
    ): Unit = {
        GDALManager.initOgr()
        val drv = ogr.GetDriverByName("ESRI Shapefile")
        require(drv != null, "ESRI Shapefile driver not available")
        val shpPath = new java.io.File(dir, s"$stem.shp").getAbsolutePath
        val srs = new SpatialReference()
        srs.ImportFromEPSG(4326)
        val ds = drv.CreateDataSource(shpPath)
        require(ds != null, s"Could not create datasource at $shpPath")
        val layer = ds.CreateLayer(stem, srs, ogrConstants.wkbPoint)
        require(layer != null, s"Could not create layer in $shpPath")
        fields.foreach { case (fname, ftype) => layer.CreateField(new FieldDefn(fname, ftype)) }
        val fdef = layer.GetLayerDefn
        val feat = new org.gdal.ogr.Feature(fdef)
        feat.SetGeometryDirectly(ogr.CreateGeometryFromWkt("POINT (0 0)"))
        // Optionally set the 'name' field so callers can prove which source a row came from.
        if (name.nonEmpty && fields.exists(_._1 == "name")) feat.SetField("name", name)
        layer.CreateFeature(feat)
        ds.FlushCache()
        ds.delete()
    }

    // ---------------------------------------------------------------------------
    // Pre-existing contract tests
    // ---------------------------------------------------------------------------

    test("shapefile_ogr must read a bare .shp path (sidecar bundle staging)") {
        val shpPath = this.getClass
            .getResource("/binary/shapefile/map.shp")
            .toString
            .replace("file:", "")

        val df = spark.read
            .format("shapefile_ogr")
            .load(shpPath)

        df.count() should be > 0L
    }

    test("shapefile_ogr must read a bare .shp path for the elevation fixture") {
        val shpPath = this.getClass
            .getResource("/binary/elevation/sd46_dtm_breakline.shp")
            .toString
            .replace("file:", "")

        val df = spark.read
            .format("shapefile_ogr")
            .load(shpPath)

        df.count() should be > 0L
    }

    // ---------------------------------------------------------------------------
    // schema-divergence guard
    // ---------------------------------------------------------------------------

    test("shapefile_ogr must raise on a directory with differing-schema shapefiles") {
        val tmpDir = Files.createTempDirectory("gbx_b4_diverge_").toFile
        // roads: one string field 'name'; rivers: one real field 'width' -- different schemas
        writeShapefile(tmpDir, "roads", Seq("name" -> ogrConstants.OFTString))
        writeShapefile(tmpDir, "rivers", Seq("width" -> ogrConstants.OFTReal))

        val ex = intercept[IllegalArgumentException] {
            spark.read.format("shapefile_ogr").load(tmpDir.getAbsolutePath)
        }
        val msg = ex.getMessage
        msg should include("differing schemas")
        msg should include("load them separately")
        // Both stems appear in the message
        (msg.contains("roads") || msg.contains("rivers")) shouldBe true
    }

    test("shapefile_ogr must NOT raise on a directory with same-schema shapefiles") {
        val tmpDir = Files.createTempDirectory("gbx_b4_same_").toFile
        // Both shapefiles have the same field list — union read should proceed normally
        val fields = Seq("name" -> ogrConstants.OFTString, "length" -> ogrConstants.OFTReal)
        writeShapefile(tmpDir, "roads", fields)
        writeShapefile(tmpDir, "rivers", fields)

        noException shouldBe thrownBy {
            val df = spark.read.format("shapefile_ogr").load(tmpDir.getAbsolutePath)
            df.count()
        }
    }

    // ---------------------------------------------------------------------------
    // unzipped shapefile directory exact-count (regression guard for .dbf over-count)
    // ---------------------------------------------------------------------------

    test("shapefile_ogr must read a directory of two unzipped shapefiles with exact count") {
        val tmpDir = Files.createTempDirectory("gbx_c73_unzip_").toFile
        val fields = Seq("name" -> ogrConstants.OFTString, "length" -> ogrConstants.OFTReal)
        // Write TWO shapefiles with 1 feature each; before the fix a .dbf sidecar was opened
        // as an attribute-only datasource, yielding 4 rows (2 .shp + 2 .dbf) instead of 2.
        writeShapefile(tmpDir, "roads", fields)
        writeShapefile(tmpDir, "rivers", fields)

        val df = spark.read.format("shapefile_ogr").load(tmpDir.getAbsolutePath)
        df.count() shouldBe 2L
    }

    // ---------------------------------------------------------------------------
    // underscore-prefixed shapefile names — Spark's hidden-file filter skips
    // all files whose names start with '_', so a dir of _foo.shp/_foo.dbf/_foo.shx
    // returned empty from binaryFile.inputFiles() and caused "Is a directory".
    // ---------------------------------------------------------------------------

    test("shapefile_ogr must read a directory of underscore-prefixed shapefile files") {
        val tmpDir = Files.createTempDirectory("gbx_us1_underscore_").toFile
        val fields = Seq("name" -> ogrConstants.OFTString)
        // Write under a normal stem first, then rename to underscore-prefixed stem
        writeShapefile(tmpDir, "tmp_normal", fields, name = "under_feat")
        // Rename all components to underscore-prefixed stem
        val exts = Seq("shp", "shx", "dbf", "prj", "cpg")
        exts.foreach { ext =>
            val src = new java.io.File(tmpDir, s"tmp_normal.$ext")
            if (src.exists()) {
                val dst = new java.io.File(tmpDir, s"_us_test.$ext")
                src.renameTo(dst)
            }
        }
        // Verify the dir now contains only _-prefixed files
        val fileNames = tmpDir.listFiles().map(_.getName).toSet
        fileNames.filter(_.endsWith(".shp")) shouldBe Set("_us_test.shp")

        val df = spark.read.format("shapefile_ogr").load(tmpDir.getAbsolutePath)
        df.count() shouldBe 1L
    }

    // ---------------------------------------------------------------------------
    // dir of .shp.zip — both tiers handle directory of zipped shapefiles
    // ---------------------------------------------------------------------------

    private def zipShapefile(shpFile: java.io.File): java.io.File = {
        import java.util.zip.{ZipEntry, ZipOutputStream}
        val dir = shpFile.getParentFile
        val stem = shpFile.getName.stripSuffix(".shp")
        val zipFile = new java.io.File(dir, s"$stem.shp.zip")
        val zos = new ZipOutputStream(new java.io.FileOutputStream(zipFile))
        try {
            dir.listFiles()
                .filter { f =>
                    val n = f.getName
                    n.startsWith(stem + ".") && !n.endsWith(".zip")
                }
                .foreach { f =>
                    zos.putNextEntry(new ZipEntry(f.getName))
                    val bytes = java.nio.file.Files.readAllBytes(f.toPath)
                    zos.write(bytes)
                    zos.closeEntry()
                }
        } finally {
            zos.close()
        }
        zipFile
    }

    test("shapefile_ogr must read a directory of .shp.zip files (same schema)") {
        val tmpDir = Files.createTempDirectory("gbx_d1_zipdir_").toFile
        val fields = Seq("name" -> ogrConstants.OFTString, "length" -> ogrConstants.OFTReal)
        writeShapefile(tmpDir, "roads", fields)
        writeShapefile(tmpDir, "rivers", fields)
        // Zip each shapefile bundle and remove unzipped sidecars
        Seq("roads", "rivers").foreach { stem =>
            val shpFile = new java.io.File(tmpDir, s"$stem.shp")
            zipShapefile(shpFile)
            tmpDir.listFiles()
                .filter(f => f.getName.startsWith(stem + ".") && !f.getName.endsWith(".zip"))
                .foreach(_.delete())
        }
        // Verify only .zip files remain
        val zips = tmpDir.listFiles().filter(_.getName.endsWith(".shp.zip"))
        zips.length shouldBe 2

        val df = spark.read.format("shapefile_ogr").load(tmpDir.getAbsolutePath)
        df.count() shouldBe 2L
    }

    test("shapefile_ogr must read a mixed directory of .shp and .shp.zip (same schema)") {
        val tmpDir = Files.createTempDirectory("gbx_d1_mixed_").toFile
        val fields = Seq("name" -> ogrConstants.OFTString)
        // Distinct attribute values so we can prove BOTH sources contribute to the union.
        writeShapefile(tmpDir, "roads", fields, name = "roads_feat")
        writeShapefile(tmpDir, "rivers", fields, name = "rivers_feat")
        // Zip only "rivers", leave "roads" as plain .shp bundle
        val riversShp = new java.io.File(tmpDir, "rivers.shp")
        zipShapefile(riversShp)
        tmpDir.listFiles()
            .filter(f => f.getName.startsWith("rivers.") && !f.getName.endsWith(".zip"))
            .foreach(_.delete())

        val df = spark.read.format("shapefile_ogr").load(tmpDir.getAbsolutePath)
        // After the sidecar-filter fix, loose .dbf files are excluded from the partition list,
        // so the plain .shp bundle contributes exactly 1 row and the .shp.zip contributes exactly 1 row.
        df.count() shouldBe 2L
        val names = df.collect().map(_.getAs[String]("name")).toSet
        names should contain("roads_feat")
        names should contain("rivers_feat")
    }

    test("shapefile_ogr must raise on a directory of divergent-schema .shp.zip files") {
        val tmpDir = Files.createTempDirectory("gbx_d1_diverge_zip_").toFile
        // roads: one string field 'name'; rivers: one real field 'width' -- different schemas
        writeShapefile(tmpDir, "roads", Seq("name" -> ogrConstants.OFTString))
        writeShapefile(tmpDir, "rivers", Seq("width" -> ogrConstants.OFTReal))
        // Zip each shapefile bundle and remove unzipped sidecars
        Seq("roads", "rivers").foreach { stem =>
            val shpFile = new java.io.File(tmpDir, s"$stem.shp")
            zipShapefile(shpFile)
            tmpDir.listFiles()
                .filter(f => f.getName.startsWith(stem + ".") && !f.getName.endsWith(".zip"))
                .foreach(_.delete())
        }

        val ex = intercept[IllegalArgumentException] {
            spark.read.format("shapefile_ogr").load(tmpDir.getAbsolutePath)
        }
        val msg = ex.getMessage
        msg should include("differing schemas")
        msg should include("load them separately")
        (msg.contains("roads") || msg.contains("rivers")) shouldBe true
    }

    // ---------------------------------------------------------------------------
    // write-guard — read-only OGR formats must fail writes with a clear message.
    // Mechanism: OGR_DataSource.supportsExternalMetadata=true routes writes through
    // OGR_Table.newWriteBuilder, which throws immediately. inferSchema is NOT called on writes,
    // so reads are completely unaffected.
    // ---------------------------------------------------------------------------

    /** Walk the exception chain to find the root cause. */
    private def rootCause(t: Throwable): Throwable =
        if (t.getCause == null) t else rootCause(t.getCause)

    /** Assert that a write of the given format to the given path produces a clear read-only error. */
    private def assertWriteRejected(fmt: String, path: String, altName: String): Unit = {
        val df = spark.range(1).toDF("id")
        val ex = intercept[Exception] {
            df.write.format(fmt).mode("overwrite").save(path)
        }
        val root = rootCause(ex)
        root.getMessage should include("read-only")
        root.getMessage should include(altName)
        root.getClass.getSimpleName should not include "NoSuchFileException"
    }

    // shapefile_ogr: new (non-existent) path
    test("shapefile_ogr must reject writes to a new path with a read-only error naming shapefile_gbx") {
        val tmpPath = Files.createTempDirectory("gbx_c1_shp_").toFile.getAbsolutePath
        assertWriteRejected("shapefile_ogr", tmpPath + "/nonexistent_output", "shapefile_gbx")
    }

    // shapefile_ogr: existing path — guard must fire for existing paths too
    test("shapefile_ogr must reject writes to an existing path with a read-only error naming shapefile_gbx") {
        val existingDir = Files.createTempDirectory("gbx_c1_shp_exist_").toFile.getAbsolutePath
        assertWriteRejected("shapefile_ogr", existingDir, "shapefile_gbx")
    }

    // gpkg_ogr: new path
    test("gpkg_ogr must reject writes to a new path with a read-only error naming gpkg_gbx") {
        val tmpPath = Files.createTempDirectory("gbx_c1_gpkg_").toFile.getAbsolutePath
        assertWriteRejected("gpkg_ogr", tmpPath + "/nonexistent_output", "gpkg_gbx")
    }

    // gpkg_ogr: existing path
    test("gpkg_ogr must reject writes to an existing path with a read-only error naming gpkg_gbx") {
        val existingDir = Files.createTempDirectory("gbx_c1_gpkg_exist_").toFile.getAbsolutePath
        assertWriteRejected("gpkg_ogr", existingDir, "gpkg_gbx")
    }

    // file_gdb_ogr: new path
    test("file_gdb_ogr must reject writes to a new path with a read-only error naming file_gdb_gbx") {
        val tmpPath = Files.createTempDirectory("gbx_c1_gdb_").toFile.getAbsolutePath
        assertWriteRejected("file_gdb_ogr", tmpPath + "/nonexistent_output", "file_gdb_gbx")
    }

    // file_gdb_ogr: existing path
    test("file_gdb_ogr must reject writes to an existing path with a read-only error naming file_gdb_gbx") {
        val existingDir = Files.createTempDirectory("gbx_c1_gdb_exist_").toFile.getAbsolutePath
        assertWriteRejected("file_gdb_ogr", existingDir, "file_gdb_gbx")
    }

    // geojson_ogr: new path
    test("geojson_ogr must reject writes to a new path with a read-only error naming geojson_gbx") {
        val tmpPath = Files.createTempDirectory("gbx_c1_gjson_").toFile.getAbsolutePath
        assertWriteRejected("geojson_ogr", tmpPath + "/nonexistent_output", "geojson_gbx")
    }

    // geojson_ogr: existing path
    test("geojson_ogr must reject writes to an existing path with a read-only error naming geojson_gbx") {
        val existingDir = Files.createTempDirectory("gbx_c1_gjson_exist_").toFile.getAbsolutePath
        assertWriteRejected("geojson_ogr", existingDir, "geojson_gbx")
    }

    // generic ogr: new path
    test("ogr must reject writes to a new path with a read-only error listing _gbx alternatives") {
        val tmpPath = Files.createTempDirectory("gbx_c1_ogr_").toFile.getAbsolutePath
        assertWriteRejected("ogr", tmpPath + "/nonexistent_output", "shapefile_gbx")
    }

    // generic ogr: existing path
    test("ogr must reject writes to an existing path with a read-only error listing _gbx alternatives") {
        val existingDir = Files.createTempDirectory("gbx_c1_ogr_exist_").toFile.getAbsolutePath
        assertWriteRejected("ogr", existingDir, "shapefile_gbx")
    }

    // Regression: a read of a NONEXISTENT path must NOT show the write-guard message.
    // Previously (a94d05b), inferSchema had a path-existence check that showed "read-only" even
    // for reads of typo paths. This test confirms that consequence is gone.
    test("shapefile_ogr read of a nonexistent path must NOT produce a read-only error message") {
        val ex = intercept[Exception] {
            spark.read.format("shapefile_ogr").load("/tmp/does_not_exist_gbx_c1_regression.shp").count()
        }
        // The error must be a not-found / no-files error, not the write-guard message
        val root = rootCause(ex)
        root.getMessage should not include "read-only"
        root.getMessage should not include "shapefile_gbx"
    }

    // Regression: existing reads must still work after the mechanism change
    test("shapefile_ogr read must NOT be affected by the write guard") {
        val shpPath = this.getClass
            .getResource("/binary/shapefile/map.shp")
            .toString
            .replace("file:", "")

        val df = spark.read.format("shapefile_ogr").load(shpPath)
        df.count() should be > 0L
    }

}
