package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.NodeFileCacheProbe.{entries, garbage, withScratchDir}
import com.databricks.labs.gbx.util.{FileGDBFixture, HadoopUtils, NodeCacheRetentionOff, NodeFileCacheProbe, NodeFilePathUtil}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.StructType
import org.gdal.ogr.{Feature, FieldDefn, ogr, ogrConstants}
import org.gdal.osr.SpatialReference
import org.scalatest.matchers.should.Matchers._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.jdk.CollectionConverters._
import scala.util.Try

/** OGR read planning (OGR_Batch): an unreadable file, a missing path or a wrong layer fails the read
  * naming the file, instead of an empty result; in a directory read, metadata files and files of
  * another vector format are skipped; chunks are planned without a trailing empty one; and planning
  * lets go of every dataset and node-file copy it opens. Every fixture is built in a temp dir. Idle
  * retention is off, so a released copy is gone at once. */
class OgrPlanningErrorsTest extends PlanTest with SilentSparkSession with NodeCacheRetentionOff {

    private val mapDir = Paths.get(getClass.getResource("/binary/shapefile/map.shp").toURI).getParent

    /** Copies the `map` test shapefile into `dir` under the stem `stem`. */
    private def copyShapefile(dir: Path, stem: String): Unit =
        Seq("shp", "shx", "dbf").foreach(ext => Files.copy(mapDir.resolve(s"map.$ext"), dir.resolve(s"$stem.$ext")))

    private def featureCount(path: String): Int = {
        GDALManager.initOgr()
        val ds = ogr.Open(path, 0)
        try ds.GetLayer(0).GetFeatureCount().toInt
        finally ds.delete()
    }

    private val mapCount: Int = featureCount(mapDir.resolve("map.shp").toString)

    /** The schema a read of the good shapefile `shp` infers: the explicit schema the planning tests pass. */
    private def shapefileSchema(shp: Path): StructType = spark.read.format("shapefile_ogr").load(shp.toString).schema

    private def messages(t: Throwable): String =
        Iterator.iterate(t)(_.getCause).takeWhile(_ != null).map(e => String.valueOf(e.getMessage)).mkString(" | ")

    /** Reads `path` with `fmt` and the explicit schema `schema`; `options` are reader options. */
    private def count(fmt: String, path: Path, schema: StructType, options: (String, String)*): Long =
        spark.read.format(fmt).schema(schema).options(options.toMap).load(path.toString).count()

    private def failureOf(body: => Any): String = messages(intercept[Exception](body))

    private val goodGeoJson =
        """{"type":"FeatureCollection","features":[
          |{"type":"Feature","properties":{"id":1},"geometry":{"type":"Point","coordinates":[1.0,0.0]}},
          |{"type":"Feature","properties":{"id":2},"geometry":{"type":"Point","coordinates":[2.0,0.0]}}]}""".stripMargin

    private def geoJsonSchema(dir: Path): StructType = {
        val good = dir.resolve("good.geojson")
        Files.write(good, goodGeoJson.getBytes(StandardCharsets.UTF_8))
        spark.read.format("ogr").option("driverName", "GeoJSON").load(good.toString).schema
    }

    /** A fresh copy of the bridges FileGDB whose table files hold junk. */
    private def corruptGdb(): Path = {
        val gdb = FileGDBFixture.unzipBridges()
        Files.list(gdb).iterator().asScala
            .filter(p => p.toString.endsWith(".gdbtable") || p.toString.endsWith(".gdbtablx"))
            .toList
            .foreach(p => Files.write(p, garbage))
        gdb
    }

    // ---------------------------------------------------------------------------------------------
    // Failures name the file
    // ---------------------------------------------------------------------------------------------

    test("E1 a corrupt .geojson with an explicit schema fails the read naming the file and the driver") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val schema = geoJsonSchema(dir)
            val bad = dir.resolve("bad.geojson")
            Files.write(bad, garbage)
            val msg = failureOf(count("ogr", bad, schema, "driverName" -> "GeoJSON"))
            msg should include("bad.geojson")
            msg should include("GeoJSON")
        }
    }

    test("E2 a nonexistent path with an explicit schema fails the read naming the path") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val schema = geoJsonSchema(dir)
            val missing = dir.resolve("no_such_file.geojson")
            failureOf(count("ogr", missing, schema, "driverName" -> "GeoJSON")) should include("no_such_file.geojson")
        }
    }

    test("E3 a FileGDB whose tables are junk, with an explicit schema, fails the read naming the .gdb") {
        val good = FileGDBFixture.unzipBridges()
        val bad = corruptGdb()
        try {
            val schema = spark.read.format("file_gdb_ogr").load(good.toString).schema
            failureOf(count("file_gdb_ogr", bad, schema)) should include(bad.getFileName.toString)
        } finally {
            NodeFileCacheProbe.deleteTree(good.getParent)
            NodeFileCacheProbe.deleteTree(bad.getParent)
        }
    }

    test("E4 a layerName the file does not have fails the read naming the layer") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            copyShapefile(dir, "map")
            val schema = shapefileSchema(dir.resolve("map.shp"))
            val msg = failureOf(count("shapefile_ogr", dir, schema, "layerName" -> "no_such_layer"))
            msg should include("no_such_layer")
            msg should include("map.shp")
        }
    }

    test("E5 a directory of three good shapefiles and a corrupt bad.shp fails the read naming bad.shp") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            Seq("a1", "a2", "a3").foreach(copyShapefile(dir, _))
            val schema = shapefileSchema(dir.resolve("a1.shp"))
            Files.write(dir.resolve("bad.shp"), garbage)
            val msg = failureOf(count("shapefile_ogr", dir, schema))
            msg should include("bad.shp")
            msg should include("1 of")
        }
    }

    test("E8 reads that fail to plan leave no node-file copy behind") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val before = entries()
            val geoDir = Files.createDirectory(dir.resolve("geo"))
            val geoSchema = geoJsonSchema(geoDir)
            val bad = geoDir.resolve("bad.geojson")
            Files.write(bad, garbage)
            Try(count("ogr", bad, geoSchema, "driverName" -> "GeoJSON"))
            Try(count("ogr", geoDir.resolve("missing.geojson"), geoSchema, "driverName" -> "GeoJSON"))
            val shpDir = Files.createDirectory(dir.resolve("shp"))
            Seq("a1", "a2").foreach(copyShapefile(shpDir, _))
            val shpSchema = shapefileSchema(shpDir.resolve("a1.shp"))
            Try(count("shapefile_ogr", shpDir, shpSchema, "layerName" -> "no_such_layer"))
            Files.write(shpDir.resolve("bad.shp"), garbage)
            Try(count("shapefile_ogr", shpDir, shpSchema))
            val gdb = corruptGdb()
            val goodGdb = FileGDBFixture.unzipBridges()
            try {
                val gdbSchema = spark.read.format("file_gdb_ogr").load(goodGdb.toString).schema
                Try(count("file_gdb_ogr", gdb, gdbSchema))
            } finally {
                NodeFileCacheProbe.deleteTree(gdb.getParent)
                NodeFileCacheProbe.deleteTree(goodGdb.getParent)
            }
            withClue("node-file cache entries left by the failed reads: ") { entries().diff(before) shouldBe empty }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // A directory read tolerates strays; a named file does not
    // ---------------------------------------------------------------------------------------------

    test("E6 a shapefile directory with a .shp.xml and a .txt next to it reads every row, inferred or explicit") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            copyShapefile(dir, "map")
            Files.write(dir.resolve("map.shp.xml"), "<metadata/>".getBytes(StandardCharsets.UTF_8))
            Files.write(dir.resolve("zz_notes.txt"), "notes".getBytes(StandardCharsets.UTF_8))
            spark.read.format("shapefile_ogr").load(dir.toString).count() shouldBe mapCount.toLong
            count("shapefile_ogr", dir, shapefileSchema(dir.resolve("map.shp"))) shouldBe mapCount.toLong
        }
    }

    test("E6b a README.txt that sorts before the shapefile is not taken as the schema head") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            copyShapefile(dir, "map")
            Files.write(dir.resolve("README.txt"), "notes".getBytes(StandardCharsets.UTF_8))
            Files.write(dir.resolve("ABOUT.md"), "notes".getBytes(StandardCharsets.UTF_8))
            spark.read.format("shapefile_ogr").load(dir.toString).count() shouldBe mapCount.toLong
        }
    }

    test("E7 a GeoJSON next to a shapefile is skipped by shapefile_ogr; the shapefile rows are read") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            copyShapefile(dir, "map")
            Files.write(dir.resolve("zz_other.geojson"), goodGeoJson.getBytes(StandardCharsets.UTF_8))
            spark.read.format("shapefile_ogr").load(dir.toString).count() shouldBe mapCount.toLong
            count("shapefile_ogr", dir, shapefileSchema(dir.resolve("map.shp"))) shouldBe mapCount.toLong
        }
    }

    test("E9 a GeoJSON named as the path is never skipped by shapefile_ogr: the read fails naming it") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            copyShapefile(dir, "map")
            val other = dir.resolve("other.geojson")
            Files.write(other, goodGeoJson.getBytes(StandardCharsets.UTF_8))
            val schema = shapefileSchema(dir.resolve("map.shp"))
            failureOf(count("shapefile_ogr", other, schema)) should include("other.geojson")
        }
    }

    test("E10 with no driver named, a directory member that fails is never taken for another format") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            copyShapefile(dir, "map")
            val schema = shapefileSchema(dir.resolve("map.shp"))
            // GDAL opens map.shp with the ESRI Shapefile driver, which differs from the empty requested name:
            // a missing layer must still fail the read, not skip the file.
            failureOf(count("ogr", dir, schema, "layerName" -> "no_such_layer")) should include("no_such_layer")
            Files.write(dir.resolve("zz_bad.shp"), garbage)
            failureOf(count("ogr", dir, schema)) should include("zz_bad.shp")
        }
    }

    test("E9b a named file whose path has a space is never skipped either") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val spaced = Files.createDirectory(dir.resolve("my dir"))
            copyShapefile(spaced, "map")
            val other = spaced.resolve("other file.geojson")
            Files.write(other, goodGeoJson.getBytes(StandardCharsets.UTF_8))
            val schema = shapefileSchema(spaced.resolve("map.shp"))
            val msg = failureOf(count("shapefile_ogr", other, schema))
            msg should include("other file.geojson")
            msg should include("could not open it")
            msg should not include "another vector format"
            msg should not include "skipped"
        }
    }

    test("a listing names a file whose path has a space as it is, not URL-encoded") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val f = Files.createDirectory(dir.resolve("my dir")).resolve("my file#1.geojson")
            Files.write(f, goodGeoJson.getBytes(StandardCharsets.UTF_8))
            val listed = HadoopUtils.listDataFilesSpark(spark, f.toString)
            listed.map(_.stripPrefix("file:").dropWhile(_ == '/')) shouldBe Seq(f.toString.dropWhile(_ == '/'))
            OGR_Batch.isDirListing(f.toString, listed) shouldBe false
            OGR_Batch.isDirListing(f.getParent.toString, listed) shouldBe true
        }
    }

    test("E11 a GeoJSON file and a shapefile folder whose paths have spaces read end to end, inferred or explicit") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val before = entries()
            val json = Files.createDirectory(dir.resolve("my json")).resolve("my points#1.geojson")
            Files.write(json, goodGeoJson.getBytes(StandardCharsets.UTF_8))
            val inferred = spark.read.format("geojson_ogr").option("multi", "false").load(json.toString)
            inferred.count() shouldBe 2L
            count("geojson_ogr", json, inferred.schema, "multi" -> "false") shouldBe 2L
            val shp = Files.createDirectory(dir.resolve("my shapes"))
            copyShapefile(shp, "my map")
            copyShapefile(shp, "map 2")
            spark.read.format("shapefile_ogr").load(shp.toString).count() shouldBe 2L * mapCount
            count("shapefile_ogr", shp, shapefileSchema(shp.resolve("my map.shp"))) shouldBe 2L * mapCount
            spark.read.format("shapefile_ogr").load(shp.resolve("my map.shp").toString).count() shouldBe mapCount.toLong
            withClue("node-file cache entries left by the reads: ") { entries().diff(before) shouldBe empty }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // A directory with no data files
    // ---------------------------------------------------------------------------------------------

    test("E12 an empty directory reads as 0 rows with an explicit schema and keeps its error when inferred") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val good = Files.createDirectory(dir.resolve("good"))
            copyShapefile(good, "map")
            val schema = shapefileSchema(good.resolve("map.shp"))
            val empty = Files.createDirectory(dir.resolve("empty"))
            count("shapefile_ogr", empty, schema) shouldBe 0L
            count("ogr", empty, schema) shouldBe 0L
            failureOf(spark.read.format("shapefile_ogr").load(empty.toString)) should include("directory")
        }
    }

    test("E12c an empty directory whose path has a space or a percent sign reads as 0 rows with an explicit schema") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val good = Files.createDirectory(dir.resolve("good"))
            copyShapefile(good, "map")
            val schema = shapefileSchema(good.resolve("map.shp"))
            Seq("my empty dir", "50% off", "a#1 b").foreach { name =>
                withClue(s"empty directory '$name': ") {
                    count("shapefile_ogr", Files.createDirectory(dir.resolve(name)), schema) shouldBe 0L
                }
            }
        }
    }

    test("E12d a directory that cannot be listed fails with the listing error, not as an empty read") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val unlistable = Files.createDirectory(dir.resolve("unlistable"))
            val e = intercept[java.io.IOException](
              HadoopUtils.listDataFilesChecked(spark, unlistable.toString, (_, _) => throw new RuntimeException("listing refused"))
            )
            e.getMessage should include("unlistable")
            messages(e) should include("listing refused")
            // The same directory listed normally is the empty read.
            HadoopUtils.listDataFilesChecked(spark, unlistable.toString)._2 shouldBe true
        }
    }

    test("E12b a directory holding only metadata files reads as 0 rows with an explicit schema") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val good = Files.createDirectory(dir.resolve("good"))
            copyShapefile(good, "map")
            val schema = shapefileSchema(good.resolve("map.shp"))
            val notes = Files.createDirectory(dir.resolve("notes"))
            Files.write(notes.resolve("README.txt"), "notes".getBytes(StandardCharsets.UTF_8))
            count("shapefile_ogr", notes, schema) shouldBe 0L
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Every file skipped; one-line errors
    // ---------------------------------------------------------------------------------------------

    test("E13 a directory read where every file is another vector format fails, naming files, drivers and the reader") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val good = Files.createDirectory(dir.resolve("good"))
            copyShapefile(good, "map")
            val schema = shapefileSchema(good.resolve("map.shp"))
            val data = Files.createDirectory(dir.resolve("data"))
            Seq("a.geojson", "b.geojson").foreach(n => Files.write(data.resolve(n), goodGeoJson.getBytes(StandardCharsets.UTF_8)))
            val msg = failureOf(count("shapefile_ogr", data, schema))
            msg should include("a.geojson")
            msg should include("b.geojson")
            msg should include("GeoJSON")
            msg should include("geojson_ogr")
            msg should include("multi")
        }
    }

    test("E13c geojson_ogr's default multi=true over FeatureCollection files fails, pointing at multi=false") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val schema = geoJsonSchema(dir)
            val data = Files.createDirectory(dir.resolve("data"))
            Seq("a.geojson", "b.geojson").foreach(n => Files.write(data.resolve(n), goodGeoJson.getBytes(StandardCharsets.UTF_8)))
            val msg = failureOf(count("geojson_ogr", data, schema))
            Seq("a.geojson", "b.geojson", "GeoJSONSeq", "GDAL detects GeoJSON", "multi").foreach(w => msg should include(w))
            // The same file named as the path is a plain failure, not a skip.
            val named = failureOf(count("geojson_ogr", data.resolve("a.geojson"), schema))
            named should include("a.geojson: GeoJSONSeq could not open it")
            named should not include "another vector format"
            count("geojson_ogr", data, schema, "multi" -> "false") shouldBe 4L
        }
    }

    test("E13d a directory of GeoJSONSeq files read with shapefile_ogr points at geojson_ogr with multi=true") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val good = Files.createDirectory(dir.resolve("good"))
            copyShapefile(good, "map")
            val schema = shapefileSchema(good.resolve("map.shp"))
            val data = Files.createDirectory(dir.resolve("data"))
            val seq =
                """{"type":"Feature","properties":{"id":1},"geometry":{"type":"Point","coordinates":[1.0,0.0]}}
                  |{"type":"Feature","properties":{"id":2},"geometry":{"type":"Point","coordinates":[2.0,0.0]}}
                  |""".stripMargin
            Seq("a.geojsonl", "b.geojsonl").foreach(n => Files.write(data.resolve(n), seq.getBytes(StandardCharsets.UTF_8)))
            val msg = failureOf(count("shapefile_ogr", data, schema))
            msg should include("GDAL detects GeoJSONSeq")
            msg should include("""geojson_ogr with .option("multi", "true")""")
        }
    }

    test("E13b a directory read where only some files are another format still reads the rest") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            copyShapefile(dir, "map")
            Files.write(dir.resolve("a.geojson"), goodGeoJson.getBytes(StandardCharsets.UTF_8))
            Files.write(dir.resolve("b.geojson"), goodGeoJson.getBytes(StandardCharsets.UTF_8))
            count("shapefile_ogr", dir, shapefileSchema(dir.resolve("map.shp"))) shouldBe mapCount.toLong
        }
    }

    test("E14 a planning failure lists each file on one line: the GDAL message, not the copy path or a stack of lines") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val schema = geoJsonSchema(dir)
            val bad = dir.resolve("bad.geojson")
            Files.write(bad, garbage)
            val e = Iterator
                .iterate(intercept[Exception](count("ogr", bad, schema, "driverName" -> "GeoJSON")): Throwable)(_.getCause)
                .takeWhile(_ != null)
                .find(_.getMessage.startsWith("OGR read failed to plan"))
                .get
            e.getMessage should not include "\n"
            e.getMessage should not include "gdal_local_files"
            e.getMessage should include("bad.geojson: GeoJSON could not open it")
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Chunk planning
    // ---------------------------------------------------------------------------------------------

    private def writeGpkg(path: Path, n: Int): Unit = {
        GDALManager.initOgr()
        val ds = ogr.GetDriverByName("GPKG").CreateDataSource(path.toString)
        try {
            val srs = new SpatialReference()
            srs.ImportFromEPSG(4326)
            val layer = ds.CreateLayer("pts", srs, ogrConstants.wkbPoint)
            layer.CreateField(new FieldDefn("id", ogrConstants.OFTInteger))
            val defn = layer.GetLayerDefn
            layer.StartTransaction()
            (0 until n).foreach { i =>
                val f = new Feature(defn)
                f.SetField("id", i)
                f.SetGeometryDirectly(ogr.CreateGeometryFromWkt(s"POINT ($i 0)"))
                layer.CreateFeature(f)
                f.delete()
            }
            layer.CommitTransaction()
        } finally {
            ds.FlushCache()
            ds.delete()
        }
    }

    test("a GPKG of exactly two chunks plans two partitions, with no trailing empty one") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val gpkg = dir.resolve("pts.gpkg")
            writeGpkg(gpkg, 20000)
            val schema = spark.read.format("gpkg_ogr").load(gpkg.toString).schema
            val batch = new OGR_Batch(schema, Map("path" -> gpkg.toString, "driverName" -> "GPKG", "chunkSize" -> "10000"))
            val parts = batch.planInputPartitions().map(_.asInstanceOf[OGR_Partition])
            parts.map(p => (p.start, p.end)).toSeq shouldBe Seq((0, 10000), (10000, 20000))
            spark.read.format("gpkg_ogr").option("chunkSize", "10000").load(gpkg.toString).count() shouldBe 20000L
        }
    }

    test("a GPKG with a partial last chunk plans it short") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val gpkg = dir.resolve("pts.gpkg")
            writeGpkg(gpkg, 25)
            val schema = spark.read.format("gpkg_ogr").load(gpkg.toString).schema
            val batch = new OGR_Batch(schema, Map("path" -> gpkg.toString, "driverName" -> "GPKG", "chunkSize" -> "10"))
            batch.planInputPartitions().map(_.asInstanceOf[OGR_Partition]).map(p => (p.start, p.end)).toSeq shouldBe
                Seq((0, 10), (10, 20), (20, 25))
        }
    }

    test("an empty GPKG layer reads as zero rows, without an exception") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            val populated = dir.resolve("populated.gpkg")
            writeGpkg(populated, 3)
            val schema = spark.read.format("gpkg_ogr").load(populated.toString).schema
            val empty = dir.resolve("empty.gpkg")
            writeGpkg(empty, 0)
            val batch = new OGR_Batch(schema, Map("path" -> empty.toString, "driverName" -> "GPKG"))
            batch.planInputPartitions().map(_.asInstanceOf[OGR_Partition]).map(p => (p.start, p.end)).toSeq shouldBe Seq((0, 0))
            count("gpkg_ogr", empty, schema) shouldBe 0L
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Planning releases the native datasets it opens
    // ---------------------------------------------------------------------------------------------

    /** Paths of the files this JVM holds open (Linux), under `prefix`, a deleted file shown with " (deleted)". */
    private def openFilesUnder(prefix: String): Seq[String] = {
        val fds = Paths.get("/proc/self/fd")
        assume(Files.isDirectory(fds), "needs /proc/self/fd (Linux)")
        val s = Files.list(fds)
        try s.iterator().asScala.flatMap(p => Try(Files.readSymbolicLink(p).toString).toOption).filter(_.startsWith(prefix)).toList
        finally s.close()
    }

    test("planning closes the dataset it opens, for a good file and for a layer that is not there") {
        withScratchDir("gbx_ogr_plan_") { dir =>
            Seq("a1", "a2", "a3").foreach(copyShapefile(dir, _))
            val schema = shapefileSchema(dir.resolve("a1.shp"))
            val root = NodeFilePathUtil.rootPath.toString
            val before = openFilesUnder(root)
            (1 to 5).foreach { _ =>
                count("shapefile_ogr", dir, schema) shouldBe (3L * mapCount)
                Try(count("shapefile_ogr", dir, schema, "layerName" -> "no_such_layer"))
            }
            withClue(s"files held open under $root after the reads: ") { openFilesUnder(root) shouldBe before }
        }
    }

}
