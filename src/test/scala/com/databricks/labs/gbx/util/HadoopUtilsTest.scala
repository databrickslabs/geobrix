package com.databricks.labs.gbx.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/**
  * Tests that HadoopUtils directory enumeration skips Hadoop marker/hidden files (names starting
  * with `_` or `.`, e.g. `_SUCCESS`, `_committed_*`). Without this, a reader pointed at a writer's
  * output directory would pick up `_SUCCESS` and fail opening it as a dataset (the GeoJSONL/OGR
  * round-trip regression this guards against).
  */
class HadoopUtilsTest extends AnyFunSuite with BeforeAndAfterEach {

    private var tempDir: java.nio.file.Path = _
    private var hconf: SerializableConfiguration = _

    override def beforeEach(): Unit = {
        tempDir = Files.createTempDirectory("HadoopUtilsTest")
        hconf = new SerializableConfiguration(new Configuration())
    }

    override def afterEach(): Unit = {
        if (tempDir != null) {
            try {
                Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
            } catch { case _: Exception => () }
        }
    }

    private def write(name: String): Unit = {
        Files.write(tempDir.resolve(name), "x".getBytes)
        ()
    }

    /** The directory as a file: URI string (cleanPath keeps file:/ paths as-is). */
    private def dirUri: String = tempDir.toUri.toString

    private def names(paths: Seq[String]): Set[String] = paths.map(p => new Path(p).getName).toSet

    test("listHadoopFiles skips marker and hidden files") {
        write("a.geojsonl")
        write("b.geojsonl")
        write("_SUCCESS")
        write("_committed_123")
        write(".hidden.geojsonl")

        names(HadoopUtils.listHadoopFiles(dirUri, hconf)) shouldBe Set("a.geojsonl", "b.geojsonl")
    }

    test("getFirstFile returns a data file, not the _SUCCESS marker") {
        write("_SUCCESS")
        write("shard.geojsonl")

        new Path(HadoopUtils.getFirstFile(dirUri, hconf)).getName shouldBe "shard.geojsonl"
    }

    test("getFirstFile throws when the directory has only marker files") {
        write("_SUCCESS")
        write("_committed_0")

        an[IllegalArgumentException] should be thrownBy HadoopUtils.getFirstFile(dirUri, hconf)
    }

    // cleanPath normalizes the supported storage fabric. UC Volumes (/Volumes/, dbfs:/Volumes/,
    // /dbfs/Volumes/, file:/Volumes/) must resolve to the BARE /Volumes/... path so Hadoop uses the
    // credentialed UC connector on executors -- a file: scheme would force LocalFileSystem (raw
    // FUSE) and EPERM in opaque DSV2 executor tasks. Workspace files (/Workspace/) route to file:,
    // and legacy pre-Volumes DBFS (/dbfs/, dbfs:/) is coerced AWAY from the retired mount -- never a
    // dbfs: URI nor a /dbfs FUSE path. Regression for issue #34: a scheme-less /Volumes path first
    // resolved against fs.defaultFS (dbfs:) and returned a null tile; a later fix proved rewriting
    // it to file:/Volumes EPERMs identically, so the correct normalization is BARE /Volumes.
    test("cleanPath keeps UC Volumes paths bare for the credentialed UC connector") {
        HadoopUtils.cleanPath("/Volumes/c/s/v/x.tif") shouldBe "/Volumes/c/s/v/x.tif"
        HadoopUtils.cleanPath("dbfs:/Volumes/c/s/v/x.tif") shouldBe "/Volumes/c/s/v/x.tif"
        HadoopUtils.cleanPath("/dbfs/Volumes/c/s/v/x.tif") shouldBe "/Volumes/c/s/v/x.tif"
    }

    test("cleanPath routes Workspace files to the file: connector") {
        HadoopUtils.cleanPath("/Workspace/Users/me/x.tif") shouldBe "file:/Workspace/Users/me/x.tif"
        HadoopUtils.cleanPath("file:/Workspace/Users/me/x.tif") shouldBe "file:/Workspace/Users/me/x.tif"
    }

    test("cleanPath strips file: from a Volumes URI and routes /tmp and local paths to file:") {
        HadoopUtils.cleanPath("file:/Volumes/c/s/v/x.tif") shouldBe "/Volumes/c/s/v/x.tif"
        HadoopUtils.cleanPath("/tmp/x.tif") shouldBe "file:/tmp/x.tif"
        HadoopUtils.cleanPath("/local/x.tif") shouldBe "file:/local/x.tif"
    }

    test("cleanPath coerces legacy DBFS away from the retired /dbfs mount and dbfs: connector") {
        // No output may use the dbfs: scheme or the /dbfs FUSE prefix.
        val legacy = Seq(HadoopUtils.cleanPath("/dbfs/FileStore/x.tif"), HadoopUtils.cleanPath("dbfs:/FileStore/x.tif"))
        legacy.foreach { p =>
            p should startWith("file:")
            p should not include "dbfs:"
            p should not startWith "file:/dbfs/"
        }
    }

    test("isMetadataOnly is a suffix test on the file name: .shp.xml, .aux.xml, .txt and .md, not bare .xml") {
        val metadata = Seq(
          "file:/data/map.shp.xml", "/Volumes/c/s/v/MAP.SHP.XML", "file:/data/dem.tif.aux.xml",
          "/data/README.txt", "file:/data/notes.TXT", "/data/ABOUT.md"
        )
        val data = Seq(
          "file:/data/map.shp", "/data/roads.gpkg", "/data/area.gml", "/data/nas.xml", "/data/points.geojson",
          "/data/archive.shp.zip", "/data/bundle.gdb", "/data/txt", "/data/xml.dir/data.json"
        )
        metadata.foreach(p => withClue(p)(HadoopUtils.isMetadataOnly(p) shouldBe true))
        data.foreach(p => withClue(p)(HadoopUtils.isMetadataOnly(p) shouldBe false))
    }

    test("primaryDataFile and primaryDataFiles leave out sidecars and metadata-only files") {
        val files = Seq(
          "file:/d/README.txt", "file:/d/map.dbf", "file:/d/map.shp", "file:/d/map.shp.xml", "file:/d/map.shx",
          "file:/d/zz_notes.md", "file:/d/zz_other.geojson", "file:/d/area.xml"
        )
        HadoopUtils.primaryDataFile(files) shouldBe Some("file:/d/map.shp")
        HadoopUtils.primaryDataFiles(files) shouldBe Seq("file:/d/map.shp", "file:/d/zz_other.geojson", "file:/d/area.xml")
        // Nothing but metadata: the head is still the first file, so inference fails naming it.
        HadoopUtils.primaryDataFile(Seq("file:/d/README.txt")) shouldBe Some("file:/d/README.txt")
    }
}
