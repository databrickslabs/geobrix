package com.databricks.labs.gbx.vectorx.ds.ogr

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import com.databricks.labs.gbx.util.NodeFileCacheProbe.{entries, garbage, withScratchDir}
import com.databricks.labs.gbx.util.NodeFileManager
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.StructType
import org.gdal.ogr.{Feature, ogr}
import org.scalatest.matchers.should.Matchers._

import java.nio.charset.StandardCharsets
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path, Paths}
import java.util.zip.{ZipEntry, ZipOutputStream}

/** Each OGR read takes one hold per node-file copy and releases it once, however often the reader is
  * closed (OGR_Reader closes itself at the end of input, then Spark closes it again), and a changed
  * remote is read fresh. Every test reads a fresh copy of a remote in a temp dir: the cache objects are
  * JVM singletons, so a remote path is never shared across tests. */
class OgrReaderLifecycleTest extends PlanTest with SilentSparkSession {

    private val shapefileParts = Seq("map.shp", "map.shx", "map.dbf")

    /** A temp dir holding a copy of the `map` test shapefile. */
    private def withShapefileDir[T](body: Path => T): T = withScratchDir("gbx_ogr_life_") { dir =>
        val src = Paths.get(getClass.getResource("/binary/shapefile/map.shp").toURI).getParent
        shapefileParts.foreach(n => Files.copy(src.resolve(n), dir.resolve(n)))
        body(dir)
    }

    private def featureCount(path: String): Int = {
        GDALManager.initOgr()
        val ds = ogr.Open(path, 0)
        try ds.GetLayer(0).GetFeatureCount().toInt
        finally ds.delete()
    }

    private def config(): ExpressionConfig = {
        val c = ExpressionConfig(spark)
        NodeFileManager.init(c.hConf)
        c
    }

    private def partition(path: String, schema: StructType, c: ExpressionConfig, layer: String = ""): OGR_Partition =
        OGR_Partition(path, "ESRI Shapefile", layer, true, schema, 0, Int.MaxValue, c)

    /** Iterates `r` to its end (where it closes itself); returns the rows it produced. */
    private def drain(r: OGR_Reader): Int = {
        var n = 0
        while (r.next()) {
            r.get() should not be null
            n += 1
        }
        n
    }

    private def leftovers(before: Set[String]): Set[String] = entries().diff(before)

    test("O1 a shapefile_ogr read split into several partitions of one file leaves no node-file copy") {
        withShapefileDir { dir =>
            val expected = featureCount(dir.resolve("map.shp").toString)
            val before = entries()
            val n = spark.read.format("shapefile_ogr").option("chunkSize", "50").load(dir.toString).count()
            n shouldBe expected.toLong
            withClue(s"node-file cache entries left by the read: ") { leftovers(before) shouldBe empty }
        }
    }

    test("O2 a reader closed at its end and again by Spark releases once; another holder's copy survives") {
        withShapefileDir { dir =>
            val remote = dir.resolve("map.shp").toString
            val c = config()
            val schema = spark.read.format("shapefile_ogr").load(remote).schema
            val b = NodeFileManager.acquire(remote) // "task B", holding the same remote
            val bLocal = Paths.get(b.localPath)
            try {
                val a = new OGR_Reader(partition(remote, schema, c))
                drain(a) shouldBe featureCount(remote)
                a.close() // Spark's task-completion close
                a.close()
                withClue(s"task B's copy $bLocal after reader A closed three times: ") {
                    Files.isRegularFile(bLocal) shouldBe true
                }
            } finally b.close()
            withClue(s"generation dir ${bLocal.getParent} after B released: ") { Files.exists(bLocal.getParent) shouldBe false }
        }
    }

    test("O3 two readers of one file, each closed twice, both read every row; the copy outlives the first") {
        withShapefileDir { dir =>
            val remote = dir.resolve("map.shp").toString
            val expected = featureCount(remote)
            val c = config()
            val part = partition(remote, spark.read.format("shapefile_ogr").load(remote).schema, c)
            val before = entries()
            val r1 = new OGR_Reader(part)
            val r2 = new OGR_Reader(part)
            val copy = leftovers(before)
            withClue("the readers' shared copy: ") { copy should not be empty }
            drain(r1) shouldBe expected
            r1.close()
            withClue("after the first reader released, the second reader's copy: ") { leftovers(before) shouldBe copy }
            drain(r2) shouldBe expected
            r2.close()
            withClue("after both readers released: ") { leftovers(before) shouldBe empty }
        }
    }

    test("O4 a changed GeoJSON is read fresh: 2 rows, then 3, then a same-length rewrite's new ids") {
        withScratchDir("gbx_ogr_life_") { dir =>
            val f = dir.resolve("points.geojson")
            var mtime = System.currentTimeMillis() - 600000L
            def write(ids: Seq[Int]): Unit = {
                val features = ids.map { i =>
                    s"""{"type":"Feature","properties":{"id":$i},"geometry":{"type":"Point","coordinates":[$i.0,0.0]}}"""
                }
                Files.write(f, s"""{"type":"FeatureCollection","features":[${features.mkString(",")}]}""".getBytes(StandardCharsets.UTF_8))
                mtime += 2000L // a distinct mtime per rewrite, whatever the clock granularity
                Files.setLastModifiedTime(f, FileTime.fromMillis(mtime))
            }
            // multi=false: a FeatureCollection file (the GeoJSON driver), not newline-delimited GeoJSONSeq.
            def read() = spark.read.format("geojson_ogr").option("multi", "false").load(dir.toString)
            val before = entries()
            write(Seq(1, 2))
            read().count() shouldBe 2L
            write(Seq(1, 2, 3))
            read().count() shouldBe 3L
            val len = Files.size(f)
            write(Seq(7, 8, 9))
            withClue("the rewrite must keep the length, so only the content and mtime tell it apart: ") {
                Files.size(f) shouldBe len
            }
            read().select("id").collect().map(_.get(0).toString.toLong).toSet shouldBe Set(7L, 8L, 9L)
            withClue("node-file cache entries left by the reads: ") { leftovers(before) shouldBe empty }
        }
    }

    test("O5 a reader whose open or layer lookup fails frees the copy it took") {
        withShapefileDir { dir =>
            val c = config()
            val bad = dir.resolve("bad.shp")
            Files.write(bad, garbage)
            val before = entries()
            intercept[Exception](new OGR_Reader(partition(bad.toString, new StructType(), c)))
            withClue("after a failed open: ") { leftovers(before) shouldBe empty }
            val remote = dir.resolve("map.shp").toString
            intercept[Exception](new OGR_Reader(partition(remote, new StructType(), c, layer = "no_such_layer")))
            withClue("after a failed layer lookup: ") { leftovers(before) shouldBe empty }
        }
    }

    // A zipped shapefile goes through schema inference's node-file copy (OGR_DataSource) as well as the
    // planner's and the readers'.

    private def zip(out: Path, files: Seq[Path]): Unit = {
        val zos = new ZipOutputStream(Files.newOutputStream(out))
        try files.foreach { p =>
            zos.putNextEntry(new ZipEntry(p.getFileName.toString))
            Files.copy(p, zos)
            zos.closeEntry()
        } finally zos.close()
    }

    /** Writes the first `keep` features of the shapefile `src` as the shapefile `dst`. */
    private def writeFirstFeatures(src: Path, dst: Path, keep: Int): Unit = {
        GDALManager.initOgr()
        val in = ogr.Open(src.toString, 0)
        try {
            val inLayer = in.GetLayer(0)
            val out = ogr.GetDriverByName("ESRI Shapefile").CreateDataSource(dst.toString)
            try {
                val outLayer = out.CreateLayer(inLayer.GetName, inLayer.GetSpatialRef, inLayer.GetGeomType)
                val defn = inLayer.GetLayerDefn
                (0 until defn.GetFieldCount).foreach(i => outLayer.CreateField(defn.GetFieldDefn(i)))
                inLayer.ResetReading()
                (0 until keep).foreach { _ =>
                    val g = new Feature(outLayer.GetLayerDefn)
                    g.SetFrom(inLayer.GetNextFeature())
                    outLayer.CreateFeature(g)
                }
            } finally {
                out.FlushCache()
                out.delete()
            }
        } finally in.delete()
    }

    test("a zipped shapefile read twice leaves no copy; a rewrite one feature short counts one fewer") {
        withShapefileDir { dir =>
            withScratchDir("gbx_ogr_zip_") { zipDir =>
                val z = zipDir.resolve("map.zip")
                val n = featureCount(dir.resolve("map.shp").toString)
                zip(z, shapefileParts.map(dir.resolve))
                val t0 = Files.getLastModifiedTime(z).toMillis
                val before = entries()
                spark.read.format("shapefile_ogr").load(z.toString).count() shouldBe n.toLong
                withClue("after the first read: ") { leftovers(before) shouldBe empty }
                withScratchDir("gbx_ogr_zip_src_") { fewer =>
                    writeFirstFeatures(dir.resolve("map.shp"), fewer.resolve("map.shp"), n - 1)
                    zip(z, shapefileParts.map(fewer.resolve))
                }
                Files.setLastModifiedTime(z, FileTime.fromMillis(t0 + 2000L))
                spark.read.format("shapefile_ogr").load(z.toString).count() shouldBe (n - 1).toLong
                withClue("after the second read: ") { leftovers(before) shouldBe empty }
            }
        }
    }

    test("a zip that schema inference cannot open still frees the copy inference took") {
        withScratchDir("gbx_ogr_zip_") { dir =>
            val z = dir.resolve("broken.zip")
            Files.write(z, garbage)
            val before = entries()
            intercept[Exception](spark.read.format("shapefile_ogr").load(z.toString).count())
            withClue("node-file cache entries left by the failed inference: ") { leftovers(before) shouldBe empty }
        }
    }

}
