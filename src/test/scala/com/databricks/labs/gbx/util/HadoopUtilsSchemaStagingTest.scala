package com.databricks.labs.gbx.util

import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

import java.io.{File, IOException}
import java.nio.file.{Files, Path, Paths}

/** On DCS dedicated clusters the JVM process is refused direct `/Volumes` reads (java.nio and native
  * GDAL alike) while query tasks are not. These tests force the java.nio steps to fail the way DCS
  * does and check that schema staging still lands identical bytes via the `binaryFile` query step. */
class HadoopUtilsSchemaStagingTest extends PlanTest with SilentSparkSession {

    private val refused = HadoopUtils.SchemaReadStep(
      "refused",
      (p: String) => throw new java.nio.file.FileSystemException(s"$p: Operation not permitted")
    )

    private def queryStep = HadoopUtils.SchemaReadStep("binaryFile query", (p: String) => HadoopUtils.binaryFileBytes(spark, p))

    private def tmpDir(): Path = Files.createTempDirectory("gbx_schema_staging_test")

    test("readBytesForSchema falls through refused steps to the binaryFile query") {
        val f = tmpDir().resolve("roads.gpkg")
        Files.write(f, Array[Byte](1, 2, 3, 4, 5))
        HadoopUtils.readBytesForSchema(f.toString, Seq(refused, refused, queryStep)) shouldBe Array[Byte](1, 2, 3, 4, 5)
    }

    test("readBytesForSchema reports every step when all fail") {
        val e = intercept[IOException] {
            HadoopUtils.readBytesForSchema("/no/such/file.gpkg", Seq(refused, refused))
        }
        e.getMessage should include("/no/such/file.gpkg")
        "refused: ".r.findAllIn(e.getMessage).size shouldBe 2
    }

    test("binaryFileBytes reads a literal file name containing glob characters") {
        val dir = tmpDir()
        val literal = dir.resolve("a[1]{x}.gpkg")
        val decoy = dir.resolve("a1x.gpkg") // what the unescaped glob a[1]{x}.gpkg would match
        Files.write(literal, "literal".getBytes)
        Files.write(decoy, "decoy".getBytes)
        new String(HadoopUtils.binaryFileBytes(spark, literal.toString)) shouldBe "literal"
    }

    test("stageHeadForSchemaSpark stages a shapefile bundle through the binaryFile step") {
        val dir = tmpDir()
        val parts = Map("roads.shp" -> "shp", "roads.shx" -> "shx", "roads.dbf" -> "dbf", "roads.prj" -> "prj")
        parts.foreach { case (n, body) => Files.write(dir.resolve(n), body.getBytes) }
        val candidates = parts.keys.toSeq.sorted.map(n => dir.resolve(n).toString)
        val local = HadoopUtils.stageHeadForSchemaSpark(spark, dir.resolve("roads.shp").toString, candidates, Seq(refused, refused, queryStep))
        new File(local).getName shouldBe "roads.shp"
        parts.foreach { case (n, body) =>
            new String(Files.readAllBytes(Paths.get(new File(local).getParent, n))) shouldBe body
        }
    }

    test("stageDatasetForSchemaSpark stages a zipped FileGDB through the binaryFile step") {
        val zip = tmpDir().resolve("NYC_Sample.gdb.zip")
        Files.write(zip, Array[Byte](9, 8, 7))
        val local = HadoopUtils.stageDatasetForSchemaSpark(spark, zip.toString, Seq(refused, refused, queryStep))
        new File(local).getName shouldBe "NYC_Sample.gdb.zip"
        Files.readAllBytes(Paths.get(local)) shouldBe Array[Byte](9, 8, 7)
    }

    test("stageDatasetForSchemaSpark stages every file of a FileGDB directory") {
        val gdb = Files.createDirectory(tmpDir().resolve("parcels.gdb"))
        val parts = Map("a00000001.gdbtable" -> "t1", "a00000001.gdbtablx" -> "x1", "gdb" -> "g", "timestamps" -> "ts")
        parts.foreach { case (n, body) => Files.write(gdb.resolve(n), body.getBytes) }
        val local = HadoopUtils.stageDatasetForSchemaSpark(spark, gdb.toString, Seq(refused, refused, queryStep))
        new File(local).getName shouldBe "parcels.gdb"
        parts.foreach { case (n, body) => new String(Files.readAllBytes(Paths.get(local, n))) shouldBe body }
    }
}
