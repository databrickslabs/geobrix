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

    // Counting fakes: each step records its calls so the tests can see which steps a staging call runs.

    private def counting(
        name: String,
        calls: scala.collection.mutable.Map[String, Int],
        skipsAfterRefusal: Boolean = false
    )(body: String => Array[Byte]): HadoopUtils.SchemaReadStep =
        HadoopUtils.SchemaReadStep(
          name,
          (p: String) => { calls(name) = calls.getOrElse(name, 0) + 1; body(p) },
          skipsAfterRefusal
        )

    private def readsFile(p: String): Array[Byte] = Files.readAllBytes(Paths.get(p))

    private def notThere(p: String): Nothing = throw new java.nio.file.NoSuchFileException(p)

    private def operationNotPermitted(p: String): Nothing =
        throw new java.nio.file.FileSystemException(p, null, "Operation not permitted")

    private def shapefileBundle(): (Path, Seq[String], Map[String, String]) = {
        val dir = tmpDir()
        val parts = Map("roads.shp" -> "shp", "roads.shx" -> "shx", "roads.dbf" -> "dbf", "roads.prj" -> "prj")
        parts.foreach { case (n, body) => Files.write(dir.resolve(n), body.getBytes) }
        (dir, parts.keys.toSeq.sorted.map(n => dir.resolve(n).toString), parts)
    }

    private def stagedAs(local: String, parts: Map[String, String]): Unit =
        parts.foreach { case (n, body) =>
            new String(Files.readAllBytes(Paths.get(new File(local).getParent, n))) shouldBe body
        }

    test("stageHeadForSchemaSpark runs only the step that worked for the sidecars of a bundle") {
        val (dir, candidates, parts) = shapefileBundle()
        val calls = scala.collection.mutable.Map.empty[String, Int]
        val steps = Seq(
          counting("driver", calls)(notThere),
          counting("executor", calls)(notThere),
          counting("query", calls)(readsFile)
        )
        val local = HadoopUtils.stageHeadForSchemaSpark(spark, dir.resolve("roads.shp").toString, candidates, steps)
        stagedAs(local, parts)
        // Four files: the first probes driver and executor, the other three start at the query step.
        calls.toMap shouldBe Map("driver" -> 1, "executor" -> 1, "query" -> 4)
    }

    test("a step that worked for the head file is also the first step for the sidecars") {
        val (dir, candidates, parts) = shapefileBundle()
        val calls = scala.collection.mutable.Map.empty[String, Int]
        val steps = Seq(
          counting("driver", calls)(notThere),
          counting("executor", calls)(readsFile),
          counting("query", calls)(readsFile)
        )
        stagedAs(HadoopUtils.stageHeadForSchemaSpark(spark, dir.resolve("roads.shp").toString, candidates, steps), parts)
        calls.toMap shouldBe Map("driver" -> 1, "executor" -> 4)
    }

    test("an Operation not permitted driver failure never calls the executor step") {
        val (dir, candidates, parts) = shapefileBundle()
        val calls = scala.collection.mutable.Map.empty[String, Int]
        val steps = Seq(
          counting("driver", calls)(operationNotPermitted),
          counting("executor", calls, skipsAfterRefusal = true)(readsFile),
          counting("query", calls)(readsFile)
        )
        val local = HadoopUtils.stageHeadForSchemaSpark(spark, dir.resolve("roads.shp").toString, candidates, steps)
        stagedAs(local, parts)
        calls.toMap shouldBe Map("driver" -> 1, "query" -> 4)
    }

    test("an AccessDeniedException driver failure never calls the executor step") {
        val f = tmpDir().resolve("roads.gpkg")
        Files.write(f, Array[Byte](1, 2, 3))
        val calls = scala.collection.mutable.Map.empty[String, Int]
        val steps = Seq(
          counting("driver", calls)(p => throw new java.nio.file.AccessDeniedException(p)),
          counting("executor", calls, skipsAfterRefusal = true)(readsFile),
          counting("query", calls)(readsFile)
        )
        HadoopUtils.readBytesForSchema(f.toString, steps) shouldBe Array[Byte](1, 2, 3)
        calls.toMap shouldBe Map("driver" -> 1, "query" -> 1)
    }

    test("a different driver failure still tries the executor step") {
        val (dir, candidates, parts) = shapefileBundle()
        val calls = scala.collection.mutable.Map.empty[String, Int]
        val steps = Seq(
          counting("driver", calls)(notThere),
          counting("executor", calls, skipsAfterRefusal = true)(readsFile),
          counting("query", calls)(readsFile)
        )
        stagedAs(HadoopUtils.stageHeadForSchemaSpark(spark, dir.resolve("roads.shp").toString, candidates, steps), parts)
        calls("executor") shouldBe 4
        calls.get("query") shouldBe None
    }

    test("a skipped step is named in the error when every remaining step fails") {
        val calls = scala.collection.mutable.Map.empty[String, Int]
        val steps = Seq(
          counting("driver", calls)(operationNotPermitted),
          counting("executor", calls, skipsAfterRefusal = true)(readsFile),
          counting("query", calls)(notThere)
        )
        val e = intercept[IOException] { HadoopUtils.readBytesForSchema("/no/such/file.gpkg", steps) }
        e.getMessage should include("executor: skipped")
        calls.get("executor") shouldBe None
    }

    test("the default steps skip the executor step after a refusal and the others never") {
        HadoopUtils.schemaReadSteps(spark).map(s => (s.name, s.skipsAfterRefusal)) shouldBe Seq(
          ("driver java.nio", false),
          ("executor java.nio", true),
          ("binaryFile query", false)
        )
    }

    test("isFileAccessRefused recognises the DCS refusal and nothing broader") {
        import HadoopUtils.isFileAccessRefused
        isFileAccessRefused(new java.nio.file.AccessDeniedException("/Volumes/a/b/c")) shouldBe true
        isFileAccessRefused(new java.nio.file.FileSystemException("/Volumes/a/b/c", null, "Operation not permitted")) shouldBe true
        isFileAccessRefused(new java.nio.file.FileSystemException("/Volumes/a/b/c: Operation not permitted")) shouldBe true
        isFileAccessRefused(new RuntimeException("wrapped", new java.nio.file.AccessDeniedException("/x"))) shouldBe true
        isFileAccessRefused(new java.nio.file.NoSuchFileException("/Volumes/a/b/c")) shouldBe false
        isFileAccessRefused(new java.nio.file.FileSystemException("/x", null, "Is a directory")) shouldBe false
        isFileAccessRefused(new IOException("Operation not permitted")) shouldBe false
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
        val local = HadoopUtils.stageDatasetForSchemaSpark(spark, gdb.toString)
        new File(local).getName shouldBe "parcels.gdb"
        parts.foreach { case (n, body) => new String(Files.readAllBytes(Paths.get(local, n))) shouldBe body }
    }

    // Driver memory is not practical to assert here. What bounds it is that the driver fetches the
    // query's result one partition at a time (toLocalIterator: one job per partition) instead of all
    // partitions at once (collect: one job). With one file per partition, fewer jobs than files means
    // the whole dataset was held on the driver at once.
    test("stageDatasetForSchemaSpark fetches a FileGDB directory a partition at a time") {
        val gdb = Files.createDirectory(tmpDir().resolve("roads.gdb"))
        val parts = Map("a00000001.gdbtable" -> "t1", "a00000001.gdbtablx" -> "x1", "gdb" -> "g", "timestamps" -> "ts")
        parts.foreach { case (n, body) => Files.write(gdb.resolve(n), body.getBytes) }
        val group = "gbx-gdb-staging-test"
        val jobs = new java.util.concurrent.atomic.AtomicInteger(0)
        val listener = new org.apache.spark.scheduler.SparkListener {
            override def onJobStart(e: org.apache.spark.scheduler.SparkListenerJobStart): Unit =
                if (Option(e.properties).exists(_.getProperty("spark.jobGroup.id") == group)) jobs.incrementAndGet()
        }
        spark.sparkContext.addSparkListener(listener)
        spark.sparkContext.setJobGroup(group, "FileGDB staging")
        try {
            val local = withSQLConf("spark.sql.files.maxPartitionBytes" -> "1", "spark.sql.files.openCostInBytes" -> "1") {
                HadoopUtils.stageDatasetForSchemaSpark(spark, gdb.toString, Seq(refused, refused, queryStep))
            }
            parts.foreach { case (n, body) => new String(Files.readAllBytes(Paths.get(local, n))) shouldBe body }
            import org.scalatest.time.{Millis, Seconds, Span}
            eventually(timeout(Span(30, Seconds)), interval(Span(100, Millis))) { jobs.get should be >= parts.size }
        } finally {
            spark.sparkContext.clearJobGroup()
            spark.sparkContext.removeSparkListener(listener)
        }
    }

    test("stageDatasetForSchemaSpark refuses a zip over the 2 GiB a schema read holds, naming the limit") {
        val zip = tmpDir().resolve("huge.gdb.zip")
        val raf = new java.io.RandomAccessFile(zip.toFile, "rw")
        try raf.setLength(3L << 30) // sparse: no 3 GiB written
        finally raf.close()
        try {
            val e = intercept[IOException] {
                HadoopUtils.stageDatasetForSchemaSpark(spark, zip.toString, Seq(refused, refused, queryStep))
            }
            e.getMessage should include(zip.toString)
            e.getMessage should include("2 GiB")
            e.getMessage should include(s"${3L << 30} bytes")
        } finally Files.deleteIfExists(zip)
    }

    // stageDatasetOrFallBack: the readRemote -> fallback decision OGR_DataSource.inferSchema makes for
    // FileGDB / zip inputs. NodeFileManager.readRemote throws on a refused copy (NodeFilePathUtil.readLock).

    private val remote = "/Volumes/main/default/data/NYC_Sample.gdb.zip"

    test("stageDatasetOrFallBack keeps the node-file copy when readRemote succeeds and the file exists") {
        val copy = tmpDir().resolve("copy.gdb.zip")
        Files.write(copy, Array[Byte](1))
        var released = List.empty[String]
        var fellBack = false
        val res = HadoopUtils.stageDatasetOrFallBack(
          remote,
          _ => copy.toString,
          p => released :+= p,
          _ => { fellBack = true; "unused" }
        )
        res shouldBe ((copy.toString, true))
        released shouldBe empty
        fellBack shouldBe false
    }

    test("stageDatasetOrFallBack falls back when readRemote throws") {
        var released = List.empty[String]
        var fellBackWith = List.empty[String]
        val res = HadoopUtils.stageDatasetOrFallBack(
          remote,
          _ => throw new RuntimeException("Failed to materialize after 3 retries."),
          p => released :+= p,
          p => { fellBackWith :+= p; "/tmp/staged/NYC_Sample.gdb.zip" }
        )
        res shouldBe (("/tmp/staged/NYC_Sample.gdb.zip", false))
        fellBackWith shouldBe List(remote)
        released shouldBe empty // the failed copy holds no read lock
    }

    test("stageDatasetOrFallBack releases the lock and falls back when readRemote returns a missing path") {
        val missing = tmpDir().resolve("not-there.gdb.zip").toString
        var released = List.empty[String]
        val res = HadoopUtils.stageDatasetOrFallBack(
          remote,
          _ => missing,
          p => released :+= p,
          _ => "/tmp/staged/NYC_Sample.gdb.zip"
        )
        res shouldBe (("/tmp/staged/NYC_Sample.gdb.zip", false))
        released shouldBe List(remote)
    }

    test("stageDatasetOrFallBack attaches the readRemote failure to the fallback's exception") {
        val original = new RuntimeException("Failed to materialize after 3 retries.")
        val fallbackErr = new IOException("binaryFile matched 0 files")
        val e = intercept[IOException] {
            HadoopUtils.stageDatasetOrFallBack(remote, _ => throw original, _ => (), _ => throw fallbackErr)
        }
        (e eq fallbackErr) shouldBe true
        e.getSuppressed.toSeq shouldBe Seq(original)
    }

    test("stageDatasetOrFallBack explains a missing copy when the fallback also fails") {
        val missing = tmpDir().resolve("not-there.gdb.zip").toString
        var released = List.empty[String]
        val e = intercept[IOException] {
            HadoopUtils.stageDatasetOrFallBack(remote, _ => missing, p => released :+= p, _ => throw new IOException("boom"))
        }
        released shouldBe List(remote)
        e.getSuppressed.map(_.getMessage).mkString should include(missing)
    }

    test("stageDatasetOrFallBack falls back when the copy is a directory with no files in it") {
        // A FileGDB copy that holds only a subfolder (or nothing) cannot be opened by OpenFileGDB.
        val remoteGdb = "/Volumes/main/default/data/NYC_Sample.gdb"
        val copy = Files.createDirectory(tmpDir().resolve("NYC_Sample.gdb"))
        Files.createDirectory(copy.resolve("NYC_Sample.gdb"))
        var released = List.empty[String]
        val res = HadoopUtils.stageDatasetOrFallBack(
          remoteGdb,
          _ => copy.toString,
          p => released :+= p,
          _ => "/tmp/staged/NYC_Sample.gdb"
        )
        res shouldBe (("/tmp/staged/NYC_Sample.gdb", false))
        released shouldBe List(remoteGdb)
        val e = intercept[IOException] {
            HadoopUtils.stageDatasetOrFallBack(remoteGdb, _ => copy.toString, _ => (), _ => throw new IOException("boom"))
        }
        e.getSuppressed.map(_.getMessage).mkString should include(copy.toString)
    }

    test("discardSchemaCopy deletes a staged schema copy and leaves any other path alone") {
        val zip = tmpDir().resolve("NYC_Sample.gdb.zip")
        Files.write(zip, Array[Byte](9, 8, 7))
        val stagedZip = Paths.get(HadoopUtils.stageDatasetForSchemaSpark(spark, zip.toString, Seq(refused, refused, queryStep)))
        val gdb = Files.createDirectory(tmpDir().resolve("parcels.gdb"))
        Files.write(gdb.resolve("a00000001.gdbtable"), "t1".getBytes)
        val stagedGdb = Paths.get(HadoopUtils.stageDatasetForSchemaSpark(spark, gdb.toString))
        for (staged <- Seq(stagedZip, stagedGdb)) {
            Files.exists(staged) shouldBe true
            HadoopUtils.discardSchemaCopy(staged.toString)
            withClue(s"staging dir ${staged.getParent} after discardSchemaCopy($staged): ") {
                Files.exists(staged.getParent) shouldBe false
            }
        }
        // Not a schema-staging dir: the source zip and the .gdb folder itself stay.
        HadoopUtils.discardSchemaCopy(zip.toString)
        HadoopUtils.discardSchemaCopy(gdb.resolve("a00000001.gdbtable").toString)
        Files.exists(zip) shouldBe true
        Files.exists(gdb.resolve("a00000001.gdbtable")) shouldBe true
    }
}
