package com.databricks.labs.gbx.util

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.jdk.CollectionConverters._

/** Tests for AtomicDistributedCopy.copyOverwriting and HadoopUtils.copyToPath on the local FS: a copy always
  * replaces what is at its destination. */
class AtomicDistributedCopyTest extends AnyFunSuite with BeforeAndAfterEach {

    private var tempDir: java.nio.file.Path = _
    private var srcPath: Path = _
    private var dstPath: Path = _
    private var localFs: org.apache.hadoop.fs.FileSystem = _

    override def beforeEach(): Unit = {
        tempDir = Files.createTempDirectory("AtomicDistributedCopyTest")
        localFs = org.apache.hadoop.fs.FileSystem.getLocal(new org.apache.hadoop.conf.Configuration())
        srcPath = new Path(tempDir.toUri.toString, "source.txt")
        dstPath = new Path(tempDir.toUri.toString, "dest.txt")
    }

    override def afterEach(): Unit = {
        if (localFs != null) localFs.close()
        if (tempDir != null) {
            try {
                Files.walk(tempDir).sorted(java.util.Comparator.reverseOrder()).forEach(p => Files.deleteIfExists(p))
            } catch { case _: Exception => }
        }
    }

    test("copyOverwriting copies a file when the destination does not exist") {
        val out = localFs.create(srcPath)
        out.write("hello".getBytes)
        out.close()

        AtomicDistributedCopy.copyOverwriting(localFs, localFs, srcPath, dstPath)

        localFs.exists(dstPath) shouldBe true
        val in = localFs.open(dstPath)
        val buf = Array.ofDim[Byte](32)
        val n = in.read(buf)
        in.close()
        new String(buf, 0, n) shouldBe "hello"
    }

    /** Helper: read dst back as a String. */
    private def readDst(): String = {
        val in = localFs.open(dstPath)
        val buf = Array.ofDim[Byte](32)
        val n = in.read(buf)
        in.close()
        new String(buf, 0, n)
    }

    test("copyOverwriting replaces a destination of the same length") {
        // Every read copy lands in a fresh generation dir, so "skip if same length" could only ever skip a
        // writer's new output.
        val out1 = localFs.create(srcPath)
        out1.write("src".getBytes) // 3 bytes
        out1.close()
        val out2 = localFs.create(dstPath)
        out2.write("dst".getBytes) // 3 bytes — same length as src
        out2.close()

        AtomicDistributedCopy.copyOverwriting(localFs, localFs, srcPath, dstPath)

        readDst() shouldBe "src"
    }

    test("copyOverwriting replaces a destination of a different length") {
        val out1 = localFs.create(srcPath)
        out1.write("src".getBytes) // 3 bytes
        out1.close()
        val out2 = localFs.create(dstPath)
        out2.write("existing".getBytes) // 8 bytes — different length
        out2.close()

        AtomicDistributedCopy.copyOverwriting(localFs, localFs, srcPath, dstPath)

        localFs.exists(dstPath) shouldBe true
        readDst() shouldBe "src"
    }

    test("copyOverwriting a file onto itself keeps it") {
        val out = localFs.create(srcPath)
        out.write("same".getBytes)
        out.close()

        AtomicDistributedCopy.copyOverwriting(localFs, localFs, srcPath, srcPath)

        localFs.exists(srcPath) shouldBe true
        val in = localFs.open(srcPath)
        val buf = Array.ofDim[Byte](32)
        val n = in.read(buf)
        in.close()
        new String(buf, 0, n) shouldBe "same"
    }

    /** Writes `files` (relative path -> text) under `dir`. */
    private def writeTree(dir: java.nio.file.Path, files: Map[String, String]): Unit =
        files.foreach { case (rel, text) =>
            val f = dir.resolve(rel)
            Files.createDirectories(f.getParent)
            Files.write(f, text.getBytes(StandardCharsets.UTF_8))
        }

    /** Relative path -> text of every regular, non-checksum file under `dir`. */
    private def readTree(dir: java.nio.file.Path): Map[String, String] = {
        val s = Files.walk(dir)
        try s.iterator().asScala
            .filter(p => Files.isRegularFile(p) && !p.getFileName.toString.endsWith(".crc"))
            .map(p => dir.relativize(p).toString -> new String(Files.readAllBytes(p), StandardCharsets.UTF_8))
            .toMap
        finally s.close()
    }

    test("copyOverwriting replaces an existing directory: no entry of the old one is kept, nothing is nested") {
        // A directory-format output (Zarr) written again to the same path.
        val src = tempDir.resolve("new.zarr")
        val dst = tempDir.resolve("out").resolve("new.zarr")
        writeTree(src, Map(".zarray" -> "v2", "b/0.0" -> "AAAA"))
        writeTree(dst, Map(".zarray" -> "v1", "b/0.0" -> "OLD!", "b/9.9" -> "stale"))

        AtomicDistributedCopy.copyOverwriting(localFs, localFs, new Path(src.toUri), new Path(dst.toUri))

        readTree(dst) shouldBe Map(".zarray" -> "v2", "b/0.0" -> "AAAA")
    }

    test("copyToPath twice to one destination: the second copy of the same length replaces the first") {
        val hconf = new SerializableConfiguration(new Configuration())
        val local = tempDir.resolve("local")
        Files.createDirectories(local)
        val tile = local.resolve("tile.tif")
        val dest = tempDir.resolve("out").resolve("tile.tif")
        Files.write(tile, "first".getBytes(StandardCharsets.UTF_8))
        HadoopUtils.copyToPath(tile.toString, dest.toString, hconf)
        Files.write(tile, "again".getBytes(StandardCharsets.UTF_8)) // same length, different bytes
        HadoopUtils.copyToPath(tile.toString, dest.toString, hconf)
        new String(Files.readAllBytes(dest), StandardCharsets.UTF_8) shouldBe "again"
    }
}
