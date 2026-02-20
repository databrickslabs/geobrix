package com.databricks.labs.gbx.util

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.Files

/** Tests for AtomicDistributedCopy (copy-if-needed and wait-until-exists behavior on local FS). */
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

    test("copyIfNeeded should copy file when destination does not exist") {
        val out = localFs.create(srcPath)
        out.write("hello".getBytes)
        out.close()

        AtomicDistributedCopy.copyIfNeeded(localFs, localFs, srcPath, dstPath)

        localFs.exists(dstPath) shouldBe true
        val in = localFs.open(dstPath)
        val buf = Array.ofDim[Byte](32)
        val n = in.read(buf)
        in.close()
        new String(buf, 0, n) shouldBe "hello"
    }

    test("copyIfNeeded when destination already exists should not throw") {
        val out1 = localFs.create(srcPath)
        out1.write("src".getBytes)
        out1.close()
        val out2 = localFs.create(dstPath)
        out2.write("existing".getBytes)
        out2.close()

        AtomicDistributedCopy.copyIfNeeded(localFs, localFs, srcPath, dstPath)

        localFs.exists(dstPath) shouldBe true
        val in = localFs.open(dstPath)
        val buf = Array.ofDim[Byte](32)
        val n = in.read(buf)
        in.close()
        new String(buf, 0, n) shouldBe "existing"
    }

    test("copyIfNeeded with same path (src == dst) should not throw when file exists") {
        val out = localFs.create(srcPath)
        out.write("same".getBytes)
        out.close()

        AtomicDistributedCopy.copyIfNeeded(localFs, localFs, srcPath, srcPath)

        localFs.exists(srcPath) shouldBe true
    }
}
