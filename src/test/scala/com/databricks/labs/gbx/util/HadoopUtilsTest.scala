package com.databricks.labs.gbx.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** HadoopUtils.cleanPath: the path forms of the supported storage fabric. */
class HadoopUtilsTest extends AnyFunSuite {

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
}
