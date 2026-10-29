package com.databricks.labs.gbx.rasterx.gdal

import com.databricks.labs.gbx.expressions.ExpressionConfig
import org.gdal.gdal.gdal
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import java.nio.file.{Files, Paths}

class GDALManagerTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp")
        gdal.AllRegister()
    }

    test("GDAL_VRT_ENABLE_PYTHON defaults to NO after configureGDAL") {
        gdal.GetConfigOption("GDAL_VRT_ENABLE_PYTHON") shouldBe "NO"
    }

    test("withVrtPython flips GDAL_VRT_ENABLE_PYTHON YES inside the block and resets NO after") {
        gdal.GetConfigOption("GDAL_VRT_ENABLE_PYTHON") shouldBe "NO"
        val inside = GDALManager.withVrtPython {
            gdal.GetConfigOption("GDAL_VRT_ENABLE_PYTHON")
        }
        inside shouldBe "YES"
        gdal.GetConfigOption("GDAL_VRT_ENABLE_PYTHON") shouldBe "NO"
    }

    test("withVrtPython resets to NO when the block throws") {
        an[RuntimeException] should be thrownBy GDALManager.withVrtPython[Unit] {
            throw new RuntimeException("boom")
        }
        gdal.GetConfigOption("GDAL_VRT_ENABLE_PYTHON") shouldBe "NO"
    }

    test("withVrtPython refcounts so nested calls keep YES until the outer exits") {
        GDALManager.withVrtPython {
            gdal.GetConfigOption("GDAL_VRT_ENABLE_PYTHON") shouldBe "YES"
            GDALManager.withVrtPython {
                gdal.GetConfigOption("GDAL_VRT_ENABLE_PYTHON") shouldBe "YES"
            }
            gdal.GetConfigOption("GDAL_VRT_ENABLE_PYTHON") shouldBe "YES"
        }
        gdal.GetConfigOption("GDAL_VRT_ENABLE_PYTHON") shouldBe "NO"
    }

    test("network-capable drivers are not registered by default") {
        for (name <- Seq("WMS", "WMTS", "WCS", "WFS", "HTTP", "CSW", "OGCAPI")) {
            withClue(s"expected $name to be skipped: ") {
                gdal.GetDriverByName(name) shouldBe null
            }
        }
    }

    test("core raster drivers remain registered") {
        gdal.GetDriverByName("GTiff") should not be null
        gdal.GetDriverByName("VRT") should not be null
    }

    test("configureGDAL strips spark.gdal. / spark.databricks.labs.gbx.gdal. prefixes before SetConfigOption") {
        val config = ExpressionConfig(
          Map(
            "spark.gdal.GDAL_SKIP" -> "FOO",
            "spark.databricks.labs.gbx.gdal.GDAL_HTTP_TIMEOUT" -> "42"
          ),
          null
        )
        try {
            GDALManager.configureGDAL(config)
            gdal.GetConfigOption("GDAL_SKIP") shouldBe "FOO"
            gdal.GetConfigOption("GDAL_HTTP_TIMEOUT") shouldBe "42"
        } finally {
            // restore defaults so later tests / suites see the hardened config
            GDALManager.configureGDAL("/tmp", "/tmp")
        }
    }

    test("a later init naming another GDAL is refused; one naming the loaded JNI is a no-op") {
        GDALManager.init(ExpressionConfig(Map.empty, null)) // an earlier init with no conf (KernelGdal's)
        val loaded = GDALManager.ensureJniLoaded(None).jni
        def dirWith(jni: java.nio.file.Path => Unit): String = {
            val d = Files.createTempDirectory("gbx_native_dir")
            Files.createDirectories(d.resolve("lib"))
            jni(d.resolve("lib/libgdalalljni.so"))
            d.toString
        }
        val other = dirWith(p => Files.write(p, Array[Byte](1, 2, 3)))
        val e = intercept[IllegalStateException] {
            GDALManager.init(ExpressionConfig(Map(GDALManager.NativeDirKey -> other), null))
        }
        e.getMessage should (include(loaded) and include(s"$other/lib/libgdalalljni.so") and include("Fix:"))
        val same = dirWith(p => Files.createSymbolicLink(p, Paths.get(loaded)))
        GDALManager.init(ExpressionConfig(Map(GDALManager.NativeDirKey -> same), null))
    }

}
