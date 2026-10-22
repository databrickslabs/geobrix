package com.databricks.labs.gbx.rasterx.expressions.web

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** The render-argument validators shared by `rst_tilexyz` and `rst_xyzpyramid` (no dataset needed). */
class RST_TileXYZValidationTest extends AnyFunSuite {

    test("validateRender normalizes format to upper case and resampling to lower case") {
        RST_TileXYZ.validateRender("rst_x", "png", 256, "Bilinear") shouldBe (("PNG", "bilinear"))
        RST_TileXYZ.validateRender("rst_x", "WebP", 1, "CubicSpline") shouldBe (("WEBP", "cubicspline"))
        RST_TileXYZ.validateRender("rst_x", "JPEG", RST_TileXYZ.MaxTileSize, "q3") shouldBe (("JPEG", "q3"))
    }

    test("validateRender accepts every allowed format and resampling name") {
        RST_TileXYZ.AllowedFormats.foreach(f => RST_TileXYZ.validateRender("rst_x", f, 8, "near")._1 shouldBe f)
        RST_TileXYZ.AllowedResampling.foreach(r => RST_TileXYZ.validateRender("rst_x", "PNG", 8, r)._2 shouldBe r)
    }

    test("validateRender rejects an unknown format, an unknown resampling and a size outside (0, 4096], naming the function") {
        val format = intercept[IllegalArgumentException](RST_TileXYZ.validateRender("rst_xyzpyramid", "GIF", 256, "bilinear"))
        format.getMessage should (include("rst_xyzpyramid: format must be one of") and include("got 'GIF'"))

        val resampling = intercept[IllegalArgumentException](RST_TileXYZ.validateRender("rst_x", "PNG", 256, "sharpest"))
        resampling.getMessage should include("unsupported resampling 'sharpest'")

        for (size <- Seq(0, -1, RST_TileXYZ.MaxTileSize + 1, 5000)) {
            val ex = intercept[IllegalArgumentException](RST_TileXYZ.validateRender("rst_x", "PNG", size, "bilinear"))
            ex.getMessage should include("size must be in (0, 4096]")
        }
    }

    test("parseRescale reads auto, none (any case, null means auto) and a min,max pair") {
        RST_TileXYZ.parseRescale("rst_x", null) shouldBe RST_TileXYZ.RescaleAuto
        RST_TileXYZ.parseRescale("rst_x", " AUTO ") shouldBe RST_TileXYZ.RescaleAuto
        RST_TileXYZ.parseRescale("rst_x", "None") shouldBe RST_TileXYZ.RescaleNone
        RST_TileXYZ.parseRescale("rst_x", "8000, 12000") shouldBe RST_TileXYZ.RescalePair(8000.0, 12000.0)
        RST_TileXYZ.parseRescale("rst_x", "-5.5,0") shouldBe RST_TileXYZ.RescalePair(-5.5, 0.0)
    }

    test("parseRescale rejects anything else") {
        for (bad <- Seq("bogus", "", "1", "1,2,3", "a,b", "5,1", "3,3")) {
            val ex = intercept[IllegalArgumentException](RST_TileXYZ.parseRescale("rst_xyzpyramid", bad))
            withClue(s"'$bad': ") { ex.getMessage should (include("rst_xyzpyramid: rescale") and include("min")) }
        }
    }

}
