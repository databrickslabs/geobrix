package com.databricks.labs.gbx.rasterx

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class RasterXFunctionsTest extends AnyFunSuite {

    // ====== Structure Tests ======

    test("functions object should be Serializable") {
        functions shouldBe a[Serializable]
    }

    test("flag should have correct value") {
        functions.flag shouldBe "com.databricks.labs.gbx.rasterx.registered"
    }

    // ====== Accessor Functions ======

    test("rst_avg should create Column") {
        val result = functions.rst_avg(col("tile"))
        result should not be null
        result.getClass.getName should include("Column")
    }

    test("rst_bandmetadata should accept tile and band") {
        val result = functions.rst_bandmetadata(col("tile"), col("band"))
        result should not be null
    }

    test("rst_boundingbox should create Column") {
        val result = functions.rst_boundingbox(col("tile"))
        result should not be null
    }

    test("rst_format should create Column") {
        val result = functions.rst_format(col("tile"))
        result should not be null
    }

    test("rst_georeference should create Column") {
        val result = functions.rst_georeference(col("tile"))
        result should not be null
    }

    test("rst_getnodata should create Column") {
        val result = functions.rst_getnodata(col("tile"))
        result should not be null
    }

    test("rst_getsubdataset should accept tile and subset name") {
        val result = functions.rst_getsubdataset(col("tile"), col("name"))
        result should not be null
    }

    test("rst_height should create Column") {
        val result = functions.rst_height(col("tile"))
        result should not be null
    }

    test("rst_max should create Column") {
        val result = functions.rst_max(col("tile"))
        result should not be null
    }

    test("rst_median should create Column") {
        val result = functions.rst_median(col("tile"))
        result should not be null
    }

    test("rst_memsize should create Column") {
        val result = functions.rst_memsize(col("tile"))
        result should not be null
    }

    test("rst_metadata should create Column") {
        val result = functions.rst_metadata(col("tile"))
        result should not be null
    }

    test("rst_min should create Column") {
        val result = functions.rst_min(col("tile"))
        result should not be null
    }

    test("rst_numbands should create Column") {
        val result = functions.rst_numbands(col("tile"))
        result should not be null
    }

    test("rst_pixelcount should create Column") {
        val result = functions.rst_pixelcount(col("tile"))
        result should not be null
    }

    test("rst_pixelheight should create Column") {
        val result = functions.rst_pixelheight(col("tile"))
        result should not be null
    }

    test("rst_pixelwidth should create Column") {
        val result = functions.rst_pixelwidth(col("tile"))
        result should not be null
    }

    test("rst_rotation should create Column") {
        val result = functions.rst_rotation(col("tile"))
        result should not be null
    }

    test("rst_scalex should create Column") {
        val result = functions.rst_scalex(col("tile"))
        result should not be null
    }

    test("rst_scaley should create Column") {
        val result = functions.rst_scaley(col("tile"))
        result should not be null
    }

    test("rst_skewx should create Column") {
        val result = functions.rst_skewx(col("tile"))
        result should not be null
    }

    test("rst_skewy should create Column") {
        val result = functions.rst_skewy(col("tile"))
        result should not be null
    }

    test("rst_srid should create Column") {
        val result = functions.rst_srid(col("tile"))
        result should not be null
    }

    test("rst_subdatasets should create Column") {
        val result = functions.rst_subdatasets(col("tile"))
        result should not be null
    }

    test("rst_summary should create Column") {
        val result = functions.rst_summary(col("tile"))
        result should not be null
    }

    test("rst_type should create Column") {
        val result = functions.rst_type(col("tile"))
        result should not be null
    }

    test("rst_upperleftx should create Column") {
        val result = functions.rst_upperleftx(col("tile"))
        result should not be null
    }

    test("rst_upperlefty should create Column") {
        val result = functions.rst_upperlefty(col("tile"))
        result should not be null
    }

    test("rst_width should create Column") {
        val result = functions.rst_width(col("tile"))
        result should not be null
    }

    // ====== Aggregator Functions ======

    test("rst_combineavg_agg should create Column") {
        val result = functions.rst_combineavg_agg(col("tile"))
        result should not be null
    }

    test("rst_derivedband_agg should accept pyfunc and funcName") {
        val result = functions.rst_derivedband_agg(col("tile"), "def f(x): return x", "f")
        result should not be null
    }

    test("rst_merge_agg should create Column") {
        val result = functions.rst_merge_agg(col("tile"))
        result should not be null
    }

    // ====== Constructor Functions ======

    test("rst_fromcontent should accept content and driver") {
        val result = functions.rst_fromcontent(col("content"), col("driver"))
        result should not be null
    }

    test("rst_fromfile should accept path and driver") {
        val result = functions.rst_fromfile(col("path"), col("driver"))
        result should not be null
    }

    test("rst_frombands should accept bands") {
        val result = functions.rst_frombands(col("bands"))
        result should not be null
    }

    // ====== Generator Functions ======

    test("rst_h3_tessellate should accept tile and resolution") {
        val result = functions.rst_h3_tessellate(col("tile"), col("res"))
        result should not be null
    }

    test("rst_maketiles should accept tile, width, and height") {
        val result = functions.rst_maketiles(col("tile"), col("w"), col("h"))
        result should not be null
    }

    test("rst_retile should accept tile, width, and height") {
        val result = functions.rst_retile(col("tile"), col("w"), col("h"))
        result should not be null
    }

    test("rst_separatebands should create Column") {
        val result = functions.rst_separatebands(col("tile"))
        result should not be null
    }

    test("rst_tooverlappingtiles should accept tile, width, height, and overlap") {
        val result = functions.rst_tooverlappingtiles(col("tile"), col("w"), col("h"), col("o"))
        result should not be null
    }

    // ====== Grid Functions ======

    test("rst_h3_rastertogridavg should accept tile and resolution") {
        val result = functions.rst_h3_rastertogridavg(col("tile"), col("res"))
        result should not be null
    }

    test("rst_h3_rastertogridcount should accept tile and resolution") {
        val result = functions.rst_h3_rastertogridcount(col("tile"), col("res"))
        result should not be null
    }

    test("rst_h3_rastertogridmax should accept tile and resolution") {
        val result = functions.rst_h3_rastertogridmax(col("tile"), col("res"))
        result should not be null
    }

    test("rst_h3_rastertogridmin should accept tile and resolution") {
        val result = functions.rst_h3_rastertogridmin(col("tile"), col("res"))
        result should not be null
    }

    test("rst_h3_rastertogridmedian should accept tile and resolution") {
        val result = functions.rst_h3_rastertogridmedian(col("tile"), col("res"))
        result should not be null
    }

    // ====== Operation Functions ======

    test("rst_asformat should accept tile and format") {
        val result = functions.rst_asformat(col("tile"), col("format"))
        result should not be null
    }

    test("rst_clip should accept tile, clip geometry, and cutline flag") {
        val result = functions.rst_clip(col("tile"), col("geom"), col("flag"))
        result should not be null
    }

    test("rst_combineavg should accept tiles") {
        val result = functions.rst_combineavg(col("tiles"))
        result should not be null
    }

    test("rst_convolve should accept tile and kernel") {
        val result = functions.rst_convolve(col("tile"), col("kernel"))
        result should not be null
    }

    test("rst_derivedband should accept tile, pyfunc, and funcName") {
        val result = functions.rst_derivedband(col("tile"), "def f(x): return x", "f")
        result should not be null
    }

    test("rst_filter should accept tile, kernel size, and operation") {
        val result = functions.rst_filter(col("tile"), col("size"), col("op"))
        result should not be null
    }

    test("rst_initnodata should create Column") {
        val result = functions.rst_initnodata(col("tile"))
        result should not be null
    }

    test("rst_isempty should create Column") {
        val result = functions.rst_isempty(col("tile"))
        result should not be null
    }

    test("rst_mapalgebra should accept tiles and expression") {
        val result = functions.rst_mapalgebra(col("tiles"), col("expr"))
        result should not be null
    }

    test("rst_merge should accept tiles") {
        val result = functions.rst_merge(col("tiles"))
        result should not be null
    }

    test("rst_ndvi should accept tile, red band, and NIR band") {
        val result = functions.rst_ndvi(col("tile"), col("red"), col("nir"))
        result should not be null
    }

    test("rst_rastertoworldcoord should accept tile, pixelX, and pixelY") {
        val result = functions.rst_rastertoworldcoord(col("tile"), col("x"), col("y"))
        result should not be null
    }

    test("rst_rastertoworldcoordx should accept tile, pixelX, and pixelY") {
        val result = functions.rst_rastertoworldcoordx(col("tile"), col("x"), col("y"))
        result should not be null
    }

    test("rst_rastertoworldcoordy should accept tile, pixelX, and pixelY") {
        val result = functions.rst_rastertoworldcoordy(col("tile"), col("x"), col("y"))
        result should not be null
    }

    test("rst_transform should accept tile and target SRID") {
        val result = functions.rst_transform(col("tile"), col("srid"))
        result should not be null
    }

    test("rst_tryopen should create Column") {
        val result = functions.rst_tryopen(col("tile"))
        result should not be null
    }

    test("rst_updatetype should accept tile and new type") {
        val result = functions.rst_updatetype(col("tile"), col("type"))
        result should not be null
    }

    test("rst_worldtorastercoord should accept tile, worldX, and worldY") {
        val result = functions.rst_worldtorastercoord(col("tile"), col("x"), col("y"))
        result should not be null
    }

    test("rst_worldtorastercoordx should accept tile, worldX, and worldY") {
        val result = functions.rst_worldtorastercoordx(col("tile"), col("x"), col("y"))
        result should not be null
    }

    test("rst_worldtorastercoordy should accept tile, worldX, and worldY") {
        val result = functions.rst_worldtorastercoordy(col("tile"), col("x"), col("y"))
        result should not be null
    }

    // ====== Registration Tests ======

    test("register should accept SparkSession without error") {
        val spark = SparkSession.builder().master("local[1]").appName("RasterXFunctionsTest").getOrCreate()
        try {
            // Should not throw regardless of registration state
            noException should be thrownBy functions.register(spark)
        } finally {
            spark.stop()
        }
    }

    test("register should be idempotent (no error on multiple calls)") {
        val spark = SparkSession.builder().master("local[1]").appName("RasterXFunctionsTest2").getOrCreate()
        try {
            // Multiple registrations should not throw
            noException should be thrownBy functions.register(spark)
            noException should be thrownBy functions.register(spark)
            noException should be thrownBy functions.register(spark)
        } finally {
            spark.stop()
        }
    }

}
