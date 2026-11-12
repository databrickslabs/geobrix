package com.databricks.labs.gbx.rasterx

import com.databricks.labs.gbx.kernels.TestL1
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class RasterXFunctionsTest extends AnyFunSuite {

    // ====== Structure Tests ======

    test("functions object should be Serializable") {
        functions shouldBe a[Serializable]
    }

    // ====== Accessor Functions ======

    test("rst_boundingbox should create Column") {
        val result = functions.rst_boundingbox(col("tile"))
        result should not be null
    }

    test("rst_numbands should create Column") {
        val result = functions.rst_numbands(col("tile"))
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

    // ====== Constructor Functions ======

    test("rst_fromcontent should accept content and driver") {
        val result = functions.rst_fromcontent(col("content"), col("driver"))
        result should not be null
    }

    // ====== Operation Functions ======

    test("rst_rastertoworldcoordx should accept tile, pixelX, and pixelY") {
        val result = functions.rst_rastertoworldcoordx(col("tile"), col("x"), col("y"))
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

    // ====== Scalar Literal Overloads ======

    test("scalar-literal overloads should accept plain values for non-Column args") {
        // int scalars
        functions.rst_rastertoworldcoordx(col("tile"), 1, 2) should not be null
        // double scalars
        functions.rst_worldtorastercoordx(col("tile"), 1.0, 2.0) should not be null
    }

    // ====== Registration Tests ======

    test("TestL1.installAll should accept a SparkSession without error") {
        val spark = SparkSession.builder().master("local[1]").appName("RasterXFunctionsTest").getOrCreate()
        try {
            // Should not throw regardless of registration state
            noException should be thrownBy TestL1.installAll(spark)
        } finally {
            spark.stop()
        }
    }

    test("TestL1.installAll should be idempotent (no error on multiple calls)") {
        val spark = SparkSession.builder().master("local[1]").appName("RasterXFunctionsTest2").getOrCreate()
        try {
            // Multiple registrations should not throw
            noException should be thrownBy TestL1.installAll(spark)
            noException should be thrownBy TestL1.installAll(spark)
            noException should be thrownBy TestL1.installAll(spark)
        } finally {
            spark.stop()
        }
    }

}
