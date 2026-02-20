package com.databricks.labs.gbx.vectorx.jts.legacy

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class LegacyFunctionsTest extends AnyFunSuite {

    // ====== Structure Tests ======

    test("functions object should be Serializable") {
        functions shouldBe a[Serializable]
    }

    test("flag should have correct value") {
        functions.flag shouldBe "com.databricks.labs.gbx.vectorx.jts.legacy.registered"
    }

    // ====== Function Tests ======

    test("st_legacyaswkb should create Column") {
        val result = functions.st_legacyaswkb(col("geom"))
        result should not be null
        result.getClass.getName should include("Column")
    }

    // ====== Registration Tests ======

    test("register should accept SparkSession without error") {
        val spark = SparkSession.builder().master("local[1]").appName("LegacyFunctionsTest").getOrCreate()
        try {
            noException should be thrownBy functions.register(spark)
        } finally {
            spark.stop()
        }
    }

    test("register should be idempotent (no error on multiple calls)") {
        val spark = SparkSession.builder().master("local[1]").appName("LegacyFunctionsTest2").getOrCreate()
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
