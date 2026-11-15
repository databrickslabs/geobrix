package com.databricks.labs.gbx.expressions

import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.funsuite.AnyFunSuite

/** A row-level expression decodes its config once per JVM, not once per row: a decode deserializes the whole Spark
  * conf and Hadoop `Configuration` (~1 ms), which was 100x the `gbx_st_transformcrs` transform it preceded. */
class ExpressionConfigDecodeOnceTest extends AnyFunSuite {

    private def encoded(app: String): String =
        ExpressionConfig(Map("spark.app.name" -> app), new SerializableConfiguration(new org.apache.hadoop.conf.Configuration())).toB64

    test("fromB64 returns the one decoded instance for an encoding it has seen") {
        val b64 = encoded("decode-once-a")
        val first = ExpressionConfig.fromB64(b64)
        assert(first.configs("spark.app.name") == "decode-once-a")
        assert(ExpressionConfig.fromB64(new String(b64.toCharArray)) eq first)
    }

    test("fromUtf8 decodes by content, and a second literal instance of the same encoding shares the decode") {
        val b64 = encoded("decode-once-b")
        val one = UTF8String.fromString(b64)
        val a = ExpressionConfig.fromUtf8(one)
        assert(ExpressionConfig.fromUtf8(one) eq a)
        assert(ExpressionConfig.fromUtf8(UTF8String.fromString(b64)) eq a)
        val other = ExpressionConfig.fromUtf8(UTF8String.fromString(encoded("decode-once-c")))
        assert(other.configs("spark.app.name") == "decode-once-c")
        assert(ExpressionConfig.fromUtf8(one).configs("spark.app.name") == "decode-once-b")
    }
}
