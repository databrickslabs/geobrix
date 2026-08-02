package com.databricks.labs.gbx.rasterx.expressions.constructor

import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class ConstructorExpressionsTest extends AnyFunSuite {

    // ====== RST_FromContent Expression ======

    test("RST_FromContent should be an Expression") {
        val expr = RST_FromContent(Literal(null), Literal("GTiff"))
        expr shouldBe a[Expression]
    }

    test("RST_FromContent should have three children") {
        val content = Literal(Array.emptyByteArray)
        val driver = Literal("GTiff")
        val expr = RST_FromContent(content, driver)
        expr.children should have length 3 // content, driver, ExpressionConfigExpr
    }

    test("RST_FromContent should have tile dataType") {
        val expr = RST_FromContent(Literal(null), Literal("GTiff"))
        expr.dataType shouldBe a[StructType]
    }

    test("RST_FromContent should be nullable") {
        val expr = RST_FromContent(Literal(null), Literal("GTiff"))
        expr.nullable shouldBe true
    }

    test("RST_FromContent should have correct prettyName") {
        val expr = RST_FromContent(Literal(null), Literal("GTiff"))
        expr.prettyName shouldBe "gbx_rst_fromcontent"
    }

    test("RST_FromContent should create replacement") {
        val expr = RST_FromContent(Literal(null), Literal("GTiff"))
        val replacement = expr.replacement
        replacement should not be null
    }


    // ====== RST_FromContent Object ======

    test("RST_FromContent object should have correct name") {
        RST_FromContent.name shouldBe "gbx_rst_fromcontent"
    }

    test("RST_FromContent object should provide builder") {
        val builder = RST_FromContent.builder()
        builder should not be null
    }

    test("RST_FromContent builder should create expression") {
        val builder = RST_FromContent.builder()
        val expr = builder(Seq(Literal(null), Literal("GTiff")))
        expr shouldBe a[RST_FromContent]
    }

    // NOTE: gbx_rst_fromfile has no Scala expression (RST_FromFile was removed) -- it is a
    // lightweight-only Python UDF (the JVM can't read UC Volumes; see register/#34). Its tests
    // live in the pyrx Python test suite.

    // ====== RST_FromBands Expression ======

    test("RST_FromBands should be an Expression") {
        val arrayType = ArrayType(
          StructType(Array(
            StructField("id", LongType),
            StructField("raster", BinaryType)
          ))
        )
        val expr = RST_FromBands(Literal.create(null, arrayType))
        expr shouldBe a[Expression]
    }

    test("RST_FromBands should have three children") {
        val arrayType = ArrayType(
          StructType(Array(
            StructField("id", LongType),
            StructField("raster", BinaryType)
          ))
        )
        val expr = RST_FromBands(Literal.create(null, arrayType))
        expr.children should have length 3 // bands, ExpressionConfigExpr, elementFieldCountLit
    }

    test("RST_FromBands should have tile dataType") {
        val arrayType = ArrayType(
          StructType(Array(
            StructField("id", LongType),
            StructField("raster", BinaryType)
          ))
        )
        val expr = RST_FromBands(Literal.create(null, arrayType))
        expr.dataType shouldBe a[StructType]
    }

    test("RST_FromBands should be nullable") {
        val arrayType = ArrayType(
          StructType(Array(
            StructField("id", LongType),
            StructField("raster", BinaryType)
          ))
        )
        val expr = RST_FromBands(Literal.create(null, arrayType))
        expr.nullable shouldBe true
    }

    test("RST_FromBands should have correct prettyName") {
        val arrayType = ArrayType(
          StructType(Array(
            StructField("id", LongType),
            StructField("raster", BinaryType)
          ))
        )
        val expr = RST_FromBands(Literal.create(null, arrayType))
        expr.prettyName shouldBe "gbx_rst_frombands"
    }

    test("RST_FromBands should create replacement") {
        val arrayType = ArrayType(
          StructType(Array(
            StructField("id", LongType),
            StructField("raster", BinaryType)
          ))
        )
        val expr = RST_FromBands(Literal.create(null, arrayType))
        val replacement = expr.replacement
        replacement should not be null
    }


    // ====== RST_FromBands Object ======

    test("RST_FromBands object should have correct name") {
        RST_FromBands.name shouldBe "gbx_rst_frombands"
    }

    test("RST_FromBands object should provide builder") {
        val builder = RST_FromBands.builder()
        builder should not be null
    }

    test("RST_FromBands builder should create expression") {
        val arrayType = ArrayType(
          StructType(Array(
            StructField("id", LongType),
            StructField("raster", BinaryType)
          ))
        )
        val builder = RST_FromBands.builder()
        val expr = builder(Seq(Literal.create(null, arrayType)))
        expr shouldBe a[RST_FromBands]
    }

}
