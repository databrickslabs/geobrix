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

}
