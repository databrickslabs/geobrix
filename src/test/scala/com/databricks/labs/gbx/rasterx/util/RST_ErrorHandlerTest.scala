package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.{BinaryType, DataType}

import scala.collection.IterableOnce
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/** Tests for RST_ErrorHandler safeEval overloads (error row creation, crashExpressions, generator). */
class RST_ErrorHandlerTest extends AnyFunSuite {

    private def emptyMetadataMapData = SerializationUtil.toMapData[String, String](Map.empty[String, String])

    /** Minimal tile row (BinaryType) that will cause rowToTile to use getOrElse in catch block (no GDAL). */
    private def minimalRow(cellId: Long = 1L): InternalRow = {
        new GenericInternalRow(Array[Any](cellId, Array.emptyByteArray, emptyMetadataMapData))
    }

    private def getMetadata(row: InternalRow): Map[String, String] = {
        SerializationUtil.createMap[String, String](row.getMap(2))
    }

    test("safeEval (InternalRow) when eval succeeds should return eval result") {
        val row = minimalRow()
        val result = RST_ErrorHandler.safeEval(() => row, row, BinaryType)
        result shouldBe row
    }

    test("safeEval (InternalRow) when eval throws should return row with error metadata") {
        val row = minimalRow()
        val throwingEval: () => InternalRow = () => throw new RuntimeException("test error")
        val result = RST_ErrorHandler.safeEval(throwingEval, row, BinaryType)
        result should not be null
        val meta = getMetadata(result)
        meta should contain key "error_message"
        meta("error_message") should include("test error")
    }

    test("safeEval (Any, conf) when eval throws and crashExpressions false should return null") {
        val row = minimalRow()
        val conf = new ExpressionConfig(
            Map("spark.databricks.labs.gbx.expressions.crash.on.error" -> "false"),
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration())
        )
        val confB64 = UTF8String.fromString(conf.toB64)
        val throwingEval: () => Any = () => throw new RuntimeException("fail")
        val result = RST_ErrorHandler.safeEval(throwingEval, row, BinaryType, confB64)
        assert(result == null)
    }

    test("safeEval (Any, conf) when eval throws and crashExpressions true should throw Error") {
        val row = minimalRow()
        val conf = new ExpressionConfig(
            Map("spark.databricks.labs.gbx.expressions.crash.on.error" -> "true"),
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration())
        )
        val confB64 = UTF8String.fromString(conf.toB64)
        assertThrows[Error] {
            RST_ErrorHandler.safeEval(
                () => throw new RuntimeException("fail"),
                row,
                BinaryType,
                confB64
            )
        }
    }

    test("safeEval (generator) when eval throws should return single row with error metadata") {
        val row = minimalRow()
        val throwingEval: () => IterableOnce[InternalRow] = () => throw new RuntimeException("gen error")
        val result = RST_ErrorHandler.safeEval(throwingEval, row, BinaryType)
        val seq = result.toSeq
        seq should have size 1
        val meta = getMetadata(seq.head)
        meta should contain key "error_message"
        meta("error_message") should include("gen error")
    }

    test("safeEval (generator) when eval succeeds should return eval result") {
        val row = minimalRow()
        val oneRow = Seq(row)
        val result = RST_ErrorHandler.safeEval(() => oneRow, row, BinaryType)
        result.toSeq shouldBe oneRow
    }

    test("safeEval (ArrayData) when eval throws and no input has error should return row with error metadata") {
        val rows = new GenericArrayData(Array(minimalRow()))
        val throwingEval: () => InternalRow = () => throw new RuntimeException("array eval fail")
        val result = RST_ErrorHandler.safeEval(throwingEval, rows, BinaryType)
        result should not be null
        val meta = getMetadata(result)
        meta should contain key "error_message"
        meta("error_message") should include("array eval fail")
    }
}
