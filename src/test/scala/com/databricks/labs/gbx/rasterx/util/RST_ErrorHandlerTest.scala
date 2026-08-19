package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types.BinaryType

import scala.collection.IterableOnce
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._



/** Tests for RST_ErrorHandler safeEval overloads (error row creation, crashExpressions, generator). */
class RST_ErrorHandlerTest extends AnyFunSuite {

    private def emptyMetadataMapData = SerializationUtil.toMapData[String, String](Map.empty[String, String])

    /** Minimal tile row (BinaryType) that will cause rowToTile to use getOrElse in catch block (no GDAL). */
    /** Minimal v1 (3-field) tile row: (cellid, raster, metadata). */
    private def minimalRow(cellId: Long = 1L): InternalRow = {
        new GenericInternalRow(Array[Any](cellId, Array.emptyByteArray, emptyMetadataMapData))
    }

    private def getMetadata(row: InternalRow): Map[String, String] = {
        // safeEval error rows are canonical 9-field v2 rows; metadata is the LAST field (position 8).
        SerializationUtil.createMap[String, String](row.getMap(8))
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

    // ---- VirtualTileException propagation through ALL four overloads --------

    test("safeEval (InternalRow) VirtualTileException is re-thrown not swallowed") {
        val row = minimalRow()
        val ex = intercept[VirtualTileException] {
            val eval: () => InternalRow = () => throw new VirtualTileException("tile-returning virtual guard")
            RST_ErrorHandler.safeEval(eval, row, BinaryType)
        }
        ex.getMessage should include("virtual guard")
    }

    test("safeEval (ArrayData) VirtualTileException is re-thrown not swallowed") {
        val rows = new GenericArrayData(Array(minimalRow()))
        intercept[VirtualTileException] {
            val eval: () => InternalRow = () => throw new VirtualTileException("array virtual guard")
            RST_ErrorHandler.safeEval(eval, rows, BinaryType)
        }
    }

    test("safeEval (Any, conf) VirtualTileException is re-thrown regardless of crashExpressions") {
        val row = minimalRow()
        // Test with crashExpressions=false (the non-crash mode that would otherwise return null)
        val conf = new ExpressionConfig(
            Map("spark.databricks.labs.gbx.expressions.crash.on.error" -> "false"),
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration())
        )
        val confB64 = UTF8String.fromString(conf.toB64)
        intercept[VirtualTileException] {
            RST_ErrorHandler.safeEval(
                () => throw new VirtualTileException("scalar virtual guard"),
                row, BinaryType, confB64)
        }
    }

    test("safeEval (generator) VirtualTileException is re-thrown not swallowed as error row") {
        val row = minimalRow()
        intercept[VirtualTileException] {
            val eval: () => IterableOnce[InternalRow] = () => throw new VirtualTileException("generator virtual guard")
            RST_ErrorHandler.safeEval(eval, row, BinaryType)
        }
    }

    test("safeEval (Any, conf) plain IAE returns null in non-crash mode (F1 regression guard)") {
        // Proves the broad IAE re-throw from Task 9 has been reverted: ordinary IAEs are swallowed.
        val row = minimalRow()
        val conf = new ExpressionConfig(
            Map("spark.databricks.labs.gbx.expressions.crash.on.error" -> "false"),
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration())
        )
        val confB64 = UTF8String.fromString(conf.toB64)
        val result = RST_ErrorHandler.safeEval(
            () => throw new IllegalArgumentException("bad epsg or non-point geom"),
            row, BinaryType, confB64)
        assert(result == null, "ordinary IAE must be swallowed to null (not re-thrown) in non-crash mode")
    }
}
