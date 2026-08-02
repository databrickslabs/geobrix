package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
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
    /** Minimal v1 (3-field) tile row: (cellid, raster, metadata). */
    private def minimalRow(cellId: Long = 1L): InternalRow = {
        new GenericInternalRow(Array[Any](cellId, Array.emptyByteArray, emptyMetadataMapData))
    }

    /** Minimal v2 (8-field) tile row: (cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata). */
    private def minimalRowV2(cellId: Long = 1L): InternalRow = {
        new GenericInternalRow(Array[Any](
            cellId,
            Array.emptyByteArray, // raster — position 1
            null,                  // path
            null,                  // window
            null,                  // clip_polygon
            null,                  // clip_crs
            null,                  // crs
            emptyMetadataMapData   // metadata — position 7
        ))
    }

    private def getMetadata(row: InternalRow): Map[String, String] = {
        // metadata is at position 7 in the v2 8-field tile schema
        SerializationUtil.createMap[String, String](row.getMap(7))
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

    // ---- v2 (8-field) array-of-tiles scan ----------------------------------

    test("safeEval (ArrayData, v2 8-field) when eval succeeds should return eval result without truncation") {
        // Regression: getStruct(i, 3) on an 8-field row reads only the first 3 fields,
        // misreading metadata (field 7) as field 2 (path). With elementFieldCount=8 this is fixed.
        val v2row = minimalRowV2()
        val rows  = new GenericArrayData(Array(v2row))
        val sentinel = minimalRow()  // distinct result to confirm eval() was called
        val result = RST_ErrorHandler.safeEval(() => sentinel, rows, BinaryType, elementFieldCount = 8)
        result shouldBe sentinel
    }

    test("safeEval (ArrayData, v2 8-field) when eval throws should scan 8-field elements without crash") {
        // Ensures the error-scan path reads each element correctly at the 8-field stride.
        val v2row = minimalRowV2()
        val rows  = new GenericArrayData(Array(v2row))
        val throwingEval: () => InternalRow = () => throw new RuntimeException("v2 array eval fail")
        val result = RST_ErrorHandler.safeEval(throwingEval, rows, BinaryType, elementFieldCount = 8)
        result should not be null
        val meta = getMetadata(result)
        meta should contain key "error_message"
        meta("error_message") should include("v2 array eval fail")
    }
}
