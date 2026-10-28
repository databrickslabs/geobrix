package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.RST_TryOpen
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
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
        meta("last_error") should include("test error") // the key the light tier and the manager use
        result.isNullAt(V2Tile.idx("raster")) shouldBe true // the one error-tile form: a NULL raster
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
        // Generators declare one `tile` column: the row wraps the error tile, like a success-path row.
        seq.head.numFields shouldBe 1
        val meta = getMetadata(seq.head.getStruct(0, 9))
        meta should contain key "error_message"
        meta("error_message") should include("gen error")
    }

    test("safeEval (generator) error tile carries the input tile's cellid") {
        // The tile holds corrupt raster bytes, so it cannot be opened; the cellid must still be taken from
        // the row without opening the raster.
        val tileRow = new GenericInternalRow(Array[Any](42L, Array[Byte](1, 2, 3, 4), emptyMetadataMapData))
        val throwingEval: () => IterableOnce[InternalRow] = () => throw new RuntimeException("gen error")
        val errorTile = RST_ErrorHandler.safeEval(throwingEval, tileRow, BinaryType).toSeq.head.getStruct(0, 9)
        errorTile.getLong(0) shouldBe 42L
    }

    test("safeEval (generator) error tile cellid falls back to -1 when the tile row is unusable") {
        val throwingEval: () => IterableOnce[InternalRow] = () => throw new RuntimeException("gen error")
        val notATile: InternalRow = new GenericInternalRow(Array[Any]("not a tile"))
        val errorTile = RST_ErrorHandler.safeEval(throwingEval, notATile, BinaryType).toSeq.head.getStruct(0, 9)
        errorTile.getLong(0) shouldBe -1L
    }

    test("safeEvalNullElement degrades a data-origin IllegalArgumentException to one NULL row") {
        // A plain IAE raised while reading the tile is a per-row data error, not an argument guard.
        val dataError: () => IterableOnce[InternalRow] = () => throw new IllegalArgumentException("Unrecognized raster tile struct")
        val seq = RST_ErrorHandler.safeEvalNullElement(dataError).toSeq
        seq should have size 1
        seq.head.numFields shouldBe 1
        seq.head.isNullAt(0) shouldBe true
    }

    test("safeEvalNullElement does not swallow fatal errors") {
        intercept[OutOfMemoryError] {
            RST_ErrorHandler.safeEvalNullElement(() => throw new OutOfMemoryError("fatal"))
        }
    }

    test("safeEvalNullElement when eval throws should return one row with a NULL element") {
        val throwingEval: () => IterableOnce[InternalRow] = () => throw new RuntimeException("gen error")
        val seq = RST_ErrorHandler.safeEvalNullElement(throwingEval).toSeq
        seq should have size 1
        seq.head.numFields shouldBe 1
        seq.head.isNullAt(0) shouldBe true
    }

    test("safeEvalNullElement when eval succeeds should return eval result") {
        val oneRow = Seq(minimalRow())
        RST_ErrorHandler.safeEvalNullElement(() => oneRow).toSeq shouldBe oneRow
    }

    test("safeEvalNullElement re-throws a generator guard so its message reaches the caller") {
        val ex = intercept[GeneratorGuardException] {
            RST_ErrorHandler.safeEvalNullElement(() => throw new GeneratorGuardException("rst_xyzpyramid: tile-count exceeds the cap"))
        }
        ex.getMessage should include("tile-count")
    }

    test("safeEvalNullElement VirtualTileException is re-thrown not swallowed as a NULL row") {
        intercept[VirtualTileException] {
            RST_ErrorHandler.safeEvalNullElement(() => throw new VirtualTileException("generator virtual guard"))
        }
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

    // ---- an error tile (NULL raster, no path) as INPUT ------------------------------------------------------------
    // A light error tile looks like this. Reading it used to pass the NULL raster to gdal.FileFromMemBuffer, which
    // kills the JVM; it must not reach GDAL, and the upstream diagnostic must go out unchanged.

    private def upstreamErrorTile: InternalRow = V2Tile.row(
      cellid = 9L,
      metadata = SerializationUtil.toMapData[String, String](Map("last_error" -> "upstream boom"))
    )

    test("a NULL-raster error tile as input is never opened and its diagnostic propagates unchanged") {
        val upstream = upstreamErrorTile
        val (cellId, ds, metadata) = RasterSerializationUtil.rowToTile(upstream, BinaryType)
        (cellId, ds, metadata("last_error")) shouldBe ((9L, null, "upstream boom"))
        RasterSerializationUtil.rowToDS(upstream, BinaryType) shouldBe null
        RasterDriver.readFromBytes(null, Map.empty) shouldBe null
        RasterSerializationUtil.isErrorTile(upstream) shouldBe true
        // the row, array and crash overloads hand the upstream tile on instead of replacing its diagnostic
        val failing: () => InternalRow = () => throw new RuntimeException("later failure")
        RST_ErrorHandler.safeEval(failing, upstream, BinaryType) shouldBe upstream
        RST_ErrorHandler.safeEval(failing, new GenericArrayData(Array(minimalRow(), upstream)), BinaryType) shouldBe upstream
        val crash = new ExpressionConfig(
          Map("spark.databricks.labs.gbx.expressions.crash.on.error" -> "true"),
          new SerializableConfiguration(new org.apache.hadoop.conf.Configuration())
        )
        val wrapped = intercept[Error] {
            RST_ErrorHandler.safeEval(() => throw new RuntimeException("fail"), upstream, BinaryType, UTF8String.fromString(crash.toB64))
        }
        wrapped.getMessage should (include("Cell ID: 9") and include("upstream boom"))
        // an error tile is not openable
        RST_TryOpen.eval(upstream, UTF8String.fromString(crash.toB64), BinaryType) shouldBe false
        // last_error alone on a tile that still has pixels is a warning (the merge aggregates stamp it), not an error tile
        val warned = V2Tile.row(
          cellid = 1L,
          raster = Array[Byte](1, 2, 3),
          metadata = SerializationUtil.toMapData[String, String](Map("last_error" -> "skipped 1 corrupt input tile(s)"))
        )
        RasterSerializationUtil.isErrorTile(warned) shouldBe false
    }

    test("safeEval does not turn a fatal error into an error tile (every overload)") {
        val row = minimalRow()
        val oom = new OutOfMemoryError("fatal")
        val tileEval: () => InternalRow = () => throw oom
        val generatorEval: () => IterableOnce[InternalRow] = () => throw oom
        intercept[OutOfMemoryError](RST_ErrorHandler.safeEval(tileEval, row, BinaryType))
        intercept[OutOfMemoryError](RST_ErrorHandler.safeEval(tileEval, new GenericArrayData(Array(row)), BinaryType))
        intercept[OutOfMemoryError](RST_ErrorHandler.safeEval(generatorEval, row, BinaryType))
        val conf = new ExpressionConfig(
          Map("spark.databricks.labs.gbx.expressions.crash.on.error" -> "false"),
          new SerializableConfiguration(new org.apache.hadoop.conf.Configuration())
        )
        intercept[OutOfMemoryError](RST_ErrorHandler.safeEval(() => throw oom, row, BinaryType, UTF8String.fromString(conf.toB64)))
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
