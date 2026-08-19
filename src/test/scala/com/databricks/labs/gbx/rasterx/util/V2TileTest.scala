package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.funsuite.AnyFunSuite

/**
  * Unit tests for [[V2Tile]] named row builder and typed accessors.
  *
  * No GDAL, no Spark session — only Catalyst InternalRow types.
  * Covers: round-trip of set fields, null behavior of unset fields,
  * ordinal consistency with v2TileType, and schema invariants.
  */
class V2TileTest extends AnyFunSuite {

    private val sampleBytes: Array[Byte] = Array[Byte](1, 2, 3, 4)

    private def mapData(entries: (String, String)*) =
        SerializationUtil.toMapData[String, String](Map(entries: _*))

    // ---- schema and ordinals ------------------------------------------------

    test("schema field count is 9") {
        assert(V2Tile.schema.length == 9)
    }

    test("idx for every field name matches v2TileType.fieldIndex") {
        val t = RST_ExpressionUtil.v2TileType
        for (name <- t.fieldNames) {
            assert(V2Tile.idx(name) == t.fieldIndex(name),
                s"V2Tile.idx(\"$name\") diverges from v2TileType.fieldIndex(\"$name\")")
        }
    }

    test("idx(\"metadata\") equals v2TileType fieldIndex(\"metadata\")") {
        val expected = RST_ExpressionUtil.v2TileType.fieldIndex("metadata")
        assert(V2Tile.idx("metadata") == expected)
    }

    // ---- builder and getters ------------------------------------------------

    test("row: numFields equals schema length") {
        val r = V2Tile.row(cellid = 7L, raster = sampleBytes)
        assert(r.numFields == V2Tile.schema.length)
    }

    test("row + getCellId: set field round-trips correctly") {
        val r = V2Tile.row(cellid = 7L)
        assert(V2Tile.getCellId(r) == 7L)
    }

    test("row + getMetadata: set metadata round-trips correctly") {
        val md = mapData("k" -> "v", "x" -> "y")
        val r = V2Tile.row(cellid = 7L, raster = sampleBytes, metadata = md)
        val got = V2Tile.getMetadata(r)
        assert(got != null)
        assert(got.numElements() == 2)
    }

    test("row: unset fields are null (path, window, clip_polygon, clip_crs, crs, path_mode)") {
        val r = V2Tile.row(cellid = 7L, raster = sampleBytes, metadata = mapData("k" -> "v"))
        for (name <- Seq("path", "window", "clip_polygon", "clip_crs", "crs", "path_mode")) {
            assert(r.isNullAt(V2Tile.idx(name)),
                s"Expected null at idx(\"$name\") = ${V2Tile.idx(name)}")
        }
    }

    // ---- typed getters: null cases -------------------------------------------

    test("getCellId returns null when cellid field is null") {
        val arr = new Array[Any](V2Tile.schema.length)
        val r = new GenericInternalRow(arr)
        assert(V2Tile.getCellId(r) == null)
    }

    test("getPath returns null when path not set") {
        val r = V2Tile.row(cellid = 1L)
        assert(V2Tile.getPath(r) == null)
    }

    test("getWindow returns null when window not set") {
        val r = V2Tile.row(cellid = 1L)
        assert(V2Tile.getWindow(r) == null)
    }

    test("getClipPolygon returns null when clip_polygon not set") {
        val r = V2Tile.row(cellid = 1L)
        assert(V2Tile.getClipPolygon(r) == null)
    }

    test("getClipCrs returns null when clip_crs not set") {
        val r = V2Tile.row(cellid = 1L)
        assert(V2Tile.getClipCrs(r) == null)
    }

    test("getCrs returns null when crs not set") {
        val r = V2Tile.row(cellid = 1L)
        assert(V2Tile.getCrs(r) == null)
    }

    test("getMetadata returns null when metadata not set") {
        val r = V2Tile.row(cellid = 1L)
        assert(V2Tile.getMetadata(r) == null)
    }

    test("getPathMode returns null when path_mode not set") {
        val r = V2Tile.row(cellid = 1L)
        assert(V2Tile.getPathMode(r) == null)
    }

    // ---- typed getters: set cases -------------------------------------------

    test("getRaster returns bytes round-trip") {
        val r = V2Tile.row(raster = sampleBytes)
        assert(java.util.Arrays.equals(V2Tile.getRaster(r), sampleBytes))
    }

    test("getPath returns value when path set") {
        val r = V2Tile.row(path = UTF8String.fromString("/Volumes/a/b.tif"))
        assert(V2Tile.getPath(r).toString == "/Volumes/a/b.tif")
    }

    test("getPathMode returns value when set") {
        val r = V2Tile.row(pathMode = UTF8String.fromString("FILE"))
        assert(V2Tile.getPathMode(r).toString == "FILE")
    }

    test("getCrs returns value when set") {
        val r = V2Tile.row(crs = UTF8String.fromString("EPSG:27700"))
        assert(V2Tile.getCrs(r).toString == "EPSG:27700")
    }

}
