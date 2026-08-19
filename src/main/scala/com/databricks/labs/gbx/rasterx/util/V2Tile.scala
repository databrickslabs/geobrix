package com.databricks.labs.gbx.rasterx.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
  * Single source for building and reading the heavy-tier v2 raster tile row by field name.
  *
  * All field ordinals are derived from [[RST_ExpressionUtil.v2TileType]] via [[idx]], so a
  * future field reorder requires only a schema edit — no changes to this object's body.
  *
  * [[row]] assembles an [[InternalRow]] with each named value placed at its correct ordinal.
  * The typed getters read back values using the same name-to-ordinal bridge.
  *
  * No literal field indices appear in this file except through [[idx]].
  */
object V2Tile {

    /** The authoritative v2 tile schema (reference to [[RST_ExpressionUtil.v2TileType]]). */
    def schema: StructType = RST_ExpressionUtil.v2TileType

    /** Number of sub-fields in the `window` nested struct (col_off, row_off, width, height). */
    private val windowFieldCount: Int = RST_ExpressionUtil.windowType.fields.length

    /** Ordinal of a named field in the v2 tile schema; fails fast on an unknown name. */
    def idx(name: String): Int = schema.fieldIndex(name)

    /**
      * Build a v2 tile [[InternalRow]] with each value placed at the ordinal for its name.
      * Unspecified fields default to `null`.  The ORDER lives only in the schema, so a
      * future field reorder requires no change to this method body.
      */
    def row(
        cellid: Any = null,
        raster: Any = null,
        path: Any = null,
        window: Any = null,
        clipPolygon: Any = null,
        clipCrs: Any = null,
        crs: Any = null,
        metadata: Any = null,
        pathMode: Any = null
    ): InternalRow = {
        val arr = new Array[Any](schema.length)
        arr(idx("cellid"))       = cellid
        arr(idx("raster"))       = raster
        arr(idx("path"))         = path
        arr(idx("window"))       = window
        arr(idx("clip_polygon")) = clipPolygon
        arr(idx("clip_crs"))     = clipCrs
        arr(idx("crs"))          = crs
        arr(idx("metadata"))     = metadata
        arr(idx("path_mode"))    = pathMode
        new GenericInternalRow(arr)
    }

    /** Null-safe cellid reader; returns `null` (boxed) if the field is null. */
    def getCellId(r: InternalRow): java.lang.Long =
        if (r.isNullAt(idx("cellid"))) null else r.getLong(idx("cellid"))

    /** Raster bytes reader; returns `null` if null. */
    def getRaster(r: InternalRow): Array[Byte] =
        if (r.isNullAt(idx("raster"))) null else r.getBinary(idx("raster"))

    /** Path string reader; returns `null` if null. */
    def getPath(r: InternalRow): UTF8String =
        if (r.isNullAt(idx("path"))) null else r.getUTF8String(idx("path"))

    /** Window sub-struct reader; returns `null` if null. */
    def getWindow(r: InternalRow): InternalRow =
        if (r.isNullAt(idx("window"))) null
        else r.getStruct(idx("window"), windowFieldCount)

    /** Clip polygon bytes reader; returns `null` if null. */
    def getClipPolygon(r: InternalRow): Array[Byte] =
        if (r.isNullAt(idx("clip_polygon"))) null else r.getBinary(idx("clip_polygon"))

    /** Clip CRS string reader; returns `null` if null. */
    def getClipCrs(r: InternalRow): UTF8String =
        if (r.isNullAt(idx("clip_crs"))) null else r.getUTF8String(idx("clip_crs"))

    /** CRS string reader; returns `null` if null. */
    def getCrs(r: InternalRow): UTF8String =
        if (r.isNullAt(idx("crs"))) null else r.getUTF8String(idx("crs"))

    /** Metadata map reader; returns `null` if null. */
    def getMetadata(r: InternalRow): MapData =
        if (r.isNullAt(idx("metadata"))) null else r.getMap(idx("metadata"))

    /** Path-mode string reader; returns `null` if null. */
    def getPathMode(r: InternalRow): UTF8String =
        if (r.isNullAt(idx("path_mode"))) null else r.getUTF8String(idx("path_mode"))

}
