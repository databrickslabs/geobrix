package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.rasterx.expressions.agg.{RST_BNG_RasterizeAgg, RST_H3_RasterizeAgg, RST_Quadbin_RasterizeAgg, RST_RasterizeAgg}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.types.{BinaryType, LongType, MapType, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

/**
  * Unit tests for layout-aware deserialization in [[RasterSerializationUtil]].
  *
  * Covers:
  *  - v1 (3-field) binary tile: metadata read at position 2
  *  - v2 (8-field) materialized tile: metadata read at position 7, not 2
  *  - v2 virtual tile (raster null, path set): guard throws with actionable message
  *  - unrecognized field count: clear error mentioning the count
  */
class RasterSerializationV2Test extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    // ---- helpers ------------------------------------------------------------

    /** Create a minimal 4x4 single-band GeoTIFF in /vsimem; return its bytes. */
    private def tinyGeotiff(): Array[Byte] = {
        val path = s"/vsimem/ser_v2_test_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv = gdal.GetDriverByName("GTiff")
        val ds = drv.Create(path, 4, 4, 1, gdalconstConstants.GDT_Float32)
        ds.SetGeoTransform(Array[Double](0.0, 1.0, 0.0, 4.0, 0.0, -1.0))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).Fill(42.0)
        ds.FlushCache()
        val bytes = RasterDriver.writeToBytes(ds, Map.empty)
        ds.delete()
        gdal.Unlink(path)
        bytes
    }

    private def emptyMap: MapData =
        ArrayBasedMapData(Array.empty[UTF8String], Array.empty[UTF8String])

    /** Build MapData from a Scala String->String map. */
    private def toMapData(m: Map[String, String]): MapData =
        SerializationUtil.toMapData[String, String](m)

    private def v1BinaryRow(cellid: Long, bytes: Array[Byte]): InternalRow =
        new GenericInternalRow(Array[Any](cellid, bytes, emptyMap))

    private def v2MaterializedRow(cellid: Long, bytes: Array[Byte], md: MapData): InternalRow =
        new GenericInternalRow(Array[Any](cellid, bytes, null, null, null, null, null, md))

    private def v2VirtualRow(cellid: Long, path: String): InternalRow =
        new GenericInternalRow(Array[Any](cellid, null, UTF8String.fromString(path), null, null, null, null, emptyMap))

    /** Open a minimal in-memory GeoTIFF as a live Dataset (caller must releaseDataset). */
    private def openTinyGeotiff(): org.gdal.gdal.Dataset = {
        RasterDriver.readFromBytes(tinyGeotiff(), Map.empty)
    }

    private val hconf: org.apache.spark.util.SerializableConfiguration =
        new org.apache.spark.util.SerializableConfiguration(new org.apache.hadoop.conf.Configuration())

    // ---- tests --------------------------------------------------------------

    test("rowToTile reads a v1 (3-field) binary tile") {
        val (cell, ds, _) = RasterSerializationUtil.rowToTile(v1BinaryRow(7L, tinyGeotiff()), BinaryType)
        assert(cell == 7L)
        assert(ds.GetRasterXSize() > 0)
        RasterDriver.releaseDataset(ds)
    }

    test("rowToTile reads a v2 (8-field) materialized tile; metadata at position 7") {
        val md = toMapData(Map("k" -> "v"))
        val (cell, ds, meta) = RasterSerializationUtil.rowToTile(
            v2MaterializedRow(9L, tinyGeotiff(), md), BinaryType)
        assert(cell == 9L)
        assert(meta.get("k").contains("v"), s"Expected metadata key 'k'='v' from position 7, got: $meta")
        RasterDriver.releaseDataset(ds)
    }

    test("rowToTile on a VIRTUAL v2 tile throws the materialize-first guard") {
        val ex = intercept[IllegalArgumentException](
            RasterSerializationUtil.rowToTile(v2VirtualRow(1L, "/Volumes/x/y.tif"), BinaryType))
        assert(ex.getMessage.contains("virtual tile"),
            s"Expected 'virtual tile' in message: ${ex.getMessage}")
        assert(ex.getMessage.toLowerCase.contains("materialize"),
            s"Expected 'materialize' in message: ${ex.getMessage}")
        assert(ex.getMessage.toLowerCase.contains("lightweight"),
            s"Expected 'lightweight' in message: ${ex.getMessage}")
    }

    test("rowToTile on an unrecognized field count throws a clear error") {
        val ex = intercept[IllegalArgumentException](
            RasterSerializationUtil.rowToTile(
                new GenericInternalRow(Array[Any](1L, Array.emptyByteArray)), BinaryType))
        assert(ex.getMessage.contains("2"),
            s"Expected field count '2' in message: ${ex.getMessage}")
    }

    test("v2TileType matches the light V2 schema exactly") {
        val t = RST_ExpressionUtil.v2TileType
        assert(t.fieldNames.toSeq == Seq("cellid", "raster", "path", "window", "clip_polygon", "clip_crs", "crs", "metadata"))
        assert(t("cellid").dataType == LongType && !t("cellid").nullable)
        assert(t("raster").dataType == BinaryType && t("raster").nullable)
        val w = t("window").dataType.asInstanceOf[org.apache.spark.sql.types.StructType]
        assert(w.fieldNames.toSeq == Seq("col_off", "row_off", "width", "height"))
        assert(t("metadata").dataType == MapType(StringType, StringType))
    }

    test("tileToRow emits an 8-field v2 materialized row with null pedigree") {
        val ds = openTinyGeotiff()
        val row = RasterSerializationUtil.tileToRow((5L, ds, Map("d" -> "GTiff")), BinaryType, hconf)
        assert(row.numFields == 8 && row.getLong(0) == 5L && !row.isNullAt(1))
        assert(row.isNullAt(2) && row.isNullAt(3) && row.isNullAt(4) && row.isNullAt(5) && row.isNullAt(6))
        assert(!row.isNullAt(7))
        RasterDriver.releaseDataset(ds)
    }

    /** Smoke-test: each rasterize aggregator's eval output MUST have the same
     *  field count as its declared dataType.  Catches the 3-vs-8 mismatch
     *  WITHOUT needing a SparkSession / UnsafeProjection.
     *
     *  For RST_RasterizeAgg we supply real geometry so eval() can actually rasterize.
     *  For the three grid aggregators (BNG/H3/Quadbin) we verify dataType size only
     *  (their eval() requires large geometry inputs and GDAL state not available here).
     */
    test("rasterize aggregator eval row numFields matches dataType field count") {
        // --- Helper: assert dataType declares 8 fields ---
        def assertDataType8(name: String, dt: org.apache.spark.sql.types.DataType): Unit = {
            val n = dt.asInstanceOf[StructType].size
            assert(n == 8, s"$name.dataType has $n fields, expected 8")
        }

        assertDataType8("RST_RasterizeAgg",
            RST_RasterizeAgg(
                geomWkbExpr  = Literal.create(null, BinaryType),
                valueExpr    = Literal(0.0),
                xminExpr     = Literal(0.0),
                yminExpr     = Literal(0.0),
                xmaxExpr     = Literal(100.0),
                ymaxExpr     = Literal(100.0),
                widthPxExpr  = Literal(100),
                heightPxExpr = Literal(100),
                outSridExpr  = Literal(32633)
            ).dataType)

        // Grid aggregator signature: cellId, value, srid, pixelSize, xmin, ymin, xmax, ymax,
        //                              width, height, mode, kringPad  (12 positional args)
        assertDataType8("RST_BNG_RasterizeAgg",
            RST_BNG_RasterizeAgg(
                cellIdExpr    = Literal.create(null, StringType),
                valueExpr     = Literal(0.0),
                outSridExpr   = Literal(27700),
                pixelSizeExpr = Literal(1.0),
                xminExpr      = Literal(0.0),
                yminExpr      = Literal(0.0),
                xmaxExpr      = Literal(100.0),
                ymaxExpr      = Literal(100.0),
                widthExpr     = Literal(100),
                heightExpr    = Literal(100),
                modeExpr      = Literal("sum"),
                kringPadExpr  = Literal(0)
            ).dataType)

        assertDataType8("RST_H3_RasterizeAgg",
            RST_H3_RasterizeAgg(
                cellIdExpr    = Literal.create(null, StringType),
                valueExpr     = Literal(0.0),
                outSridExpr   = Literal(4326),
                pixelSizeExpr = Literal(1.0),
                xminExpr      = Literal(0.0),
                yminExpr      = Literal(0.0),
                xmaxExpr      = Literal(100.0),
                ymaxExpr      = Literal(100.0),
                widthExpr     = Literal(100),
                heightExpr    = Literal(100),
                modeExpr      = Literal("sum"),
                kringPadExpr  = Literal(0)
            ).dataType)

        assertDataType8("RST_Quadbin_RasterizeAgg",
            RST_Quadbin_RasterizeAgg(
                cellIdExpr    = Literal.create(null, StringType),
                valueExpr     = Literal(0.0),
                outSridExpr   = Literal(4326),
                pixelSizeExpr = Literal(1.0),
                xminExpr      = Literal(0.0),
                yminExpr      = Literal(0.0),
                xmaxExpr      = Literal(100.0),
                ymaxExpr      = Literal(100.0),
                widthExpr     = Literal(100),
                heightExpr    = Literal(100),
                modeExpr      = Literal("sum"),
                kringPadExpr  = Literal(0)
            ).dataType)

        // --- Also exercise RST_RasterizeAgg.eval and confirm the row shape ---
        import com.databricks.labs.gbx.expressions.ExpressionConfig
        import org.apache.spark.util.SerializableConfiguration
        import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
        import com.databricks.labs.gbx.vectorx.jts.JTS

        val gf = new GeometryFactory()
        val poly = gf.createPolygon(Array(
            new Coordinate(0.0, 0.0), new Coordinate(50.0, 0.0),
            new Coordinate(50.0, 50.0), new Coordinate(0.0, 50.0),
            new Coordinate(0.0, 0.0)))
        val wkb = JTS.toWKB(poly)

        val cfg = new ExpressionConfig(
            Map.empty[String, String],
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration()))
        val baos = new java.io.ByteArrayOutputStream()
        val oos = new java.io.ObjectOutputStream(baos); oos.writeObject(cfg); oos.close()
        val encodedCfg = UTF8String.fromString(java.util.Base64.getEncoder.encodeToString(baos.toByteArray))

        val agg = RST_RasterizeAgg(
            Literal.create(null, BinaryType), Literal(0.0),
            Literal(0.0), Literal(0.0), Literal(100.0), Literal(100.0),
            Literal(100), Literal(100), Literal(32633),
            Literal.create(encodedCfg, StringType))
        val buf = agg.createAggregationBuffer()
        agg.update(buf, wkb, 5.0)
        val emittedRow = agg.eval(buf).asInstanceOf[InternalRow]
        val declaredSize = agg.dataType.asInstanceOf[StructType].size
        assert(emittedRow.numFields == declaredSize,
            s"RST_RasterizeAgg.eval row has ${emittedRow.numFields} fields but dataType declares $declaredSize")
        assert(emittedRow.numFields == 8, s"Expected 8 fields, got ${emittedRow.numFields}")
    }

}
