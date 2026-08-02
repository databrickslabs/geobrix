package com.databricks.labs.gbx.rasterx.util

import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.util.SerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, MapData}
import org.apache.spark.sql.types.{BinaryType, StringType}
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

}
