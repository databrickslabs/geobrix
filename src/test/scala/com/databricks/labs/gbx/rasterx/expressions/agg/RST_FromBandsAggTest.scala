package com.databricks.labs.gbx.rasterx.expressions.agg

import com.databricks.labs.gbx.expressions.ExpressionConfig
import com.databricks.labs.gbx.rasterx.expressions.constructor.RST_FromBands
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.RasterSerializationUtil
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}

import java.nio.file.Files

/** Direct-execute tests for [[RST_FromBandsAgg]].
 *
 *  We construct the aggregator with Literal children and drive update/merge/eval
 *  directly -- no Spark session required.
 *
 *  Three 4x4 single-band GeoTIFF tiles are created in /vsimem, each filled with
 *  a distinct constant (band A=10, band B=20, band C=30). They are inserted into
 *  the buffer in SHUFFLED order: (tileC, idx=3), (tileA, idx=1), (tileB, idx=2).
 *  After eval, the output tile must have 3 bands where band 1=10, band 2=20,
 *  band 3=30 -- proving sort-by-band_index regardless of insertion order.
 */
class RST_FromBandsAggTest extends AnyFunSuite with BeforeAndAfterAll {

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
        import com.databricks.labs.gbx.util.NodeFilePathUtil
        Files.createDirectories(NodeFilePathUtil.rootPath)
    }

    // ---- ExpressionConfig helper --------------------------------------------

    private def encodedEmpty(): UTF8String = {
        val cfg = new ExpressionConfig(
            Map.empty[String, String],
            new SerializableConfiguration(new org.apache.hadoop.conf.Configuration()))
        val baos = new java.io.ByteArrayOutputStream()
        val oos  = new java.io.ObjectOutputStream(baos)
        oos.writeObject(cfg); oos.close()
        UTF8String.fromString(java.util.Base64.getEncoder.encodeToString(baos.toByteArray))
    }

    // ---- tile creation helper -----------------------------------------------

    /** Create a 4x4 single-band GeoTIFF in /vsimem filled with a constant value.
     *  Returns InternalRow (cellid, raster_bytes, metadata).
     */
    private def makeSingleBandTileRow(tag: String, fillValue: Int): InternalRow = {
        val path = s"/vsimem/frombands_agg_test_$tag.tif"
        val drv = gdal.GetDriverByName("GTiff")
        val ds = drv.Create(path, 4, 4, 1, gdalconstConstants.GDT_Float32)
        ds.SetGeoTransform(Array[Double](0.0, 1.0, 0.0, 4.0, 0.0, -1.0))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        val band = ds.GetRasterBand(1)
        band.Fill(fillValue.toDouble)
        band.FlushCache()
        ds.FlushCache()
        val bytes = RasterDriver.writeToBytes(ds, Map.empty)
        ds.delete()
        gdal.Unlink(path)

        InternalRow.fromSeq(Seq(
            1L,                                       // cellid
            bytes,                                    // raster (BinaryType)
            org.apache.spark.sql.catalyst.util.ArrayBasedMapData(
                Array.empty[UTF8String],
                Array.empty[UTF8String]
            )                                         // metadata (empty map)
        ))
    }

    // ---- agg factory --------------------------------------------------------

    private val v1TileType = org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("cellid",   org.apache.spark.sql.types.LongType,   nullable = false),
        org.apache.spark.sql.types.StructField("raster",   BinaryType,                            nullable = false),
        org.apache.spark.sql.types.StructField("metadata", org.apache.spark.sql.types.MapType(
            org.apache.spark.sql.types.StringType, org.apache.spark.sql.types.StringType),        nullable = true)
    ))

    private val v2TileType = org.apache.spark.sql.types.StructType(Seq(
        org.apache.spark.sql.types.StructField("cellid",       org.apache.spark.sql.types.LongType,   nullable = false),
        org.apache.spark.sql.types.StructField("raster",       BinaryType,                            nullable = true),
        org.apache.spark.sql.types.StructField("path",         org.apache.spark.sql.types.StringType, nullable = true),
        org.apache.spark.sql.types.StructField("window",       org.apache.spark.sql.types.StringType, nullable = true),
        org.apache.spark.sql.types.StructField("clip_polygon", BinaryType,                            nullable = true),
        org.apache.spark.sql.types.StructField("clip_crs",     org.apache.spark.sql.types.StringType, nullable = true),
        org.apache.spark.sql.types.StructField("crs",          org.apache.spark.sql.types.StringType, nullable = true),
        org.apache.spark.sql.types.StructField("metadata",     org.apache.spark.sql.types.MapType(
            org.apache.spark.sql.types.StringType, org.apache.spark.sql.types.StringType),        nullable = true)
    ))

    private def makeAgg(): RST_FromBandsAgg = makeAggForTileType(v1TileType)

    private def makeAggForTileType(tileType: org.apache.spark.sql.types.StructType): RST_FromBandsAgg =
        RST_FromBandsAgg(
            tile      = Literal.create(null, tileType),
            bandIndexExpr = Literal(0),
            exprConfExpr  = Literal.create(encodedEmpty(), StringType)
        )

    /** Create a v2 (8-field) tile row from raster bytes; fields 2-6 are null, metadata at position 7. */
    private def makeSingleBandTileRowV2(tag: String, fillValue: Int): InternalRow = {
        val path = s"/vsimem/frombands_agg_v2_test_$tag.tif"
        val drv = gdal.GetDriverByName("GTiff")
        val ds = drv.Create(path, 4, 4, 1, gdalconstConstants.GDT_Float32)
        ds.SetGeoTransform(Array[Double](0.0, 1.0, 0.0, 4.0, 0.0, -1.0))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        val band = ds.GetRasterBand(1)
        band.Fill(fillValue.toDouble)
        band.FlushCache()
        ds.FlushCache()
        val bytes = RasterDriver.writeToBytes(ds, Map.empty)
        ds.delete()
        gdal.Unlink(path)

        val emptyMeta = ArrayBasedMapData(Array.empty[UTF8String], Array.empty[UTF8String])
        // v2 layout: cellid, raster, path, window, clip_polygon, clip_crs, crs, metadata
        InternalRow.fromSeq(Seq(
            1L,        // cellid
            bytes,     // raster (BinaryType)  — position 1
            null,      // path
            null,      // window
            null,      // clip_polygon
            null,      // clip_crs
            null,      // crs
            emptyMeta  // metadata — position 7
        ))
    }

    // ---- pixel readback helper ----------------------------------------------

    /** Read the mean value of all pixels in the given band (1-based) from a tile InternalRow. */
    private def readBandMean(tileRow: Any, bandNum: Int): Double = {
        val ir    = tileRow.asInstanceOf[InternalRow]
        val bytes = ir.getBinary(1)
        bytes should not be null
        val tmp = s"/vsimem/frombands_agg_verify_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(tmp, bytes)
        val ds = gdal.Open(tmp)
        try {
            val w = ds.GetRasterXSize
            val h = ds.GetRasterYSize
            val buf = new Array[Double](w * h)
            ds.GetRasterBand(bandNum).ReadRaster(0, 0, w, h, gdalconstConstants.GDT_Float64, buf)
            buf.sum / buf.length
        } finally {
            ds.delete()
            gdal.Unlink(tmp)
        }
    }

    private def readBandCount(tileRow: Any): Int = {
        val ir    = tileRow.asInstanceOf[InternalRow]
        val bytes = ir.getBinary(1)
        bytes should not be null
        val tmp = s"/vsimem/frombands_agg_count_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(tmp, bytes)
        val ds = gdal.Open(tmp)
        try { ds.GetRasterCount() } finally { ds.delete(); gdal.Unlink(tmp) }
    }

    // ---- tests --------------------------------------------------------------

    test("band-order correctness: shuffled insertion sorted by band_index") {
        val tileA = makeSingleBandTileRow("A", 10)
        val tileB = makeSingleBandTileRow("B", 20)
        val tileC = makeSingleBandTileRow("C", 30)

        val agg = makeAgg()
        val buf = agg.createAggregationBuffer()

        // SHUFFLED: insert C(idx=3), A(idx=1), B(idx=2)
        agg.updateWithIndex(buf, tileC, 3)
        agg.updateWithIndex(buf, tileA, 1)
        agg.updateWithIndex(buf, tileB, 2)

        val result = agg.eval(buf).asInstanceOf[InternalRow]
        result should not be null

        val bandCount = readBandCount(result)
        bandCount shouldBe 3

        // After sort by band_index: band1=A(10), band2=B(20), band3=C(30)
        readBandMean(result, 1) shouldBe 10.0 +- 0.5
        readBandMean(result, 2) shouldBe 20.0 +- 0.5
        readBandMean(result, 3) shouldBe 30.0 +- 0.5
    }

    test("merge then eval: partial buffers merged in arbitrary order produce correct band order") {
        val tileA = makeSingleBandTileRow("mA", 10)
        val tileB = makeSingleBandTileRow("mB", 20)
        val tileC = makeSingleBandTileRow("mC", 30)

        val agg = makeAgg()

        // buf1 has only tileC (idx=3)
        val buf1 = agg.createAggregationBuffer()
        agg.updateWithIndex(buf1, tileC, 3)

        // buf2 has tileA (idx=1) and tileB (idx=2)
        val buf2 = agg.createAggregationBuffer()
        agg.updateWithIndex(buf2, tileA, 1)
        agg.updateWithIndex(buf2, tileB, 2)

        val merged = agg.merge(buf1, buf2)
        merged should have length 3

        val result = agg.eval(merged).asInstanceOf[InternalRow]
        result should not be null

        // After sort by band_index: band1=A(10), band2=B(20), band3=C(30)
        readBandCount(result) shouldBe 3
        readBandMean(result, 1) shouldBe 10.0 +- 0.5
        readBandMean(result, 2) shouldBe 20.0 +- 0.5
        readBandMean(result, 3) shouldBe 30.0 +- 0.5
    }

    test("update tolerates LongType band_index (PySpark Connect path)") {
        // PySpark / Spark Connect serialises Python int literals as LongType.
        // The old code called .asInstanceOf[Int] and threw ClassCastException.
        // This test constructs the agg with Literal(1L) (a Long literal) and
        // drives update() directly to confirm no exception is thrown and the
        // buffer grows by one entry.
        val tileA = makeSingleBandTileRow("long_idx", 42)

        val tileType = org.apache.spark.sql.types.StructType(Seq(
            org.apache.spark.sql.types.StructField("cellid",   org.apache.spark.sql.types.LongType, nullable = false),
            org.apache.spark.sql.types.StructField("raster",   BinaryType,                          nullable = false),
            org.apache.spark.sql.types.StructField("metadata", org.apache.spark.sql.types.MapType(
                org.apache.spark.sql.types.StringType, org.apache.spark.sql.types.StringType),      nullable = true)
        ))
        val aggLong = RST_FromBandsAgg(
            tile      = Literal.create(tileA, tileType),
            bandIndexExpr = Literal(1L),    // Long literal — this is what PySpark sends
            exprConfExpr  = Literal.create(encodedEmpty(), StringType)
        )

        val buf = aggLong.createAggregationBuffer()
        buf should have length 0

        // Must not throw ClassCastException (the pre-fix behaviour).
        noException should be thrownBy aggLong.update(buf, InternalRow.empty)
        buf should have length 1
    }

    // ---- v2 (8-field) tile tests -------------------------------------------

    test("v2 tiles: band-order correctness from 8-field input rows") {
        // Regression: getStruct(1, 3) on an 8-field row truncates to 3 fields,
        // causing rowToTile to misread the metadata position (field 2 instead of 7).
        val tileA = makeSingleBandTileRowV2("v2A", 10)
        val tileB = makeSingleBandTileRowV2("v2B", 20)
        val tileC = makeSingleBandTileRowV2("v2C", 30)

        val agg = makeAggForTileType(v2TileType)
        val buf = agg.createAggregationBuffer()

        // SHUFFLED: insert C(idx=3), A(idx=1), B(idx=2)
        agg.updateWithIndex(buf, tileC, 3)
        agg.updateWithIndex(buf, tileA, 1)
        agg.updateWithIndex(buf, tileB, 2)

        val result = agg.eval(buf).asInstanceOf[InternalRow]
        result should not be null

        val bandCount = readBandCount(result)
        bandCount shouldBe 3

        // After sort by band_index: band1=A(10), band2=B(20), band3=C(30)
        readBandMean(result, 1) shouldBe 10.0 +- 0.5
        readBandMean(result, 2) shouldBe 20.0 +- 0.5
        readBandMean(result, 3) shouldBe 30.0 +- 0.5
    }

    test("v2 tiles: merge then eval with 8-field inputs assembles correct band order") {
        val tileA = makeSingleBandTileRowV2("v2mA", 10)
        val tileB = makeSingleBandTileRowV2("v2mB", 20)
        val tileC = makeSingleBandTileRowV2("v2mC", 30)

        val agg = makeAggForTileType(v2TileType)

        val buf1 = agg.createAggregationBuffer()
        agg.updateWithIndex(buf1, tileC, 3)

        val buf2 = agg.createAggregationBuffer()
        agg.updateWithIndex(buf2, tileA, 1)
        agg.updateWithIndex(buf2, tileB, 2)

        val merged = agg.merge(buf1, buf2)
        merged should have length 3

        val result = agg.eval(merged).asInstanceOf[InternalRow]
        result should not be null

        readBandCount(result) shouldBe 3
        readBandMean(result, 1) shouldBe 10.0 +- 0.5
        readBandMean(result, 2) shouldBe 20.0 +- 0.5
        readBandMean(result, 3) shouldBe 30.0 +- 0.5
    }

    test("buffer serde roundtrip preserves band indices") {
        val tileA = makeSingleBandTileRow("sA", 11)
        val tileB = makeSingleBandTileRow("sB", 22)

        val agg = makeAgg()
        val buf = agg.createAggregationBuffer()

        agg.updateWithIndex(buf, tileB, 2)
        agg.updateWithIndex(buf, tileA, 1)

        val serialized   = agg.serialize(buf)
        val deserialized = agg.deserialize(serialized)

        deserialized should have length 2
        // After deserialize the two entries must carry their indices
        val indices = deserialized.map(_.asInstanceOf[InternalRow].getInt(0)).toSeq.sorted
        indices shouldBe Seq(1, 2)
    }

}
