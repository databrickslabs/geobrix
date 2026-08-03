package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.expressions.agg.{RST_CombineAvgAgg, RST_DerivedBandAgg, RST_MergeAgg}
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.rasterx.util.RasterSerializationUtil
import com.databricks.labs.gbx.util.SerializationUtil
import com.databricks.labs.gbx.udfs
import com.databricks.labs.gbx.udfs.st_buffer
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow, Literal}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{BinaryType, LongType, MapType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.matchers.should.Matchers._

class RST_AggEvalTest extends PlanTest with SilentSparkSession {

    /**
     * Valid GDAL VRT Python pixel-function signature: (in_ar, out_ar, xoff,
     * yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt,
     * **kwargs). The previous test used `def myfunc(x): return x*2`, which
     * GDAL silently rejected; the surrounding `noException` only checked that
     * nothing threw, so the malformed pyfunc went unnoticed for as long as
     * the underlying PixelCombineRasters bug prevented any pyfunc from
     * actually firing.
     */
    private val doublePyFunc =
        """
          |import numpy as np
          |def myfunc(in_ar, out_ar, xoff, yoff, xsize, ysize, raster_xsize, raster_ysize, buf_radius, gt, **kwargs):
          |    out_ar[:] = np.mean(np.asarray(in_ar, dtype=np.float64), axis=0) * 2
          |""".stripMargin

    test("RST_AggEvalTest should evaluate expressions on raster columns") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        def runQuery(df: DataFrame): Unit = {
            df
                .withColumn("bbox", rst_boundingbox(col("raster")))
                .withColumn("clipper", st_buffer(col("bbox"), lit(-500000.0))) // projection in meters 1 px is ~470m
                .withColumn("raster", rst_clip(col("raster"), col("clipper"), lit(true)))
                .groupBy(lit(1))
                .agg(
                  rst_combineavg_agg(col("raster")),
                  rst_derivedband_agg(col("raster"), doublePyFunc, "myfunc"),
                  rst_merge_agg(col("raster"))
                )
                .collect()
        }

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          (2, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF"),
          (3, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF")
        ).toDF("id", "path")
            .withColumn("raster", udfs.rasterFromPath(col("path")))

        noException should be thrownBy runQuery(df)

        val df2 = spark.read
            .format("binaryFile")
            .load(tifPath)
            .withColumn("raster", rst_fromcontent(col("content"), lit("GTiff")))

        noException should be thrownBy runQuery(df2)

    }

    test("rst_combineavg on a single tile column raises a friendly error pointing at the _agg form") {
        // Regression for the user-reported notebook error:
        //   .selectExpr("gbx_rst_combineavg(tile) AS tile")
        // The non-agg form expects ARRAY<tile>; passing a single tile struct
        // previously produced a raw ClassCastException from inside Catalyst
        // analysis. After RST_ExpressionUtil.arrayOfTileRasterType is in
        // place we should see an IllegalArgumentException with a message that
        // names the function, the actual type received, and the aggregator
        // companion that the user likely wanted.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString
        val df = Seq(
          s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
        ).toDF("path").withColumn("tile", udfs.rasterFromPath(col("path")))

        val thrown = intercept[Throwable] {
            df.selectExpr("gbx_rst_combineavg(tile) AS tile").collect()
        }
        // Spark wraps analysis-time IllegalArgumentException so the actual
        // class can be either IllegalArgumentException or one of Spark's
        // catalyst-wrapper types — what matters is that our diagnostic
        // message survives in the chain.
        val joined = LazyList
            .iterate(Option(thrown))(_.flatMap(t => Option(t.getCause)))
            .takeWhile(_.isDefined)
            .flatMap(_.map(_.getMessage).filter(_ != null))
            .mkString(" || ")
        joined should include ("gbx_rst_combineavg expects ARRAY<tile>")
        joined should include ("gbx_rst_combineavg_agg")
    }

    test("rst_derivedband_agg actually transforms pixel values (parity with combineavg_agg fix)") {
        // End-to-end Spark aggregation: three constant Byte tiles (10, 20, 30)
        // averaged then doubled by the pyfunc should yield 40 everywhere
        // (mean(10,20,30)=20, *2 = 40). Before the PixelCombineRasters
        // ordering fix this returned one of the inputs unchanged through the
        // aggregator path, so the output would be 10 / 20 / 30 — not 40.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tmpDir = java.nio.file.Files.createTempDirectory("gbx_derivedband_agg_").toFile

        val w = 8; val h = 8
        // Inline byte raster writer; can't reuse RST_AggregationsTest's helper
        // from a separate suite without lifting it to a shared location and
        // this is the only place outside that suite that needs it.
        def writeByteConst(p: String, v: Int): Unit = {
            val drv = gdal.GetDriverByName("GTiff")
            val ds = drv.Create(p, w, h, 1, gdalconstConstants.GDT_Byte, Array[String]("COMPRESS=DEFLATE"))
            ds.SetGeoTransform(Array[Double](149.0, 0.01, 0.0, -35.0, 0.0, -0.01))
            val sr = new org.gdal.osr.SpatialReference()
            sr.ImportFromEPSG(4326)
            ds.SetProjection(sr.ExportToWkt())
            val band = ds.GetRasterBand(1)
            band.WriteRaster(0, 0, w, h, Array.fill[Byte](w * h)(v.toByte))
            band.FlushCache()
            ds.FlushCache()
            ds.delete()
        }
        val paths = Seq(10, 20, 30).map { v =>
            val p = s"${tmpDir.getAbsolutePath}/const_$v.tif"
            writeByteConst(p, v)
            p
        }

        try {
            val agg = paths.toDF("path")
                .withColumn("tile", udfs.rasterFromPath(col("path")))
                .groupBy(lit(1).alias("g"))
                .agg(rst_derivedband_agg(col("tile"), doublePyFunc, "myfunc").alias("out"))
                .select(col("out.raster").alias("raster"))

            val bytes = agg.collect().head.getAs[Array[Byte]]("raster")
            // Decode in-memory GTiff bytes; verify uniform 40.
            val mem = s"/vsimem/derivedband_agg_check_${java.util.UUID.randomUUID()}.tif"
            gdal.FileFromMemBuffer(mem, bytes)
            val ds = gdal.Open(mem)
            try {
                val buf = Array.ofDim[Double](ds.GetRasterXSize * ds.GetRasterYSize)
                ds.GetRasterBand(1).ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize, gdalconstConstants.GDT_Float64, buf)
                buf.min shouldBe 40.0 +- 0.5
                buf.max shouldBe 40.0 +- 0.5
            } finally {
                RasterDriver.releaseDataset(ds)
                gdal.Unlink(mem)
            }
        } finally {
            tmpDir.listFiles().foreach(_.delete())
            tmpDir.delete()
        }
    }

    test("rst_merge_agg overlap winner is deterministic regardless of row order") {
        // Two same-size tiles whose extents overlap (origins differ by half a tile),
        // filled with distinct constants. The mosaic is last-wins; the aggregator now
        // orders tiles by their raw serialized content, so the same tile reliably wins
        // the overlap regardless of the order rows reach the aggregator.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tmpDir = java.nio.file.Files.createTempDirectory("gbx_merge_agg_det_").toFile
        val w = 8; val h = 8
        // part_0: origin (149.0, -35.0); part_1: origin shifted +0.04 east / -0.04 south
        // (half the 8 px * 0.01 extent) so the two extents overlap in their inner corner.
        def writeByteConst(p: String, v: Int, ox: Double, oy: Double): Unit = {
            val drv = gdal.GetDriverByName("GTiff")
            val ds = drv.Create(p, w, h, 1, gdalconstConstants.GDT_Byte, Array[String]("COMPRESS=DEFLATE"))
            ds.SetGeoTransform(Array[Double](ox, 0.01, 0.0, oy, 0.0, -0.01))
            val sr = new org.gdal.osr.SpatialReference()
            sr.ImportFromEPSG(4326)
            ds.SetProjection(sr.ExportToWkt())
            val band = ds.GetRasterBand(1)
            band.WriteRaster(0, 0, w, h, Array.fill[Byte](w * h)(v.toByte))
            band.FlushCache(); ds.FlushCache(); ds.delete()
        }
        val p0 = s"${tmpDir.getAbsolutePath}/part_0.tif"  // origin 149.00, value 10
        val p1 = s"${tmpDir.getAbsolutePath}/part_1.tif"  // origin 149.04, value 20 (wins overlap)
        writeByteConst(p0, 10, 149.0, -35.0)
        writeByteConst(p1, 20, 149.04, -35.04)

        def mergeMean(order: Seq[String]): Double = {
            val bytes = order.toDF("path")
                .withColumn("tile", udfs.rasterFromPath(col("path")))
                .groupBy(lit(1).alias("g"))
                .agg(rst_merge_agg(col("tile")).alias("out"))
                .select(col("out.raster").alias("raster"))
                .collect().head.getAs[Array[Byte]]("raster")
            val mem = s"/vsimem/merge_agg_det_${java.util.UUID.randomUUID()}.tif"
            gdal.FileFromMemBuffer(mem, bytes)
            val ds = gdal.Open(mem)
            try {
                val n = ds.GetRasterXSize * ds.GetRasterYSize
                val buf = Array.ofDim[Double](n)
                ds.GetRasterBand(1).ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize,
                    gdalconstConstants.GDT_Float64, buf)
                buf.sum / n
            } finally { RasterDriver.releaseDataset(ds); gdal.Unlink(mem) }
        }

        try {
            // Same group, both row orders -> identical mosaic (content sort, not arrival).
            val meanAB = mergeMean(Seq(p0, p1))
            val meanBA = mergeMean(Seq(p1, p0))
            meanAB shouldBe meanBA +- 1e-9
        } finally {
            tmpDir.listFiles().foreach(_.delete()); tmpDir.delete()
        }
    }

    test("rst_merge_agg same-origin overlap winner is deterministic for in-memory tiles") {
        // The residual nondeterminism hole the content-key fix closes: two tiles sharing
        // the SAME geotransform origin but different content fully overlap. The previous
        // key (origin, GetDescription) tied on origin and fell back to GetDescription --
        // for BinaryType (rst_fromcontent) tiles that is a per-open /vsimem/<uuid> path,
        // i.e. random -- so the last-wins winner varied run to run. Sorting on raw content
        // gives a total order with no tie, so the winner is fixed regardless of row order.
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val w = 8; val h = 8
        // Two byte rasters, IDENTICAL georef/origin, distinct constant fills -> they fully
        // overlap; the canonical content order alone decides the winner.
        def byteConstBytes(v: Int): Array[Byte] = {
            val mem = s"/vsimem/merge_agg_sameorigin_src_${java.util.UUID.randomUUID()}.tif"
            val drv = gdal.GetDriverByName("GTiff")
            val ds = drv.Create(mem, w, h, 1, gdalconstConstants.GDT_Byte, Array[String]("COMPRESS=DEFLATE"))
            ds.SetGeoTransform(Array[Double](149.0, 0.01, 0.0, -35.0, 0.0, -0.01))
            val sr = new org.gdal.osr.SpatialReference()
            sr.ImportFromEPSG(4326)
            ds.SetProjection(sr.ExportToWkt())
            val band = ds.GetRasterBand(1)
            band.WriteRaster(0, 0, w, h, Array.fill[Byte](w * h)(v.toByte))
            band.FlushCache(); ds.FlushCache(); ds.delete()
            val buf = gdal.GetMemFileBuffer(mem)
            gdal.Unlink(mem)
            buf
        }
        val a = byteConstBytes(10)
        val b = byteConstBytes(20)

        def mergeMean(order: Seq[Array[Byte]]): Double = {
            val bytes = order.toDF("content")
                .withColumn("tile", rst_fromcontent(col("content"), lit("GTiff")))
                .groupBy(lit(1).alias("g"))
                .agg(rst_merge_agg(col("tile")).alias("out"))
                .select(col("out.raster").alias("raster"))
                .collect().head.getAs[Array[Byte]]("raster")
            val mem = s"/vsimem/merge_agg_sameorigin_${java.util.UUID.randomUUID()}.tif"
            gdal.FileFromMemBuffer(mem, bytes)
            val ds = gdal.Open(mem)
            try {
                val n = ds.GetRasterXSize * ds.GetRasterYSize
                val buf = Array.ofDim[Double](n)
                ds.GetRasterBand(1).ReadRaster(0, 0, ds.GetRasterXSize, ds.GetRasterYSize,
                    gdalconstConstants.GDT_Float64, buf)
                buf.sum / n
            } finally { RasterDriver.releaseDataset(ds); gdal.Unlink(mem) }
        }

        // Both row orders must yield the SAME constant mosaic (one tile wins everywhere).
        val meanAB = mergeMean(Seq(a, b))
        val meanBA = mergeMean(Seq(b, a))
        meanAB shouldBe meanBA +- 1e-9
        // The winner is one of the two inputs (10 or 20), uniform across the tile.
        (meanAB === 10.0 +- 1e-9 || meanAB === 20.0 +- 1e-9) shouldBe true
    }

    // =========================================================================
    // v1 (3-field) input normalization — regression for AIOOBE on size==1
    // fast-path and serialize UnsafeProjection.
    //
    // Drives the aggregators directly (update / serialize / eval) with genuine
    // 3-field InternalRows. Before the normalizeToV2Row fix these paths threw
    // ArrayIndexOutOfBoundsException because the 8-field UnsafeProjection
    // tried to read fields 3..7 on a 3-element row.
    // =========================================================================

    /** Produce a tiny in-memory GeoTIFF as bytes via /vsimem (no temp file).
      * Calls GDALManager.init defensively so the helper works whether or not a
      * prior test has already initialized GDAL in this JVM. */
    private def tinyGTiffBytes(value: Int = 42): Array[Byte] = {
        import com.databricks.labs.gbx.expressions.ExpressionConfig
        import org.apache.spark.util.SerializableConfiguration
        import org.apache.hadoop.conf.Configuration
        GDALManager.init(new ExpressionConfig(Map.empty, new SerializableConfiguration(new Configuration())))
        val path = s"/vsimem/agg_v1_test_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        val drv  = gdal.GetDriverByName("GTiff")
        val ds   = drv.Create(path, 4, 4, 1, gdalconstConstants.GDT_Byte, Array[String]("COMPRESS=DEFLATE"))
        ds.SetGeoTransform(Array[Double](149.0, 0.01, 0.0, -35.0, 0.0, -0.01))
        val sr = new org.gdal.osr.SpatialReference()
        sr.ImportFromEPSG(4326)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).Fill(value.toDouble)
        ds.FlushCache()
        val bytes = gdal.GetMemFileBuffer(path)
        ds.delete()
        gdal.Unlink(path)
        bytes
    }

    /** Build a v1 (3-field) InternalRow: (cellid, raster, metadata). */
    private def v1Row(cellid: Long, bytes: Array[Byte]) = {
        val emptyMap = ArrayBasedMapData(Array.empty[UTF8String], Array.empty[UTF8String])
        new GenericInternalRow(Array[Any](cellid, bytes, emptyMap))
    }

    /** v1 tile struct type: 3 fields (cellid, raster, metadata). */
    private val v1TileType: StructType = StructType(Seq(
        StructField("cellid",   LongType,               nullable = false),
        StructField("raster",   BinaryType,              nullable = true),
        StructField("metadata", MapType(StringType, StringType), nullable = true)
    ))

    /** BoundReference that extracts field 0 (the tile struct) from a 1-element input row,
      * bound to v1TileType so child.eval(inputRow) returns the InternalRow as-is. */
    private def v1TileRef: BoundReference = BoundReference(0, v1TileType, nullable = true)

    /** Wrap a v1 tile row in a 1-element container row (the input row to update()). */
    private def inputRow(tileRow: GenericInternalRow): GenericInternalRow =
        new GenericInternalRow(Array[Any](tileRow))

    test("RST_MergeAgg: size==1 v1 input through update() returns 8-field row (no AIOOBE)") {
        functions.register(spark)
        // BoundReference(0, v1TileType) binds child to field 0 of the input row,
        // so child.eval(inputRow(tileRow)) returns the v1 InternalRow directly.
        val agg = RST_MergeAgg(v1TileRef)
        val buf = agg.createAggregationBuffer()
        val tile = v1Row(1L, tinyGTiffBytes(10))
        agg.update(buf, inputRow(tile))

        // Buffered row must be 8-field after normalization in update()
        buf.head.asInstanceOf[org.apache.spark.sql.catalyst.InternalRow].numFields shouldBe 8

        // eval size==1 returns buffer.head; must be 8-field — before fix was 3-field
        val out = agg.eval(buf)
        assert(out != null)
        out.asInstanceOf[org.apache.spark.sql.catalyst.InternalRow].numFields shouldBe 8
    }

    test("RST_MergeAgg: serialize+deserialize roundtrip on v1 input via update() does not throw") {
        functions.register(spark)
        val agg = RST_MergeAgg(v1TileRef)
        val buf = agg.createAggregationBuffer()
        agg.update(buf, inputRow(v1Row(1L, tinyGTiffBytes(10))))
        agg.update(buf, inputRow(v1Row(1L, tinyGTiffBytes(20))))
        buf.size shouldBe 2
        buf.foreach(_.asInstanceOf[org.apache.spark.sql.catalyst.InternalRow].numFields shouldBe 8)

        // serialize must NOT throw AIOOBE — before fix threw on 8-field UnsafeProjection
        // applied to 3-field rows
        noException should be thrownBy {
            val bytes = agg.serialize(buf)
            val buf2  = agg.deserialize(bytes)
            buf2.size shouldBe 2
        }
    }

    test("RST_CombineAvgAgg: size==1 v1 input through update() returns 8-field row (no AIOOBE)") {
        functions.register(spark)
        val agg = RST_CombineAvgAgg(v1TileRef)
        val buf = agg.createAggregationBuffer()
        agg.update(buf, inputRow(v1Row(1L, tinyGTiffBytes(50))))

        buf.head.asInstanceOf[org.apache.spark.sql.catalyst.InternalRow].numFields shouldBe 8

        val out = agg.eval(buf)
        assert(out != null)
        out.asInstanceOf[org.apache.spark.sql.catalyst.InternalRow].numFields shouldBe 8
    }

    test("RST_CombineAvgAgg: serialize+deserialize roundtrip on v1 input via update() does not throw") {
        functions.register(spark)
        val agg = RST_CombineAvgAgg(v1TileRef)
        val buf = agg.createAggregationBuffer()
        agg.update(buf, inputRow(v1Row(1L, tinyGTiffBytes(50))))
        agg.update(buf, inputRow(v1Row(1L, tinyGTiffBytes(100))))

        noException should be thrownBy {
            val bytes = agg.serialize(buf)
            val buf2  = agg.deserialize(bytes)
            buf2.size shouldBe 2
        }
    }

    test("RST_DerivedBandAgg: serialize+deserialize roundtrip on v1 input via update() does not throw") {
        functions.register(spark)
        val pyfuncLit = Literal(UTF8String.fromString(
            "import numpy as np\ndef identity(in_ar, out_ar, *a, **kw): out_ar[:] = in_ar[0]"))
        val funcNameLit = Literal(UTF8String.fromString("identity"))
        val agg = RST_DerivedBandAgg(v1TileRef, pyfuncLit, funcNameLit)
        val buf = agg.createAggregationBuffer()
        agg.update(buf, inputRow(v1Row(1L, tinyGTiffBytes(30))))
        agg.update(buf, inputRow(v1Row(1L, tinyGTiffBytes(60))))

        noException should be thrownBy {
            val bytes = agg.serialize(buf)
            val buf2  = agg.deserialize(bytes)
            buf2.size shouldBe 2
        }
    }

    test("normalizeToV2Row: v1 3-field row becomes 8-field, v2 8-field row is unchanged") {
        val emptyMap = ArrayBasedMapData(Array.empty[UTF8String], Array.empty[UTF8String])
        val v1 = new GenericInternalRow(Array[Any](42L, Array[Byte](1, 2, 3), emptyMap))
        val n  = RasterSerializationUtil.normalizeToV2Row(v1)
        n.numFields shouldBe 8
        n.getLong(0) shouldBe 42L
        n.getBinary(1) shouldBe Array[Byte](1, 2, 3)
        // Fields 2..6 (pedigree) must be null
        (2 to 6).foreach(i => n.isNullAt(i) shouldBe true)
        // metadata still present at position 7
        n.isNullAt(7) shouldBe false

        // v2 row passes through unchanged (same object reference)
        val v2 = new GenericInternalRow(Array[Any](7L, Array[Byte](9), null, null, null, null, null, emptyMap))
        RasterSerializationUtil.normalizeToV2Row(v2) should be theSameInstanceAs v2
    }

}
