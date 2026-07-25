package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.gridx.grid.{BNG, Quadbin}
import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.expressions.agg.{RST_BNG_RasterizeAgg, RST_Quadbin_RasterizeAgg}
import com.databricks.labs.gbx.rasterx.gdal.{GDALManager, RasterDriver}
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.gdal.gdal.gdal
import org.gdal.gdalconst.gdalconstConstants
import org.scalatest.matchers.should.Matchers._

/**
 * End-to-end integration test for the 9 BNG/quadbin raster-grid functions wired up in
 * [[com.databricks.labs.gbx.rasterx.functions]] (Task 7). This closes the deferred
 * registration + Spark-path assertions from Tasks 2/4/5/6:
 *
 *  - all 9 functions resolve in the session function registry after `register`;
 *  - a BNG `rastertogrid` reducer produces String cell ids over a real EPSG:27700 raster;
 *  - both quadbin and BNG `tessellate` generators emit tile rows over real rasters;
 *  - `rasterize_agg` (quadbin and BNG) burns a group of cells into a tile whose band
 *    declares GetNoDataValue == -9999.0 (spec §2.6).
 *
 * Uses real GDAL and real in-memory rasters written to temp GTiffs; nothing is mocked.
 */
class RST_GridIntegrationTest extends PlanTest with SilentSparkSession {

    override def beforeAll(): Unit = {
        super.beforeAll()
        // Register GDAL drivers on the driver thread so the raster fixture writers below
        // (gdal.GetDriverByName("GTiff")) resolve. The rasterize_agg / rastertogrid Spark
        // paths init GDAL on their own (executor) threads, but the fixture writers run
        // driver-side before any GDAL Spark job fires.
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp", logCPL = true, CPL_DEBUG = "OFF")
        gdal.AllRegister()
    }

    /** The 9 SQL names Task 7 registers. */
    private val theNine = Seq(
        "gbx_rst_bng_rastertogridavg", "gbx_rst_bng_rastertogridcount", "gbx_rst_bng_rastertogridmax",
        "gbx_rst_bng_rastertogridmin", "gbx_rst_bng_rastertogridmedian", "gbx_rst_bng_tessellate",
        "gbx_rst_quadbin_tessellate", "gbx_rst_quadbin_rasterize_agg", "gbx_rst_bng_rasterize_agg"
    )

    // ---- raster fixtures (written to temp GTiffs so udfs.rasterFromPath can read them) ----

    /** A 4x4 EPSG:27700 raster over London (TQ region), 1km pixels, all valid, values 1..16. */
    private def writeLondonBng(path: String): Unit = {
        val drv = gdal.GetDriverByName("GTiff")
        val ds = drv.Create(path, 4, 4, 1, gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(528000.0, 1000.0, 0.0, 182000.0, 0.0, -1000.0))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(27700)
        sr.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, 4, 4, (1 to 16).map(_.toDouble).toArray)
        ds.FlushCache(); ds.delete(); sr.delete()
    }

    /** A 4x4 EPSG:4326 raster over London (~lon -0.1, lat 51.5), ~0.01deg pixels, values 1..16. */
    private def writeLondon4326(path: String): Unit = {
        val drv = gdal.GetDriverByName("GTiff")
        val ds = drv.Create(path, 4, 4, 1, gdalconstConstants.GDT_Float64)
        ds.SetGeoTransform(Array(-0.12, 0.01, 0.0, 51.52, 0.0, -0.01))
        val sr = new org.gdal.osr.SpatialReference(); sr.ImportFromEPSG(4326)
        sr.SetAxisMappingStrategy(org.gdal.osr.osrConstants.OAMS_TRADITIONAL_GIS_ORDER)
        ds.SetProjection(sr.ExportToWkt())
        ds.GetRasterBand(1).WriteRaster(0, 0, 4, 4, (1 to 16).map(_.toDouble).toArray)
        ds.FlushCache(); ds.delete(); sr.delete()
    }

    /** Read back the tile-struct `raster` bytes and return the band-1 declared NoData value. */
    private def declaredNoData(rasterBytes: Array[Byte]): java.lang.Double = {
        val mem = s"/vsimem/grid_int_${java.util.UUID.randomUUID().toString.replace("-", "")}.tif"
        gdal.FileFromMemBuffer(mem, rasterBytes)
        val ds = gdal.Open(mem)
        try {
            val nd = new Array[java.lang.Double](1)
            ds.GetRasterBand(1).GetNoDataValue(nd)
            nd(0)
        } finally {
            RasterDriver.releaseDataset(ds)
            gdal.Unlink(mem)
        }
    }

    // ---- 1. registration ----------------------------------------------------

    test("all 9 grid functions are registered") {
        functions.register(spark)
        val fns = spark.sessionState.functionRegistry.listFunction().map(_.funcName).toSet
        theNine.foreach(n => assert(fns.contains(n), s"$n not registered"))
    }

    // ---- 2. BNG rastertogrid reducer: real raster -> String cell ids --------

    test("bng rastertogrid reducer emits String cell ids end-to-end") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tmp = java.nio.file.Files.createTempDirectory("gbx_grid_int_bng_").toFile
        try {
            val p = s"${tmp.getAbsolutePath}/london_bng.tif"
            writeLondonBng(p)

            // resolution 3 = 1km cells. The reducer returns ARRAY<ARRAY<STRUCT<cellID,measure>>>
            // (outer = bands, inner = cells); Spark rejects nested explode(explode(...)), so
            // collect the nested array and flatten it Scala-side.
            val bands = Seq(p).toDF("path")
                .withColumn("raster", udfs.rasterFromPath(col("path")))
                .select(rst_bng_rastertogridavg(col("raster"), lit(3)).alias("grid"))
                .collect().head.getSeq[Seq[Row]](0)

            val cells = bands.flatten
            cells.length should be > 0
            // Every emitted cell id is a BNG STRING id (^[A-Z]{2}\d*$), not a Long.
            cells.foreach { r =>
                val id = r.getString(0)
                assert(id.matches("^[A-Z]{2}\\d*$"), s"expected BNG string id, got '$id'")
                r.getDouble(1) should be >= 0.0
            }
        } finally {
            tmp.listFiles().foreach(_.delete()); tmp.delete()
        }
    }

    // ---- 3a. quadbin tessellate generator: real raster -> tile rows ---------

    test("quadbin tessellate generator emits tile rows end-to-end") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tmp = java.nio.file.Files.createTempDirectory("gbx_grid_int_qb_").toFile
        try {
            val p = s"${tmp.getAbsolutePath}/london_4326.tif"
            writeLondon4326(p)

            // zoom 12 -> quadbin tiles overlap this ~0.04deg raster.
            val n = Seq(p).toDF("path")
                .withColumn("raster", udfs.rasterFromPath(col("path")))
                .select(rst_quadbin_tessellate(col("raster"), lit(12)).alias("tile"))
                .collect()
            n.length should be > 0
        } finally {
            tmp.listFiles().foreach(_.delete()); tmp.delete()
        }
    }

    // ---- 3b. bng tessellate generator: real raster -> tile rows -------------

    test("bng tessellate generator emits tile rows end-to-end") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tmp = java.nio.file.Files.createTempDirectory("gbx_grid_int_bngt_").toFile
        try {
            val p = s"${tmp.getAbsolutePath}/london_bng.tif"
            writeLondonBng(p)

            // resolution 3 = 1km cells; the 4km x 4km raster overlaps several cells.
            val n = Seq(p).toDF("path")
                .withColumn("raster", udfs.rasterFromPath(col("path")))
                .select(rst_bng_tessellate(col("raster"), lit(3)).alias("tile"))
                .collect()
            n.length should be > 0
        } finally {
            tmp.listFiles().foreach(_.delete()); tmp.delete()
        }
    }

    // ---- 4. rasterize_agg: cell set -> tile with NoData == -9999 (spec 2.6) --

    test("quadbin rasterize_agg output declares -9999 NoData (spec 2.6)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        // A handful of central-London points -> res-12 quadbin cells (Long ids).
        val z = 12
        val cells = Seq(
            (-0.1276, 51.5074), (-0.1419, 51.5014), (-0.1195, 51.5033)
        ).map { case (lon, lat) => Quadbin.pointToCell(lon, lat, z) }
        val rasterBytes = cells.zipWithIndex.map { case (c, i) => (c, (i + 1).toDouble) }
            .toDF("cellid", "value")
            .groupBy(lit(1).alias("g"))
            .agg(rst_quadbin_rasterize_agg(col("cellid"), col("value")).alias("out"))
            .select(col("out.raster").alias("raster"))
            .collect().head.getAs[Array[Byte]]("raster")

        assert(rasterBytes != null, "quadbin rasterize_agg must produce raster bytes")
        val nd = declaredNoData(rasterBytes)
        assert(nd != null, "output band must declare a NoData value")
        nd.doubleValue() shouldBe RST_Quadbin_RasterizeAgg.NoData +- 1e-9
        nd.doubleValue() shouldBe -9999.0 +- 1e-9
    }

    test("bng rasterize_agg output declares -9999 NoData (spec 2.6)") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        // Central-London BNG cells at resolution 3 (1km); STRING ids fed to the UDAF.
        val res = 3
        val bngIds = Seq(
            (530000.0, 180000.0), (531000.0, 181000.0), (529000.0, 179000.0)
        ).map { case (e, n) => BNG.format(BNG.pointToCellID(e, n, res)) }
        // srid arg is a no-op for BNG (forced to 27700); the 2-arg wrapper passes 27700.
        val rasterBytes = bngIds.zipWithIndex.map { case (id, i) => (id, (i + 1).toDouble) }
            .toDF("cellid", "value")
            .groupBy(lit(1).alias("g"))
            .agg(rst_bng_rasterize_agg(col("cellid"), col("value")).alias("out"))
            .select(col("out.raster").alias("raster"))
            .collect().head.getAs[Array[Byte]]("raster")

        assert(rasterBytes != null, "bng rasterize_agg must produce raster bytes")
        val nd = declaredNoData(rasterBytes)
        assert(nd != null, "output band must declare a NoData value")
        nd.doubleValue() shouldBe RST_BNG_RasterizeAgg.NoData +- 1e-9
        nd.doubleValue() shouldBe -9999.0 +- 1e-9
    }

    // ---- 5. cross-cutting round-trip + NoData masking on read-back (spec §5) ---
    //
    // Inverse-operation invariant: rasterize_agg (cell set -> tile) followed by
    // rastertogrid<avg> (tile -> cell set) must recover the EXACT input cells and their
    // per-cell values. Because rasterize_agg pads the grid by kring_pad=1, the tile also
    // contains -9999 NoData pixels around the burned cells. If that sentinel were
    // aggregated instead of masked on read-back, the reducer would either (a) surface extra
    // cells whose measure is ~-9999, or (b) corrupt a burned cell's average with -9999. So
    // asserting `recovered.keySet == input.keySet` AND every measure matches the assigned
    // value (never -9999) proves BOTH the round-trip and that the NoData sentinel is masked
    // (not aggregated) when the tile is read back. Task 7 asserts the tile *declares*
    // GetNoDataValue == -9999; this closes the "excluded on read-back" half.

    test("quadbin round-trip: rasterize_agg -> rastertogridavg recovers per-cell values, masks NoData") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        // z=18 (~0.00137deg / ~95m cells) so the three central-London points below fall in
        // three DISTINCT quadbin cells; at coarse zooms (e.g. z=12, ~9.5km) they collapse
        // into one cell and the round-trip has nothing to distinguish.
        val z = 18
        val cells = Seq((-0.1276, 51.5074), (-0.1419, 51.5014), (-0.1195, 51.5033))
            .map { case (lon, lat) => Quadbin.pointToCell(lon, lat, z) }
        assert(cells.distinct.length == cells.length, s"fixture cells must be distinct at z=$z")
        val expected = cells.zipWithIndex.map { case (c, i) => (c, (i + 1).toDouble) }.toMap

        val gridDf = cells.zipWithIndex.map { case (c, i) => (c, (i + 1).toDouble) }
            .toDF("cellid", "value")
            .groupBy(lit(1).alias("g"))
            .agg(rst_quadbin_rasterize_agg(col("cellid"), col("value")).alias("out"))

        val bands = gridDf
            .select(rst_quadbin_rastertogridavg(col("out"), lit(z)).alias("grid"))
            .collect().head.getSeq[Seq[Row]](0)
        val recovered = bands.flatten.map(r => (r.getLong(0), r.getDouble(1))).toMap

        recovered.keySet shouldBe expected.keySet
        recovered.foreach { case (c, v) =>
            v shouldBe expected(c) +- 1e-9
            assert(math.abs(v - RST_Quadbin_RasterizeAgg.NoData) > 1e-6,
              s"read-back leaked the NoData sentinel for cell $c: $v")
        }
    }

    test("bng round-trip: rasterize_agg -> rastertogridavg recovers per-cell values, masks NoData") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val res = 3 // 1km cells
        // Three distinct 1km BNG cells in central London (EPSG:27700 eastings/northings).
        val ids = Seq((530000.0, 180000.0), (531000.0, 181000.0), (529000.0, 179000.0))
            .map { case (e, n) => BNG.format(BNG.pointToCellID(e, n, res)) }
        assert(ids.distinct.length == ids.length, "fixture cells must be distinct at res 3")
        val expected = ids.zipWithIndex.map { case (id, i) => (id, (i + 1).toDouble) }.toMap

        val gridDf = ids.zipWithIndex.map { case (id, i) => (id, (i + 1).toDouble) }
            .toDF("cellid", "value")
            .groupBy(lit(1).alias("g"))
            .agg(rst_bng_rasterize_agg(col("cellid"), col("value")).alias("out"))

        val bands = gridDf
            .select(rst_bng_rastertogridavg(col("out"), lit(res)).alias("grid"))
            .collect().head.getSeq[Seq[Row]](0)
        val recovered = bands.flatten.map(r => (r.getString(0), r.getDouble(1))).toMap

        recovered.keySet shouldBe expected.keySet
        recovered.foreach { case (id, v) =>
            v shouldBe expected(id) +- 1e-9
            assert(math.abs(v - RST_BNG_RasterizeAgg.NoData) > 1e-6,
              s"read-back leaked the NoData sentinel for cell $id: $v")
        }
    }
}
