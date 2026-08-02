package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import com.databricks.labs.gbx.rasterx.util.RST_ExpressionUtil
import com.databricks.labs.gbx.udfs
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.BoundReference
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions.{col, explode, lit}
import org.apache.spark.sql.test.SilentSparkSession
import org.apache.spark.sql.types.{LongType, MapType, StringType, StructField, StructType}
import org.scalatest.matchers.should.Matchers._

/**
  * Tests for understanding and working with the tile structure directly.
  * 
  * A tile in GeoBrix is a struct with three fields:
  * - cellid: Long (nullable) - Grid cell ID for tessellated rasters, null for non-tessellated
  * - raster: Binary - Raster bytes loaded from file or content
  * - metadata: Map[String, String] - Driver info, extension, size, etc.
  */
class RST_TileStructureTest extends PlanTest with SilentSparkSession {

    test("Tile structure should have cellid, raster, and metadata fields") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString

        val df = spark.range(1)
          .withColumn("tile", udfs.rasterFromPath(lit(tifPath)))

        // Verify tile schema has expected fields
        val tileSchema = df.schema("tile").dataType
        tileSchema.toString should include("cellid")
        tileSchema.toString should include("raster")
        tileSchema.toString should include("metadata")

        // Access tile fields directly
        val tileComponents = df.select(
          col("tile.cellid").alias("cellid"),
          col("tile.raster").alias("raster"),
          col("tile.metadata").alias("metadata")
        )

        val result = tileComponents.collect()
        result should not be empty

        // cellid should be null for non-tessellated rasters (nullable field)
        result(0).isNullAt(result(0).fieldIndex("cellid")) should be(true)

        // raster should be binary bytes
        val rasterValue = result(0).getAs[Array[Byte]]("raster")
        rasterValue should not be null
        rasterValue.length should be > 0

        // metadata should be a map
        val metadata = result(0).getAs[Map[String, String]]("metadata")
        metadata should not be empty
        metadata should contain key "driver"
        metadata should contain key "extension"
        metadata("driver") should be("GTiff")
        metadata("size").toInt should be(rasterValue.length)
    }

    test("Tile from file should contain binary in raster field") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString

        val df = spark.range(1)
          .withColumn("tile", udfs.rasterFromPath(lit(tifPath)))
          .select(
            col("tile.raster").alias("raster_binary"),
            col("tile.metadata").alias("metadata")
          )

        val result = df.collect()
        val rasterBinary = result(0).getAs[Array[Byte]]("raster_binary")
        val metadata = result(0).getAs[Map[String, String]]("metadata")

        rasterBinary should not be null
        rasterBinary.length should be > 0
        metadata("size").toInt should be(rasterBinary.length)
    }

    test("Tile from content should contain binary in raster field") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df = spark.read
          .format("binaryFile")
          .load(tifPath)
          .withColumn("tile", rst_fromcontent(col("content"), lit("GTiff")))
          .select(col("tile.raster").alias("raster_binary"))

        val result = df.collect()
        val rasterBinary = result(0).getAs[Array[Byte]]("raster_binary")

        rasterBinary should not be null
        rasterBinary.length should be > 0
    }

    test("Tile metadata should contain driver information") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString

        val df = spark.range(1)
          .withColumn("tile", udfs.rasterFromPath(lit(tifPath)))
          .select(
            col("tile.metadata").alias("metadata"),
            col("tile.metadata.driver").alias("driver"),
            col("tile.metadata.extension").alias("extension")
          )

        val result = df.collect()

        // Full metadata map
        val metadata = result(0).getAs[Map[String, String]]("metadata")
        metadata should contain key "driver"
        metadata should contain key "extension"

        // Individual metadata fields
        val driver = result(0).getAs[String]("driver")
        val extension = result(0).getAs[String]("extension")

        driver should be("GTiff")
        // Extension may vary based on actual file extension
        extension should (be(".tif") or be(".TIF") or include("tif") or include("TIF"))
    }

    test("Tessellated tiles should have non-null cellid") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString

        // rst_h3_tessellate is a generator that produces multiple rows directly (no explode needed)
        // Use resolution 1 (coarse) to avoid generating too many cells
        val df = spark.range(1)
          .withColumn("tile", udfs.rasterFromPath(lit(tifPath)))
          .withColumn("tessellated", rst_h3_tessellate(col("tile"), lit(1)))
          .select(col("tessellated.cellid").alias("cellid"))
          .limit(10) // Limit results for test efficiency

        val result = df.collect()

        result should not be empty
        // Tessellated tiles should have non-null cellid
        result.forall(row => row.getAs[Long]("cellid") != 0) should be(true)
        result.forall(row => row.getAs[Long]("cellid") > 0) should be(true)
    }

    test("Tile structure should be compatible with accessor functions") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString

        val df = spark.range(1)
          .withColumn("tile", udfs.rasterFromPath(lit(tifPath)))
          .select(
            col("tile"),
            col("tile.metadata").alias("metadata_direct"),
            rst_width(col("tile")).alias("width"),
            rst_height(col("tile")).alias("height")
          )

        val result = df.collect()

        // Verify direct metadata access works
        val metadataDirect = result(0).getAs[Map[String, String]]("metadata_direct")
        metadataDirect should not be empty
        metadataDirect should contain key "driver"

        // Verify accessor functions work with tile
        val width = result(0).getAs[Int]("width")
        val height = result(0).getAs[Int]("height")
        width should be > 0
        height should be > 0
    }

    test("Filter tiles by metadata fields") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          (2, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF")
        ).toDF("id", "path")
          .withColumn("tile", udfs.rasterFromPath(col("path")))

        // Filter by metadata
        val gTiffTiles = df.filter(col("tile.metadata.driver") === "GTiff")

        val result = gTiffTiles.collect()
        result.length should be(2)
    }

    test("rasterType rejects a v1 String path-tile schema") {
        val v1path = StructType(Seq(
            StructField("cellid", LongType, nullable = false),
            StructField("raster", StringType, nullable = false),
            StructField("metadata", MapType(StringType, StringType), nullable = true)))
        val ex = intercept[Exception](
            RST_ExpressionUtil.rasterType(BoundReference(0, v1path, nullable = true)))
        assert(ex.getMessage.toLowerCase.contains("path-tile") ||
               ex.getMessage.toLowerCase.contains("materialize"))
    }

    test("Use sibling path column for conditional processing") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"),
          (2, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF")
        ).toDF("id", "path")
          .withColumn("tile", udfs.rasterFromPath(col("path")))
          .withColumn("is_b01", col("path").contains("B01"))

        val result = df.collect()

        result(0).getAs[Boolean]("is_b01") should be(true)
        result(1).getAs[Boolean]("is_b01") should be(false)
    }

}
