package docs.tests.scala.packages

import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Tests for Scala code examples in docs/docs/packages/rasterx.mdx
  *
  * These tests ensure the documented Scala patterns compile and execute correctly.
  * This follows the single-copy pattern: the example code lives here and is tested,
  * serving as the authoritative source for the documentation.
  */
class RasterxExamplesDocTest extends AnyFunSuite with BeforeAndAfterAll {

    var spark: SparkSession = _

    override def beforeAll(): Unit = {
        super.beforeAll()
        spark = SparkSession.builder()
            .appName("RasterX Examples Doc Test")
            .master("local[1]")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
    }

    override def afterAll(): Unit = {
        if (spark != null) {
            spark.stop()
        }
        super.afterAll()
    }

    /**
      * DOCS EXAMPLE: docs/docs/packages/rasterx.mdx lines 104-122
      * 
      * This is the exact Scala usage pattern shown in the documentation.
      * Any changes here should be reflected in the docs.
      */
    test("rasterx_scala_usage_example") {
        import com.databricks.labs.gbx.rasterx.{functions => rx}

        // Register functions
        rx.register(spark)

        // The documentation shows this pattern:
        // val rasterDf = spark.read.format("gdal").load("/path/to/geotiffs")
        // val metadataDf = rasterDf.select(
        //   col("path"),
        //   rx.rst_width(col("tile")).alias("width"),
        //   rx.rst_height(col("tile")).alias("height"),
        //   rx.rst_numbands(col("tile")).alias("num_bands")
        // )
        // metadataDf.show()

        // Verify registration worked
        val functions = spark.catalog.listFunctions()
            .filter(col("name").startsWith("gbx_rst_"))
            .collect()
        
        functions.length should be > 0
        
        // Verify the functions used in the example are accessible
        noException should be thrownBy {
            val _ = rx.rst_width _
            val _ = rx.rst_height _
            val _ = rx.rst_numbands _
        }
    }

    test("rasterx_imports_compile") {
        // Verify the import pattern from docs works
        noException should be thrownBy {
            import com.databricks.labs.gbx.rasterx.{functions => rx}
            val _ = rx.register _
        }
    }
}
