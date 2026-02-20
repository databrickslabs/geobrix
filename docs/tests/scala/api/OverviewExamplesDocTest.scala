package docs.tests.scala.api

import com.databricks.labs.gbx.rasterx.{functions => rx}
import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

/**
  * Tests for Scala code examples in docs/docs/api/overview.mdx
  *
  * These tests ensure the documented Scala patterns compile and execute correctly.
  * This follows the single-copy pattern: the example code lives here and is tested,
  * serving as the authoritative source for the documentation.
  */
class OverviewExamplesDocTest extends AnyFunSuite with BeforeAndAfterAll {

    var spark: SparkSession = _

    override def beforeAll(): Unit = {
        super.beforeAll()
        spark = SparkSession.builder()
            .appName("API Overview Examples Doc Test")
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
      * DOCS EXAMPLE: docs/docs/api/overview.mdx lines 62-71
      * 
      * This is the exact Scala registration pattern shown in the documentation.
      * Any changes here should be reflected in the docs.
      */
    test("register_all_packages_example") {
        import com.databricks.labs.gbx.rasterx.{functions => rx}
        import com.databricks.labs.gbx.gridx.bng.{functions => bx}
        import com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}

        // Register each package
        rx.register(spark)
        bx.register(spark)
        vx.register(spark)

        // Verify registration worked for each package
        val functions = spark.catalog.listFunctions().collect()
        val functionNames = functions.map(_.name).toSet
        
        // Should have functions from each package
        functionNames.exists(_.startsWith("gbx_rst_")) should be(true)
        functionNames.exists(_.startsWith("gbx_bng_")) should be(true)
        functionNames.exists(_.startsWith("gbx_st_")) should be(true)
    }

    test("all_package_imports_compile") {
        // Verify the import pattern from docs works
        noException should be thrownBy {
            import com.databricks.labs.gbx.rasterx.{functions => rx}
            import com.databricks.labs.gbx.gridx.bng.{functions => bx}
            import com.databricks.labs.gbx.vectorx.jts.legacy.{functions => vx}
            
            val _ = rx.register _
            val _ = bx.register _
            val _ = vx.register _
        }
    }
}
