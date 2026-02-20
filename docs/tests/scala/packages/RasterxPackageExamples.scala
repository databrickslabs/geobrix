/**
 * Scala code examples for docs/docs/packages/rasterx.mdx (RasterX package page).
 * Single source of truth: displayed in the doc via CodeFromTest; validated by RasterxExamplesDocTest.
 */
package docs.tests.scala.packages

object RasterxPackageExamples {

  /** Example shown in packages/rasterx.mdx § Scala. Tested by RasterxExamplesDocTest.rasterx_scala_usage_example. */
  val RASTERX_SCALA_USAGE: String = """import com.databricks.labs.gbx.rasterx.{functions => rx}
import org.apache.spark.sql.functions._

// Register functions
rx.register(spark)

// Read raster files (sample data path; see Sample Data guide)
val rasterPath = "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif"
val rasterDf = spark.read.format("gdal").load(rasterPath)

// Get metadata
val metadataDf = rasterDf.select(
  col("path"),
  rx.rst_width(col("tile")).alias("width"),
  rx.rst_height(col("tile")).alias("height"),
  rx.rst_numbands(col("tile")).alias("num_bands")
)

metadataDf.show()"""

  /** Example output for packages/rasterx.mdx § Scala (displayed via outputConstant). */
  val RASTERX_SCALA_USAGE_output: String = """+--------------------+-----+------+----------+
|path                |width|height|num_bands |
+--------------------+-----+------+----------+
|.../nyc_sentinel2...|10980|10980 |1         |
+--------------------+-----+------+----------+"""

}
