/**
 * Scala code examples for docs/docs/packages/gridx.mdx (GridX package page).
 * Single source of truth: displayed in the doc via CodeFromTest; validated by GridxExamplesDocTest.
 */
package docs.tests.scala.packages

object GridxPackageExamples {

  /** Example shown in packages/gridx.mdx § Scala. Tested by GridxExamplesDocTest.gridx_scala_usage_example. */
  val GRIDX_SCALA_USAGE: String = """import com.databricks.labs.gbx.gridx.bng.{functions => bx}
import org.apache.spark.sql.functions._

// Register functions
bx.register(spark)

// Calculate cell area
val areaDf = spark.sql("SELECT gbx_bng_cellarea('TQ', 1000) as area")
areaDf.show()

// Create BNG cells from points (point as WKT; GeoBrix does not accept st_point)
val pointsDf = Seq(
  (51.5074, -0.1278)
).toDF("lat", "lon")

val bngCells = pointsDf.select(
  col("lat"),
  col("lon"),
  expr("gbx_bng_pointascell(concat('POINT(', cast(lon as string), ' ', cast(lat as string), ')'), 1000)").alias("bng_cell")
)

bngCells.show()"""

  /** Example output for packages/gridx.mdx § Scala (displayed via outputConstant). */
  val GRIDX_SCALA_USAGE_output: String = """+-----------+
|area       |
+-----------+
|1000000.0  |
+-----------+

+-------+-------+----------+
|lat    |lon    |bng_cell  |
+-------+-------+----------+
|51.5074|-0.1278|TQ 30 80  |
+-------+-------+----------+"""

}
