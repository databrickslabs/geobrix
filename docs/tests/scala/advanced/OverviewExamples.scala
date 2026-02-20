/**
 * Scala code examples for docs/docs/advanced/overview.mdx.
 * Single source of truth: displayed via CodeFromTest; validated by OverviewDocTest.
 */
package tests.docs.scala.advanced

object OverviewExamples {

  /** Execute methods example (docs/docs/advanced/overview.mdx § Execute Methods). */
  val EXECUTE_METHODS_EXAMPLE: String =
    """import com.databricks.labs.gbx.rasterx.expressions.accessors.RST_BoundingBox
import org.gdal.gdal.Dataset

// Direct GDAL dataset manipulation
val bbox = RST_BoundingBox.execute(dataset)"""

}
