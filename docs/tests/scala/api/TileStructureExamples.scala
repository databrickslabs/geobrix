/*
 * Scala Tile Structure examples - Single source of truth for docs/docs/api/tile-structure.mdx § Working with Tiles (Scala).
 * Displayed via CodeFromTest; validated by TileStructureExamplesDocTest.
 */
package docs.tests.scala.api

object TileStructureExamples {

  /** Accessing Tile Fields in Scala (sample-data path). */
  val ACCESSING_TILE_FIELDS_SCALA: String =
    """import org.apache.spark.sql.functions._
import com.databricks.labs.gbx.rasterx.{functions => rx}

val df = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")

// Access individual fields
df.select(
  col("tile.cellid"),
  col("tile.raster"),
  col("tile.metadata"),
  col("tile.metadata.driver")
).show()"""

  /** Example output for ACCESSING_TILE_FIELDS_SCALA (same shape as Python/SQL). */
  val ACCESSING_TILE_FIELDS_SCALA_output: String =
    """+------+--------+------------------+-------+
|cellid|raster  |metadata          |driver |
+------+--------+------------------+-------+
|null  |[BINARY]|{driver=GTiff,...}|GTiff  |
+------+--------+------------------+-------+"""

}
