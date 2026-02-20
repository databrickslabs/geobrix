/**
 * Scala code examples for docs/docs/advanced/custom-udfs.mdx.
 * Single source of truth: displayed via CodeFromTest; validated by CustomUdfsDocTest.
 */
package tests.docs.scala.advanced

object CustomUdfExamples {

  val EXECUTE_METHODS_EXAMPLE: String =
    """import com.databricks.labs.gbx.rasterx.expressions.accessors.RST_BoundingBox
import org.gdal.gdal.Dataset

// Direct GDAL dataset access
val bbox = RST_BoundingBox.execute(dataset)"""

  val SCALA_UDF_EXAMPLE: String =
    """import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import com.databricks.labs.gbx.rasterx.expressions.accessors._
import com.databricks.labs.gbx.rasterx.gdal.RasterDriver
import org.gdal.gdal.Dataset

object CustomRasterUDFs {

  /**
   * Extract custom statistics from raster
   */
  def customRasterStats: UserDefinedFunction = udf((tileBytes: Array[Byte]) => {
    try {
      // Read Dataset from binary raster data
      val ds: Dataset = RasterDriver.readFromBytes(tileBytes, Map.empty[String, String])
      
      // Use execute methods to get statistics
      val width = RST_Width.execute(ds)
      val height = RST_Height.execute(ds)
      val numBands = RST_NumBands.execute(ds)
      
      // Custom calculation
      val totalPixels = width * height * numBands
      val pixelWidth = RST_PixelWidth.execute(ds)
      val pixelHeight = RST_PixelHeight.execute(ds)
      val coverage = width * pixelWidth * height * pixelHeight
      
      // Clean up
      RasterDriver.releaseDataset(ds)
      
      // Return custom result
      Map(
        "total_pixels" -> totalPixels.toString,
        "coverage_sqm" -> coverage.toString,
        "pixel_density" -> (totalPixels / coverage).toString
      )
    } catch {
      case e: Exception => Map("error" -> e.getMessage)
    }
  })

  /**
   * Custom bounding box with buffer
   */
  def boundingBoxWithBuffer(bufferMeters: Double): UserDefinedFunction = 
    udf((tileBytes: Array[Byte]) => {
      try {
        // Read Dataset from binary raster data
        val ds: Dataset = RasterDriver.readFromBytes(tileBytes, Map.empty[String, String])
        
        // Get bounding box using execute
        val bbox = RST_BoundingBox.execute(ds)
        
        // Apply buffer (custom logic)
        val buffered = bbox.buffer(bufferMeters)
        
        // Clean up
        RasterDriver.releaseDataset(ds)
        
        // Convert to WKB
        val wkb = buffered.getBinary
        
        wkb
      } catch {
        case e: Exception => Array.empty[Byte]
      }
    })

  /**
   * Filter rasters by custom criteria
   */
  def meetsQualityCriteria: UserDefinedFunction = udf((tileBytes: Array[Byte]) => {
    try {
      // Read Dataset from binary raster data
      val ds: Dataset = RasterDriver.readFromBytes(tileBytes, Map.empty[String, String])
      
      // Multiple execute calls for criteria
      val width = RST_Width.execute(ds)
      val height = RST_Height.execute(ds)
      val numBands = RST_NumBands.execute(ds)
      val band = ds.GetRasterBand(1)
      val noData = RST_GetNoData.execute(band)
      
      // Custom quality logic
      val validSize = width >= 512 && height >= 512
      val hasMultipleBands = numBands >= 3
      val hasNoDataValue = noData.isDefined
      
      validSize && hasMultipleBands && hasNoDataValue
    } catch {
      case _: Exception => false
    }
  })
}"""

  val CLOUD_MASK_EXAMPLE: String =
    """import com.databricks.labs.gbx.rasterx.expressions.accessors._
import org.apache.spark.sql.functions.udf

/**
 * Apply custom cloud masking logic based on multiple bands
 */
def applyCloudMask: UserDefinedFunction = udf((tileBytes: Array[Byte]) => {
  // Read Dataset from binary raster data
  val ds: Dataset = RasterDriver.readFromBytes(tileBytes, Map.empty[String, String])
  
  try {
    // Get band data using execute methods
    val band1 = ds.GetRasterBand(1)
    val band2 = ds.GetRasterBand(2)
    val band3 = ds.GetRasterBand(3)
    
    val width = RST_Width.execute(ds)
    val height = RST_Height.execute(ds)
    
    // Read pixel data
    val pixels1 = band1.ReadRaster(0, 0, width, height)
    val pixels2 = band2.ReadRaster(0, 0, width, height)
    val pixels3 = band3.ReadRaster(0, 0, width, height)
    
    // Apply custom cloud detection algorithm
    // (simplified example - actual algorithm would be more complex)
    val cloudMask = detectClouds(pixels1, pixels2, pixels3)
    
    // Create new raster with cloud mask applied
    val maskedRaster = applyMask(ds, cloudMask)
    
    // Write to bytes and return
    val result = RasterDriver.writeToBytes(maskedRaster, Map.empty[String, String])
    RasterDriver.releaseDataset(maskedRaster)
    result
  } finally {
    RasterDriver.releaseDataset(ds)
  }
})"""

  val MULTI_TEMPORAL_EXAMPLE: String =
    """/**
 * Compare rasters from different time periods
 */
def calculateNDVIChange: UserDefinedFunction = 
  udf((before: Array[Byte], after: Array[Byte]) => {
    // Read Datasets from binary raster data
    val dsBefore: Dataset = RasterDriver.readFromBytes(before, Map.empty[String, String])
    val dsAfter: Dataset = RasterDriver.readFromBytes(after, Map.empty[String, String])
    
    try {
      // Extract NIR and Red bands using execute methods
      val nirBefore = dsBefore.GetRasterBand(4)
      val redBefore = dsBefore.GetRasterBand(3)
      val nirAfter = dsAfter.GetRasterBand(4)
      val redAfter = dsAfter.GetRasterBand(3)
      
      // Calculate NDVI for both periods
      val ndviBefore = calculateNDVI(nirBefore, redBefore)
      val ndviAfter = calculateNDVI(nirAfter, redAfter)
      
      // Calculate change
      val change = ndviAfter - ndviBefore
      
      // Return statistics
      Map(
        "mean_change" -> change.mean.toString,
        "max_gain" -> change.max.toString,
        "max_loss" -> change.min.toString,
        "percent_improved" -> (change.filter(_ > 0.1).count.toDouble / change.size * 100).toString
      )
    } finally {
      RasterDriver.releaseDataset(dsBefore)
      RasterDriver.releaseDataset(dsAfter)
    }
  })"""

  val CUSTOM_FORMAT_EXAMPLE: String =
    """/**
 * Handle proprietary raster format
 */
def processProprietaryFormat: UserDefinedFunction = 
  udf((filePath: String) => {
    try {
      // Use GDAL's flexible driver system
      val ds = gdal.Open(filePath)
      
      // Extract metadata using execute methods
      val metadata = RST_MetaData.execute(ds)
      
      // Apply domain-specific interpretation
      val calibrationFactor = metadata.getOrElse("CAL_FACTOR", "1.0").toDouble
      val sensorType = metadata.getOrElse("SENSOR", "unknown")
      
      // Get band data
      val band = ds.GetRasterBand(1)
      val width = RST_Width.execute(ds)
      val height = RST_Height.execute(ds)
      
      // Read and calibrate
      val pixels = band.ReadRaster(0, 0, width, height)
      val calibrated = applyCalibration(pixels, calibrationFactor, sensorType)
      
      // Create calibrated raster
      val output = createCalibratedDataset(calibrated, width, height, ds)
      
      // Write to bytes
      val result = RasterDriver.writeToBytes(output, Map.empty[String, String])
      RasterDriver.releaseDataset(ds)
      RasterDriver.releaseDataset(output)
      result
    } catch {
      case e: Exception => 
        log.error(s"Failed to process $filePath: ${e.getMessage}")
        Array.empty[Byte]
    }
  })"""

  val RESOURCE_MANAGEMENT_PATTERN: String =
    """def safeExecute[T](f: Dataset => T): UserDefinedFunction = udf((bytes: Array[Byte]) => {
  // Read Dataset from binary raster data
  val ds: Dataset = RasterDriver.readFromBytes(bytes, Map.empty[String, String])
  try {
    f(ds)
  } finally {
    RasterDriver.releaseDataset(ds)  // Always clean up!
  }
})"""

  val ERROR_HANDLING_PATTERN: String =
    """@udf
def robustUDF(bytes: Array[Byte]): Option[String] = {
  try {
    val ds = loadDataset(bytes)
    val result = RST_Format.execute(ds)
    ds.delete()
    Some(result)
  } catch {
    case e: Exception =>
      log.warn(s"UDF failed: ${e.getMessage}")
      None
  }
}"""

  val PERFORMANCE_PATTERN: String =
    """def efficientBatchUDF: UserDefinedFunction = udf((bytes: Array[Byte]) => {
  val ds = loadDataset(bytes)
  
  try {
    // Single dataset load, multiple operations
    val results = Map(
      "format" -> RST_Format.execute(ds),
      "width" -> RST_Width.execute(ds).toString,
      "height" -> RST_Height.execute(ds).toString,
      "bands" -> RST_NumBands.execute(ds).toString,
      "srid" -> RST_SRID.execute(ds).toString
    )
    results
  } finally {
    ds.delete()
  }
})"""

  val TYPE_SAFETY_PATTERN: String =
    """import org.apache.spark.sql.types._

val schema = StructType(Seq(
  StructField("width", IntegerType, nullable = false),
  StructField("height", IntegerType, nullable = false),
  StructField("aspect_ratio", DoubleType, nullable = false)
))

spark.udf.register("raster_dims", 
  (bytes: Array[Byte]) => {
    val ds = loadDataset(bytes)
    val w = RST_Width.execute(ds)
    val h = RST_Height.execute(ds)
    ds.delete()
    (w, h, w.toDouble / h.toDouble)
  }, 
  schema
)"""

  // Example output for docs (CodeFromTest outputConstant) - Scala UDF Example
  val SCALA_UDF_EXAMPLE_output: String =
    """+--------------------+------------------------------------------+
|path                |custom_stats                              |
+--------------------+------------------------------------------+
|.../raster.tif      |{total_pixels=..., coverage_sqm=..., ...} |
+--------------------+------------------------------------------+"""

  val CLOUD_MASK_EXAMPLE_output: String =
    """+--------------------+-----------+
|path                |masked_tile|
+--------------------+-----------+
|.../multiband.tif   |[BINARY]   |
+--------------------+-----------+"""

  val MULTI_TEMPORAL_EXAMPLE_output: String =
    """+--------------------+--------------------+----------------------------------+
|before              |after               |change_stats                      |
+--------------------+--------------------+----------------------------------+
|.../t1.tif          |.../t2.tif          |{mean_change=..., percent_impr...}|
+--------------------+--------------------+----------------------------------+"""

  val CUSTOM_FORMAT_EXAMPLE_output: String =
    """+------------------+---------------+
|file_path         |calibrated_tile|
+------------------+---------------+
|/data/custom.xyz  |[BINARY]       |
+------------------+---------------+"""

  val RESOURCE_MANAGEMENT_PATTERN_output: String =
    """// safeExecute ensures ds is always released
// Example: width = safeExecute(ds => RST_Width.execute(ds))(bytes)"""

  val ERROR_HANDLING_PATTERN_output: String =
    """+--------------------+------------------+
|tile                |result            |
+--------------------+------------------+
|[BINARY]            |Some(GTiff)       |
|[BINARY]            |None              |
+--------------------+------------------+"""

  val PERFORMANCE_PATTERN_output: String =
    """+--------------------+----------------------------------------+
|tile                |results                                 |
+--------------------+----------------------------------------+
|[BINARY]            |{format=GTiff, width=1024, height=1...} |
+--------------------+----------------------------------------+"""

  val TYPE_SAFETY_PATTERN_output: String =
    """+--------------------+-----+------+------------+
|tile                |width|height|aspect_ratio|
+--------------------+-----+------+------------+
|[BINARY]            |1024 |1024  |1.0         |
+--------------------+-----+------+------------+"""
}
