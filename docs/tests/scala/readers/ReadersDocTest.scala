package tests.docs.scala.readers

import com.databricks.labs.gbx.docs.readers.FileGDBExamples
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._
import tests.docs.scala.SampleDataPath

/**
  * Tests for reader code examples in documentation.
  *
  * These tests verify that documented reader patterns work with actual sample data.
  */
class ReadersDocTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = SparkSession.builder()
      .appName("Readers Doc Test")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) spark.stop()
    super.afterAll()
  }

  // ============================================================================
  // SHAPEFILE READER TESTS
  // ============================================================================

  test("shapefile: read basic") {
    val df = ShapefileExamples.readShapefile(spark)
    df should not be null
    df.count() should be >= 1L  // full bundle 2000+; minimal 10
    df.columns should contain("geom_0")
  }

  test("shapefile: read with chunk size option") {
    val df = ShapefileExamples.readWithOptions(spark)
    df should not be null
    df.count() should be >= 1L  // full bundle 2000+; minimal 10
  }

  test("shapefile: constants defined") {
    ShapefileExamples.READ_SHAPEFILE should not be empty
    ShapefileExamples.READ_SHAPEFILE_output should not be empty
    ShapefileExamples.READ_WITH_OPTIONS should not be empty
    ShapefileExamples.SQL_SHAPEFILE should not be empty
  }

  // ============================================================================
  // GEOJSON READER TESTS
  // ============================================================================

  test("geojson: read standard format") {
    val df = GeoJSONExamples.readGeoJSON(spark)
    df should not be null
    df.count() should be >= 1L
    df.count() should be <= 10L  // full bundle 5; minimal 1
    df.columns should contain("geom_0")
  }

  test("geojson: read GeoJSONSeq format") {
    val df = GeoJSONExamples.readGeoJSONSeq(spark)
    df should not be null
    df.count() should be >= 1L
    df.count() should be <= 10L  // full bundle 5; minimal 1
    df.columns should contain("geom_0")
  }

  test("geojson: constants defined") {
    GeoJSONExamples.READ_GEOJSON should not be empty
    GeoJSONExamples.READ_GEOJSON_output should not be empty
    GeoJSONExamples.READ_GEOJSONSEQ should not be empty
    GeoJSONExamples.SQL_GEOJSON should not be empty
  }

  // ============================================================================
  // GEOPACKAGE READER TESTS
  // ============================================================================

  test("geopackage: read basic") {
    val df = GeoPackageExamples.readGeoPackage(spark)
    df should not be null
    df.count() should be > 0L
    // GeoPackage uses 'shape' column for geometry
    df.columns.exists(c => c.toLowerCase.contains("shape") || c.toLowerCase.contains("geom")) shouldBe true
  }

  test("geopackage: read specific layer") {
    val df = GeoPackageExamples.readSpecificLayer(spark)
    df should not be null
    df.count() should be >= 1L
    df.count() should be <= 10L  // full bundle 5 boroughs; minimal 1
  }

  test("geopackage: constants defined") {
    GeoPackageExamples.READ_GEOPACKAGE should not be empty
    GeoPackageExamples.READ_GEOPACKAGE_output should not be empty
    GeoPackageExamples.READ_SPECIFIC_LAYER should not be empty
    GeoPackageExamples.SQL_GEOPACKAGE should not be empty
  }

  // ============================================================================
  // OGR READER TESTS
  // ============================================================================

  test("ogr: read basic") {
    val df = OGRExamples.readOGR(spark)
    df should not be null
    df.count() should be >= 1L
    df.count() should be <= 10L  // full bundle 5; minimal 1
    df.columns should contain("geom_0")
  }

  test("ogr: read with driver name") {
    val df = OGRExamples.readWithDriver(spark)
    df should not be null
    df.count() should be >= 1L
    df.count() should be <= 10L
  }

  test("ogr: constants defined") {
    OGRExamples.READ_OGR should not be empty
    OGRExamples.READ_OGR_output should not be empty
    OGRExamples.READ_WITH_DRIVER should not be empty
    OGRExamples.SQL_OGR should not be empty
  }

  // ============================================================================
  // FILEGDB READER TESTS
  // ============================================================================

  test("filegdb: read basic") {
    val path = new Path(FileGDBExamples.FILEGDB_ZIP_PATH)
    assume(path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path),
      "FileGDB sample data not present; add nyc/filegdb/NYC_Sample.gdb.zip")
    val df = FileGDBExamples.readFileGDB()(spark)
    df should not be null
    df.count() should be > 0L
    // FileGDB typically uses SHAPE for geometry
    df.columns.exists(c => c.toLowerCase.contains("shape") || c.toLowerCase.contains("geom")) shouldBe true
  }

  test("filegdb: read with layer") {
    val path = new Path(FileGDBExamples.FILEGDB_ZIP_PATH)
    assume(path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path),
      "FileGDB sample data not present; add nyc/filegdb/NYC_Sample.gdb.zip")
    val df = FileGDBExamples.readWithLayer("NYC_Boroughs")(spark)
    df should not be null
    // Note: Test may need adjustment based on actual feature classes in NYC_Sample.gdb
  }

  test("filegdb: constants defined") {
    FileGDBExamples.READ_FILEGDB should not be empty
    FileGDBExamples.READ_FILEGDB_output should not be empty
    FileGDBExamples.READ_WITH_LAYER should not be empty
    FileGDBExamples.SQL_FILEGDB should not be empty
  }

  // ============================================================================
  // GTIFF READER TESTS
  // ============================================================================

  test("gtiff: read basic") {
    val path = new Path(SampleDataPath.nycSentinel2)
    assume(path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path),
      "GeoTIFF sample not present; add nyc/sentinel2 or set GBX_SAMPLE_DATA_ROOT")
    val df = GTiffExamples.readGTiff(spark)
    df should not be null
    val count = df.count()
    assume(count > 0L, "No raster rows; use full bundle or generate minimal bundle")
    df.columns should contain("tile")
  }

  test("gtiff: read with options") {
    val path = new Path(SampleDataPath.nycSentinel2)
    assume(path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path),
      "GeoTIFF sample not present; add nyc/sentinel2 or set GBX_SAMPLE_DATA_ROOT")
    val df = GTiffExamples.readWithOptions(spark)
    df should not be null
    val count = df.count()
    assume(count > 0L, "No raster rows; use full bundle or generate minimal bundle")
  }

  test("gtiff: constants defined") {
    GTiffExamples.READ_GTIFF should not be empty
    GTiffExamples.READ_GTIFF_output should not be empty
    GTiffExamples.READ_WITH_OPTIONS should not be empty
    GTiffExamples.SQL_GTIFF should not be empty
  }

  // ============================================================================
  // GDAL READER TESTS
  // ============================================================================

  test("gdal: read basic") {
    val path = new Path(SampleDataPath.nycSentinel2)
    assume(path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path),
      "Raster sample not present; add nyc/sentinel2 or set GBX_SAMPLE_DATA_ROOT")
    val df = GDALExamples.readGDAL(spark)
    df should not be null
    val count = df.count()
    assume(count > 0L, "No raster rows; use full bundle or generate minimal bundle")
    df.columns should contain("tile")
  }

  test("gdal: read with driver") {
    val path = new Path(SampleDataPath.nycSentinel2)
    assume(path.getFileSystem(spark.sparkContext.hadoopConfiguration).exists(path),
      "Raster sample not present; add nyc/sentinel2 or set GBX_SAMPLE_DATA_ROOT")
    val df = GDALExamples.readWithDriver(spark)
    df should not be null
    val count = df.count()
    assume(count > 0L, "No raster rows; use full bundle or generate minimal bundle")
  }

  test("gdal: constants defined") {
    GDALExamples.READ_GDAL should not be empty
    GDALExamples.READ_GDAL_output should not be empty
    GDALExamples.READ_WITH_DRIVER should not be empty
    GDALExamples.SQL_GDAL should not be empty
  }
}
