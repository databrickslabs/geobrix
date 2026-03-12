# GeoBriX Scala Tests

This directory contains Scala tests for the GeoBriX library using ScalaTest.

## Test Structure

GeoBriX has two types of Scala tests:

### 1. Execute Tests (Lower-Level)
Tests that directly test operation classes and utilities **without Spark**. These test the core GDAL operations and business logic.

**Pattern**: `*Test.scala` or `*ExecuteTest.scala`

**Examples**:
- `RST_AccessorsExecuteTest` - Tests accessor operations directly on GDAL datasets
- `ClipToGeomTest` - Tests ClipToGeom operation logic
- `KernelFilterTest` - Tests KernelFilter operation logic
- `RasterProjectTest` - Tests RasterProject operation logic

### 2. Eval Tests (Spark Execution)
Tests that test expressions **through Spark execution**. These test the end-to-end Spark SQL integration.

**Pattern**: `*EvalTest.scala`

**Examples**:
- `RST_AccessorsEvalTest` - Tests accessor expressions via Spark DataFrames
- `RST_TransformationsEvalTest` - Tests clip, filter, transform via Spark
- `BNG_ExpressionEvalTest` - Tests BNG expressions via Spark

## Directory Structure

```
src/test/scala/
├── README.md (this file)
├── com/databricks/labs/gbx/
│   ├── ds/
│   │   ├── RegisterDSTest.scala           # Data source registration tests
│   │   └── WhitelistDSTest.scala          # Whitelist data source tests
│   ├── expressions/
│   │   └── CoreClassesTest.scala          # Core expression classes
│   ├── gridx/bng/
│   │   ├── BNG_ExpressionEvalTest.scala   # BNG Spark execution tests
│   │   └── BNG_ExpressionExecuteTest.scala # BNG operation tests
│   ├── rasterx/
│   │   ├── ds/
│   │   │   └── GDAL_DataSourceTest.scala  # GDAL data source tests
│   │   ├── expressions/
│   │   │   ├── RST_AccessorsEvalTest.scala          # Accessor Spark tests
│   │   │   ├── RST_AccessorsExecuteTest.scala       # Accessor operation tests
│   │   │   ├── RST_AggEvalTest.scala                # Aggregation Spark tests
│   │   │   ├── RST_ConstructorsEvalTest.scala       # Constructor Spark tests
│   │   │   ├── RST_ConstructorsExecuteTest.scala    # Constructor operation tests
│   │   │   ├── RST_ExpressionEvalTest.scala         # Expression Spark tests
│   │   │   ├── RST_ExpressionExecuteTest.scala      # Expression operation tests
│   │   │   ├── RST_GeneratorsEvalTest.scala         # Generator Spark tests
│   │   │   ├── RST_GridEvalTest.scala               # Grid Spark tests
│   │   │   ├── RST_GridExecuteTest.scala            # Grid operation tests
│   │   │   └── RST_TransformationsEvalTest.scala    # ✨ NEW: Transformations (clip, filter, transform)
│   │   ├── operations/
│   │   │   ├── ClipToGeomTest.scala         # ✨ NEW: Clip operation tests
│   │   │   ├── KernelFilterTest.scala       # ✨ NEW: Filter operation tests
│   │   │   └── RasterProjectTest.scala      # ✨ NEW: Projection operation tests
│   │   ├── GDALOperationsTest.scala         # GDAL operations tests
│   │   └── RasterDebugger.scala             # Debugging utilities
│   ├── vectorx/
│   │   ├── ds/
│   │   │   ├── OGR_DataSourceTest.scala     # OGR data source tests
│   │   │   └── OGRInferenceTest.scala       # OGR type inference tests
│   │   └── jts/
│   │       ├── JTSTest.scala                # JTS tests
│   │       └── ST_LegacyTest.scala          # Legacy ST functions tests
│   └── udfs.scala                            # UDF registration
└── org/apache/spark/sql/test/
    └── SilentSparkSession.scala              # Test SparkSession trait
```

## Running Tests

### Run All Tests

```bash
mvn test
```

### Run Specific Test Class

```bash
# Operation tests (Execute)
mvn test -Dtest=ClipToGeomTest
mvn test -Dtest=KernelFilterTest
mvn test -Dtest=RasterProjectTest

# Expression tests (Eval)
mvn test -Dtest=RST_TransformationsEvalTest
mvn test -Dtest=RST_AccessorsEvalTest
mvn test -Dtest=BNG_ExpressionEvalTest
```

### Run Specific Test Method

```bash
mvn test -Dtest=ClipToGeomTest#"ClipToGeom should clip raster to geometry"
```

### Run with Specific Profile

```bash
mvn test -P scala-2.13
```

### Run with Coverage

This project uses **Scoverage** (not JaCoCo) for Scala coverage. From the repo root:

```bash
mvn clean scoverage:test
mvn scoverage:report-only -Dscoverage.aggregate=true -Dscoverage.aggregateOnly=true
```

Or use the Cursor command: `gbx:coverage:scala` (runs in Docker with the same flow).

## Test Traits and Utilities

### SilentSparkSession
Trait that provides a SparkSession with reduced logging for tests.

```scala
class MyTest extends PlanTest with SilentSparkSession {
  test("my test") {
    val df = spark.range(10)
    // ...
  }
}
```

### PlanTest
ScalaTest trait from Spark for testing query plans.

### BeforeAndAfterAll
ScalaTest trait for setup/teardown:

```scala
class MyTest extends AnyFunSuite with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    // Setup (e.g., initialize GDAL)
  }
  
  override def afterAll(): Unit = {
    // Cleanup (e.g., delete datasets)
  }
}
```

## Test Resources

Test data is located in `src/test/resources/`:

### Raster Data
- `modis/*.TIF` - MODIS satellite imagery (single-band GeoTIFF)
- `binary/geotiff-small/*.tif` - Small test GeoTIFFs
- `binary/netcdf-CMIP5/*.nc` - NetCDF climate data
- `binary/netcdf-coral/*.nc` - Coral reef NetCDF data
- `binary/netcdf-ECMWF/*.nc` - ECMWF forecast data
- `binary/grib-cams/*.grb` - GRIB format data

### Vector Data
- `binary/shapefile/*.shp` - Shapefiles
- `binary/gdb/*.zip` - File geodatabases
- `binary/gpkg/*.gpkg` - GeoPackage files
- `text/*.geojson` - GeoJSON files

## Phase 1 New Tests (2025)

### Operation Tests (Execute Level)

**ClipToGeomTest** (6 tests):
- Basic clipping functionality
- Small geometry handling
- Parameter variations
- Property preservation

**KernelFilterTest** (11 tests):
- All filter operations (avg, median, mode, max, min)
- Multiple kernel sizes
- Property preservation
- Error handling

**RasterProjectTest** (10 tests):
- Multiple CRS transformations
- Property preservation
- Dimension validation
- Options handling

### Expression Tests (Spark Level)

**RST_TransformationsEvalTest** (15 tests):
- RST_Clip with WKT and WKB
- RST_Filter with multiple operations
- RST_Transform to various CRS
- Combined workflows
- Binary content handling

## Writing New Tests

### Execute Test Template

```scala
package com.databricks.labs.gbx.rasterx.operations

import com.databricks.labs.gbx.rasterx.gdal.GDALManager
import org.gdal.gdal.{Dataset, gdal}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers._

class MyOperationTest extends AnyFunSuite with BeforeAndAfterAll {

    var ds: Dataset = _

    override def beforeAll(): Unit = {
        GDALManager.loadSharedObjects(Iterable.empty[String])
        GDALManager.configureGDAL("/tmp", "/tmp")
        gdal.AllRegister()
        val tifPath = this.getClass.getResource("/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF").toString.replace("file:/", "/")
        ds = gdal.Open(tifPath)
    }

    override def afterAll(): Unit = {
        ds.delete()
    }

    test("MyOperation should do something") {
        val (resultDs, metadata) = MyOperation.execute(ds, params)
        
        resultDs should not be null
        metadata should not be null
        // ... more assertions
        
        resultDs.delete()
    }
}
```

### Eval Test Template

```scala
package com.databricks.labs.gbx.rasterx.expressions

import com.databricks.labs.gbx.rasterx.functions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SilentSparkSession
import org.scalatest.matchers.should.Matchers._

class MyExpressionEvalTest extends PlanTest with SilentSparkSession {

    test("MyExpression should work via Spark") {
        val sc = spark
        import com.databricks.labs.gbx.rasterx.functions._
        import sc.implicits._
        functions.register(spark)

        val tifPath = this.getClass.getResource("/modis/").toString

        val df: DataFrame = Seq(
          (1, s"$tifPath/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")
        ).toDF("id", "path")
            .withColumn("raster", rst_fromfile(col("path"), lit("GTiff")))
            .withColumn("result", my_expression(col("raster"), lit(param)))

        noException should be thrownBy df.collect()
        val result = df.select("result").collect()
        result should not be empty
    }
}
```

## Environment Setup

Tests require:
1. GDAL natives properly configured
2. Hadoop natives for distributed file operations
3. Sufficient memory for raster processing

Set environment variables:
```bash
export LD_LIBRARY_PATH=/usr/local/lib:/usr/local/hadoop/lib/native:$LD_LIBRARY_PATH
```

## Debugging Tests

### Enable Debug Logging

Edit test to use different log level:
```scala
spark.sparkContext.setLogLevel("DEBUG")
```

### Use RasterDebugger

For visual debugging of rasters:
```scala
import com.databricks.labs.gbx.rasterx.RasterDebugger

RasterDebugger.printRasterInfo(ds)
RasterDebugger.visualizeRaster(ds, band = 1)
```

## Coverage Goals

The project aims for **80% code coverage** (see `pom.xml`):
```xml
<minimum.coverage>80</minimum.coverage>
```

## Continuous Integration

Tests run automatically on CI/CD pipelines. Ensure:
- All tests pass locally before pushing
- Tests are deterministic (no random failures)
- Resources are properly cleaned up
- Tests run in reasonable time (<5 minutes per test class)

## Contact

For questions about tests:
- See main README: `/README.md`
- See test coverage summary: `/TEST_COVERAGE_PHASE1_SUMMARY.md`
- Check existing test patterns in similar test files

