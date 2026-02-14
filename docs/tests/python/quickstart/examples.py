"""
Quick Start Examples - Documentation Code

This module contains tested code examples for the GeoBrix Quick Start guide.
Display snippets (constants) are shown in the docs; test functions verify they work.

Documentation: docs/docs/quick-start.mdx
- Tested by: docs/tests/python/quickstart/test_examples.py

Usage: CodeFromTest uses functionName="REGISTER_RASTERX" etc. to show snippet constants.
"""

# =============================================================================
# DISPLAY CODE (snippets shown in docs - copy-paste executable with sample data)
# =============================================================================
# Paths match Sample Data guide. See docs/docs/sample-data.mdx.

REGISTER_RASTERX = """# Register RasterX functions (required for gbx_rst_* in SQL)
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)"""

REGISTER_GRIDX = """# Register GridX BNG functions (required for gbx_bng_* in SQL)
from databricks.labs.gbx.gridx.bng import functions as bx
bx.register(spark)"""

REGISTER_VECTORX = """# Register VectorX functions (required for gbx_st_* in SQL)
from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
vx.register(spark)"""

# Scala snippet (quick-start § Scala); same content as Python register, in Scala.
REGISTER_RASTERX_SCALA = """import com.databricks.labs.gbx.rasterx.{functions => rx}
rx.register(spark)"""

READ_GEOTIFF = """# Read GeoTIFF rasters (GDAL reader)
rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
rasters.limit(3).show()"""

READ_SHAPEFILE = """# Read shapefile (supports .zip)
shapes = spark.read.format("shapefile_ogr").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip")
shapes.limit(3).show()"""

READ_GEOJSON = """# Read GeoJSON
geojson_df = spark.read.format("geojson_ogr").option("multi", "false").load(
    "/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/taxi-zones/nyc_taxi_zones.geojson"
)
geojson_df.limit(3).show()"""

USE_RASTERX = """# Register, load raster, apply RasterX
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)
rasters = spark.read.format("gdal").load("/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/sentinel2/nyc_sentinel2_red.tif")
rasters.select(rx.rst_boundingbox("tile").alias("bbox"), rx.rst_width("tile"), rx.rst_height("tile")).limit(3).show()"""

USE_BNG = """# Register GridX BNG, then use in SQL (gbx_bng_cellarea returns square kilometres)
from databricks.labs.gbx.gridx.bng import functions as bx
bx.register(spark)
spark.sql("SELECT gbx_bng_cellarea('TQ3080') as area_km2").show()"""

USE_VECTORX = '''# Register VectorX, create legacy point struct, convert to WKB
from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
from pyspark.sql import Row
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StructField, StructType
vx.register(spark)
legacy_schema = StructType([
    StructField("typeId", IntegerType()),
    StructField("srid", IntegerType()),
    StructField("boundaries", ArrayType(ArrayType(ArrayType(DoubleType())))),
    StructField("holes", ArrayType(ArrayType(ArrayType(ArrayType(DoubleType()))))),
])
row = Row(geom_legacy=(1, 0, [[[30.0, 10.0]]], []))
shapes = spark.createDataFrame([row], StructType([StructField("geom_legacy", legacy_schema)]))
shapes.select(vx.st_legacyaswkb("geom_legacy").alias("wkb")).show()'''

SQL_LIST_FUNCTIONS = """-- List GeoBrix functions
SHOW FUNCTIONS LIKE 'gbx_rst_*';
SHOW FUNCTIONS LIKE 'gbx_bng_*';
SHOW FUNCTIONS LIKE 'gbx_st_*';"""

SQL_DESCRIBE = """-- Describe a function
DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox;"""

SQL_READ_AND_USE = """-- Read shapefile and query in SQL
CREATE OR REPLACE TEMP VIEW my_shapes AS
SELECT * FROM shapefile_ogr.`/Volumes/main/default/geobrix_samples/geobrix-examples/nyc/subway/nyc_subway.shp.zip`;

SELECT * FROM my_shapes LIMIT 3;"""

# =============================================================================
# EXAMPLE OUTPUT (for docs "Example output" block via CodeFromTest outputConstant)
# Run snippets with sample data to capture; update when paths or output change.
# =============================================================================

READ_GEOTIFF_output = """
+--------------------+----+-----+------+
|source              |bbox|width|height|
+--------------------+----+-----+------+
|.../nyc/sentinel2/..|... |10980|10980 |
+--------------------+----+-----+------+
"""

READ_SHAPEFILE_output = """
+----+--------+-----+
|path|geom_0  |...  |
+----+--------+-----+
|... |[BINARY]|...  |
+----+--------+-----+
"""

READ_GEOJSON_output = """
+----------+--------+-----+
|path      |geom_0  |...  |
+----------+--------+-----+
|...       |[BINARY]|...  |
+----------+--------+-----+
"""

USE_RASTERX_output = """
+--------------------+-----+------+
|bbox                |width|height|
+--------------------+-----+------+
|POLYGON ((...))     |10980|10980 |
+--------------------+-----+------+
"""

USE_BNG_output = """
+----------+
|area_km2  |
+----------+
|1.0       |
+----------+
"""

USE_VECTORX_output = """
+-----------+
|wkb        |
+-----------+
|[BINARY]   |
+-----------+
"""

SQL_LIST_FUNCTIONS_output = """
+--------------------+
|function            |
+--------------------+
|gbx_rst_asformat    |
|gbx_rst_avg         |
|gbx_rst_bandmetadata|
...
"""

SQL_DESCRIBE_output = """
-DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox
Function: gbx_rst_boundingbox
Type: ...
"""

SQL_READ_AND_USE_output = """
+----+--------+-----+
|path|geom_0  |...  |
+----+--------+-----+
|... |[BINARY]|...  |
+----+--------+-----+
"""

# =============================================================================
# Conditional imports for testability
# =============================================================================
try:
    from pyspark.sql import SparkSession
    from databricks.labs.gbx.rasterx import functions as rx
    from databricks.labs.gbx.gridx.bng import functions as bx
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    from pyspark.sql.functions import expr
except ImportError:
    SparkSession = None
    rx = None
    bx = None
    vx = None
    expr = None


def register_functions(spark=None):
    """
    Register GeoBrix functions with Spark.
    
    This is the first step to using GeoBrix functions in your Spark session.
    You do not need to register functions if you are only using the included readers.
    
    Parameters:
        spark: SparkSession instance (optional, uses active session if None)
    
    Returns:
        None - Functions are registered in the Spark session
    
    Example:
        >>> from databricks.labs.gbx.rasterx import functions as rx
        >>> 
        >>> # Register RasterX functions with Spark
        >>> rx.register(spark)
    
    See Also:
        - register_gridx_functions: Register BNG grid functions
        - register_vectorx_functions: Register vector functions
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
    
    if rx is not None:
        rx.register(spark)


def register_gridx_functions(spark=None):
    """
    Register GridX BNG functions with Spark.
    
    Parameters:
        spark: SparkSession instance (optional, uses active session if None)
    
    Returns:
        None - Functions are registered in the Spark session
    
    Example:
        >>> from databricks.labs.gbx.gridx.bng import functions as bx
        >>> 
        >>> # Register BNG functions
        >>> bx.register(spark)
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
    
    if bx is not None:
        bx.register(spark)


def register_vectorx_functions(spark=None):
    """
    Register VectorX functions with Spark.
    
    Parameters:
        spark: SparkSession instance (optional, uses active session if None)
    
    Returns:
        None - Functions are registered in the Spark session
    
    Example:
        >>> from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
        >>> 
        >>> # Register VectorX functions
        >>> vx.register(spark)
    """
    if spark is None:
        spark = SparkSession.getActiveSession()
    
    if vx is not None:
        vx.register(spark)


def read_geotiff_files(spark, path="/path/to/geotiff/files"):
    """
    Read GeoTiff raster files using the GDAL reader.
    
    Parameters:
        spark: SparkSession instance
        path: Path to GeoTiff files
    
    Returns:
        DataFrame with raster tile data
    
    Example:
        >>> # Read GeoTiff raster files
        >>> df = (
        ...     spark
        ...     .read
        ...     .format("gdal")
        ...     .load("/path/to/geotiff/files")
        ... )
        >>> 
        >>> df.show()
    
    See Also:
        - read_shapefiles: Read vector shapefiles
        - read_geojson: Read GeoJSON files
    """
    return (
        spark
        .read
        .format("gdal")
        .load(path)
    )


def read_shapefiles(spark, path="/path/to/shapefiles"):
    """
    Read shapefile vector data.
    
    Parameters:
        spark: SparkSession instance
        path: Path to shapefiles
    
    Returns:
        DataFrame with geometry data
    
    Example:
        >>> # Read shapefiles
        >>> df = (
        ...     spark
        ...     .read
        ...     .format("shapefile_ogr")
        ...     .load("/path/to/shapefiles")
        ... )
        >>> 
        >>> df.show()
    
    See Also:
        - read_geotiff_files: Read raster data
        - read_geopackage: Read GeoPackage files
    """
    return (
        spark
        .read
        .format("shapefile_ogr")
        .load(path)
    )


def read_geojson(spark, path="/path/to/geojson/files", multi=False):
    """
    Read GeoJSON vector files.
    
    Parameters:
        spark: SparkSession instance
        path: Path to GeoJSON files
        multi: Whether to read multi-geometry features (default: False)
    
    Returns:
        DataFrame with GeoJSON data
    
    Example:
        >>> # Read GeoJSON files
        >>> df = (
        ...     spark
        ...     .read
        ...     .format("geojson_ogr")
        ...     .option("multi", "false")
        ...     .load("/path/to/geojson/files")
        ... )
        >>> 
        >>> df.show()
    
    See Also:
        - read_shapefiles: Read shapefile data
        - read_geopackage: Read GeoPackage files
    """
    return (
        spark
        .read
        .format("geojson_ogr")
        .option("multi", str(multi).lower())
        .load(path)
    )


def read_geopackage(spark, path="/path/to/packages"):
    """
    Read GeoPackage files.
    
    Parameters:
        spark: SparkSession instance
        path: Path to GeoPackage files
    
    Returns:
        DataFrame with GeoPackage data
    
    Example:
        >>> # Read GeoPackage files
        >>> df = spark.read.format("gpkg").load("/path/to/packages")
        >>> df.show()
    """
    return spark.read.format("gpkg").load(path)


def use_rasterx_functions(spark, path="/path/to/rasters"):
    """
    Demonstrate using RasterX functions on raster data.
    
    This example shows the complete workflow: read rasters, register functions,
    and apply RasterX operations.
    
    Parameters:
        spark: SparkSession instance
        path: Path to raster files
    
    Returns:
        DataFrame with bounding box information
    
    Example:
        >>> from databricks.labs.gbx.rasterx import functions as rx
        >>> 
        >>> # Register functions
        >>> rx.register(spark)
        >>> 
        >>> # Read raster data
        >>> raster_df = spark.read.format("gdal").load("/path/to/rasters")
        >>> 
        >>> # Get bounding box of rasters
        >>> result = raster_df.select(
        ...     rx.rst_boundingbox("tile").alias("bbox")
        ... )
        >>> 
        >>> result.show()
    
    See Also:
        - pattern_batch_processing: Process multiple rasters in parallel
    """
    if rx is None:
        raise ImportError("GeoBrix rasterx module not available")
    
    # Register functions
    rx.register(spark)
    
    # Read raster data
    raster_df = spark.read.format("gdal").load(path)
    
    # Get bounding box of rasters
    result = raster_df.select(
        rx.rst_boundingbox("tile").alias("bbox")
    )
    
    return result


def convert_to_databricks_geometry(spark, path="/path/to/shapefiles"):
    """
    Convert GeoBrix WKB output to Databricks GEOMETRY type.
    
    GeoBrix Beta outputs WKB or WKT formats. This example shows how to convert
    to Databricks built-in spatial types for using ST functions.
    
    Parameters:
        spark: SparkSession instance
        path: Path to shapefiles
    
    Returns:
        DataFrame with Databricks GEOMETRY column and spatial calculations
    
    Example:
        >>> # Read shapefile
        >>> df = spark.read.format("shapefile_ogr").load("/path/to/shapefiles")
        >>> 
        >>> # Convert WKB to Databricks GEOMETRY type
        >>> from pyspark.sql.functions import expr
        >>> 
        >>> geometry_df = df.select(
        ...     "*",
        ...     expr("st_geomfromwkb(geom_0)").alias("geometry")
        ... )
        >>> 
        >>> # Now you can use built-in ST functions
        >>> result = geometry_df.select(
        ...     "geometry",
        ...     expr("st_area(geometry)").alias("area"),
        ...     expr("st_length(geometry)").alias("length")
        ... )
        >>> 
        >>> result.show()
    
    See Also:
        - read_shapefiles: Read shapefile data
    """
    if expr is None:
        raise ImportError("PySpark SQL functions not available")
    
    # Read shapefile
    df = spark.read.format("shapefile_ogr").load(path)
    
    # Convert WKB to Databricks GEOMETRY type
    geometry_df = df.select(
        "*",
        expr("st_geomfromwkb(geom_0)").alias("geometry")
    )
    
    # Use built-in ST functions
    result = geometry_df.select(
        "geometry",
        expr("st_area(geometry)").alias("area"),
        expr("st_length(geometry)").alias("length")
    )
    
    return result


def pattern_read_process_convert(spark, path="/data/shapes"):
    """
    Pattern 1: Read → Process → Convert workflow.
    
    This common pattern demonstrates:
    1. Reading data with GeoBrix reader
    2. Processing with GeoBrix functions (if needed)
    3. Converting to Databricks types for further analysis
    
    Parameters:
        spark: SparkSession instance
        path: Path to shapefile data
    
    Returns:
        DataFrame with converted geometry
    
    Example:
        >>> # 1. Read data with GeoBrix reader
        >>> df = spark.read.format("shapefile_ogr").load("/data/shapes")
        >>> 
        >>> # 2. Process with GeoBrix functions
        >>> from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
        >>> vx.register(spark)
        >>> 
        >>> # 3. Convert to Databricks types for further analysis
        >>> result = df.select(
        ...     "*",
        ...     expr("st_geomfromwkb(geom_0)").alias("geometry")
        ... )
    
    See Also:
        - pattern_multi_format_reading: Read different formats
        - pattern_batch_processing: Process multiple files
    """
    if vx is None or expr is None:
        raise ImportError("GeoBrix or PySpark not available")
    
    # 1. Read data with GeoBrix reader
    df = spark.read.format("shapefile_ogr").load(path)
    
    # 2. Process with GeoBrix functions
    vx.register(spark)
    
    # 3. Convert to Databricks types for further analysis
    result = df.select(
        "*",
        expr("st_geomfromwkb(geom_0)").alias("geometry")
    )
    
    return result


def pattern_multi_format_reading(spark):
    """
    Pattern 2: Read different geospatial formats.
    
    GeoBrix supports multiple readers for different formats.
    This pattern shows how to read various format types.
    
    Parameters:
        spark: SparkSession instance
    
    Returns:
        Dictionary with DataFrames for each format
    
    Example:
        >>> # Read different formats
        >>> geotiffs = spark.read.format("gdal").load("/data/rasters")
        >>> shapefiles = spark.read.format("shapefile_ogr").load("/data/vectors")
        >>> geojson = spark.read.format("geojson_ogr").load("/data/json")
        >>> geopackage = spark.read.format("gpkg").load("/data/packages")
    
    See Also:
        - read_geotiff_files: Read raster data
        - read_shapefiles: Read vector shapefiles
        - read_geojson: Read GeoJSON files
        - read_geopackage: Read GeoPackage files
    """
    return {
        "geotiffs": spark.read.format("gdal").load("/data/rasters"),
        "shapefiles": spark.read.format("shapefile_ogr").load("/data/vectors"),
        "geojson": spark.read.format("geojson_ogr").load("/data/json"),
        "geopackage": spark.read.format("gpkg").load("/data/packages")
    }


def pattern_batch_processing(spark, path="/data/many_rasters"):
    """
    Pattern 3: Batch process multiple rasters in parallel.
    
    GeoBrix leverages Spark's distributed processing to handle
    multiple raster files efficiently in parallel.
    
    Parameters:
        spark: SparkSession instance
        path: Path to directory with many raster files
    
    Returns:
        DataFrame with raster catalog information
    
    Example:
        >>> from databricks.labs.gbx.rasterx import functions as rx
        >>> rx.register(spark)
        >>> 
        >>> # Process multiple rasters in parallel
        >>> rasters = spark.read.format("gdal").load("/data/many_rasters")
        >>> 
        >>> results = rasters.select(
        ...     "path",
        ...     rx.rst_boundingbox("tile").alias("bbox"),
        ...     rx.rst_metadata("tile").alias("metadata"),
        ...     rx.rst_width("tile").alias("width"),
        ...     rx.rst_height("tile").alias("height")
        ... )
        >>> 
        >>> results.write.mode("overwrite").saveAsTable("raster_catalog")
    
    See Also:
        - use_rasterx_functions: Basic RasterX usage
    """
    if rx is None:
        raise ImportError("GeoBrix rasterx module not available")
    
    # Register functions
    rx.register(spark)
    
    # Process multiple rasters in parallel (GDAL reader returns "source", not "path")
    rasters = spark.read.format("gdal").load(path)
    path_col = rasters["source"].alias("path") if "source" in rasters.columns else rasters["path"]
    results = rasters.select(
        path_col,
        rx.rst_boundingbox("tile").alias("bbox"),
        rx.rst_metadata("tile").alias("metadata"),
        rx.rst_width("tile").alias("width"),
        rx.rst_height("tile").alias("height")
    )
    return results


# SQL Examples (as constants for documentation)
SQL_LIST_FUNCTIONS = """
-- List all RasterX functions
SHOW FUNCTIONS LIKE 'gbx_rst_*';

-- List all GridX functions
SHOW FUNCTIONS LIKE 'gbx_bng_*';

-- List all VectorX functions
SHOW FUNCTIONS LIKE 'gbx_st_*';

-- List ALL GeoBrix functions
SHOW FUNCTIONS LIKE 'gbx_*';
"""

SQL_DESCRIBE_FUNCTION = """
-- Get function description
DESCRIBE FUNCTION EXTENDED gbx_rst_boundingbox;
"""

SQL_WORKING_WITH_SQL = """
-- Read a shapefile and query it
CREATE OR REPLACE TEMP VIEW my_shapes AS
SELECT * FROM shapefile_ogr.`/path/to/shapefiles`;

-- Use VectorX functions
SELECT 
    shape_id,
    gbx_st_legacyaswkb(geom_0) as geometry_wkb
FROM my_shapes;

-- Read GeoTiff and use RasterX functions
CREATE OR REPLACE TEMP VIEW my_rasters AS
SELECT * FROM gdal.`/path/to/geotiffs`;

SELECT
    tile_id,
    gbx_rst_boundingbox(tile) as bbox,
    gbx_rst_metadata(tile) as metadata
FROM my_rasters;
"""

SQL_CONVERSION = """
-- Read shapefile
CREATE OR REPLACE TEMP VIEW shapes AS
SELECT * FROM shapefile_ogr.`/path/to/shapefiles`;

-- Convert to GEOMETRY type
CREATE OR REPLACE TEMP VIEW shapes_with_geom AS
SELECT 
    *,
    st_geomfromwkb(geom_0) as geometry
FROM shapes;

-- Use built-in spatial functions
SELECT
    shape_id,
    st_area(geometry) as area,
    st_centroid(geometry) as centroid,
    st_envelope(geometry) as envelope
FROM shapes_with_geom;
"""


if __name__ == "__main__":
    """
    Example usage - demonstrates the quick start workflow.
    """
    from pyspark.sql import SparkSession
    
    # Create Spark session
    spark = SparkSession.builder.appName("GeoBrix Quick Start").getOrCreate()
    
    # Register functions
    register_functions(spark)
    
    # Read some data
    print("Reading GeoTiff files...")
    rasters = read_geotiff_files(spark, "/path/to/rasters")
    
    print("Reading shapefiles...")
    shapes = read_shapefiles(spark, "/path/to/shapes")
    
    print("GeoBrix quick start examples loaded successfully!")



def use_gridx_functions(spark):
    """Demonstrate using GridX (BNG) functions."""
    from databricks.labs.gbx.gridx.bng import functions as bx
    bx.register(spark)
    
    # Calculate cell area (returns square kilometres)
    df = spark.sql("""
        SELECT gbx_bng_cellarea('TQ3080') as area_km2
    """)
    df.show()
    return df


def use_vectorx_functions(spark):
    """Demonstrate using VectorX functions."""
    from databricks.labs.gbx.vectorx.jts.legacy import functions as vx
    vx.register(spark)
    
    # Convert legacy Mosaic geometry to WKB
    # Note: This shows the function signature. In real use, provide actual legacy geometry data.
    df = spark.sql("""
        SELECT 'example' as id, gbx_st_legacyaswkb(null) as wkb_geom
    """)
    df.show()
    return df

