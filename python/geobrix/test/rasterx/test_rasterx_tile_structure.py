"""
Tests for understanding and working with the tile structure directly.

A tile in GeoBrix is a struct with three fields:
- cellid: Long (nullable) - Grid cell ID for tessellated rasters, null for non-tessellated
- raster: String or Binary - Either a file path or binary content
- metadata: Map[String, String] - Driver info, extension, size, etc.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()
JAR_URI = JAR.as_uri()

MODIS_B01 = (
    HERE.parents[4]
    / "src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
).resolve()
MODIS_DIR = (HERE.parents[4] / "src/test/resources/modis").resolve()


@pytest.fixture(scope="module")
def spark():
    """Create SparkSession with GeoBrix JAR loaded"""
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark_session = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark_session)
    return spark_session


def test_tile_structure_has_required_fields(spark):
    """Test that tile structure has cellid, raster, and metadata fields"""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")).alias("tile")
    )

    # Access tile fields directly
    tile_components = df.select(
        f.col("tile.cellid").alias("cellid"),
        f.col("tile.raster").alias("raster"),
        f.col("tile.metadata").alias("metadata"),
    )

    result = tile_components.collect()
    assert result is not None
    assert len(result) == 1

    # cellid should be None for non-tessellated rasters
    assert result[0]["cellid"] is None

    # raster should be a string path
    raster_value = result[0]["raster"]
    assert raster_value is not None
    assert ".TIF" in raster_value or ".tif" in raster_value

    # metadata should be a dict
    metadata = result[0]["metadata"]
    assert metadata is not None
    assert "driver" in metadata
    assert "extension" in metadata
    assert metadata["driver"] == "GTiff"


def test_tile_from_file_contains_path(spark):
    """Test that tile from file contains path in raster field"""
    from databricks.labs.gbx.rasterx import functions as rx

    df = (
        spark.range(1)
        .select(rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")).alias("tile"))
        .select(f.col("tile.raster").alias("raster_path"))
    )

    result = df.collect()
    raster_path = result[0]["raster_path"]

    assert raster_path is not None
    assert "MCD43A4" in raster_path
    assert raster_path.endswith(".TIF") or raster_path.endswith(".tif")


def test_tile_from_content_contains_binary(spark):
    """Test that tile from content contains binary in raster field"""
    from databricks.labs.gbx.rasterx import functions as rx

    df = (
        spark.read.format("binaryFile")
        .load(str(MODIS_DIR))
        .select(rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile"))
        .select(f.col("tile.raster").alias("raster_binary"))
    )

    result = df.collect()
    raster_binary = result[0]["raster_binary"]

    assert raster_binary is not None
    assert isinstance(raster_binary, (bytes, bytearray))
    assert len(raster_binary) > 0


def test_tile_metadata_contains_driver_info(spark):
    """Test that tile metadata contains driver information"""
    from databricks.labs.gbx.rasterx import functions as rx

    df = (
        spark.range(1)
        .select(rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")).alias("tile"))
        .select(
            f.col("tile.metadata").alias("metadata"),
            f.col("tile.metadata.driver").alias("driver"),
            f.col("tile.metadata.extension").alias("extension"),
        )
    )

    result = df.collect()

    # Full metadata map
    metadata = result[0]["metadata"]
    assert "driver" in metadata
    assert "extension" in metadata

    # Individual metadata fields
    driver = result[0]["driver"]
    extension = result[0]["extension"]

    assert driver == "GTiff"
    assert extension == "tif"  # Extension without dot


def test_tessellated_tiles_have_cellid(spark):
    """Test that tessellated tiles have non-null cellid"""
    from databricks.labs.gbx.rasterx import functions as rx

    # rst_h3_tessellate is a generator that produces rows directly (no explode needed)
    # Use resolution 1 (coarse) to avoid generating too many cells
    df = (
        spark.range(1)
        .select(rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")).alias("tile"))
        .select(rx.rst_h3_tessellate(f.col("tile"), f.lit(1)).alias("tess_tile"))
        .select(f.col("tess_tile.cellid").alias("cellid"))
        .limit(10)
    )  # Limit for test efficiency

    result = df.collect()

    assert len(result) > 0
    # Tessellated tiles should have non-null cellid
    for row in result:
        assert row["cellid"] is not None
        assert row["cellid"] > 0


def test_tile_compatible_with_accessor_functions(spark):
    """Test that tile structure is compatible with accessor functions"""
    from databricks.labs.gbx.rasterx import functions as rx

    df = (
        spark.range(1)
        .select(rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")).alias("tile"))
        .select(
            f.col("tile.metadata").alias("metadata_direct"),
            rx.rst_metadata(f.col("tile")).alias("metadata_function"),
            rx.rst_boundingbox(f.col("tile")).alias("bbox"),
            rx.rst_width(f.col("tile")).alias("width"),
            rx.rst_height(f.col("tile")).alias("height"),
        )
    )

    result = df.collect()

    # Verify metadata direct access works
    metadata_direct = result[0]["metadata_direct"]
    assert metadata_direct["driver"] == "GTiff"

    # Verify accessor functions work
    # rst_metadata returns a map/dict with metadata information
    metadata_function = result[0]["metadata_function"]
    assert metadata_function is not None
    assert isinstance(metadata_function, dict)

    # Other accessor functions should work
    assert result[0]["width"] == 2400
    assert result[0]["height"] == 2400
    assert result[0]["bbox"] is not None


def test_filter_tiles_by_metadata(spark):
    """Test filtering tiles by metadata fields"""
    from databricks.labs.gbx.rasterx import functions as rx

    modis_files = [
        (1, str(MODIS_DIR / "MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")),
        (2, str(MODIS_DIR / "MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF")),
    ]

    df = spark.createDataFrame(modis_files, ["id", "path"]).select(
        "id", "path", rx.rst_fromfile(f.col("path"), f.lit("GTiff")).alias("tile")
    )

    # Filter by metadata
    gtiff_tiles = df.filter(f.col("tile.metadata.driver") == "GTiff")

    result = gtiff_tiles.collect()
    assert len(result) == 2


def test_extract_raster_path_for_conditional_processing(spark):
    """Test extracting raster path from tile for conditional processing"""
    from databricks.labs.gbx.rasterx import functions as rx

    modis_files = [
        (1, str(MODIS_DIR / "MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF")),
        (2, str(MODIS_DIR / "MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF")),
    ]

    df = (
        spark.createDataFrame(modis_files, ["id", "path"])
        .select("id", rx.rst_fromfile(f.col("path"), f.lit("GTiff")).alias("tile"))
        .select(
            "id",
            f.col("tile.raster").alias("raster_path"),
            f.col("tile.raster").contains("B01").alias("is_b01"),
        )
    )

    result = df.collect()

    assert result[0]["is_b01"] is True
    assert result[1]["is_b01"] is False


def test_access_raster_binary_for_custom_udfs(spark):
    """Test accessing raster binary data for custom UDF processing"""
    from databricks.labs.gbx.rasterx import functions as rx

    df = (
        spark.read.format("binaryFile")
        .load(str(MODIS_DIR))
        .limit(1)
        .select(rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile"))
        .select(
            f.col("tile.raster").alias("raster_binary"),
            f.col("tile.metadata").alias("metadata"),
        )
    )

    result = df.collect()

    raster_binary = result[0]["raster_binary"]
    metadata = result[0]["metadata"]

    # Binary should be available for UDF processing
    assert raster_binary is not None
    assert len(raster_binary) > 1000  # GeoTIFF should be reasonably sized

    # Metadata should indicate it's from content
    assert metadata["driver"] == "GTiff"


def test_tile_schema_documentation(spark):
    """Test and document the tile schema structure"""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_fromfile(f.lit(str(MODIS_B01)), f.lit("GTiff")).alias("tile")
    )

    # Print schema for documentation
    schema = df.schema["tile"]

    # Verify schema structure
    assert schema.dataType.simpleString().startswith("struct<")
    assert "cellid" in schema.dataType.simpleString()
    assert "raster" in schema.dataType.simpleString()
    assert "metadata" in schema.dataType.simpleString()

    # The tile is a struct with three fields
    assert len(schema.dataType.fields) == 3

    # Field 0: cellid (LongType, nullable)
    cellid_field = schema.dataType.fields[0]
    assert cellid_field.name == "cellid"
    assert cellid_field.dataType.simpleString() == "bigint"

    # Field 1: raster (StringType or BinaryType, not nullable)
    raster_field = schema.dataType.fields[1]
    assert raster_field.name == "raster"
    assert raster_field.dataType.simpleString() in ["string", "binary"]

    # Field 2: metadata (MapType, nullable)
    metadata_field = schema.dataType.fields[2]
    assert metadata_field.name == "metadata"
    assert "map<string,string>" in metadata_field.dataType.simpleString()
