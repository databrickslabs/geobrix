"""
Tests for understanding and working with the tile structure directly.

A tile in GeoBrix is the v2 8-field struct (shared by both tiers):
- cellid: Long (nullable) - Grid cell ID for tessellated rasters, null for non-tessellated
- raster: Binary (nullable) - Raster bytes; null on a virtual (bytes-free) tile
- path: String (nullable) - Source path; set on a virtual tile
- window: struct<col_off,row_off,width,height> (nullable) - Pixel window
- clip_polygon: Binary (nullable) - Clip geometry (WKB)
- clip_crs: String (nullable) - CRS of clip_polygon
- crs: String (nullable) - Working/target CRS
- metadata: Map[String, String] (nullable) - Driver info, extension, size, etc.
"""

import logging
from pathlib import Path
from test.rasterx._helpers import read_bytes, tile_from_path

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
        tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile")
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

    # raster should be loaded bytes from the file
    raster_value = result[0]["raster"]
    assert raster_value is not None
    assert isinstance(raster_value, (bytes, bytearray))
    assert len(raster_value) > 0

    # metadata should be a dict
    metadata = result[0]["metadata"]
    assert metadata is not None
    assert "driver" in metadata
    assert "extension" in metadata
    assert metadata["driver"] == "GTiff"


def test_tile_from_file_contains_binary(spark):
    """Loading a tile populates the raster field with the file bytes (BinaryType)."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = (
        spark.range(1)
        .select(tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile"))
        .select(
            f.col("tile.raster").alias("raster_binary"),
            f.col("tile.metadata.size").alias("size"),
        )
    )

    result = df.collect()
    raster_binary = result[0]["raster_binary"]

    assert raster_binary is not None
    assert isinstance(raster_binary, (bytes, bytearray))
    assert len(raster_binary) > 0
    # Metadata "size" should reflect the real byte length now that we load content.
    assert int(result[0]["size"]) == len(raster_binary)


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
        .select(tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile"))
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
        .select(tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile"))
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
        .select(tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile"))
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

    # rst_fromfile is lightweight-only (issue #34); carry the file bytes alongside
    # the path column and decode via the heavy-native gbx_rst_fromcontent.
    b01 = MODIS_DIR / "MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
    b02 = MODIS_DIR / "MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF"
    modis_files = [
        (1, str(b01), read_bytes(b01)),
        (2, str(b02), read_bytes(b02)),
    ]

    df = spark.createDataFrame(modis_files, ["id", "path", "content"]).select(
        "id", "path", rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile")
    )

    # Filter by metadata
    gtiff_tiles = df.filter(f.col("tile.metadata.driver") == "GTiff")

    result = gtiff_tiles.collect()
    assert len(result) == 2


def test_extract_raster_bytes_for_conditional_processing(spark):
    """Conditional processing should key off metadata / input path columns, not the binary raster field."""
    from databricks.labs.gbx.rasterx import functions as rx

    # rst_fromfile is lightweight-only (issue #34); carry the file bytes alongside
    # the path column and decode via the heavy-native gbx_rst_fromcontent.
    b01 = MODIS_DIR / "MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
    b02 = MODIS_DIR / "MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF"
    modis_files = [
        (1, str(b01), read_bytes(b01)),
        (2, str(b02), read_bytes(b02)),
    ]

    df = (
        spark.createDataFrame(modis_files, ["id", "path", "content"])
        .select(
            "id",
            "path",
            rx.rst_fromcontent(f.col("content"), f.lit("GTiff")).alias("tile"),
        )
        .select(
            "id",
            f.col("tile.raster").alias("raster_bytes"),
            f.col("path").contains("B01").alias("is_b01"),
        )
    )

    result = df.collect()

    # Raster field is now binary; branching by path uses the sibling path column.
    assert isinstance(result[0]["raster_bytes"], (bytes, bytearray))
    assert len(result[0]["raster_bytes"]) > 0
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
        tile_from_path(rx, f, str(MODIS_B01), "GTiff").alias("tile")
    )

    # Print schema for documentation
    schema = df.schema["tile"]

    # Verify schema structure
    assert schema.dataType.simpleString().startswith("struct<")
    assert "cellid" in schema.dataType.simpleString()
    assert "raster" in schema.dataType.simpleString()
    assert "metadata" in schema.dataType.simpleString()

    # The tile is the v2 9-field struct (shared by both tiers): the 8 v2 fields
    # plus path_mode, the FILE storage mode (null / 'external' / 'managed').
    fields = {fld.name: fld for fld in schema.dataType.fields}
    assert set(fields) == {
        "cellid",
        "raster",
        "path",
        "path_mode",
        "window",
        "clip_polygon",
        "clip_crs",
        "crs",
        "metadata",
    }

    # cellid: LongType, not nullable
    assert fields["cellid"].dataType.simpleString() == "bigint"

    # raster: BinaryType, nullable (null on a virtual tile)
    assert fields["raster"].dataType.simpleString() == "binary"

    # path: StringType (source path for a virtual tile)
    assert fields["path"].dataType.simpleString() == "string"

    # path_mode: StringType — FILE storage mode (null / 'external' / 'managed')
    assert fields["path_mode"].dataType.simpleString() == "string"

    # window: struct of four ints
    assert "col_off" in fields["window"].dataType.simpleString()

    # metadata: MapType
    assert "map<string,string>" in fields["metadata"].dataType.simpleString()
