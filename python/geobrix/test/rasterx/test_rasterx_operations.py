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
MODIS_B02 = (
    HERE.parents[4]
    / "src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B02.TIF"
).resolve()
MODIS_B03 = (
    HERE.parents[4]
    / "src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B03.TIF"
).resolve()


@pytest.fixture(scope="module")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    return spark


def test_rst_clip(spark):
    """Test clipping a raster by a geometry."""
    from databricks.labs.gbx.rasterx import functions as rx

    # Create a simple polygon geometry in WKT format
    # This polygon should intersect with the MODIS tile
    wkt_geom = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"

    df = spark.range(1).select(
        rx.rst_clip(
            tile_from_path(rx, f, str(MODIS_B01), "GTiff"),
            f.lit(wkt_geom),
            f.lit(True),  # cutline_all_touched
        ).alias("clipped_tile")
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    tile = result[0]["clipped_tile"]
    assert tile is not None
    # Guard against empty-TIFF-stub regression: writeToBytes returning a ~422-byte
    # unfinalized GTiff header if the output Dataset isn't closed before reading
    # /vsimem. Real clipped raster bytes are much larger than the header alone.
    assert len(tile["raster"]) > 2000, (
        f"clipped tile raster unexpectedly small ({len(tile['raster'])}B) "
        "— likely unfinalized GTiff output"
    )


def test_rst_filter(spark):
    """Test filtering a raster with kernel operations."""
    from databricks.labs.gbx.rasterx import functions as rx

    # Test with median filter
    df = spark.range(1).select(
        rx.rst_filter(
            tile_from_path(rx, f, str(MODIS_B01), "GTiff"),
            f.lit(3),  # kernel_size
            f.lit("median"),  # operation
        ).alias("filtered_tile")
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["filtered_tile"] is not None


def test_rst_filter_operations(spark):
    """Test different filter operations."""
    from databricks.labs.gbx.rasterx import functions as rx

    operations = ["avg", "median", "mode", "max", "min"]

    for op in operations:
        df = spark.range(1).select(
            rx.rst_filter(
                tile_from_path(rx, f, str(MODIS_B01), "GTiff"),
                f.lit(3),
                f.lit(op),
            ).alias("result")
        )
        result = df.collect()
        assert result is not None, f"Filter operation '{op}' failed"
        assert result[0]["result"] is not None


def test_rst_transform(spark):
    """Test transforming raster to different SRID."""
    from databricks.labs.gbx.rasterx import functions as rx

    # Transform to EPSG:4326 (WGS84)
    df = spark.range(1).select(
        rx.rst_transform(
            tile_from_path(rx, f, str(MODIS_B01), "GTiff"),
            f.lit(4326),  # target SRID
        ).alias("transformed_tile")
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["transformed_tile"] is not None


def test_rst_convolve(spark):
    """Test convolution operation on raster."""
    from databricks.labs.gbx.rasterx import functions as rx

    # Simple 3x3 averaging kernel - use f.array to create proper array type
    kernel = f.array(
        f.array(f.lit(0.111), f.lit(0.111), f.lit(0.111)),
        f.array(f.lit(0.111), f.lit(0.111), f.lit(0.111)),
        f.array(f.lit(0.111), f.lit(0.111), f.lit(0.111)),
    )

    df = spark.range(1).select(
        rx.rst_convolve(tile_from_path(rx, f, str(MODIS_B01), "GTiff"), kernel).alias(
            "convolved_tile"
        )
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["convolved_tile"] is not None


def test_rst_merge(spark):
    """Test merging multiple rasters."""
    from databricks.labs.gbx.rasterx import functions as rx

    # rst_fromfile is lightweight-only (issue #34); load each local tile's bytes
    # and decode via the heavy-native gbx_rst_fromcontent.
    df = spark.createDataFrame(
        [
            (1, read_bytes(MODIS_B01)),
            (2, read_bytes(MODIS_B02)),
            (3, read_bytes(MODIS_B03)),
        ],
        ["id", "content"],
    )

    df = df.withColumn("tile", rx.rst_fromcontent(f.col("content"), f.lit("GTiff")))

    result_df = df.select(rx.rst_merge(f.collect_list("tile")).alias("merged_tile"))

    result = result_df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["merged_tile"] is not None


def test_rst_combineavg(spark):
    """Test combining rasters using average."""
    from databricks.labs.gbx.rasterx import functions as rx

    # rst_fromfile is lightweight-only (issue #34); load each local tile's bytes
    # and decode via the heavy-native gbx_rst_fromcontent.
    df = spark.createDataFrame(
        [(1, read_bytes(MODIS_B01)), (2, read_bytes(MODIS_B02))], ["id", "content"]
    )

    df = df.withColumn("tile", rx.rst_fromcontent(f.col("content"), f.lit("GTiff")))

    result_df = df.select(
        rx.rst_combineavg(f.collect_list("tile")).alias("combined_tile")
    )

    result = result_df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["combined_tile"] is not None


def test_rst_ndvi(spark):
    """Test NDVI calculation."""
    from databricks.labs.gbx.rasterx import functions as rx

    # NDVI typically uses red (band 1) and NIR (band 2)
    # For this test, we'll use bands from the same tile
    df = spark.range(1).select(
        rx.rst_ndvi(
            tile_from_path(rx, f, str(MODIS_B01), "GTiff"),
            f.lit(1),  # red band
            f.lit(1),  # NIR band (using same for test purposes)
        ).alias("ndvi_tile")
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["ndvi_tile"] is not None


def test_rst_asformat(spark):
    """Test converting raster format.

    Note: Converts from GTiff to Zarr. Output compression defaults to DEFLATE
    (baseline TIFF, previewable by Databricks image preview / Java ImageIO).
    """
    from databricks.labs.gbx.rasterx import functions as rx

    # Convert from GTiff to Zarr format
    df = spark.range(1).select(
        rx.rst_asformat(
            tile_from_path(rx, f, str(MODIS_B01), "GTiff"), f.lit("Zarr")
        ).alias("zarr_tile")
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["zarr_tile"] is not None


def test_rst_initnodata(spark):
    """Test initializing NoData values."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_initnodata(tile_from_path(rx, f, str(MODIS_B01), "GTiff")).alias(
            "nodata_tile"
        )
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["nodata_tile"] is not None


def test_rst_isempty(spark):
    """Test checking if raster is empty."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_isempty(tile_from_path(rx, f, str(MODIS_B01), "GTiff")).alias("is_empty")
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    # MODIS tile should not be empty
    assert result[0]["is_empty"] is False


def test_rst_tryopen(spark):
    """Test safely opening a raster."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_tryopen(tile_from_path(rx, f, str(MODIS_B01), "GTiff")).alias(
            "opened_tile"
        )
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["opened_tile"] is not None


def test_rst_updatetype(spark):
    """Test updating raster data type."""
    from databricks.labs.gbx.rasterx import functions as rx

    df = spark.range(1).select(
        rx.rst_updatetype(
            tile_from_path(rx, f, str(MODIS_B01), "GTiff"), f.lit("Float32")
        ).alias("updated_tile")
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["updated_tile"] is not None


def test_rst_coordinate_conversions(spark):
    """Test raster/world coordinate conversions."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = tile_from_path(rx, f, str(MODIS_B01), "GTiff")

    # Test world to raster coordinate conversion
    df = spark.range(1).select(
        rx.rst_worldtorastercoord(
            tile_col, f.lit(-8895604.157333), f.lit(2223901.039333)
        ).alias("raster_coord"),
        rx.rst_worldtorastercoordx(
            tile_col, f.lit(-8895604.157333), f.lit(2223901.039333)
        ).alias("raster_x"),
        rx.rst_worldtorastercoordy(
            tile_col, f.lit(-8895604.157333), f.lit(2223901.039333)
        ).alias("raster_y"),
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["raster_coord"] is not None
    assert result[0]["raster_x"] is not None
    assert result[0]["raster_y"] is not None


def test_rst_raster_to_world_conversions(spark):
    """Test raster to world coordinate conversions."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = tile_from_path(rx, f, str(MODIS_B01), "GTiff")

    # Test raster to world coordinate conversion
    df = spark.range(1).select(
        rx.rst_rastertoworldcoord(tile_col, f.lit(0), f.lit(0)).alias("world_coord"),
        rx.rst_rastertoworldcoordx(tile_col, f.lit(0), f.lit(0)).alias("world_x"),
        rx.rst_rastertoworldcoordy(tile_col, f.lit(0), f.lit(0)).alias("world_y"),
    )

    result = df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["world_coord"] is not None
    assert result[0]["world_x"] is not None
    assert result[0]["world_y"] is not None


def test_rst_mapalgebra(spark):
    """Test map algebra operations.

    Note: Uses rst_fromcontent with binaryFile format to keep raster in memory
    and avoid GDAL temp file lifecycle issues.
    """
    from databricks.labs.gbx.rasterx import functions as rx

    # Load raster from binary content (avoids temp file issues)
    df = (
        spark.read.format("binaryFile")
        .load(str(MODIS_B01))
        .withColumn("tile", rx.rst_fromcontent("content", f.lit("GTiff")))
        .drop("content")
    )

    # Apply simple map algebra: A + 2*A = 3*A
    result_df = df.select(
        rx.rst_mapalgebra(f.array("tile"), f.lit('{"calc": "A+2*A"}')).alias(
            "algebra_result"
        )
    )

    result = result_df.collect()
    assert result is not None
    assert len(result) == 1
    assert result[0]["algebra_result"] is not None
    assert result[0]["algebra_result"] is not None


def test_rst_scalar_literal_args(spark):
    """Plain Python non-string scalars (bool/int/float) are accepted in place of f.lit(...).

    Verifies the ColLike/_col() widening: booleans, ints, and floats now
    auto-wrap via f.lit so ``rst_clip(tile, geom, True)`` works without
    ``f.lit(True)``. Strings stay as-is (pyspark column-ref convention).
    """
    from databricks.labs.gbx.rasterx import functions as rx

    wkt = "POLYGON((-8900000 2220000, -8900000 2200000, -8880000 2200000, -8880000 2220000, -8900000 2220000))"
    tile = tile_from_path(rx, f, str(MODIS_B01), "GTiff")

    df = spark.range(1).select(
        rx.rst_clip(tile, f.lit(wkt), True).alias("clipped"),  # bool scalar
        rx.rst_transform(tile, 4326).alias("transformed"),  # int scalar
    )

    row = df.collect()[0]
    assert row["clipped"] is not None
    assert row["transformed"] is not None
