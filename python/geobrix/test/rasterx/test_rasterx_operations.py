import logging
from pathlib import Path
from test.rasterx._helpers import read_bytes

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


def test_rst_mapalgebra(spark):
    """Test map algebra operations.

    Note: Uses rst_fromcontent with binaryFile format to keep raster in memory
    and avoid GDAL temp file lifecycle issues.
    """
    # Served by the light level (or a heavy level that dispatches to it): needs the light deps, which the
    # heavyweight CI env does not have (test/conftest.py).
    pytest.importorskip("rasterio")
    pytest.importorskip("pandas")
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
