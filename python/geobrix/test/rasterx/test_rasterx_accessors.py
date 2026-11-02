import logging
from pathlib import Path
from test.rasterx._helpers import tile_from_path

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
NETCDF = (
    HERE.parents[4]
    / "src/test/resources/binary/netcdf-CMIP5/prAdjust_day_HadGEM2-CC_SMHI-DBSrev930-GFD-1981-2010-postproc_rcp45_r1i1p1_20201201-20201231.nc"
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


def test_rst_subdatasets(spark):
    """Test getting subdatasets from NetCDF."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = tile_from_path(rx, f, str(NETCDF), "NetCDF")

    df = spark.range(1).select(rx.rst_subdatasets(tile_col).alias("subdatasets"))

    result = df.collect()
    assert result is not None
    assert result[0]["subdatasets"] is not None


def test_rst_getsubdataset(spark):
    """Test extracting subdataset from NetCDF."""
    from databricks.labs.gbx.rasterx import functions as rx

    tile_col = tile_from_path(rx, f, str(NETCDF), "NetCDF")

    df = spark.range(1).select(
        rx.rst_getsubdataset(tile_col, f.lit("prAdjust")).alias("subdataset")
    )

    result = df.collect()
    assert result is not None
    assert result[0]["subdataset"] is not None
