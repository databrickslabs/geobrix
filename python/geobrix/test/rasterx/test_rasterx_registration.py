import logging
from pathlib import Path
from test.rasterx._helpers import tile_from_path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()  # simpler, robust
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()
JAR_URI = JAR.as_uri()


@pytest.fixture(scope="session")
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
    print(JAR_URI)
    spark.sql("LIST JAR").show(truncate=False)
    return spark


def test_rasterx_functions_registration(spark):
    # Served by the light level (or a heavy level that dispatches to it): needs the light deps, which the
    # heavyweight CI env does not have (test/conftest.py).
    from databricks.labs.gbx import manager as gbx_manager

    pytest.importorskip("rasterio")
    pytest.importorskip("pandas")
    from databricks.labs.gbx.rasterx import functions as rx

    gbx_manager.register_family(spark, ("gbx_rst_",))
    path = (
        HERE.parents[4]
        / "src/test/resources/modis/MCD43A4.A2018185.h10v07.006.2018194033728_B01.TIF"
    ).resolve()
    df = spark.range(1).select(
        rx.rst_avg(tile_from_path(rx, f, str(path), "GTiff")).alias("avg")
    )
    row = df.collect()[0]
    assert row["avg"] is not None
