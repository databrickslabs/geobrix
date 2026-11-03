import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

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
    from databricks.labs.gbx import manager as gbx_manager

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

    gbx_manager.register_family(spark, ("gbx_rst_",))
    return spark
