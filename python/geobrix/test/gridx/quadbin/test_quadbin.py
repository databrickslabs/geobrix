"""Quadbin explode wrapper tests (the scalar and array functions are in the family oracle table).

Register the SQL
functions, build small DataFrames, evaluate each Column wrapper, and
assert on collected rows.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[3] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()
JAR_URI = JAR.as_uri()


@pytest.fixture(scope="session")
def spark():
    logging.getLogger("py4j").setLevel(logging.ERROR)
    spark = (
        SparkSession.builder.config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=INFO,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(JAR))
        .getOrCreate()
    )
    return spark


@pytest.fixture(scope="session")
def quadbin_registered(spark):
    """Register quadbin functions once for all tests."""
    from databricks.labs.gbx import manager as gbx_manager
    from databricks.labs.gbx.gridx.quadbin import functions as qx

    gbx_manager.register_family(spark, ("gbx_quadbin_",))
    return qx


def _cell_at(spark, qx, lon: float, lat: float, z: int) -> int:
    """Compute a quadbin cell id via the SQL function (round-trip through Spark)."""
    df = spark.createDataFrame([(lon, lat)], ["lon", "lat"])
    return df.select(
        qx.quadbin_pointascell(f.col("lon"), f.col("lat"), z).alias("cell")
    ).first()["cell"]


def _rows(df):
    return sorted(tuple(r) for r in df.collect())


@pytest.mark.parametrize("name", ["geomkringexplode", "geomkloopexplode"])
def test_quadbin_geom_explode_wrappers_match_sql(spark, quadbin_registered, name):
    """The wrappers bind geom/resolution/k[/mode/coverage] in the SQL
    function's positional order: same rows as the LATERAL call, with defaults and
    with an explicit non-default mode + coverage (which change the result)."""
    qx = quadbin_registered
    wrapper = getattr(qx, f"quadbin_{name}")
    df = spark.createDataFrame(
        [("POLYGON((-1 -1, 1 -1, 1 1, -1 1, -1 -1))",)], ["geom"]
    )
    df.createOrReplaceTempView("quadbin_explode_in")

    def lateral(args):
        return spark.sql(
            f"SELECT t.cellid FROM quadbin_explode_in, "
            f"LATERAL gbx_quadbin_{name}(geom, {args}) t"
        )

    got = _rows(wrapper(df, "geom", 5, 1).select("cellid"))
    want = _rows(lateral("5, 1"))
    assert got and got == want
    got_in = _rows(wrapper(df, "geom", 5, 1, "boundary-in", "core").select("cellid"))
    want_in = _rows(lateral("5, 1, 'boundary-in', 'core'"))
    assert got_in == want_in and want_in != want
