"""The JVM TIN expressions take the trailing ``mode`` argument and answer in the conforming mode (Steiner points).

The oracle table (``VECTOR_GENERATOR``) pins the conforming answers on every level; this checks the JVM expressions take
the argument through their Column call. ``gbx_st_interpolateelevationgeom`` has no JVM form.

Heavy generators are top-level Generator Columns in Spark 4.0: invoke directly in ``select(...)`` (the single-field struct
unwraps to the WKB value). Needs the staged JAR (the geobrix-dev container).
"""

import logging
from pathlib import Path
from test._heavy import heavy_register, session_has_jar

import pytest
from pyspark.sql.functions import call_function, col, lit
from pyspark.sql.types import ArrayType, DoubleType, StringType, StructField, StructType

pytestmark = pytest.mark.integration

_JARS = sorted(
    (Path(__file__).resolve().parents[2] / "lib").glob(
        "geobrix-*-jar-with-dependencies.jar"
    )
)

# 4 corners of a 10x10 square with Z=0; PySpark cannot infer the type of an empty list, so declare it.
_SQUARE = [
    "POINT Z (0 0 0)",
    "POINT Z (10 0 0)",
    "POINT Z (0 10 0)",
    "POINT Z (10 10 0)",
]
_SCHEMA = StructType(
    [
        StructField("pts", ArrayType(StringType()), nullable=False),
        StructField("breaks", ArrayType(StringType()), nullable=False),
        StructField("merge_tol", DoubleType(), nullable=False),
        StructField("snap_tol", DoubleType(), nullable=False),
        StructField("finder", StringType(), nullable=False),
    ]
)
# the arguments after the five TIN ones, before the mode
_AFTER = {
    "gbx_st_triangulate": (),
    # xmin, ymin, xmax, ymax, width_px, height_px, srid
    "gbx_st_interpolateelevationbbox": (0.0, 0.0, 10.0, 10.0, 5, 5, 32633),
}


@pytest.fixture(scope="module")
def spark():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ - run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)
    # spark.jars is a JVM-startup setting: a session already live without the JAR cannot take it. A cluster that installed the
    # JAR as a library (a Databricks pool) lists none there; its JVM has the classes (session_has_jar)
    active = SparkSession.getActiveSession()
    if active is not None and not session_has_jar(active, _JARS[-1]):
        pytest.skip(
            "a JAR-free Spark session is already live in this process; run this file alone"
        )
    s = (
        SparkSession.builder.appName("gbx-vectorx-tin-conforming")
        .config(
            "spark.driver.extraJavaOptions",
            "-Dlog4j.rootLogger=ERROR,console "
            "-Djava.library.path=/usr/local/lib:/usr/java/packages/lib:/usr/lib64:/lib64:/lib:/usr/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    heavy_register(s, ("gbx_st_",))
    yield s


@pytest.mark.parametrize("name", sorted(_AFTER))
def test_conforming_mode_answers_on_the_jvm(spark, name):
    df = spark.createDataFrame([(_SQUARE, [], 0.01, 0.01, "NONENCROACHING")], _SCHEMA)
    rows = df.select(
        call_function(
            name,
            col("pts"),
            col("breaks"),
            col("merge_tol"),
            col("snap_tol"),
            col("finder"),
            *[lit(v) for v in _AFTER[name]],
            lit("conforming"),
        ).alias("t")
    ).collect()
    assert rows and all(r["t"] is not None and len(r["t"]) > 0 for r in rows)
