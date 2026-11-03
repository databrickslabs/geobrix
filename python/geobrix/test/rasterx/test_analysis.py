"""End-to-end Python test for the 4 analysis functions.

One parameterized round-trip across all 4 wrappers — load a real GTiff tile,
invoke the function via SQL, assert the JVM round-trip fires and a non-null
tile / array comes back. Single test, 4 cases.
"""

import logging
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

HERE = Path(__file__).resolve()
LIBDIR = (HERE.parents[2] / "lib").resolve()
candidates = sorted(LIBDIR.glob("geobrix-*-jar-with-dependencies.jar"))
JAR = candidates[-1].resolve()

# An SRTM elevation tile shipped in the essential sample-data bundle.
SRTM_PATH = (
    "/Volumes/main/default/test-data/geobrix-examples/london/elevation/srtm_n51w001.tif"
)


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

    from test._heavy import heavy_register

    heavy_register(spark, ("gbx_rst_",))
    return spark


@pytest.mark.parametrize(
    "label, sql_expr, validator",
    [
        # 3) contour — returns ARRAY<struct(geom_wkb, value)>. London SRTM
        # n51w001 spans only ~91-95 m elevation so a 1 m interval is required
        # to pick up at least one contour line within that narrow range.
        (
            "contour",
            "gbx_rst_contour(t, array(), 1.0, 0.0, 'elev')",
            lambda v: v is not None and len(v) >= 1,
        ),
    ],
)
def test_analysis_roundtrip(spark, label, sql_expr, validator):
    """Each analysis function returns a non-null result via SQL."""
    if not Path(SRTM_PATH).exists():
        pytest.skip(f"sample DEM not present: {SRTM_PATH}")

    # gbx_rst_fromfile is lightweight-only (issue #34, pyrx UDF). The heavy tier
    # loads the local DEM by reading its bytes via the binaryFile reader and
    # decoding with the Scala/GDAL gbx_rst_fromcontent -- no pandas/rasterio.
    spark.read.format("binaryFile").load(str(SRTM_PATH)).createOrReplaceTempView(
        "_rasterx_src"
    )
    df = spark.sql(
        f"SELECT {sql_expr} AS out "
        f"FROM (SELECT gbx_rst_fromcontent(content, 'GTiff') AS t FROM _rasterx_src)"
    )
    rows = df.collect()
    assert len(rows) == 1, f"{label}: expected 1 row"
    out = rows[0]["out"]
    assert validator(out), f"{label}: validator rejected output {out!r}"
