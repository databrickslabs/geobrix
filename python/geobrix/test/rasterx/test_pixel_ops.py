"""End-to-end Python test for the 7 pixel-ops + extraction functions.

One parameterized round-trip across all 7 wrappers — load an SRTM tile,
apply the function via SQL, assert the JVM round-trip fires and a non-null
tile / array / map comes back. Following the Wave-N budget guidance, we
cover all 7 functions in one parametrized test rather than 7 near-identical
copies.
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
    from databricks.labs.gbx.rasterx import functions as rx

    rx.register(spark)
    return spark


@pytest.mark.parametrize(
    "label, sql_expr, expected_col, validator",
    [
        # 1) fillnodata - returns a tile struct.
        (
            "fillnodata",
            "gbx_rst_fillnodata(t, 100.0, 0)",
            "out",
            lambda v: v is not None and v["raster"] is not None,
        ),
        # 2) sample - returns ARRAY<DOUBLE>. Pick a lon/lat inside the SRTM
        # tile (SRTM n51w001 covers W001..E000, N51..N52). London = ~-0.13, 51.5.
        # Construct a WKT POINT inline.
        (
            "sample",
            "gbx_rst_sample(t, 'POINT(-0.13 51.5)')",
            "out",
            lambda v: v is not None and len(v) >= 1,
        ),
        # 3) setsrid - stamp 4326 explicitly. Returns a tile struct.
        (
            "setsrid",
            "gbx_rst_setsrid(t, 4326)",
            "out",
            lambda v: v is not None and v["raster"] is not None,
        ),
        # 4) histogram - returns MAP<STRING, ARRAY<LONG>>. Force min/max so we
        # don't depend on the band's statistics being precomputed.
        (
            "histogram",
            "gbx_rst_histogram(t, 16, cast(0 as double), cast(1000 as double), false)",
            "out",
            lambda v: v is not None
            and any(k.startswith("band_") for k in v.keys())
            and all(len(buckets) == 16 for buckets in v.values()),
        ),
        # 5) threshold - returns a tile struct.
        (
            "threshold",
            "gbx_rst_threshold(t, '>', 100.0)",
            "out",
            lambda v: v is not None and v["raster"] is not None,
        ),
        # 6) buildoverviews - returns a tile struct.
        (
            "buildoverviews",
            "gbx_rst_buildoverviews(t, array(2, 4), 'average')",
            "out",
            lambda v: v is not None and v["raster"] is not None,
        ),
        # 7) band - extract band 1 from the (single-band) SRTM. Returns a tile.
        (
            "band",
            "gbx_rst_band(t, 1)",
            "out",
            lambda v: v is not None and v["raster"] is not None,
        ),
    ],
)
def test_pixel_ops_roundtrip(spark, label, sql_expr, expected_col, validator):
    """Each pixel-ops function returns a non-null result via SQL."""
    if not Path(SRTM_PATH).exists():
        pytest.skip(f"sample DEM not present: {SRTM_PATH}")

    # gbx_rst_fromfile is lightweight-only (issue #34, pyrx UDF). The heavy tier
    # loads the local DEM by reading its bytes via the binaryFile reader and
    # decoding with the Scala/GDAL gbx_rst_fromcontent -- no pandas/rasterio.
    spark.read.format("binaryFile").load(str(SRTM_PATH)).createOrReplaceTempView(
        "_rasterx_src"
    )
    df = spark.sql(
        f"SELECT {sql_expr} AS {expected_col} "
        f"FROM (SELECT gbx_rst_fromcontent(content, 'GTiff') AS t FROM _rasterx_src)"
    )
    rows = df.collect()
    assert len(rows) == 1, f"{label}: expected 1 row"
    out = rows[0][expected_col]
    assert validator(out), f"{label}: validator rejected output {out!r}"
