"""Task 9: Light pygx registered UDFs degrade bad cell-id/geometry DATA to NULL.

Parameter errors (bad resolution, bad grid spec) must still raise.
"""

import pytest
from pyspark.sql import SparkSession

from databricks.labs.gbx.pygx import functions as gx


@pytest.fixture(scope="module")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("pygx-err")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    gx.register(s)
    yield s


# ---------------------------------------------------------------------------
# BNG scalar UDFs — bad cell id -> NULL
# ---------------------------------------------------------------------------


def test_bng_aswkb_null_on_bad_cellid(spark):
    r = spark.sql("SELECT gbx_bng_aswkb('!!') AS g").first()
    assert r["g"] is None


def test_bng_aswkt_null_on_bad_cellid(spark):
    r = spark.sql("SELECT gbx_bng_aswkt('!!') AS g").first()
    assert r["g"] is None


def test_bng_centroid_null_on_bad_cellid(spark):
    r = spark.sql("SELECT gbx_bng_centroid('!!') AS g").first()
    assert r["g"] is None


def test_bng_cellarea_null_on_bad_cellid(spark):
    r = spark.sql("SELECT gbx_bng_cellarea('!!') AS a").first()
    assert r["a"] is None


def test_bng_distance_null_on_bad_cellid(spark):
    r = spark.sql("SELECT gbx_bng_distance('!!', 'TQ3080') AS d").first()
    assert r["d"] is None


def test_bng_euclideandistance_null_on_bad_cellid(spark):
    r = spark.sql("SELECT gbx_bng_euclideandistance('!!', 'TQ3080') AS d").first()
    assert r["d"] is None


def test_bng_kring_null_on_bad_cellid(spark):
    r = spark.sql("SELECT gbx_bng_kring('!!', 1) AS arr").first()
    assert r["arr"] is None


def test_bng_kloop_null_on_bad_cellid(spark):
    r = spark.sql("SELECT gbx_bng_kloop('!!', 1) AS arr").first()
    assert r["arr"] is None


# ---------------------------------------------------------------------------
# BNG @udtf explode — bad cell id -> zero rows
# ---------------------------------------------------------------------------


def test_bng_kringexplode_zero_rows_on_bad_cellid(spark):
    # Direct UDTF call (PySpark UDTF syntax: no LATERAL VIEW for direct invocation)
    df = spark.sql("SELECT cellid FROM gbx_bng_kringexplode('!!', 1)")
    assert df.count() == 0


def test_bng_kloopexplode_zero_rows_on_bad_cellid(spark):
    df = spark.sql("SELECT cellid FROM gbx_bng_kloopexplode('!!', 1)")
    assert df.count() == 0


# ---------------------------------------------------------------------------
# BNG scalar UDFs — bad resolution PARAMETER -> raises
# ---------------------------------------------------------------------------


def test_bng_pointascell_raises_on_bad_resolution(spark):
    with pytest.raises(Exception):
        spark.sql(
            "SELECT gbx_bng_pointascell('POINT (530000 180000)', 'bogus')"
        ).first()


def test_bng_polyfill_raises_on_bad_resolution(spark):
    with pytest.raises(Exception):
        spark.sql("SELECT gbx_bng_polyfill('POINT (530000 180000)', 'bogus')").first()


# ---------------------------------------------------------------------------
# Custom scalar UDF — bad geometry DATA -> NULL; bad resolution -> raises
# ---------------------------------------------------------------------------


def test_custom_pointascell_null_on_bad_geom(spark):
    """NaN coordinates degrade to NULL, resolution parameter still raises."""
    # Build a minimal valid grid spec
    grid_sql = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 500000, 500000)"
    r = spark.sql(f"SELECT gbx_custom_pointascell(NULL, {grid_sql}, 1) AS cid").first()
    assert r["cid"] is None
