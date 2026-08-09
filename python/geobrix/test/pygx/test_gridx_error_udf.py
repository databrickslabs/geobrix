"""Task 9: Light pygx registered UDFs degrade bad cell-id/geometry DATA to NULL.

Parameter errors (bad resolution, bad grid spec) must still raise.
"""

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    StringType,
    StructField,
    StructType,
)

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
# BNG geom-input scalar/array UDFs — bad WKB DATA -> NULL (matches heavy tier)
# Load-bearing rule: bad RESOLUTION PARAMETER still raises.
# ---------------------------------------------------------------------------

_BAD_WKB = b"\x01\x02\x03"  # garbage bytes — not valid WKB
_GOOD_GEOM = "POLYGON ((529900 179900, 530100 179900, 530100 180100, 529900 180100, 529900 179900))"


def test_bng_polyfill_null_on_bad_wkb(spark):
    r = spark.sql("SELECT gbx_bng_polyfill(X'010203', 3) AS arr").first()
    assert r["arr"] is None


def test_bng_geomkring_null_on_bad_wkb(spark):
    r = spark.sql("SELECT gbx_bng_geomkring(X'010203', 3, 1) AS arr").first()
    assert r["arr"] is None


def test_bng_geomkloop_null_on_bad_wkb(spark):
    r = spark.sql("SELECT gbx_bng_geomkloop(X'010203', 3, 1) AS arr").first()
    assert r["arr"] is None


def test_bng_tessellate_null_on_bad_wkb(spark):
    r = spark.sql("SELECT gbx_bng_tessellate(X'010203', 3) AS arr").first()
    assert r["arr"] is None


# ---------------------------------------------------------------------------
# BNG geom-input UDTFs — bad WKB DATA -> zero rows (matches heavy tier)
# ---------------------------------------------------------------------------


def test_bng_geomkringexplode_zero_rows_on_bad_wkb(spark):
    df = spark.sql("SELECT cellid FROM gbx_bng_geomkringexplode(X'010203', 3, 1)")
    assert df.count() == 0


def test_bng_geomkloopexplode_zero_rows_on_bad_wkb(spark):
    df = spark.sql("SELECT cellid FROM gbx_bng_geomkloopexplode(X'010203', 3, 1)")
    assert df.count() == 0


def test_bng_tessellateexplode_zero_rows_on_bad_wkb(spark):
    df = spark.sql("SELECT cellid FROM gbx_bng_tessellateexplode(X'010203', 3)")
    assert df.count() == 0


# ---------------------------------------------------------------------------
# Confirm bad RESOLUTION still raises for geom-input ops (param path untouched)
# ---------------------------------------------------------------------------


def test_bng_geomkring_raises_on_bad_resolution(spark):
    with pytest.raises(Exception):
        spark.sql(f"SELECT gbx_bng_geomkring('{_GOOD_GEOM}', 'bogus', 1)").first()


def test_bng_geomkloop_raises_on_bad_resolution(spark):
    with pytest.raises(Exception):
        spark.sql(f"SELECT gbx_bng_geomkloop('{_GOOD_GEOM}', 'bogus', 1)").first()


def test_bng_tessellate_raises_on_bad_resolution(spark):
    with pytest.raises(Exception):
        spark.sql(f"SELECT gbx_bng_tessellate('{_GOOD_GEOM}', 'bogus')").first()


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
# BNG aggregator UDFs — bad member cell id -> skip that member, not raise
# ---------------------------------------------------------------------------

# Chip-struct schema: (cellid STRING, core BOOLEAN, chip BINARY)
_CHIP_SCHEMA = StructType(
    [
        StructField("cellid", StringType()),
        StructField("core", BooleanType()),
        StructField("chip", BinaryType()),
    ]
)


def _make_chip_df(spark, rows):
    """Create a DataFrame with the chip struct schema from (cellid, core, chip) tuples."""
    return spark.createDataFrame(rows, schema=_CHIP_SCHEMA)


def test_bng_cellunion_agg_skips_bad_cellid_member(spark):
    """A group with one bad cell id member does not raise; valid core chip is returned."""
    # One valid core chip (chip=None, core=True: the fold materializes its polygon)
    # plus one malformed id.  The agg should return a non-null geometry, not raise.
    rows = [
        ("TQ3080", True, None),  # valid core chip — fold materializes full cell polygon
        ("!!", False, None),  # BAD cell id — must be skipped
    ]
    df = _make_chip_df(spark, rows)
    df.createOrReplaceTempView("_test_chips_union")
    result = spark.sql(
        "SELECT gbx_bng_cellunion_agg(struct(cellid, core, chip)) AS geom "
        "FROM _test_chips_union"
    ).first()
    # If the bad cell id were NOT skipped it would raise StopIteration.
    # With only the valid core chip remaining, the fold materializes TQ3080's polygon.
    assert result["geom"] is not None, "expected geometry from valid core member"


def test_bng_cellintersection_agg_skips_bad_cellid_member(spark):
    """A group with one bad cell id member does not raise on intersection agg."""
    rows = [
        ("TQ3080", True, None),  # valid core chip
        ("!!", False, None),  # BAD cell id — must be skipped
    ]
    df = _make_chip_df(spark, rows)
    df.createOrReplaceTempView("_test_chips_intersection")
    # Does not raise even with the malformed member present.
    result = spark.sql(
        "SELECT gbx_bng_cellintersection_agg(struct(cellid, core, chip)) AS geom "
        "FROM _test_chips_intersection"
    ).first()
    # After skipping '!!', only one valid core chip remains; the fold materializes
    # its polygon.  Load-bearing assertion: no exception, and geom is non-null.
    assert result["geom"] is not None, "expected geometry from valid core member"


# ---------------------------------------------------------------------------
# Custom scalar UDF — out-of-bounds coordinate DATA -> NULL
# (exercises point_to_cell_id_or_none degrade path, not the early None-input exit)
# ---------------------------------------------------------------------------


def test_custom_pointascell_null_on_null_geom(spark):
    """NULL geometry input hits the pre-existing early-exit and returns NULL."""
    grid_sql = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 500000, 500000)"
    r = spark.sql(f"SELECT gbx_custom_pointascell(NULL, {grid_sql}, 1) AS cid").first()
    assert r["cid"] is None


def test_custom_pointascell_null_on_oob_coordinate(spark):
    """A point outside the grid bounds degrades to NULL via point_to_cell_id_or_none."""
    # Grid covers [0,1000000)x[0,1000000); POINT(2000000 2000000) is out-of-bounds.
    grid_sql = "gbx_custom_grid(0, 1000000, 0, 1000000, 2, 500000, 500000)"
    r = spark.sql(
        f"SELECT gbx_custom_pointascell('POINT (2000000 2000000)', {grid_sql}, 1) AS cid"
    ).first()
    assert r["cid"] is None
