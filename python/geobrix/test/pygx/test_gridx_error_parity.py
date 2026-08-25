"""Cross-tier parity: heavy (JAR) vs light (pygx) GridX error-handling contract.

This is the Task-10 spine for the GridX error-handling plan.  It asserts that the
heavy Scala tier (registered ``gbx_bng_*`` / ``gbx_quadbin_*`` SQL, run against the
staged JAR) and the light pygx tier (its Python UDFs) agree on EVERY degenerate
input the plan defines:

1. **Malformed BNG cell id → NULL on both tiers, no raise.**
   e.g. ``gbx_bng_aswkb('!!')`` → None.

2. **Bad BNG resolution → RAISES on both tiers (parameter condition).**
   e.g. ``gbx_bng_pointascell(<valid geom>, 'bogus')`` raises.

3. **Quadbin out-of-range latitude → CLAMPED (not NULL) on both tiers, same cell.**
   e.g. ``gbx_quadbin_pointascell(10.0, 89.0, 10)`` is non-null and equal.

4. **BNG kringexplode zero rows on both tiers.**
   e.g. ``SELECT cellid FROM gbx_bng_kringexplode('!!', 1)`` yields 0 rows.

## Session isolation

Both tiers register the SAME ``gbx_bng_*`` / ``gbx_quadbin_*`` SQL names; the last
``register`` call wins.  The pattern mirrors ``test_parity_bng.py``:

- Collect ALL light-tier results first (light session registers the pygx UDFs).
- Then register heavy (JAR session), which OVERWRITES the SQL names in the catalog.
- Collect ALL heavy-tier results.
- Assert parity.

Both share the same underlying ``spark_with_jar`` session; the UDTF / SQL functions
are swapped in place by the register call.

## Running — this is a GATE, not an optional extra

The suite is ``@integration`` and requires a staged JAR.  A plain
``gbx:test:python`` SKIPS it — and a skipped suite reads as green.  Use the gate::

    bash scripts/commands/gbx-test-parity.sh -k gridx_error --log gridx-parity.log

"""

import logging
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pygx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

_GATE_CMD = "bash scripts/commands/gbx-test-parity.sh"


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "CROSS-TIER PARITY NOT VERIFIED (this skip is NOT a pass): no geobrix "
            "assembly JAR staged under python/geobrix/lib/, so the heavy tier cannot "
            f"be called. Run the gate: {_GATE_CMD}"
        )

    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "CROSS-TIER PARITY NOT VERIFIED (this skip is NOT a pass): a JAR-free "
                "Spark session is already live in this process, so spark.jars cannot "
                f"take effect. Run the gate, which runs this suite properly: {_GATE_CMD}"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pygx-gridx-error-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.library.path=/usr/local/lib:/usr/lib:/usr/java/packages/lib:"
            "/usr/lib64:/lib64:/lib:/usr/local/hadoop/lib/native",
        )
        .config("spark.jars", str(_JARS[-1]))
        .getOrCreate()
    )
    yield session


# ---------------------------------------------------------------------------
# BNG case 1: malformed cell id → NULL on both tiers
# ---------------------------------------------------------------------------


def test_bng_malformed_cellid_null_both_tiers(spark_with_jar):
    """A malformed BNG cell id returns NULL on both tiers, no exception.

    The degenerate input '!!' cannot be parsed by either tier's BNG codec.
    Both tiers must degrade to NULL (data condition, not a parameter error).
    Checked via multiple scalar functions so the contract is exercised broadly.
    """
    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    # Collect light results FIRST, then register heavy (heavy register overwrites names).
    gx.register(spark)
    light_aswkb = spark.sql("SELECT gbx_bng_aswkb('!!') g").first()["g"]
    light_aswkt = spark.sql("SELECT gbx_bng_aswkt('!!') g").first()["g"]
    light_cellarea = spark.sql("SELECT gbx_bng_cellarea('!!') a").first()["a"]
    light_centroid = spark.sql("SELECT gbx_bng_centroid('!!') g").first()["g"]
    light_kring = spark.sql("SELECT gbx_bng_kring('!!', 1) arr").first()["arr"]

    hx.register(spark)
    heavy_aswkb = spark.sql("SELECT gbx_bng_aswkb('!!') g").first()["g"]
    heavy_aswkt = spark.sql("SELECT gbx_bng_aswkt('!!') g").first()["g"]
    heavy_cellarea = spark.sql("SELECT gbx_bng_cellarea('!!') a").first()["a"]
    heavy_centroid = spark.sql("SELECT gbx_bng_centroid('!!') g").first()["g"]
    heavy_kring = spark.sql("SELECT gbx_bng_kring('!!', 1) arr").first()["arr"]

    # Both tiers: NULL, no exception.
    assert light_aswkb is None, f"light aswkb('!!'): expected None, got {light_aswkb!r}"
    assert heavy_aswkb is None, f"heavy aswkb('!!'): expected None, got {heavy_aswkb!r}"

    assert light_aswkt is None, f"light aswkt('!!'): expected None, got {light_aswkt!r}"
    assert heavy_aswkt is None, f"heavy aswkt('!!'): expected None, got {heavy_aswkt!r}"

    assert (
        light_cellarea is None
    ), f"light cellarea('!!'): expected None, got {light_cellarea!r}"
    assert (
        heavy_cellarea is None
    ), f"heavy cellarea('!!'): expected None, got {heavy_cellarea!r}"

    assert (
        light_centroid is None
    ), f"light centroid('!!'): expected None, got {light_centroid!r}"
    assert (
        heavy_centroid is None
    ), f"heavy centroid('!!'): expected None, got {heavy_centroid!r}"

    assert light_kring is None, f"light kring('!!'): expected None, got {light_kring!r}"
    assert heavy_kring is None, f"heavy kring('!!'): expected None, got {heavy_kring!r}"


# ---------------------------------------------------------------------------
# BNG case 2: bad resolution → RAISES on both tiers (parameter condition)
# ---------------------------------------------------------------------------


def test_bng_bad_resolution_raises_both_tiers(spark_with_jar):
    """A bad resolution string raises on both tiers (parameter condition, not data).

    'bogus' is not a valid BNG resolution key (not in resolutionMap and not a
    valid integer index ±1..±6). Both tiers must raise because the caller explicitly
    supplied an unresolvable parameter — this is not a degrade path.
    """
    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    # London in BNG coords (EPSG:27700)
    valid_geom = "'POINT (530000 180000)'"

    gx.register(spark)
    with pytest.raises(Exception):
        spark.sql(f"SELECT gbx_bng_pointascell({valid_geom}, 'bogus')").first()

    hx.register(spark)
    with pytest.raises(Exception):
        spark.sql(f"SELECT gbx_bng_pointascell({valid_geom}, 'bogus')").first()


# ---------------------------------------------------------------------------
# Quadbin case 3: out-of-range latitude → CLAMPED (not NULL) on both tiers
# ---------------------------------------------------------------------------


def test_quadbin_out_of_range_lat_clamped_not_null(spark_with_jar):
    """Quadbin clamps out-of-range lat to web-mercator bounds on both tiers.

    Latitude 89.0 exceeds the quadbin max (85.05112878°) but is not invalid —
    it is clamped to the nearest valid value by both tiers.  The result must
    be non-null AND identical across tiers.  This is NOT a degrade path; it is
    intentional web-mercator behavior documented in both tier implementations.
    """
    from databricks.labs.gbx.gridx.quadbin import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    # SQL numeric literals are Decimal by default; heavy Quadbin_PointAsCell.execute
    # expects (Double, Double, Int), so CAST is required to avoid a method-not-found
    # error on the heavy side. Light's pandas_udf accepts any numeric type, but the
    # CAST makes both tiers use the same type path.
    gx.register(spark)
    light_val = spark.sql(
        "SELECT gbx_quadbin_pointascell(CAST(10.0 AS DOUBLE), CAST(89.0 AS DOUBLE), 10) c"
    ).first()["c"]

    hx.register(spark)
    heavy_val = spark.sql(
        "SELECT gbx_quadbin_pointascell(CAST(10.0 AS DOUBLE), CAST(89.0 AS DOUBLE), 10) c"
    ).first()["c"]

    assert (
        light_val is not None
    ), "light gbx_quadbin_pointascell(10.0, 89.0, 10): expected clamped cell, got None"
    assert (
        heavy_val is not None
    ), "heavy gbx_quadbin_pointascell(10.0, 89.0, 10): expected clamped cell, got None"
    assert (
        light_val == heavy_val
    ), f"quadbin clamped cell mismatch: light={light_val!r} heavy={heavy_val!r}"


# ---------------------------------------------------------------------------
# BNG case 4: kringexplode zero rows on bad cell id (both tiers)
# ---------------------------------------------------------------------------


def test_bng_kringexplode_zero_rows_both_tiers(spark_with_jar):
    """A malformed BNG cell id yields 0 rows from kringexplode on both tiers.

    Neither tier should raise; both degrade the bad cell id DATA to an empty
    result set (zero rows from the table function).

    Light: direct UDTF call (PySpark UDTF syntax: SELECT ... FROM udtf_name(...)).
    Heavy: ``gbx_bng_kringexplode`` is a Spark Generator expression registered via
    the JAR; Task 9 confirmed the Spark-4.0 invocation is
    ``SELECT ... FROM gbx_bng_kringexplode(...)`` (NOT LATERAL VIEW).
    """
    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    gx.register(spark)
    light_count = spark.sql("SELECT cellid FROM gbx_bng_kringexplode('!!', 1)").count()

    hx.register(spark)
    heavy_count = spark.sql("SELECT cellid FROM gbx_bng_kringexplode('!!', 1)").count()

    assert (
        light_count == 0
    ), f"light kringexplode('!!'): expected 0 rows, got {light_count}"
    assert (
        heavy_count == 0
    ), f"heavy kringexplode('!!'): expected 0 rows, got {heavy_count}"


# ---------------------------------------------------------------------------
# Supplemental: kloopexplode zero rows on bad cell id (both tiers)
# ---------------------------------------------------------------------------


def test_bng_kloopexplode_zero_rows_both_tiers(spark_with_jar):
    """kloopexplode on a malformed BNG cell id yields 0 rows on both tiers."""
    from databricks.labs.gbx.gridx.bng import functions as hx
    from databricks.labs.gbx.pygx import functions as gx

    spark = spark_with_jar

    gx.register(spark)
    light_count = spark.sql("SELECT cellid FROM gbx_bng_kloopexplode('!!', 1)").count()

    hx.register(spark)
    heavy_count = spark.sql("SELECT cellid FROM gbx_bng_kloopexplode('!!', 1)").count()

    assert (
        light_count == 0
    ), f"light kloopexplode('!!'): expected 0 rows, got {light_count}"
    assert (
        heavy_count == 0
    ), f"heavy kloopexplode('!!'): expected 0 rows, got {heavy_count}"
