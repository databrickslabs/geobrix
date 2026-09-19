"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for gbx_rst_binpoints.

Both tiers are exercised with the SAME point arrays passed as **inline SQL
literals** (``array(0.5, 0.5, ...)``) and scalar bounds/size/srid also as
inline literals.  This is the primary regression guard for the Spark 4.0
Decimal/Double type-inference path: without ``override def inputTypes`` in
``RST_BinPoints``, these literals arrive as ``ARRAY<DECIMAL>`` at the eval
path, the coercion fails, ``RST_ErrorHandler`` swallows it, and the heavy tier
silently returns null.  Both tiers must produce non-null results.

Output tiles are compared per-pixel:
  * ``np.allclose(light, heavy, equal_nan=True, atol=1e-5)`` on the Float32
    band array.
  * NoData sentinel cells (-9999.0) must occupy identical positions in both
    tiers.
  * Non-null guard on the heavy result (primary Decimal-coercion regression
    guard).

Tested statistics: ``max``, ``min``, ``mean``, ``count``, ``median``.

Fixture point layout — 6 points over a 2×2 grid on [0,2]×[0,2], EPSG:2227:

  Row 0 = top (high y), Row 1 = bottom; Col 0 = left, Col 1 = right.

  arr[0,0] ← x∈[0,1), y∈[1,2): (0.5,1.5,10), (0.5,1.5,25)
  arr[0,1] ← x∈[1,2), y∈[1,2): (1.5,1.5,5),  (1.5,1.5,15), (1.5,1.5,30)
  arr[1,0] ← x∈[0,1), y∈[0,1): (0.5,0.5,7)
  arr[1,1] ← empty → NoData for all statistics

Per-statistic expected values:
  stat    arr[0,0]  arr[0,1]       arr[1,0]  arr[1,1]
  max     25.0      30.0           7.0       NoData
  min     10.0       5.0           7.0       NoData
  mean    17.5      50/3 ≈ 16.667  7.0       NoData
  count    2.0       3.0           1.0       NoData
  median  17.5      15.0           7.0       NoData

TIER INVOCATION STRATEGY:
  Both tiers register the same SQL name ``gbx_rst_binpoints``; the last
  ``register()`` call wins.  Results are collected SEQUENTIALLY:
    1. Register light (pyrx.register), run SQL, **collect** → light tile struct.
    2. Register heavy (rasterx.register, overwrites the SQL name), run SQL,
       collect → heavy tile struct.
  The ``.collect()`` materialises the light DataFrame BEFORE the heavy
  registration overwrites the SQL name — this ordering is load-bearing.  A
  lazy plan captured across the overwrite would compare heavy-vs-heavy (false
  pass).

Heavy requires the geobrix JAR staged under python/geobrix/lib/.
Auto-skips when the JAR is absent or a JAR-free Spark session is already live.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pyrx/test_binpoints_cross_tier.py \\
        --with-integration --log binpoints-crosstier.log
"""

import logging
from pathlib import Path

import pytest

rasterio = pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (geobrix[light_env6] required)",
)
import numpy as np  # noqa: E402
from rasterio.io import MemoryFile  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix  (test/pyrx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

_NODATA = -9999.0


# ---------------------------------------------------------------------------
# Spark fixture — JAR loaded (module scope), same pattern as test_chm_cross_tier
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting; no effect if JVM is already live.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; run "
                "this test in isolation: gbx:test:python --path "
                "python/geobrix/test/pyrx/test_binpoints_cross_tier.py --with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-binpoints-cross-tier-parity")
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
# Band-reading helper
# ---------------------------------------------------------------------------


def _read_band(tile_row):
    """Return (arr, nodata) from a tile struct's raster field.

    *tile_row* is a PySpark Row with a ``raster`` field containing GTiff bytes.
    Returns a (float32 ndarray, float nodata) pair.
    """
    with MemoryFile(bytes(tile_row["raster"])) as mf:
        with mf.open() as ds:
            arr = ds.read(1).astype("float32")
            nd = ds.nodata
    return arr, nd


# ---------------------------------------------------------------------------
# Two-phase execution helpers
# ---------------------------------------------------------------------------
#
# The SQL template uses inline literals for all 11 arguments:
#   array(...) literals → Spark 4.0 may infer ARRAY<DECIMAL>, coerced by inputTypes.
#   0.0 / 2.0 scalar bounds → Spark 4.0 may infer DECIMAL, coerced to DOUBLE.
#   2, 2227 integer literals → INT (already correct).
#
# Using two separate view names ("_bp_parity_light", "_bp_parity_heavy") keeps
# the light and heavy plan references independent so a re-register cannot
# silently re-evaluate the light plan against the new heavy implementation.
#
_SQL_TEMPLATE = (
    "SELECT gbx_rst_binpoints("
    "  array(0.5, 0.5, 1.5, 1.5, 1.5, 0.5),"
    "  array(1.5, 1.5, 1.5, 1.5, 1.5, 0.5),"
    "  array(10.0, 25.0, 5.0, 15.0, 30.0, 7.0),"
    "  0.0, 0.0, 2.0, 2.0,"
    "  2, 2, 2227,"
    "  '{stat}'"
    ") AS r "
    "FROM {view}"
)


def _run_light(spark, stat):
    """Register light pyrx, run gbx_rst_binpoints with inline SQL, collect tile.

    MUST be called before _run_heavy so the result is materialised before heavy
    registration overwrites the SQL name.

    Returns (float32 band array, nodata value).
    """
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    spark.range(1).createOrReplaceTempView("_bp_parity_light")
    sql = _SQL_TEMPLATE.format(stat=stat, view="_bp_parity_light")
    row = spark.sql(sql).collect()[0]["r"]
    assert (
        row is not None
    ), f"light gbx_rst_binpoints(stat='{stat}') returned null — unexpected"
    return _read_band(row)


def _run_heavy(spark, stat):
    """Register heavy rasterx (overwrites SQL name), collect tile.

    Returns (float32 band array, nodata value).  The non-null assertion is the
    primary regression guard for the Decimal→Double coercion path.
    """
    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)

    spark.range(1).createOrReplaceTempView("_bp_parity_heavy")
    sql = _SQL_TEMPLATE.format(stat=stat, view="_bp_parity_heavy")
    row = spark.sql(sql).collect()[0]["r"]
    assert row is not None, (
        f"heavy gbx_rst_binpoints(stat='{stat}') returned null — "
        "possible Decimal→Double coercion failure: check RST_BinPoints.inputTypes override"
    )
    return _read_band(row)


# ---------------------------------------------------------------------------
# Shared assertion helpers
# ---------------------------------------------------------------------------


def _nodata_to_nan(arr, nodata):
    """Return float64 copy of *arr* with nodata pixels replaced by NaN."""
    out = arr.astype("float64")
    if nodata is not None:
        out[arr == nodata] = float("nan")
    return out


def _assert_parity(light_arr, heavy_arr, light_nd, heavy_nd, label=""):
    """Per-pixel cross-tier parity assertion.

    Checks:
      1. Same shape.
      2. Both tiers report the NoData sentinel as -9999.0.
      3. NoData cell positions (mask) are identical.
      4. Non-NoData pixels agree within atol=1e-5 (float32 arithmetic).
    """
    prefix = f"[{label}] " if label else ""

    assert (
        light_arr.shape == heavy_arr.shape
    ), f"{prefix}shape mismatch: light={light_arr.shape} heavy={heavy_arr.shape}"
    assert light_nd == pytest.approx(
        _NODATA
    ), f"{prefix}light nodata sentinel must be {_NODATA}, got {light_nd}"
    assert heavy_nd == pytest.approx(
        _NODATA
    ), f"{prefix}heavy nodata sentinel must be {_NODATA}, got {heavy_nd}"

    light_mask = light_arr == _NODATA
    heavy_mask = heavy_arr == _NODATA
    assert np.array_equal(light_mask, heavy_mask), (
        f"{prefix}NoData cell positions disagree:\n"
        f"  light:\n{light_arr}\n  heavy:\n{heavy_arr}"
    )

    ln = _nodata_to_nan(light_arr, light_nd)
    hn = _nodata_to_nan(heavy_arr, heavy_nd)
    assert np.allclose(ln, hn, equal_nan=True, atol=1e-5), (
        f"{prefix}per-pixel parity FAILED.\n"
        f"light:\n{light_arr}\nheavy:\n{heavy_arr}\n"
        f"max abs diff: {np.nanmax(np.abs(ln - hn))}"
    )


# ---------------------------------------------------------------------------
# Test 1: Primary parity test — inline SQL literals, max statistic.
#
#   This is the primary regression guard for the Decimal→Double coercion path:
#     - array(0.5, ...) → ARRAY<DECIMAL> in Spark 4.0 without inputTypes fix.
#     - Without the fix, RST_ErrorHandler swallows the ClassCastException and
#       the heavy tier silently returns null.
#     - The non-null assertion in _run_heavy() catches that regression.
#
#   Expected:
#     arr[0,0] = max(10, 25) = 25.0
#     arr[0,1] = max(5, 15, 30) = 30.0
#     arr[1,0] = max(7) = 7.0
#     arr[1,1] = -9999.0 (empty cell)
# ---------------------------------------------------------------------------


def test_binpoints_max_parity(spark_with_jar):
    """Light and heavy gbx_rst_binpoints agree on max statistic (inline SQL literals).

    All 11 arguments are inline SQL literals — the primary guard for the Spark 4.0
    ARRAY<DECIMAL> coercion path.  Both tiers must return non-null and agree
    per-pixel within atol=1e-5.
    """
    spark = spark_with_jar

    # Collect LIGHT first; heavy registration overwrites the SQL name next.
    light_arr, light_nd = _run_light(spark, "max")
    heavy_arr, heavy_nd = _run_heavy(spark, "max")

    _assert_parity(light_arr, heavy_arr, light_nd, heavy_nd, label="max")

    # Spot-check expected values against known ground truth.
    assert light_arr.shape == (2, 2), f"output must be 2×2, got {light_arr.shape}"
    assert light_arr[0, 0] == pytest.approx(25.0), "arr[0,0]=max(10,25)=25"
    assert light_arr[0, 1] == pytest.approx(30.0), "arr[0,1]=max(5,15,30)=30"
    assert light_arr[1, 0] == pytest.approx(7.0), "arr[1,0]=max(7)=7"
    assert light_arr[1, 1] == pytest.approx(_NODATA), "arr[1,1]=empty→NoData"


# ---------------------------------------------------------------------------
# Test 2: min statistic
#
#   arr[0,0] = min(10, 25) = 10.0
#   arr[0,1] = min(5, 15, 30) = 5.0
#   arr[1,0] = min(7) = 7.0
#   arr[1,1] = -9999.0
# ---------------------------------------------------------------------------


def test_binpoints_min_parity(spark_with_jar):
    """Light and heavy gbx_rst_binpoints agree on min statistic."""
    spark = spark_with_jar

    light_arr, light_nd = _run_light(spark, "min")
    heavy_arr, heavy_nd = _run_heavy(spark, "min")

    _assert_parity(light_arr, heavy_arr, light_nd, heavy_nd, label="min")

    assert light_arr[0, 0] == pytest.approx(10.0), "arr[0,0]=min(10,25)=10"
    assert light_arr[0, 1] == pytest.approx(5.0), "arr[0,1]=min(5,15,30)=5"
    assert light_arr[1, 0] == pytest.approx(7.0), "arr[1,0]=min(7)=7"
    assert light_arr[1, 1] == pytest.approx(_NODATA), "arr[1,1]=empty→NoData"


# ---------------------------------------------------------------------------
# Test 3: mean statistic
#
#   arr[0,0] = mean(10, 25) = 17.5
#   arr[0,1] = mean(5, 15, 30) = 50/3 ≈ 16.667
#   arr[1,0] = mean(7) = 7.0
#   arr[1,1] = -9999.0
# ---------------------------------------------------------------------------


def test_binpoints_mean_parity(spark_with_jar):
    """Light and heavy gbx_rst_binpoints agree on mean statistic."""
    spark = spark_with_jar

    light_arr, light_nd = _run_light(spark, "mean")
    heavy_arr, heavy_nd = _run_heavy(spark, "mean")

    _assert_parity(light_arr, heavy_arr, light_nd, heavy_nd, label="mean")

    assert light_arr[0, 0] == pytest.approx(17.5, abs=1e-4), "arr[0,0]=mean(10,25)=17.5"
    assert light_arr[0, 1] == pytest.approx(
        50.0 / 3, abs=1e-4
    ), "arr[0,1]=mean(5,15,30)=50/3"
    assert light_arr[1, 0] == pytest.approx(7.0, abs=1e-4), "arr[1,0]=mean(7)=7"
    assert light_arr[1, 1] == pytest.approx(_NODATA), "arr[1,1]=empty→NoData"


# ---------------------------------------------------------------------------
# Test 4: count statistic
#
#   arr[0,0] = 2.0  (two points)
#   arr[0,1] = 3.0  (three points)
#   arr[1,0] = 1.0  (one point)
#   arr[1,1] = -9999.0  (count=0 → NoData)
# ---------------------------------------------------------------------------


def test_binpoints_count_parity(spark_with_jar):
    """Light and heavy gbx_rst_binpoints agree on count statistic.

    Zero-count cells (arr[1,1]) must produce NoData in both tiers —
    not 0.0.
    """
    spark = spark_with_jar

    light_arr, light_nd = _run_light(spark, "count")
    heavy_arr, heavy_nd = _run_heavy(spark, "count")

    _assert_parity(light_arr, heavy_arr, light_nd, heavy_nd, label="count")

    assert light_arr[0, 0] == pytest.approx(2.0), "arr[0,0]=count=2"
    assert light_arr[0, 1] == pytest.approx(3.0), "arr[0,1]=count=3"
    assert light_arr[1, 0] == pytest.approx(1.0), "arr[1,0]=count=1"
    assert light_arr[1, 1] == pytest.approx(_NODATA), "arr[1,1]=count=0→NoData"


# ---------------------------------------------------------------------------
# Test 5: median statistic (numpy-compatible linear interpolation)
#
#   Both tiers implement numpy's method='linear' percentile:
#     idx = p/100 * (n-1)
#     result = sorted[floor(idx)] + frac*(sorted[ceil(idx)] - sorted[floor(idx)])
#
#   arr[0,0] = median([10,25]):   idx=0.5*(2-1)=0.5,  result=10+0.5*15=17.5
#   arr[0,1] = median([5,15,30]): idx=0.5*(3-1)=1.0,  result=sorted[1]=15.0
#   arr[1,0] = median([7]):       idx=0,               result=7.0
#   arr[1,1] = -9999.0
# ---------------------------------------------------------------------------


def test_binpoints_median_parity(spark_with_jar):
    """Light and heavy gbx_rst_binpoints agree on median (numpy linear percentile).

    arr[0,0] has an even-count set ([10,25]) which exercises the linear
    interpolation path (result=17.5, not a simple middle element).  Both
    tiers implement numpy's ``method='linear'`` algorithm identically.
    """
    spark = spark_with_jar

    light_arr, light_nd = _run_light(spark, "median")
    heavy_arr, heavy_nd = _run_heavy(spark, "median")

    _assert_parity(light_arr, heavy_arr, light_nd, heavy_nd, label="median")

    assert light_arr[0, 0] == pytest.approx(
        17.5, abs=1e-4
    ), "arr[0,0]=median([10,25])=17.5 (linear interp)"
    assert light_arr[0, 1] == pytest.approx(
        15.0, abs=1e-4
    ), "arr[0,1]=median([5,15,30])=15.0 (exact middle)"
    assert light_arr[1, 0] == pytest.approx(7.0, abs=1e-4), "arr[1,0]=median([7])=7"
    assert light_arr[1, 1] == pytest.approx(_NODATA), "arr[1,1]=empty→NoData"


# ---------------------------------------------------------------------------
# Test 6: Half-open edge — point at x==xmax is dropped in both tiers
#
#   xmax = 2.0; a point at x=2.0 computes col = floor((2-0)/2*2) = 2 == width_px.
#   The strict half-open interval rule drops it; z=99 must NOT appear in output.
#   The only non-NoData cell is arr[0,0] = 42.0 (the valid point at x=0.5).
# ---------------------------------------------------------------------------


def test_binpoints_half_open_edge_parity(spark_with_jar):
    """Point at x==xmax (2.0) is dropped by both tiers (strict half-open interval).

    Two points: one valid (x=0.5, z=42) landing in arr[0,0] and one on the
    right boundary (x=xmax=2.0, z=99) that must be dropped.  Neither tier
    should produce z=99 in any output cell.
    """
    spark = spark_with_jar

    edge_sql = (
        "SELECT gbx_rst_binpoints("
        "  array(0.5, 2.0),"
        "  array(1.5, 1.5),"
        "  array(42.0, 99.0),"
        "  0.0, 0.0, 2.0, 2.0,"
        "  2, 2, 2227,"
        "  'max'"
        ") AS r "
        "FROM {view}"
    )

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    spark.range(1).createOrReplaceTempView("_bp_edge_light")
    light_row = spark.sql(edge_sql.format(view="_bp_edge_light")).collect()[0]["r"]
    assert light_row is not None, "light half-open edge test returned null"
    light_arr, light_nd = _read_band(light_row)

    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    spark.range(1).createOrReplaceTempView("_bp_edge_heavy")
    heavy_row = spark.sql(edge_sql.format(view="_bp_edge_heavy")).collect()[0]["r"]
    assert heavy_row is not None, "heavy half-open edge test returned null"
    heavy_arr, heavy_nd = _read_band(heavy_row)

    _assert_parity(light_arr, heavy_arr, light_nd, heavy_nd, label="half-open-edge")

    # Boundary point (z=99) must be absent from both tiers.
    for arr, tier in ((light_arr, "light"), (heavy_arr, "heavy")):
        non_nodata = arr[arr != _NODATA]
        assert 99.0 not in non_nodata, (
            f"{tier}: boundary point (x=xmax=2.0, z=99) was NOT dropped — "
            f"found in output: {non_nodata}"
        )

    # Valid point (x=0.5, y=1.5, z=42) must land in arr[0,0].
    assert light_arr[0, 0] == pytest.approx(
        42.0
    ), f"light arr[0,0] must be 42.0 (valid point), got {light_arr[0, 0]}"
    assert heavy_arr[0, 0] == pytest.approx(
        42.0
    ), f"heavy arr[0,0] must be 42.0 (valid point), got {heavy_arr[0, 0]}"
