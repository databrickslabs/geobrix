"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for gbx_rst_binpoints_agg.

Both tiers are exercised with the SAME point rows piped through a GROUP BY
aggregate.  Extent, size, srid, and statistic are inline SQL literals, which
is the primary regression guard for the Spark 4.0 DECIMAL coercion path.

TIER RETURN ASYMMETRY — this test's central contract:
  * Light SQL ``gbx_rst_binpoints_agg(...)`` returns **BINARY** (raw GTiff
    bytes) — the UDF is declared ``@pandas_udf(BinaryType())``.  The result
    column ``r`` is accessed as plain bytes: ``bytes(row["r"])``.
  * Heavy SQL ``gbx_rst_binpoints_agg(...)`` returns a **tile STRUCT** whose
    ``raster`` field holds the GTiff bytes.  The result is accessed as
    ``bytes(row["r"]["raster"])``.
  Both paths are then opened with ``rasterio.MemoryFile`` and compared
  pixel-by-pixel.

TWO-PHASE SEQUENTIAL COLLECT:
  Both tiers register the same SQL name ``gbx_rst_binpoints_agg``; the last
  ``register()`` call wins.  Results are collected in order:
    1. Register light (pyrx.register), run GROUP BY SQL, ``collect()`` →
       materialise the light result BEFORE the heavy registration overwrites
       the SQL name.
    2. Register heavy (rasterx.register, overwrites the name), run the
       equivalent GROUP BY SQL on a separate view, collect heavy result.
  The separate view names (``_bpa_parity_light``, ``_bpa_parity_heavy``) keep
  the plans independent so a re-register cannot cross-contaminate.

Fixture point layout — 6 points, ONE group, 2×2 grid on [0,2]×[0,2], EPSG:2227:

  Row 0 = top (high y), Row 1 = bottom; Col 0 = left, Col 1 = right.
  (Same layout as the scalar test_binpoints_cross_tier.py.)

  arr[0,0] ← x∈[0,1), y∈[1,2): (0.5,1.5,10), (0.5,1.5,25)
  arr[0,1] ← x∈[1,2), y∈[1,2): (1.5,1.5,5),  (1.5,1.5,15), (1.5,1.5,30)
  arr[1,0] ← x∈[0,1), y∈[0,1): (0.5,0.5,7)
  arr[1,1] ← empty → NoData for all statistics

Per-statistic expected values:
  stat    arr[0,0]  arr[0,1]       arr[1,0]  arr[1,1]
  max     25.0      30.0           7.0       NoData
  mean    17.5      50/3 ≈ 16.667  7.0       NoData
  count    2.0       3.0           1.0       NoData
  median  17.5      15.0           7.0       NoData

Tested statistics: ``max``, ``mean``, ``count``, ``median``.

Heavy requires the geobrix JAR staged under python/geobrix/lib/.
Auto-skips when the JAR is absent or a JAR-free Spark session is already live.

Run in geobrix-dev Docker::

    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pyrx/test_binpoints_agg_cross_tier.py \\
        --with-integration --log binpointsagg-crosstier.log
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
# Fixture point rows — 6 points in ONE group, mirroring the scalar test.
# ---------------------------------------------------------------------------
_POINT_ROWS = [
    # (grp, x,   y,   z)
    ("g1", 0.5, 1.5, 10.0),
    ("g1", 0.5, 1.5, 25.0),
    ("g1", 1.5, 1.5, 5.0),
    ("g1", 1.5, 1.5, 15.0),
    ("g1", 1.5, 1.5, 30.0),
    ("g1", 0.5, 0.5, 7.0),
]


# ---------------------------------------------------------------------------
# Spark fixture — JAR loaded, module scope; same pattern as test_binpoints_cross_tier
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; run "
                "this test in isolation: gbx:test:python --path "
                "python/geobrix/test/pyrx/test_binpoints_agg_cross_tier.py --with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-binpoints-agg-cross-tier-parity")
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
# Band-reading helpers (tier-aware — DIFFERENT read path per tier)
# ---------------------------------------------------------------------------


def _read_band_from_binary(raw):
    """Read band 1 from a raw BINARY result (light SQL aggregate).

    The light ``gbx_rst_binpoints_agg`` UDF is declared ``BinaryType()``;
    the SQL result column is raw GTiff bytes.  Open directly with MemoryFile.
    """
    with MemoryFile(bytes(raw)) as mf:
        with mf.open() as ds:
            arr = ds.read(1).astype("float32")
            nd = ds.nodata
    return arr, nd


def _read_band_from_tile_struct(tile_struct):
    """Read band 1 from a tile STRUCT result (heavy SQL aggregate).

    The heavy ``gbx_rst_binpoints_agg`` returns the canonical 9-field tile
    struct.  The GTiff bytes live in the ``raster`` field.
    """
    with MemoryFile(bytes(tile_struct["raster"])) as mf:
        with mf.open() as ds:
            arr = ds.read(1).astype("float32")
            nd = ds.nodata
    return arr, nd


# ---------------------------------------------------------------------------
# Two-phase GROUP BY execution helpers
# ---------------------------------------------------------------------------
#
# The SQL template runs GROUP BY over a registered view of _POINT_ROWS.
# Extent / size / srid / statistic are inline SQL literals — matching the
# scalar test's pattern for the DECIMAL coercion regression guard.
#
_SQL_TEMPLATE = (
    "SELECT gbx_rst_binpoints_agg("
    "  x, y, z,"
    "  0.0, 0.0, 2.0, 2.0,"
    "  2, 2, 2227,"
    "  '{stat}'"
    ") AS r "
    "FROM {view} "
    "GROUP BY grp"
)


def _run_light(spark, stat):
    """Register light pyrx, run GROUP BY SQL, collect and materialise.

    MUST be called before _run_heavy so the DataFrame is collected before
    heavy registration overwrites the SQL name.

    Returns (float32 band array, nodata value).
    Light returns BINARY → read directly via _read_band_from_binary.
    """
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    df = spark.createDataFrame(
        _POINT_ROWS,
        ["grp", "x", "y", "z"],
    )
    df.createOrReplaceTempView("_bpa_parity_light")

    sql = _SQL_TEMPLATE.format(stat=stat, view="_bpa_parity_light")
    rows = spark.sql(sql).collect()
    assert (
        len(rows) == 1
    ), f"light GROUP BY must return exactly 1 group row; got {len(rows)}"
    raw = rows[0]["r"]
    assert (
        raw is not None
    ), f"light gbx_rst_binpoints_agg(stat='{stat}') returned null — unexpected"
    return _read_band_from_binary(raw)


def _run_heavy(spark, stat):
    """Register heavy rasterx (overwrites SQL name), run GROUP BY SQL, collect.

    Returns (float32 band array, nodata value).
    Heavy returns a tile STRUCT → read via _read_band_from_tile_struct.
    The non-null assertion is the primary regression guard for the agg plumbing.

    GDAL init: ``RST_BinPointsAgg.eval()`` carries an ``ExpressionConfigExpr``
    field (same pattern as ``RST_CombineAvgAgg``) and calls
    ``RST_ExpressionUtil.init(exprConf)`` at the top of ``eval`` before invoking
    ``RST_BinPoints.execute``.  No scalar warm-up is needed; the aggregate
    self-initialises GDAL on first eval on any executor JVM.
    """
    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)

    df = spark.createDataFrame(
        _POINT_ROWS,
        ["grp", "x", "y", "z"],
    )
    df.createOrReplaceTempView("_bpa_parity_heavy")

    sql = _SQL_TEMPLATE.format(stat=stat, view="_bpa_parity_heavy")
    rows = spark.sql(sql).collect()
    assert (
        len(rows) == 1
    ), f"heavy GROUP BY must return exactly 1 group row; got {len(rows)}"
    tile_struct = rows[0]["r"]
    assert tile_struct is not None, (
        f"heavy gbx_rst_binpoints_agg(stat='{stat}') returned null — "
        "possible agg plumbing or Decimal coercion failure"
    )
    return _read_band_from_tile_struct(tile_struct)


# ---------------------------------------------------------------------------
# Shared assertion helpers (identical to scalar test)
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
# Test 1: max statistic
#
#   Both tiers must agree; heavy returns tile STRUCT, light returns BINARY.
#   Expected:
#     arr[0,0] = max(10, 25) = 25.0
#     arr[0,1] = max(5, 15, 30) = 30.0
#     arr[1,0] = max(7) = 7.0
#     arr[1,1] = -9999.0 (empty cell)
# ---------------------------------------------------------------------------


def test_binpoints_agg_max_parity(spark_with_jar):
    """Light (BINARY) and heavy (tile struct) gbx_rst_binpoints_agg agree on max.

    Verifies tier-asymmetry read paths: light result read as raw bytes,
    heavy result read via tile struct's ``raster`` field.  Per-pixel parity
    within atol=1e-5; empty cell must be NoData in both tiers.
    """
    spark = spark_with_jar

    # Collect LIGHT first; heavy registration overwrites the SQL name next.
    light_arr, light_nd = _run_light(spark, "max")
    heavy_arr, heavy_nd = _run_heavy(spark, "max")

    _assert_parity(light_arr, heavy_arr, light_nd, heavy_nd, label="max")

    assert light_arr.shape == (2, 2), f"output must be 2×2, got {light_arr.shape}"
    assert light_arr[0, 0] == pytest.approx(25.0), "arr[0,0]=max(10,25)=25"
    assert light_arr[0, 1] == pytest.approx(30.0), "arr[0,1]=max(5,15,30)=30"
    assert light_arr[1, 0] == pytest.approx(7.0), "arr[1,0]=max(7)=7"
    assert light_arr[1, 1] == pytest.approx(_NODATA), "arr[1,1]=empty→NoData"


# ---------------------------------------------------------------------------
# Test 2: mean statistic
#
#   arr[0,0] = mean(10, 25) = 17.5
#   arr[0,1] = mean(5, 15, 30) = 50/3 ≈ 16.667
#   arr[1,0] = mean(7) = 7.0
#   arr[1,1] = -9999.0
# ---------------------------------------------------------------------------


def test_binpoints_agg_mean_parity(spark_with_jar):
    """Light and heavy gbx_rst_binpoints_agg agree on mean statistic."""
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
# Test 3: count statistic
#
#   arr[0,0] = 2.0   arr[0,1] = 3.0   arr[1,0] = 1.0   arr[1,1] = -9999.0
# ---------------------------------------------------------------------------


def test_binpoints_agg_count_parity(spark_with_jar):
    """Light and heavy gbx_rst_binpoints_agg agree on count statistic.

    Zero-count cells (arr[1,1]) must produce NoData in both tiers, not 0.0.
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
# Test 4: median statistic (numpy-compatible linear interpolation)
#
#   arr[0,0] = median([10,25]) = 17.5   (linear interp on even-count set)
#   arr[0,1] = median([5,15,30]) = 15.0  (exact middle)
#   arr[1,0] = median([7]) = 7.0
#   arr[1,1] = -9999.0
# ---------------------------------------------------------------------------


def test_binpoints_agg_median_parity(spark_with_jar):
    """Light and heavy gbx_rst_binpoints_agg agree on median (numpy linear).

    arr[0,0] has an even-count set ([10,25]) exercising the linear
    interpolation path (result=17.5, not a simple middle element).
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
