"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for gbx_rst_isoband.

Both tiers are run on the SAME in-memory GTiff bytes with a five-break band
schedule ``[0.0, 50.0, 100.0, 150.0, 200.0]``.  Their ARRAY<STRUCT> outputs are
compared as SORTED SETS — not positionally — because ``gdal.Polygonize`` (heavy)
and ``rasterio.features.shapes`` (light) return contiguous-patch polygons in
arbitrary emission order.

Comparison contract:
  1. Identical total patch count.
  2. Identical set of ``(band, lower, upper)`` tuples.
  3. Within each ``(band, lower, upper)`` group, patches sorted by area; per-pair
     area equivalence within ``abs=1e-5``.

TIER INVOCATION STRATEGY:
  Both tiers register the same SQL name ``gbx_rst_isoband``; the last
  ``register()`` call wins.  Results are collected SEQUENTIALLY:
    1. Register light (pyrx.register), run SQL, **collect** → light rows.
    2. Register heavy (rasterx.register, overwrites the SQL name), run SQL,
       collect → heavy rows.
  The ``.collect()`` materialises the light DataFrame BEFORE the heavy
  registration overwrites the SQL name — this ordering is load-bearing.  A lazy
  plan captured across the overwrite would compare heavy-vs-heavy (false pass).

Heavy requires the geobrix JAR (GDAL JNI) staged under python/geobrix/lib/.
Auto-skips when the JAR is absent or a JAR-free Spark session is already live.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pyrx/test_isoband_cross_tier.py \\
        --with-integration --log isoband-crosstier.log
"""

import logging
from pathlib import Path

import pytest

rasterio = pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (geobrix[light_env6] required)",
)
import numpy as np  # noqa: E402
import shapely.wkb  # noqa: E402
from rasterio.io import MemoryFile  # noqa: E402
from rasterio.transform import from_origin  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix  (test/pyrx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


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
                "python/geobrix/test/pyrx/test_isoband_cross_tier.py --with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-isoband-cross-tier-parity")
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
# GTiff helper (mirrors _make_geotiff_bytes in test_chm.py / test_chm_cross_tier.py)
# ---------------------------------------------------------------------------


def _make_geotiff_bytes(
    data,
    nodata=-9999.0,
    epsg=4326,
    ulx=10.0,
    uly=50.0,
    pixel_size=0.5,
):
    """Return single-band Float32 GTiff bytes from *data* (2-D array)."""
    arr = np.asarray(data, dtype="float32")
    h, w = arr.shape
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(ulx, uly, pixel_size, pixel_size),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as dst:
            dst.write(arr, 1)
        return mf.read()


# ---------------------------------------------------------------------------
# Sorted-set comparison helpers
# ---------------------------------------------------------------------------

_BREAKS_5 = [0.0, 50.0, 100.0, 150.0, 200.0]

# ---------------------------------------------------------------------------
# Schema helpers: pass breaks as a typed ARRAY<DOUBLE> column so that the
# Scala eval path receives real Double values.  Inline SQL literals like
# array(0.0, 50.0, …) are inferred as DECIMAL in Spark 4.0, which causes
# breaksData.toDoubleArray() to throw ClassCastException at runtime (the JVM
# cannot cast Decimal to java.lang.Double in the GenericArrayData path).
# Passing a pre-typed column avoids the type-inference gap without touching
# RST_Isoband.scala.
# ---------------------------------------------------------------------------

from pyspark.sql.types import (  # noqa: E402
    ArrayType,
    BinaryType,
    DoubleType,
    StructField,
    StructType,
)

_ISOBAND_INPUT_SCHEMA = StructType([
    StructField("tile", BinaryType()),
    StructField("breaks", ArrayType(DoubleType())),
])


def _collect_isoband_structs(rows):
    """Extract list of (band, lower, upper, area) from a collected isoband row.

    *rows* is the list of PySpark Row objects returned by ``.collect()``.  The
    first (and only) row contains field ``r`` which is the isoband array result —
    a list of Rows each with fields ``geom_wkb``, ``band``, ``lower``, ``upper``.
    """
    result = rows[0]["r"]
    structs = []
    for s in result:
        geom = shapely.wkb.loads(bytes(s["geom_wkb"]))
        structs.append((int(s["band"]), float(s["lower"]), float(s["upper"]), geom.area))
    return structs


def _sorted_structs(structs):
    """Sort (band, lower, upper, area) by (band, lower, upper, area) for deterministic comparison."""
    return sorted(structs, key=lambda t: (t[0], t[1], t[2], t[3]))


def _assert_isoband_parity(light_structs, heavy_structs, label=""):
    """Compare light and heavy isoband struct lists as sorted sets.

    Checks:
      1. Identical total patch count.
      2. Identical set of (band, lower, upper).
      3. Within each (band, lower, upper) group, same patch count and per-patch
         area equivalence within abs=1e-5.
    """
    prefix = f"[{label}] " if label else ""

    assert len(light_structs) == len(heavy_structs), (
        f"{prefix}patch count mismatch: light={len(light_structs)} "
        f"heavy={len(heavy_structs)}\n"
        f"  light: {light_structs}\n"
        f"  heavy: {heavy_structs}"
    )

    light_key_set = {(b, lo, hi) for b, lo, hi, _ in light_structs}
    heavy_key_set = {(b, lo, hi) for b, lo, hi, _ in heavy_structs}
    assert light_key_set == heavy_key_set, (
        f"{prefix}(band,lower,upper) set mismatch:\n"
        f"  light only: {light_key_set - heavy_key_set}\n"
        f"  heavy only: {heavy_key_set - light_key_set}"
    )

    # Per-(band, lower, upper) group: compare sorted areas.
    for key in sorted(light_key_set):
        b, lo, hi = key
        l_areas = sorted(a for bb, ll, uu, a in light_structs if (bb, ll, uu) == key)
        h_areas = sorted(a for bb, ll, uu, a in heavy_structs if (bb, ll, uu) == key)
        assert len(l_areas) == len(h_areas), (
            f"{prefix}band({b},{lo},{hi}) patch count: light={len(l_areas)} heavy={len(h_areas)}"
        )
        for i, (la, ha) in enumerate(zip(l_areas, h_areas)):
            assert la == pytest.approx(ha, abs=1e-5), (
                f"{prefix}band({b},{lo},{hi}) patch[{i}] area mismatch: "
                f"light={la} heavy={ha}"
            )


# ---------------------------------------------------------------------------
# Two-phase isoband execution helpers
# ---------------------------------------------------------------------------

# Use a column reference (``breaks``) rather than inline SQL literals so that
# the breaks value arrives at the Scala eval path as ARRAY<DOUBLE>, not as
# the DECIMAL type that Spark 4.0 infers from ``array(0.0, 50.0, …)`` literals.
_SQL = (
    "SELECT gbx_rst_isoband("
    "  gbx_rst_fromcontent(tile, 'GTiff'),"
    "  breaks"
    ") AS r "
    "FROM {view}"
)


def _run_light_isoband(spark, raster_bytes):
    """Register light pyrx, run gbx_rst_isoband via SQL, collect → list of Row.

    MUST be called before _run_heavy_isoband so the light result is materialised
    before heavy registration overwrites the SQL name.
    """
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    df = spark.createDataFrame(
        [(bytearray(raster_bytes), _BREAKS_5)],
        _ISOBAND_INPUT_SCHEMA,
    )
    df.createOrReplaceTempView("_isoband_parity_light")
    return spark.sql(_SQL.format(view="_isoband_parity_light")).collect()


def _run_heavy_isoband(spark, raster_bytes):
    """Register heavy rasterx (overwrites SQL name), run gbx_rst_isoband, collect.

    The SQL name gbx_rst_isoband is now bound to the GDAL Scala expression.
    """
    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)

    df = spark.createDataFrame(
        [(bytearray(raster_bytes), _BREAKS_5)],
        _ISOBAND_INPUT_SCHEMA,
    )
    df.createOrReplaceTempView("_isoband_parity_heavy")
    return spark.sql(_SQL.format(view="_isoband_parity_heavy")).collect()


# ---------------------------------------------------------------------------
# Test 1: Four-quadrant multi-bin raster
#   A 6×6 raster partitioned into four 3×3 quadrants, one per band:
#     TL = 25  → band 0 [0,  50)
#     TR = 75  → band 1 [50, 100)
#     BL = 125 → band 2 [100, 150)
#     BR = 175 → band 3 [150, 200)
#   Expected: exactly 4 patches, one per quadrant/band; areas must agree.
# ---------------------------------------------------------------------------


def test_isoband_four_quadrant_parity(spark_with_jar):
    """Light and heavy agree on a 4-quadrant, 4-band raster.

    Each quadrant maps to exactly one band.  Both tiers must produce the same
    4 patches with equal per-patch areas (within 1e-5).
    """
    spark = spark_with_jar

    data = np.zeros((6, 6), dtype="float32")
    data[0:3, 0:3] = 25.0   # band 0 [0,  50)
    data[0:3, 3:6] = 75.0   # band 1 [50, 100)
    data[3:6, 0:3] = 125.0  # band 2 [100, 150)
    data[3:6, 3:6] = 175.0  # band 3 [150, 200)

    raster = _make_geotiff_bytes(data)

    # Collect LIGHT first — see module docstring for why ordering is load-bearing.
    light_rows = _run_light_isoband(spark, raster)
    heavy_rows = _run_heavy_isoband(spark, raster)

    light_structs = _collect_isoband_structs(light_rows)
    heavy_structs = _collect_isoband_structs(heavy_rows)

    # Sanity: four patches expected (one per quadrant).
    assert len(light_structs) == 4, (
        f"light: expected 4 patches, got {len(light_structs)}: {light_structs}"
    )
    assert len(heavy_structs) == 4, (
        f"heavy: expected 4 patches, got {len(heavy_structs)}: {heavy_structs}"
    )

    _assert_isoband_parity(light_structs, heavy_structs, label="four-quadrant")


# ---------------------------------------------------------------------------
# Test 2: NoData exclusion — nodata pixels are absent from both tiers
#   A 4×4 raster with the centre 2×2 set to NoData; the outer ring carries
#   values in [0, 50).  NoData must produce 0 patches in the NoData cell, and
#   neither tier must invent a patch there.
# ---------------------------------------------------------------------------


def test_isoband_nodata_exclusion_parity(spark_with_jar):
    """Both tiers exclude NoData pixels and agree on the remaining patch(es).

    Inner 2×2 = NoData (-9999).  Outer ring = 25 (band 0 [0, 50)).
    Both tiers must return the same ring-shaped patch (one polygon with a hole,
    or four/eight separate polygons at corners — either way, the total area and
    the (band, lower, upper) set must agree).
    """
    spark = spark_with_jar

    data = np.full((4, 4), 25.0, dtype="float32")
    data[1:3, 1:3] = -9999.0  # centre nodata

    raster = _make_geotiff_bytes(data, nodata=-9999.0)

    light_rows = _run_light_isoband(spark, raster)
    heavy_rows = _run_heavy_isoband(spark, raster)

    light_structs = _collect_isoband_structs(light_rows)
    heavy_structs = _collect_isoband_structs(heavy_rows)

    # All patches must be band 0 [0, 50).
    for b, lo, hi, _ in light_structs:
        assert (b, lo, hi) == (0, 0.0, 50.0), (
            f"light: unexpected (band,lower,upper)=({b},{lo},{hi})"
        )
    for b, lo, hi, _ in heavy_structs:
        assert (b, lo, hi) == (0, 0.0, 50.0), (
            f"heavy: unexpected (band,lower,upper)=({b},{lo},{hi})"
        )

    # Cross-tier: same total area (sum over all patches).
    light_total_area = sum(a for _, _, _, a in light_structs)
    heavy_total_area = sum(a for _, _, _, a in heavy_structs)
    assert light_total_area == pytest.approx(heavy_total_area, abs=1e-5), (
        f"nodata-exclusion total area mismatch: "
        f"light={light_total_area} heavy={heavy_total_area}"
    )

    # And total patch count must agree.
    assert len(light_structs) == len(heavy_structs), (
        f"nodata-exclusion patch count mismatch: "
        f"light={len(light_structs)} heavy={len(heavy_structs)}"
    )


# ---------------------------------------------------------------------------
# Test 3: Out-of-range values — both tiers return empty for all-excluded pixels
# ---------------------------------------------------------------------------


def test_isoband_out_of_range_parity(spark_with_jar):
    """Both tiers return an empty array when all pixel values are out of range.

    All values are 300 (>= breaks[-1] = 200) so no pixel falls in any band.
    Both light and heavy must return an empty (or null) array.
    """
    spark = spark_with_jar

    data = np.full((4, 4), 300.0, dtype="float32")
    raster = _make_geotiff_bytes(data)

    light_rows = _run_light_isoband(spark, raster)
    heavy_rows = _run_heavy_isoband(spark, raster)

    light_result = light_rows[0]["r"]
    heavy_result = heavy_rows[0]["r"]

    light_empty = light_result is None or len(light_result) == 0
    heavy_empty = heavy_result is None or len(heavy_result) == 0

    assert light_empty, f"light: expected empty result for all-out-of-range, got {light_result}"
    assert heavy_empty, f"heavy: expected empty result for all-out-of-range, got {heavy_result}"


# ---------------------------------------------------------------------------
# Test 4: Disjoint same-band patches — two non-contiguous regions in one band
#   A 8×8 raster where the top-left 3×3 and bottom-right 3×3 both carry value
#   25 (band 0), separated by a band-1 region (value 75).  Each tier must
#   return 2 patches for band 0 and 1 for band 1 (or however many — but they
#   must agree on count and area).
# ---------------------------------------------------------------------------


def test_isoband_disjoint_patches_parity(spark_with_jar):
    """Both tiers produce identical disjoint-patch structure for the same band.

    Top-left 3×3 and bottom-right 3×3 hold value 25 (band 0);
    the rest (middle cross + band-1 region) holds value 75 (band 1).
    Both tiers must agree on patch counts, band/lower/upper sets, and areas.
    """
    spark = spark_with_jar

    data = np.full((8, 8), 75.0, dtype="float32")  # band 1 background
    data[0:3, 0:3] = 25.0  # top-left: band 0
    data[5:8, 5:8] = 25.0  # bottom-right: band 0 (disjoint)

    raster = _make_geotiff_bytes(data)

    light_rows = _run_light_isoband(spark, raster)
    heavy_rows = _run_heavy_isoband(spark, raster)

    light_structs = _collect_isoband_structs(light_rows)
    heavy_structs = _collect_isoband_structs(heavy_rows)

    _assert_isoband_parity(light_structs, heavy_structs, label="disjoint-patches")

    # Both tiers must see band 0 (value 25) and band 1 (value 75).
    light_bands = {b for b, _, _, _ in light_structs}
    assert 0 in light_bands, f"light: expected band 0, got bands {light_bands}"
    assert 1 in light_bands, f"light: expected band 1, got bands {light_bands}"
