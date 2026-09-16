"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for gbx_rst_chm.

Both tiers are run on the SAME in-memory DSM/DEM bytes and their single-band
Float32 output arrays compared pixel-by-pixel:

  * Normal same-grid case: identical CHM values within 1e-5 (float32 arithmetic).
  * Padded/below-datum case: DSM smaller than a below-datum DEM with no nodata;
    covered pixels and uncovered (padded) pixels must BOTH agree on value and on
    the NoData sentinel (-9999), proving that neither tier invents canopy on padding.

TIER INVOCATION STRATEGY:
  Both tiers register the same SQL name ``gbx_rst_chm``; the last ``register()``
  call wins.  Results are collected SEQUENTIALLY:
    1. Register light (pyrx.register), run SQL, collect → light tile struct.
    2. Register heavy (rasterx.register, overwrites the SQL name), run SQL,
       collect → heavy tile struct.
  This mirrors the two-phase pattern used in test_parity_custom_raster.py and
  test_parity_bng_quadbin_raster_grid.py.

Heavy requires the geobrix JAR (GDAL JNI) staged under python/geobrix/lib/.
Auto-skips when the JAR is absent or a JAR-free Spark session is already live.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pyrx/test_chm_cross_tier.py \\
        --with-integration --log chm-crosstier.log
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
from rasterio.transform import from_origin  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix  (test/pyrx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

_NODATA = -9999.0


# ---------------------------------------------------------------------------
# Spark fixture — JAR loaded (module scope), same pattern as other parity tests
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
                "python/geobrix/test/pyrx/test_chm_cross_tier.py --with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-chm-cross-tier-parity")
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
# Shared GTiff helper (mirrors _make_geotiff_bytes in test_chm.py)
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
# Two-phase CHM execution helpers
# ---------------------------------------------------------------------------


def _run_light_chm(spark, dsm_bytes, dem_bytes):
    """Register light pyrx, run gbx_rst_chm via SQL, collect tile struct.

    Returns the raster bytes (band 1 array) as float32 numpy array, plus the
    recorded nodata value from the output raster.
    """
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)

    df = spark.createDataFrame(
        [(bytearray(dsm_bytes), bytearray(dem_bytes))],
        "dsm binary, dem binary",
    )
    df.createOrReplaceTempView("_chm_parity_light")
    row = spark.sql(
        "SELECT gbx_rst_chm("
        "  gbx_rst_fromcontent(dsm, 'GTiff'),"
        "  gbx_rst_fromcontent(dem, 'GTiff')"
        ") AS r "
        "FROM _chm_parity_light"
    ).collect()[0]["r"]

    with MemoryFile(bytes(row["raster"])) as mf:
        with mf.open() as ds:
            arr = ds.read(1).astype("float32")
            nd = ds.nodata
    return arr, nd


def _run_heavy_chm(spark, dsm_bytes, dem_bytes):
    """Register heavy rasterx (overwrites SQL name), run gbx_rst_chm, collect.

    Returns band 1 array as float32 numpy array, plus the nodata value.
    The SQL name gbx_rst_chm is now bound to the GDAL Scala expression.
    """
    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)

    df = spark.createDataFrame(
        [(bytearray(dsm_bytes), bytearray(dem_bytes))],
        "dsm binary, dem binary",
    )
    df.createOrReplaceTempView("_chm_parity_heavy")
    row = spark.sql(
        "SELECT gbx_rst_chm("
        "  gbx_rst_fromcontent(dsm, 'GTiff'),"
        "  gbx_rst_fromcontent(dem, 'GTiff')"
        ") AS r "
        "FROM _chm_parity_heavy"
    ).collect()[0]["r"]

    with MemoryFile(bytes(row["raster"])) as mf:
        with mf.open() as ds:
            arr = ds.read(1).astype("float32")
            nd = ds.nodata
    return arr, nd


# ---------------------------------------------------------------------------
# Helper: convert NoData pixels to NaN for np.allclose(equal_nan=True) comparison
# ---------------------------------------------------------------------------


def _nodata_to_nan(arr, nodata):
    """Return a float64 copy with nodata pixels replaced by NaN."""
    out = arr.astype("float64")
    if nodata is not None:
        out[arr == nodata] = float("nan")
    return out


# ---------------------------------------------------------------------------
# Test 1: Normal same-grid case
#   DSM [[10,20],[5,8]] on the same 2×2 grid as DEM [[10,15],[7,8]].
#   Expected CHM = clamp(DSM-DEM, 0) = [[0,5],[0,0]].
#   Both tiers must agree on all four pixels.
# ---------------------------------------------------------------------------


def test_chm_same_grid_parity(spark_with_jar):
    """Light and heavy gbx_rst_chm agree on a normal same-grid input.

    DSM [[10,20],[5,8]] - DEM [[10,15],[7,8]] = [[0,5],[-2,0]] → clamp [[0,5],[0,0]].
    Both tiers must produce the same float32 output within tolerance 1e-5.
    """
    spark = spark_with_jar

    dsm = _make_geotiff_bytes(
        np.array([[10.0, 20.0], [5.0, 8.0]], dtype="float32")
    )
    dem = _make_geotiff_bytes(
        np.array([[10.0, 15.0], [7.0, 8.0]], dtype="float32")
    )

    # Collect LIGHT first; heavy registration overwrites the SQL name next.
    light_arr, light_nd = _run_light_chm(spark, dsm, dem)
    heavy_arr, heavy_nd = _run_heavy_chm(spark, dsm, dem)

    assert light_arr.shape == heavy_arr.shape, (
        f"same-grid: shape mismatch light={light_arr.shape} heavy={heavy_arr.shape}"
    )
    assert light_nd == pytest.approx(_NODATA), (
        f"light nodata sentinel must be {_NODATA}, got {light_nd}"
    )
    assert heavy_nd == pytest.approx(_NODATA), (
        f"heavy nodata sentinel must be {_NODATA}, got {heavy_nd}"
    )

    ln = _nodata_to_nan(light_arr, light_nd)
    hn = _nodata_to_nan(heavy_arr, heavy_nd)

    assert np.allclose(ln, hn, equal_nan=True, atol=1e-5), (
        f"same-grid CHM parity FAILED.\n"
        f"light:\n{light_arr}\nheavy:\n{heavy_arr}\ndiff:\n{light_arr - heavy_arr}"
    )

    # Spot-check computed values (clamp means no negatives anywhere).
    assert np.all(light_arr[light_arr != _NODATA] >= 0.0), (
        "light CHM has negative values (clamping broken)"
    )
    assert np.all(heavy_arr[heavy_arr != _NODATA] >= 0.0), (
        "heavy CHM has negative values (clamping broken)"
    )


# ---------------------------------------------------------------------------
# Test 2: Padded / below-datum discriminating case
#   DEM: 4×4 at -20 m (entirely below datum, pixel_size=1 deg).
#   DSM: 2×2 covering only the top-left quadrant, NO nodata attribute.
#   After alignment the bottom-right 2×2 pixels of the DSM must be NoData in
#   BOTH tiers (not the spurious clamp(0-(-20),0)=20 that a fill-value-0 path
#   would produce).  Covered top-left pixels must equal clamp(5-(-20),0)=25.
# ---------------------------------------------------------------------------


def test_chm_padded_below_datum_parity(spark_with_jar):
    """Light and heavy agree on padded/below-datum NoData handling.

    When the DSM has nodata=None (no nodata attribute) and a smaller extent
    than the DEM, GDAL's warp fill (0) would leave below-datum DEM pixels with
    a spurious positive CHM (clamp(0-(-20),0)=20).  Both tiers must clamp
    uncovered pixels to the NoData sentinel (-9999) and agree on which pixels
    are covered (top-left 2×2 = 25 m canopy) vs uncovered (rest = -9999).
    """
    spark = spark_with_jar

    # DEM: 4×4, uniformly -20 m below datum, fully valid.
    dem = _make_geotiff_bytes(
        np.full((4, 4), -20.0, dtype="float32"),
        nodata=-9999.0,
        pixel_size=1.0,
        ulx=10.0,
        uly=50.0,
    )
    # DSM: 2×2 covering only the DEM's top-left quadrant, NO nodata attribute.
    dsm = _make_geotiff_bytes(
        np.full((2, 2), 5.0, dtype="float32"),
        nodata=None,
        pixel_size=1.0,
        ulx=10.0,
        uly=50.0,
    )

    # Collect LIGHT first.
    light_arr, light_nd = _run_light_chm(spark, dsm, dem)
    heavy_arr, heavy_nd = _run_heavy_chm(spark, dsm, dem)

    assert light_arr.shape == (4, 4), (
        f"below-datum: light output must be 4×4 (DEM grid), got {light_arr.shape}"
    )
    assert heavy_arr.shape == (4, 4), (
        f"below-datum: heavy output must be 4×4 (DEM grid), got {heavy_arr.shape}"
    )
    assert light_nd == pytest.approx(_NODATA), (
        f"light nodata sentinel must be {_NODATA}, got {light_nd}"
    )
    assert heavy_nd == pytest.approx(_NODATA), (
        f"heavy nodata sentinel must be {_NODATA}, got {heavy_nd}"
    )

    # Covered top-left pixels: clamp(5 - (-20), 0) = 25 in BOTH tiers.
    assert light_arr[0, 0] == pytest.approx(25.0), (
        f"light covered [0,0] must be 25 (5-(-20)); got {light_arr[0, 0]}"
    )
    assert heavy_arr[0, 0] == pytest.approx(25.0), (
        f"heavy covered [0,0] must be 25 (5-(-20)); got {heavy_arr[0, 0]}"
    )

    # Uncovered pixels must be NoData (-9999), not the spurious 20 m canopy.
    assert light_arr[3, 3] == pytest.approx(_NODATA), (
        f"light uncovered [3,3] must be NoData ({_NODATA}); "
        f"got {light_arr[3, 3]} (spurious canopy if ~20)"
    )
    assert heavy_arr[3, 3] == pytest.approx(_NODATA), (
        f"heavy uncovered [3,3] must be NoData ({_NODATA}); "
        f"got {heavy_arr[3, 3]} (spurious canopy if ~20)"
    )

    # Full pixel-level parity between tiers (treating -9999 as NaN).
    ln = _nodata_to_nan(light_arr, light_nd)
    hn = _nodata_to_nan(heavy_arr, heavy_nd)

    assert np.allclose(ln, hn, equal_nan=True, atol=1e-5), (
        f"below-datum CHM parity FAILED.\n"
        f"light:\n{light_arr}\nheavy:\n{heavy_arr}\ndiff:\n{light_arr - heavy_arr}"
    )
