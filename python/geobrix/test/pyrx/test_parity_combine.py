"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for the combine family
and rst_align_to.

Capstone parity check.  Both tiers run on the SAME in-memory
raster stack and their outputs compared pixel-by-pixel.

Stack layout (2×2 FLOAT64 tiles, row-major, NoData=-9999):
  tile 0: [1, 10, ND, 4]
  tile 1: [2, ND, ND, 4]
  tile 2: [3, 20, ND, 4]

Pixel positions in flattened order (matches test_combine_family.py and
RST_CombineFamilyTest.scala):
  p0=(row0,col0): values=[1,2,3]    → all valid
  p1=(row0,col1): values=[10,ND,20] → valid=[10,20]   (even-count median / stddev exercised)
  p2=(row1,col0): values=[ND,ND,ND] → all-NoData      → NoData in output
  p3=(row1,col1): values=[4,4,4]    → all valid

Per-pixel exact expected values:
         p0         p1       p2   p3
  min:   1.0        10.0     ND   4.0
  max:   3.0        20.0     ND   4.0
  sum:   6.0        30.0     ND  12.0
  count: 3.0         2.0     ND   3.0
  median:2.0        15.0     ND   4.0     (even-count: (10+20)/2=15)
  stddev:√(2/3)≈    5.0      ND   0.0     (population ddof=0; {10,20}: std=5)

Two-phase pattern: register LIGHT first (prx.register → collect via SQL
`gbx_rst_combine{stat}(tiles)`), then register HEAVY (hx.register which
OVERWRITES the same SQL names) and collect again on the same temp view.
Sequential collection materialises before re-registration.

rst_align_to: warp a 4326 source tile to a 27700 reference grid; assert the
light and heavy warped outputs match grid dims + geotransform + pixel values.

Sanity-check (run during development, confirmed before finalising):
  Replacing the heavy comparison with ``heavy_arr + 1.0`` made the assertion
  fail with a mean absolute difference ~1.0.  The correct assertion passes
  with rtol=1e-9 for all 6 stats.

Heavy requires the geobrix JAR and GDAL JNI libraries; both present in the
geobrix-dev Docker container.  Auto-skips when the JAR is absent or a
JAR-free Spark session is already live.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-parity.sh \\
        --path python/geobrix/test/pyrx/test_parity_combine.py \\
        --log stage3-t14.log
"""

import logging
from pathlib import Path

import pytest

rasterio = pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (geobrix[light_env6] required)",
)
import numpy as np  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix  (test/pyrx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

_ND = -9999.0


# ---------------------------------------------------------------------------
# Spark fixture — JAR loaded, module scope
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
                "A JAR-free Spark session is already live; run this test in isolation: "
                "gbx:test:parity --path "
                "python/geobrix/test/pyrx/test_parity_combine.py"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-combine-parity")
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
# Tile helpers (canonical fixture, mirrors test_combine_family.py + Scala)
# ---------------------------------------------------------------------------


def _make_tile(values, *, crs, ulx, uly, px, nodata=_ND, dtype="float64"):
    """Single-band GTiff bytes from a flat list of pixel values."""
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    n = len(values)
    # Use a square grid (2×2 for the 4-pixel canonical fixture).
    side = int(n**0.5)
    assert side * side == n, f"_make_tile: {n} values must form a square grid"
    arr = np.array(values, dtype=dtype).reshape(1, side, side)
    profile = dict(
        driver="GTiff",
        width=side,
        height=side,
        count=1,
        dtype=dtype,
        crs=crs,
        transform=from_origin(ulx, uly, px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**profile) as ds:
            ds.write(arr)
        return mf.read()


def _canonical_tiles():
    """Three aligned 2×2 FLOAT64 EPSG:4326 tiles with the standard NoData hole.

    Pixel layout (row-major):
      tile 0: [1, 10, ND, 4]
      tile 1: [2, ND, ND, 4]
      tile 2: [3, 20, ND, 4]

    Matches test_combine_family.py and RST_CombineFamilyTest.scala exactly so
    expected values are pre-verified.
    """
    return [
        _make_tile([1.0, 10.0, _ND, 4.0], crs="EPSG:4326", ulx=0.0, uly=2.0, px=1.0),
        _make_tile([2.0, _ND, _ND, 4.0], crs="EPSG:4326", ulx=0.0, uly=2.0, px=1.0),
        _make_tile([3.0, 20.0, _ND, 4.0], crs="EPSG:4326", ulx=0.0, uly=2.0, px=1.0),
    ]


def _tile_struct(b: bytes) -> dict:
    """Minimal v1-compatible tile struct (3-field schema accepted by both tiers)."""
    return {"cellid": 0, "raster": bytearray(b), "metadata": {}}


# ---------------------------------------------------------------------------
# DataFrame helpers
# ---------------------------------------------------------------------------


def _combine_df(spark, tiles):
    """One-row DataFrame with `tiles` = ARRAY of 3 tile structs (v1 schema)."""
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    tile_schema = StructType(
        [
            StructField("cellid", LongType(), False),
            StructField("raster", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )
    rows = [{"tiles": [_tile_struct(t) for t in tiles]}]
    return spark.createDataFrame(
        rows,
        schema=StructType([StructField("tiles", ArrayType(tile_schema), True)]),
    )


def _align_df(spark, src_bytes: bytes, ref_bytes: bytes):
    """One-row DataFrame with `tile` and `ref` columns (v1 schema)."""
    from pyspark.sql.types import (
        BinaryType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )

    tile_schema = StructType(
        [
            StructField("cellid", LongType(), False),
            StructField("raster", BinaryType(), True),
            StructField("metadata", MapType(StringType(), StringType()), True),
        ]
    )
    rows = [{"tile": _tile_struct(src_bytes), "ref": _tile_struct(ref_bytes)}]
    return spark.createDataFrame(
        rows,
        schema=StructType(
            [
                StructField("tile", tile_schema, True),
                StructField("ref", tile_schema, True),
            ]
        ),
    )


# ---------------------------------------------------------------------------
# Two-phase collect helpers
# ---------------------------------------------------------------------------


def _collect_combine_light(spark, stat: str, tiles) -> bytes:
    """Phase 1: register pyrx (light), run SQL, collect tile bytes."""
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = _combine_df(spark, tiles)
    df.createOrReplaceTempView("_combine_parity_tbl")
    rows = spark.sql(
        f"SELECT gbx_rst_combine{stat}(tiles) AS result FROM _combine_parity_tbl"
    ).collect()
    assert rows and rows[0]["result"] is not None, f"light {stat} returned no row"
    return bytes(rows[0]["result"]["raster"])


def _collect_combine_heavy(spark, stat: str) -> bytes:
    """Phase 2: register rasterx (heavy, OVERWRITES SQL names), run same SQL."""
    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    rows = spark.sql(
        f"SELECT gbx_rst_combine{stat}(tiles) AS result FROM _combine_parity_tbl"
    ).collect()
    assert rows and rows[0]["result"] is not None, f"heavy {stat} returned no row"
    return bytes(rows[0]["result"]["raster"])


def _collect_align_light(spark, src_bytes: bytes, ref_bytes: bytes) -> bytes:
    """Phase 1: register pyrx, run SQL rst_align_to, collect tile bytes."""
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = _align_df(spark, src_bytes, ref_bytes)
    df.createOrReplaceTempView("_align_parity_tbl")
    rows = spark.sql(
        "SELECT gbx_rst_align_to(tile, ref) AS result FROM _align_parity_tbl"
    ).collect()
    assert rows and rows[0]["result"] is not None, "light rst_align_to returned no row"
    return bytes(rows[0]["result"]["raster"])


def _collect_align_heavy(spark) -> bytes:
    """Phase 2: register rasterx (heavy), run same SQL rst_align_to."""
    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    rows = spark.sql(
        "SELECT gbx_rst_align_to(tile, ref) AS result FROM _align_parity_tbl"
    ).collect()
    assert rows and rows[0]["result"] is not None, "heavy rst_align_to returned no row"
    return bytes(rows[0]["result"]["raster"])


# ---------------------------------------------------------------------------
# Comparison helpers
# ---------------------------------------------------------------------------


def _decode(tile_bytes: bytes):
    """Return (flat_pixels: ndarray, nodata: float | None) from tile GTiff bytes."""
    from databricks.labs.gbx.pyrx import _serde

    with _serde.open_tile(tile_bytes) as ds:
        arr = ds.read(1).astype("float64").flatten()
        nodata = ds.nodata
    return arr, nodata


def _parity_compare(light_bytes: bytes, heavy_bytes: bytes, label: str, tol=1e-9):
    """Assert per-pixel equality and identical NoData mask, within tol.

    Checks:
      1. Output shapes are identical.
      2. NoData sentinels match.
      3. NoData masks are bit-identical.
      4. Valid-pixel values agree within tol * max(1.0, |heavy_val|).
    """
    light_arr, light_nd = _decode(light_bytes)
    heavy_arr, heavy_nd = _decode(heavy_bytes)

    assert (
        light_arr.shape == heavy_arr.shape
    ), f"{label}: shape mismatch light={light_arr.shape} heavy={heavy_arr.shape}"

    # NoData sentinel: if both declare one, they must be close.
    if light_nd is not None and heavy_nd is not None:
        assert abs(light_nd - heavy_nd) <= tol * (
            1.0 + abs(heavy_nd)
        ), f"{label}: NoData sentinel mismatch light={light_nd} heavy={heavy_nd}"

    # Determine NoData mask (per-pixel).
    sentinel = (
        light_nd
        if light_nd is not None
        else (heavy_nd if heavy_nd is not None else None)
    )
    if sentinel is not None:
        light_nodata_mask = light_arr == sentinel
        # Deliberately reuse the same sentinel for both masks: the guard above
        # verified light_nd ≈ heavy_nd within tol, so they are the same value
        # and building both masks from the shared sentinel is equivalent to
        # computing them independently.
        heavy_nodata_mask = heavy_arr == sentinel
        if not np.array_equal(light_nodata_mask, heavy_nodata_mask):
            mismatch_idx = np.where(light_nodata_mask != heavy_nodata_mask)[0].tolist()[
                :8
            ]
            pytest.fail(
                f"{label}: NoData mask mismatch at pixel indices {mismatch_idx}; "
                f"light_nodata={light_nodata_mask.tolist()} "
                f"heavy_nodata={heavy_nodata_mask.tolist()}"
            )
        valid_mask = ~light_nodata_mask  # same for both after mask-equality check
    else:
        valid_mask = np.ones(light_arr.shape, dtype=bool)

    # Per-pixel value comparison on valid pixels only.
    for idx in np.where(valid_mask)[0]:
        lv, hv = float(light_arr[idx]), float(heavy_arr[idx])
        threshold = tol * max(1.0, abs(hv))
        assert abs(lv - hv) <= threshold, (
            f"{label} pixel[{idx}] diverged: "
            f"light={lv:.15g} heavy={hv:.15g} (|diff|={abs(lv - hv):.3e} > tol={threshold:.3e})"
        )


# ---------------------------------------------------------------------------
# Combine parity tests (6 stats, parametrised)
# ---------------------------------------------------------------------------

_STATS = ["min", "max", "sum", "count", "median", "stddev"]


@pytest.mark.parametrize("stat", _STATS)
def test_combine_stat_parity(spark_with_jar, stat):
    """Light vs heavy per-pixel parity for gbx_rst_combine{stat}.

    Covers:
      - Valid-pixel agreement within 1e-9.
      - Identical NoData mask (all-NoData pixel p2 must be NoData in both tiers).
      - Even-count median (p1: median([10,20]) = 15.0) and population stddev (ddof=0).
    """
    spark = spark_with_jar
    tiles = _canonical_tiles()

    # Phase 1: light register → collect (materialises before heavy re-registers).
    light_bytes = _collect_combine_light(spark, stat, tiles)

    # Phase 2: heavy register (overwrites SQL name) → collect.
    heavy_bytes = _collect_combine_heavy(spark, stat)

    _parity_compare(light_bytes, heavy_bytes, label=f"combine_{stat}", tol=1e-9)

    # Additional sanity: both tiers must produce the known expected values.
    light_arr, _ = _decode(light_bytes)
    heavy_arr, _ = _decode(heavy_bytes)

    expected = {
        "min": [1.0, 10.0, _ND, 4.0],
        "max": [3.0, 20.0, _ND, 4.0],
        "sum": [6.0, 30.0, _ND, 12.0],
        "count": [3.0, 2.0, _ND, 3.0],
        "median": [2.0, 15.0, _ND, 4.0],
        "stddev": [
            (2.0 / 3.0) ** 0.5,  # std([1,2,3], ddof=0) = sqrt(2/3)
            5.0,  # std([10,20], ddof=0) = 5
            _ND,
            0.0,  # std([4,4,4], ddof=0) = 0
        ],
    }[stat]

    nodata_sentinel = _ND
    for idx, exp in enumerate(expected):
        if exp == nodata_sentinel:
            # All-NoData pixel: already verified by NoData mask check above.
            continue
        assert abs(float(light_arr[idx]) - exp) <= 1e-9 * max(
            1.0, abs(exp)
        ), f"combine_{stat} light pixel[{idx}] expected={exp} got={light_arr[idx]:.15g}"
        assert abs(float(heavy_arr[idx]) - exp) <= 1e-9 * max(
            1.0, abs(exp)
        ), f"combine_{stat} heavy pixel[{idx}] expected={exp} got={heavy_arr[idx]:.15g}"


# ---------------------------------------------------------------------------
# rst_align_to parity test
# ---------------------------------------------------------------------------


def test_rst_align_to_parity(spark_with_jar):
    """Light vs heavy parity for gbx_rst_align_to.

    Warps a 4326 source tile onto a 27700 reference tile.  Asserts:
      1. Both outputs have the same width/height/geotransform as the reference.
      2. Both outputs have the same CRS as the reference.
      3. Pixel arrays match within 1e-9.

    The reference tile is a uniform 10×10 EPSG:27700 grid in the London area;
    the source is a 10×10 EPSG:4326 tile covering a similar footprint.  After
    nearest-neighbour warp both tiers must produce the same pixel array.
    """
    spark = spark_with_jar

    # Source: 10×10 EPSG:4326 ramp tile near London.
    src_values = list(range(100))  # 0..99 uniform ramp
    src_bytes = _make_tile(
        src_values,
        crs="EPSG:4326",
        ulx=-0.5,
        uly=51.7,
        px=0.01,
        dtype="float64",
        nodata=_ND,
    )

    # Reference: 10×10 EPSG:27700 (BNG) uniform-value tile at London BNG coords.
    ref_values = [5.0] * 100
    ref_bytes = _make_tile(
        ref_values,
        crs="EPSG:27700",
        ulx=520000.0,
        uly=185000.0,
        px=1000.0,
        dtype="float64",
        nodata=_ND,
    )

    # Phase 1: light.
    light_bytes = _collect_align_light(spark, src_bytes, ref_bytes)

    # Phase 2: heavy (overwrites SQL name).
    heavy_bytes = _collect_align_heavy(spark)

    # Grid parity: both must match the reference dimensions, geotransform, and CRS.
    from databricks.labs.gbx.pyrx import _serde

    with _serde.open_tile(ref_bytes) as ref_ds:
        ref_w, ref_h = ref_ds.width, ref_ds.height
        ref_transform = ref_ds.transform
        ref_crs_str = str(ref_ds.crs)

    for label, out_bytes in [("light", light_bytes), ("heavy", heavy_bytes)]:
        with _serde.open_tile(out_bytes) as ds:
            assert (
                ds.width == ref_w
            ), f"rst_align_to {label}: width {ds.width} != ref {ref_w}"
            assert (
                ds.height == ref_h
            ), f"rst_align_to {label}: height {ds.height} != ref {ref_h}"
            assert ds.transform == ref_transform, (
                f"rst_align_to {label}: geotransform {ds.transform} != ref {ref_transform}; "
                "output does not carry the reference grid's extent/resolution"
            )
            out_crs_str = str(ds.crs)
        # CRS must match (try pyproj equivalence, fall back to string).
        try:
            from pyproj import CRS as _ProjCRS

            assert _ProjCRS.from_user_input(out_crs_str).equals(
                _ProjCRS.from_user_input(ref_crs_str)
            ), f"rst_align_to {label}: CRS mismatch {out_crs_str!r} != {ref_crs_str!r}"
        except Exception:
            assert (
                out_crs_str == ref_crs_str
            ), f"rst_align_to {label}: CRS string mismatch"

    # Pixel parity: both warped outputs must agree within 1e-9.
    _parity_compare(light_bytes, heavy_bytes, label="rst_align_to", tol=1e-9)
