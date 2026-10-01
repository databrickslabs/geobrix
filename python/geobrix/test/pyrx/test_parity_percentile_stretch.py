"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for ``rst_percentile_stretch``.

Both tiers stretch the SAME multi-band raster with the same ``(lo_pct, hi_pct)`` and
their uint8 outputs are compared. This is the heavy-tier verification for the newly
added ``gbx_rst_percentile_stretch``: it confirms the heavy Scala expression

  * REGISTERS against the assembly JAR (the ``gbx_rst_percentile_stretch`` SQL name
    resolves — a stale/absent JAR would raise UNRESOLVED_ROUTINE),
  * EXECUTES on a real Spark JVM with the GDAL JNI libraries, and
  * MATCHES the light per-band percentile stretch (uint8, same shape/bands, NoData
    preserved where NoData in every band, per-band values close).

Small raster on purpose: below the light subsample cap, so the light percentiles are
exact and the comparison is deterministic. Collect LIGHT first, then register HEAVY
(``hx.register`` OVERWRITES the shared ``gbx_rst_*`` SQL names) and collect.

Heavy requires the geobrix JAR + GDAL JNI libs (both present in geobrix-dev Docker).
Auto-skips when the JAR is absent or a JAR-free Spark session is already live.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-parity.sh \\
        --path python/geobrix/test/pyrx/test_parity_percentile_stretch.py \\
        --log pstretch-parity.log
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
# parents[2] == python/geobrix (test/pyrx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))


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
                "gbx:test:python --path "
                "python/geobrix/test/pyrx/test_parity_percentile_stretch.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-percentile-stretch-parity")
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


def _rgb_tif_bytes():
    """3-band uint16 GeoTIFF (16x16, 256 px/band < subsample cap) with distinct
    per-band value ranges and one all-band-zero (NoData) pixel at (0, 0)."""
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    rng = np.random.default_rng(7)
    b1 = rng.integers(100, 4000, size=(16, 16), dtype="uint16")
    b2 = rng.integers(2000, 9000, size=(16, 16), dtype="uint16")
    b3 = rng.integers(50, 600, size=(16, 16), dtype="uint16")
    for b in (b1, b2, b3):
        b[0, 0] = 0  # NoData in every band → must stay 0 in both tiers
    data = np.stack([b1, b2, b3])
    prof = dict(
        driver="GTiff",
        width=16,
        height=16,
        count=3,
        dtype="uint16",
        nodata=0,
        crs="EPSG:3857",
        transform=from_origin(0, 16, 1, 1),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data)
        return mf.read()


def _tile_bytes(value):
    """Extract GTiff bytes from a tile struct Row/dict (``raster`` field) or raw bytes."""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    # Spark Row or dict with a v2 tile struct
    raster = value["raster"]
    assert (
        raster is not None
    ), "tile struct carried no raster bytes (virtual/unmaterialized?)"
    return bytes(raster)


def _read(out_bytes):
    with rasterio.io.MemoryFile(out_bytes) as mf, mf.open() as ds:
        return ds.read(), list(ds.dtypes)


def test_percentile_stretch_light_vs_heavy_parity(spark_with_jar):
    """Light and heavy ``rst_percentile_stretch`` agree on a 3-band raster.

    Structural parity is exact (uint8, band count, shape, NoData-in-all-bands
    preserved as 0). Per-band values are compared with a small tolerance: heavy
    (Scala/GDAL) and light (numpy) may differ by a grey level or two at the
    percentile clip edges, but a gross divergence (wrong algorithm, unregistered
    routine, wrong dtype) fails the gate.
    """
    from pyspark.sql import functions as f

    spark = spark_with_jar
    raster = _rgb_tif_bytes()

    # --- LIGHT first (both tiers share the gbx_rst_percentile_stretch SQL name) ---
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    light_val = (
        spark.createDataFrame([(bytearray(raster),)], ["content"])
        .select(prx.rst_fromcontent("content", f.lit("GTiff")).alias("tile"))
        .select(
            prx.rst_percentile_stretch("tile", f.lit(2.0), f.lit(98.0)).alias("out")
        )
        .collect()[0]["out"]
    )
    light_bytes = _tile_bytes(light_val)

    # --- HEAVY (hx.register overwrites the shared SQL name with the GDAL expr) ---
    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    heavy_val = (
        spark.createDataFrame([(bytearray(raster),)], ["content"])
        .select(hx.rst_fromcontent("content", f.lit("GTiff")).alias("tile"))
        .select(hx.rst_percentile_stretch("tile", f.lit(2.0), f.lit(98.0)).alias("out"))
        .collect()[0]["out"]
    )
    heavy_bytes = _tile_bytes(heavy_val)

    light, light_dt = _read(light_bytes)
    heavy, heavy_dt = _read(heavy_bytes)

    # Structural parity (exact).
    assert all(d == "uint8" for d in light_dt), f"light not uint8: {light_dt}"
    assert all(d == "uint8" for d in heavy_dt), f"heavy not uint8: {heavy_dt}"
    assert (
        light.shape == heavy.shape == (3, 16, 16)
    ), f"shape mismatch: light={light.shape} heavy={heavy.shape}"

    # NoData in every band → 0 in both tiers (the (0,0) corner).
    assert light[:, 0, 0].tolist() == [0, 0, 0], "light NoData corner not 0"
    assert heavy[:, 0, 0].tolist() == [0, 0, 0], "heavy NoData corner not 0"

    # Each band stretched toward the full 0..255 range in both tiers.
    for b in range(3):
        assert (
            light[b].max() >= 200
        ), f"light band {b} not stretched: max={light[b].max()}"
        assert (
            heavy[b].max() >= 200
        ), f"heavy band {b} not stretched: max={heavy[b].max()}"

    # Per-band value parity (small tolerance for percentile/rounding differences).
    diff = np.abs(light.astype(np.int32) - heavy.astype(np.int32))
    assert diff.mean() < 2.0, (
        f"light vs heavy mean abs diff too high: {diff.mean():.3f} "
        f"(per-band max diffs: {[int(diff[b].max()) for b in range(3)]})"
    )
    assert (diff <= 2).mean() > 0.95, (
        f"only {(diff <= 2).mean():.3f} of pixels within +/-2 grey levels "
        f"(per-band max diffs: {[int(diff[b].max()) for b in range(3)]})"
    )
