"""Light (pyrx) vs heavy (rasterx) ``rst_h3_tessellate`` cell-set parity.

THE cross-tier parity gate for the H3 tessellate modes. For a small EPSG:4326
raster spanning a tile **border** (so border cells exist), at a coarse-ish
resolution, both tiers must emit the **same set of H3 cells** for each mode:

- ``covering`` — light uses h3-py ``polygon_to_cells_experimental(contain='overlap')``;
  heavy hand-rolls polyfill+buffer candidates filtered by a JTS
  ``hexagon.intersects(bbox)`` keep-test (H3-Java is pinned 3.7.0). The spec
  accepts covering parity "by definition + tests" — this test IS that gate.
- ``centroid`` — both tiers use the identical pixel-centroid rule
  (pixel centroid lon/lat -> H3 cell), so the cell sets must match **exactly**,
  and so must each cell's chip: width, height, geotransform, and the source
  pixels it holds (covered-but-empty cells are the same 1x1 chip in both).

A second fixture, a UTM 18N scene (EPSG:32618), pins the reprojected footprint:
both tiers keep the same cells in ``centroid`` + ``complete`` and in ``covering``.

Both tiers represent the H3 cell id as the same 64-bit value reinterpreted as a
signed int64 (light: ``h3.str_to_int`` folded into signed; heavy: H3-Java
``Long`` cell id), so the ``cellid`` fields are directly comparable.

Heavy requires the geobrix JAR *and* the GDAL native libraries (JNI); both are
present in the geobrix-dev Docker container. This test auto-skips when the JAR
is not staged under ``python/geobrix/lib/``.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pyvx/test_parity_h3_tessellate.py \\
        --with-integration --log h3-parity.log
"""

import logging
from pathlib import Path

import pytest

# Light core (h3-py + rasterio) and heavy (JNI GDAL) deps; skip cleanly if absent.
rasterio = pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (a light-tier extra (e.g. geobrix[light_env6]) required)",
)
pytest.importorskip(
    "h3",
    reason="h3-py not installed (a light-tier extra (e.g. geobrix[light_env6]) required)",
)
from test.pyrx import _tessellate_parity as tp  # noqa: E402

import numpy as np  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pyvx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

# A small EPSG:4326 raster that spans a tile border so border cells exist:
# 48x48 px at 0.02 deg/px => ~0.96 deg square near London. At resolution 5 this
# extent crosses several H3 cells (interior + border), exercising the covering
# keep-test on cells whose hexagons only partly overlap the bbox.
_SIZE = 48
_RES_DEG = 0.02
_ORIGIN = (-0.4, 51.7)  # (west, north)
_TESS_RES = 5


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting: it has no effect if a JVM (and therefore
    # a Spark session) is already live in this process. Skip instead of producing a
    # misleading failure when another test suite already created a JAR-free session.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; "
                "run this test in isolation: "
                "gbx:test:python --path python/geobrix/test/pyvx/test_parity_h3_tessellate.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-pyvx-h3-tessellate-parity")
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


def _raster_4326_bytes() -> bytes:
    """Identical EPSG:4326 GeoTIFF bytes fed to BOTH tiers (same pixels, CRS, transform)."""
    from rasterio.io import MemoryFile

    data = np.arange(_SIZE * _SIZE, dtype="float32").reshape(_SIZE, _SIZE)
    prof = dict(
        driver="GTiff",
        height=_SIZE,
        width=_SIZE,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=rasterio.transform.from_origin(
            _ORIGIN[0], _ORIGIN[1], _RES_DEG, _RES_DEG
        ),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


def _light_cells(spark, raster: bytes, mode: str) -> set:
    """Light pyrx cell set: feed bytes through pyrx.rst_fromcontent, LATERAL tessellate."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_ras_light")
    rows = spark.sql(
        f"SELECT t.cellid AS cellid FROM _ras_light, "
        f"LATERAL gbx_rst_h3_tessellate(tile, {_TESS_RES}, '{mode}') t"
    ).collect()
    return {r["cellid"] for r in rows}


def _heavy_cells(spark, raster: bytes, mode: str) -> set:
    """Heavy rasterx cell set: feed bytes through rasterx.rst_fromcontent, select generator."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        hx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    rows = (
        df.select(
            hx.rst_h3_tessellate(f.col("tile"), f.lit(_TESS_RES), mode).alias("tt")
        )
        .select(f.col("tt.cellid").alias("cellid"))
        .collect()
    )
    return {r["cellid"] for r in rows}


@pytest.mark.parametrize("mode", ["covering", "centroid"])
def test_light_vs_heavy_h3_tessellate_cellset_parity(spark_with_jar, mode):
    """Light and heavy rst_h3_tessellate must emit the SAME H3 cell set for each mode.

    For ``centroid`` the cell-set match implies the same pixel->cell partition
    (both tiers use the identical pixel-centroid assignment rule). For
    ``covering`` this is the parity-by-tests gate between h3-py
    ``contain='overlap'`` and the heavy JTS hexagon-overlap hand-roll.
    """
    spark = spark_with_jar
    raster = _raster_4326_bytes()

    light = _light_cells(spark, raster, mode)
    heavy = _heavy_cells(spark, raster, mode)

    assert light, f"light emitted no cells for mode={mode}"
    assert heavy, f"heavy emitted no cells for mode={mode}"

    if light != heavy:
        import h3

        light_only = sorted(light - heavy)
        heavy_only = sorted(heavy - light)

        def _border(cellids):
            # A cell is a "border" cell if its hexagon is not fully inside the bbox
            # (i.e. it only partly overlaps) — report a small sample for diagnosis.
            return [h3.int_to_str(c if c >= 0 else c + 2**64) for c in cellids[:8]]

        pytest.fail(
            f"mode={mode} cell-set mismatch: "
            f"|light|={len(light)} |heavy|={len(heavy)} "
            f"light_only={len(light_only)} heavy_only={len(heavy_only)}; "
            f"light_only_sample(h3)={_border(light_only)} "
            f"heavy_only_sample(h3)={_border(heavy_only)}"
        )

    assert light == heavy


def _same_id(cid):
    return cid


def test_light_vs_heavy_h3_centroid_chips_match_per_cell(spark_with_jar):
    """Centroid chips match per cell: size, geotransform and source pixels.

    The fixture declares no NoData, so both tiers also stamp the float32 fallback.
    """
    spark = spark_with_jar
    raster = _raster_4326_bytes()
    args = (_TESS_RES, "centroid", "complete", _same_id)
    light_rows = tp.light_chips(spark, raster, "gbx_rst_h3_tessellate", *args)
    heavy_rows = tp.heavy_chips(spark, raster, "rst_h3_tessellate", *args)
    light = tp.by_cell(light_rows, "light")
    heavy = tp.by_cell(heavy_rows, "heavy")
    assert light, "light emitted no chips"

    src_tf = tp.source_transform(raster)
    tp.assert_centroid_chips_match(light, heavy, src_tf, "h3 centroid")
    covered = {px[:2] for b in heavy.values() for px in tp.chip_pixels(b, src_tf)}
    assert len(covered) == _SIZE * _SIZE, "the chips must hold every source pixel"
    fallback = float(np.finfo(np.float32).min)
    for tier, chips in (("light", light), ("heavy", heavy)):
        nodatas = {tp.chip_shape(b)[3] for b in chips.values()}
        assert nodatas == {fallback}, f"{tier} chip NoData {nodatas}"


# A UTM 18N scene with the footprint of a ~110 km Sentinel-2 granule (origin
# 499980, 4500000), sampled at 300 m (366 x 366) to keep the run short. Its edges
# curve in lon/lat, so the keep-test footprint is the reprojected, densified edge.
_UTM_SIZE = 366
_UTM_BOUNDS = (499980.0, 4390200.0, 609780.0, 4500000.0)
_UTM_RES = 7


def _utm_scene_bytes() -> bytes:
    from rasterio.io import MemoryFile
    from rasterio.transform import from_bounds

    n = _UTM_SIZE
    data = (np.arange(n * n, dtype="uint32").reshape(n, n) % 4000 + 1).astype("uint16")
    prof = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=1,
        dtype="uint16",
        nodata=0,
        crs="EPSG:32618",
        transform=from_bounds(*_UTM_BOUNDS, n, n),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read()


@pytest.mark.parametrize("mode", ["centroid", "covering"])
def test_light_vs_heavy_h3_tessellate_utm_scene(spark_with_jar, mode):
    """A projected (UTM) scene keeps the same cells in both tiers.

    ``complete`` coverage: covered cells are kept when they overlap the reprojected
    raster footprint, so this checks the footprint rule, not only pixel assignment.
    Under ``centroid`` the chips also match per cell, including the 1x1 chips of
    covered-but-empty cells, which sit at each cell's centroid pixel.
    """
    spark = spark_with_jar
    raster = _utm_scene_bytes()
    args = (_UTM_RES, mode, "complete", _same_id)
    light_rows = tp.light_chips(spark, raster, "gbx_rst_h3_tessellate", *args)
    heavy_rows = tp.heavy_chips(spark, raster, "rst_h3_tessellate", *args)
    assert not tp.error_rows(light_rows), "light error rows"
    assert not tp.error_rows(heavy_rows), "heavy error rows"
    light = tp.by_cell(light_rows, f"utm {mode} light")
    heavy = tp.by_cell(heavy_rows, f"utm {mode} heavy")
    assert len(light) > 1000, f"fixture should span many res-{_UTM_RES} cells"

    if set(light) != set(heavy):
        pytest.fail(
            f"utm {mode} cell-set mismatch: |light|={len(light)} |heavy|={len(heavy)} "
            f"light_only={sorted(set(light) - set(heavy))[:8]} "
            f"heavy_only={sorted(set(heavy) - set(light))[:8]}"
        )
    if mode == "centroid":
        empties = tp.assert_centroid_chips_match(
            light, heavy, tp.source_transform(raster), "utm centroid"
        )
        assert empties, "fixture should have covered-but-empty cells"
        for cid in empties:
            assert tp.chip_shape(light[cid])[:2] == (1, 1), f"empty {cid} not 1x1"
