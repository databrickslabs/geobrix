"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for the 9 BNG/quadbin
raster-grid functions.

THE Phase-2 parity def-of-done. For each of the nine functions, both tiers are
run on the SAME in-memory sample raster + resolution and their outputs compared
against the design §4.1 parity bar:

  * 5 BNG reducers (``rst_bng_rastertogrid{avg,count,max,min,median}``) — EXACT
    BNG-String cell-set equality + per-cell measure within 1e-9.
  * ``rst_quadbin_tessellate`` / ``rst_bng_tessellate`` — identical emitted
    cell-id set (quadbin Long / BNG String) + identical chip count, for both
    ``covering`` and ``centroid`` modes; under ``centroid`` the chips also match
    per cell (size, geotransform and source pixels), for h3 too.
  * ``rst_quadbin_rasterize_agg`` / ``rst_bng_rasterize_agg`` — same output
    raster on a shared explicit grid: band NoData == -9999 both tiers, and the
    covered-pixel mask + burned values identical (NoData-aware).

HARNESS REUSE (no new comparison framework invented):
  * Reducers + tessellate reuse the ``test/pyvx/test_parity_h3_tessellate.py``
    pattern — register ONE tier, collect its rows, then register the other and
    collect (both tiers register the same ``gbx_rst_*`` SQL name, so collection
    must be sequential; ``.collect()`` materialises before re-registration).
  * rasterize_agg reuses ``test/rasterx/test_h3_rasterize_parity.py`` — the
    shared grid comes from the light ``cellraster.compute_gridspec`` and both
    tiers burn onto that identical canvas, so the pixel-centroid burn is
    deterministic and the masks must match exactly.

CRS handling mirrors the design:
  * BNG fixtures are EPSG:27700-native so BOTH tiers skip the internal warp —
    this isolates cell-math + reducer parity from any gdalwarp-vs-rasterio.warp
    boundary difference (which the heavy-only reproject-equivalence Scala test,
    RST_BNG_RasterToGridTest, already pins). One extra BNG reducer check feeds a
    4326 fixture to BOTH tiers to exercise the warp path cross-tier.
  * Quadbin fixtures are EPSG:4326 (the quadbin API input contract).

Heavy requires the geobrix JAR *and* the GDAL native libraries (JNI); both are
present in the geobrix-dev Docker container. Auto-skips when the JAR is not
staged under ``python/geobrix/lib/`` or when a JAR-free Spark session is already
live in this process.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/pyrx/test_parity_bng_quadbin_raster_grid.py \\
        --with-integration --log bng-quadbin-parity.log
"""

import logging
from pathlib import Path

import pytest

rasterio = pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (a light-tier extra (e.g. geobrix[light_env6]) required)",
)
pytest.importorskip(
    "shapely",
    reason="shapely not installed (a light-tier extra (e.g. geobrix[light_env6]) required)",
)
from test.pyrx import _tessellate_parity as tp  # noqa: E402

import numpy as np  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pyrx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

_NODATA = -9999.0


# ---------------------------------------------------------------------------
# Spark fixture — JAR loaded (module scope), matches the pyvx/h3 parity harness
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def spark_with_jar():
    if not _JARS:
        pytest.skip(
            "no geobrix JAR staged under python/geobrix/lib/ — run in geobrix-dev Docker"
        )
    from pyspark.sql import SparkSession

    logging.getLogger("py4j").setLevel(logging.ERROR)

    # spark.jars is a JVM-startup-time setting: no effect if a JVM (and Spark
    # session) is already live. Skip rather than mislead.
    active = SparkSession.getActiveSession()
    if active is not None:
        active_jars = active.conf.get("spark.jars", "")
        if str(_JARS[-1]) not in active_jars:
            pytest.skip(
                "A JAR-free Spark session is already live in this process; run "
                "this test in isolation: gbx:test:python --path "
                "python/geobrix/test/pyrx/test_parity_bng_quadbin_raster_grid.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-bng-quadbin-raster-grid-parity")
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
# Shared in-memory raster fixtures (identical bytes fed to BOTH tiers)
# ---------------------------------------------------------------------------


def _gtiff_bytes(data, *, epsg, origin, px, nodata=_NODATA):
    """Single-band north-up GTiff from a 2-D array at the given CRS/georeference."""
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    h, w = data.shape
    prof = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype="float32",
        crs=f"EPSG:{epsg}",
        transform=from_origin(origin[0], origin[1], px, px),
        nodata=nodata,
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data.astype("float32"), 1)
        return mf.read()


def _bng_raster_27700():
    """8x8 EPSG:27700 raster over central London, 100 m pixels, ramp values.

    27700-native so BOTH tiers skip the internal warp — exact cell-math parity.
    Origin (530000, 181000); the 800 m x 800 m footprint spans <= a few 1 km
    (res 3) cells.
    """
    data = np.arange(1, 65, dtype="float32").reshape(8, 8)
    return _gtiff_bytes(data, epsg=27700, origin=(530000.0, 181000.0), px=100.0)


def _bng_raster_4326():
    """6x6 EPSG:4326 raster over central London — BOTH tiers warp to 27700.

    Exercises the gdalwarp (heavy) vs rasterio.warp (light) cross-tier path.
    """
    data = np.arange(1, 37, dtype="float32").reshape(6, 6)
    return _gtiff_bytes(data, epsg=4326, origin=(-0.12, 51.52), px=0.005)


def _quadbin_raster_4326():
    """16x16 EPSG:4326 raster over central London, 0.01 deg pixels, ramp values."""
    data = np.arange(1, 257, dtype="float32").reshape(16, 16)
    return _gtiff_bytes(data, epsg=4326, origin=(-0.13, 51.55), px=0.01)


def _small_covering_raster_4326():
    """4x4 EPSG:4326 raster (16 pixels, 0.1 deg each) at the equator.

    Performance guard for ``covering`` assignment tests: covering is O(pixels ×
    candidate-cells-per-pixel) in both tiers. 16 pixels at coarse resolution keeps
    the geometry-intersection count tiny (<100 JTS/shapely ops) so the suite does not
    time out.  Use with H3 res=4 or quadbin res=5 so the raster fits in a handful of
    cells.
    """
    data = np.arange(1, 17, dtype="float32").reshape(4, 4)
    return _gtiff_bytes(data, epsg=4326, origin=(0.0, 0.4), px=0.1)


def _small_covering_bng_raster():
    """4x4 EPSG:27700 raster (16 pixels, 1000 m each) with origin offset to straddle
    BNG 1 km cell boundaries.

    Origin (530300, 181700): BNG 1 km cell boundaries fall at every exact 1000 m;
    the 300 m offset means every pixel spans one internal boundary in x and one in y,
    so each pixel has fractional overlap (weights ≈ 0.49/0.21/0.21/0.09) with its
    four neighbouring BNG 1 km cells.  This prevents the degenerate all-1.0-weight
    case that occurs when pixels are grid-aligned to cell boundaries.

    Use with ``_RTG_COVERING_RES["bng"] = 3`` (1 km cells).  16 pixels × ~4 cells each
    = ~64 JTS/shapely intersection operations — fast even for the covering O(pixels) path.
    """
    data = np.arange(1, 17, dtype="float32").reshape(4, 4)
    return _gtiff_bytes(data, epsg=27700, origin=(530300.0, 181700.0), px=1000.0)


# ---------------------------------------------------------------------------
# Reducer parity (5 BNG functions) — exact cell-set + measure within 1e-9
# ---------------------------------------------------------------------------

_REDUCERS = ["avg", "count", "max", "min", "median", "sum", "variance", "stddev"]


def _heavy_reducer_rows(
    spark, raster, resolution, agg, *, coverage="complete", assignment="centroid"
):
    """Heavy tier: ARRAY<ARRAY<struct(cellID,measure)>> -> flat {(band,cellID): measure}.

    ``coverage`` and ``assignment`` are forwarded to the SQL expression.
    Defaults mirror the expression's built-in defaults (complete, centroid) so
    existing 4-positional-arg callers that omit these keyword params continue to
    produce identical results to the previous 2-arg SQL form.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    fn = getattr(hx, f"rst_bng_rastertogrid{agg}")
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        hx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    rows = (
        df.select(
            fn(f.col("tile"), f.lit(resolution), coverage, assignment).alias("bands")
        )
        # posexplode outer array -> 0-based band index + inner cell array
        .select(f.posexplode("bands").alias("band0", "cells"))
        .select((f.col("band0") + 1).alias("band"), f.explode("cells").alias("c"))
        .select(
            "band",
            f.col("c.cellID").alias("cellID"),
            f.col("c.measure").alias("measure"),
        )
        .collect()
    )
    return {(r["band"], r["cellID"]): r["measure"] for r in rows}


def _light_reducer_rows(
    spark, raster, resolution, agg, *, coverage="complete", assignment="centroid"
):
    """Light tier: SQL LATERAL over the registered pyrx UDTF -> {(band,cellID): measure}.

    ``coverage`` and ``assignment`` are forwarded to the SQL LATERAL call.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_ras_light_bng")
    rows = spark.sql(
        f"SELECT t.band AS band, t.cellID AS cellID, t.measure AS measure "
        f"FROM _ras_light_bng, "
        f"LATERAL gbx_rst_bng_rastertogrid{agg}(tile, {resolution}, '{coverage}', '{assignment}') t"
    ).collect()
    return {(r["band"], r["cellID"]): r["measure"] for r in rows}


@pytest.mark.parametrize("agg", _REDUCERS)
def test_bng_rastertogrid_reducer_parity_27700(spark_with_jar, agg):
    """Exact BNG-String cell-set equality + per-cell measure within 1e-9.

    27700-native fixture: BOTH tiers skip the warp, so any divergence is a real
    cell-math / reducer difference, not a resampling artifact.
    """
    spark = spark_with_jar
    raster = _bng_raster_27700()
    resolution = 3  # 1 km

    # Collect LIGHT first (both tiers share the gbx_rst_* SQL name; sequential
    # collection materialises light rows before heavy re-registration).
    light = _light_reducer_rows(spark, raster, resolution, agg)
    heavy = _heavy_reducer_rows(spark, raster, resolution, agg)

    assert light, f"light emitted no cells for agg={agg}"
    assert heavy, f"heavy emitted no cells for agg={agg}"

    # Cell ids must be BNG strings both tiers.
    assert all(isinstance(k[1], str) for k in light), "light cellID must be BNG String"
    assert all(isinstance(k[1], str) for k in heavy), "heavy cellID must be BNG String"

    light_keys, heavy_keys = set(light), set(heavy)
    if light_keys != heavy_keys:
        pytest.fail(
            f"agg={agg} BNG cell-set MISMATCH (real cross-tier divergence): "
            f"|light|={len(light_keys)} |heavy|={len(heavy_keys)} "
            f"light_only={sorted(light_keys - heavy_keys)[:8]} "
            f"heavy_only={sorted(heavy_keys - light_keys)[:8]}"
        )

    for key in light_keys:
        lv, hv = float(light[key]), float(heavy[key])
        assert abs(lv - hv) < 1e-9, (
            f"agg={agg} cell {key} measure diverged beyond 1e-9: "
            f"light={lv} heavy={hv} (diff={abs(lv - hv):.3e})"
        )


def test_bng_rastertogrid_avg_parity_4326_warp_path(spark_with_jar):
    """BNG avg on a 4326 fixture: exercises gdalwarp (heavy) vs rasterio.warp (light).

    The design warns this is exactly where a real cross-tier boundary divergence
    could surface. We assert exact cell-set + measure parity; a genuine
    divergence here is a FINDING (do not weaken), reported with the exact cells.
    """
    spark = spark_with_jar
    raster = _bng_raster_4326()
    resolution = 3  # 1 km

    light = _light_reducer_rows(spark, raster, resolution, "avg")
    heavy = _heavy_reducer_rows(spark, raster, resolution, "avg")

    assert light, "light emitted no cells (4326 warp path)"
    assert heavy, "heavy emitted no cells (4326 warp path)"

    light_keys, heavy_keys = set(light), set(heavy)
    if light_keys != heavy_keys:
        pytest.fail(
            "BNG avg 4326-warp cell-set MISMATCH (gdalwarp vs rasterio.warp — "
            "REAL FINDING): "
            f"|light|={len(light_keys)} |heavy|={len(heavy_keys)} "
            f"light_only={sorted(light_keys - heavy_keys)[:8]} "
            f"heavy_only={sorted(heavy_keys - light_keys)[:8]}"
        )

    for key in light_keys:
        lv, hv = float(light[key]), float(heavy[key])
        assert abs(lv - hv) < 1e-9, (
            f"BNG avg 4326-warp cell {key} measure diverged: "
            f"light={lv} heavy={hv} (diff={abs(lv - hv):.3e}) — "
            "REAL gdalwarp-vs-rasterio.warp boundary finding"
        )


# ---------------------------------------------------------------------------
# Sum reducer parity (h3 + quadbin, Long cell ids) — cell-set + within_tol
# ---------------------------------------------------------------------------
# sum is the same machinery as avg (bincount weighted sum, without /count), so
# it shares avg's within_tol summation-order class cross-tier. We assert the
# EXACT Long cell-set and per-cell measure within a relative tolerance on a
# real multi-value tile (ramp values, several cells with >1 pixel each).


def _heavy_grid_rows(
    spark, raster, resolution, grid, agg, *, coverage="complete", assignment="centroid"
):
    """Heavy tier (Long cell id grids): -> {(band,cellID): measure}.

    ``coverage`` and ``assignment`` are forwarded to the SQL expression.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    fn = getattr(hx, f"rst_{grid}_rastertogrid{agg}")
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        hx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    rows = (
        df.select(
            fn(f.col("tile"), f.lit(resolution), coverage, assignment).alias("bands")
        )
        .select(f.posexplode("bands").alias("band0", "cells"))
        .select((f.col("band0") + 1).alias("band"), f.explode("cells").alias("c"))
        .select(
            "band",
            f.col("c.cellID").alias("cellID"),
            f.col("c.measure").alias("measure"),
        )
        .collect()
    )
    return {(r["band"], r["cellID"]): r["measure"] for r in rows}


def _light_grid_rows(
    spark, raster, resolution, grid, agg, *, coverage="complete", assignment="centroid"
):
    """Light tier (Long cell id grids): SQL LATERAL over the pyrx UDTF.

    ``coverage`` and ``assignment`` are forwarded to the SQL LATERAL call.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_ras_light_grid")
    rows = spark.sql(
        f"SELECT t.band AS band, t.cellID AS cellID, t.measure AS measure "
        f"FROM _ras_light_grid, "
        f"LATERAL gbx_rst_{grid}_rastertogrid{agg}(tile, {resolution}, '{coverage}', '{assignment}') t"
    ).collect()
    return {(r["band"], r["cellID"]): r["measure"] for r in rows}


@pytest.mark.parametrize("agg", ["sum", "variance", "stddev"])
@pytest.mark.parametrize("grid,resolution", [("quadbin", 12), ("h3", 7)])
def test_grid_rastertogrid_reducer_parity(spark_with_jar, grid, resolution, agg):
    """h3/quadbin sum/variance/stddev: exact Long cell-set + measure within tol.

    Real multi-value tile (ramp 1..256) so cells carry several pixels and the
    reducer exercises REAL spread — variance/stddev are non-degenerate (not the
    single-pixel variance-0 case). variance/stddev use the SAME two-pass
    population formula on both tiers, so they share sum/avg's within_tol class;
    a formula mismatch would surface here as a divergence (do NOT loosen tol —
    fix the formula).

    Note: ``coverage="sparse"`` is explicit here (only cells that have pixels).
    The Stage-2 ``complete`` behaviour (empty-cell parity) is exercised by
    ``test_rastertogrid_coverage_assignment_parity``.
    """
    spark = spark_with_jar
    raster = _quadbin_raster_4326()  # 16x16 4326 ramp — valid for both h3 & quadbin

    light = _light_grid_rows(spark, raster, resolution, grid, agg, coverage="sparse")
    heavy = _heavy_grid_rows(spark, raster, resolution, grid, agg, coverage="sparse")

    assert light, f"light emitted no cells for {grid} {agg}"
    assert heavy, f"heavy emitted no cells for {grid} {agg}"

    light_keys, heavy_keys = set(light), set(heavy)
    if light_keys != heavy_keys:
        pytest.fail(
            f"{grid} {agg} cell-set MISMATCH: "
            f"|light|={len(light_keys)} |heavy|={len(heavy_keys)} "
            f"light_only={sorted(light_keys - heavy_keys)[:8]} "
            f"heavy_only={sorted(heavy_keys - light_keys)[:8]}"
        )

    # A tile ramp with real spread means at least one multi-pixel cell has
    # nonzero variance -- guard against a vacuous all-zero (single-pixel) pass.
    if agg in ("variance", "stddev"):
        assert any(
            abs(v) > 1e-6 for v in light.values()
        ), f"{grid} {agg}: all cells zero -- fixture lacks real spread"

    # within_tol (same summation-order class as avg): relative tolerance.
    for key in light_keys:
        lv, hv = float(light[key]), float(heavy[key])
        assert abs(lv - hv) <= 1e-9 * max(1.0, abs(hv)), (
            f"{grid} {agg} cell {key} diverged beyond within_tol: "
            f"light={lv} heavy={hv} (diff={abs(lv - hv):.3e})"
        )


# ---------------------------------------------------------------------------
# Tessellate parity (quadbin + BNG) — identical cell-id set + chip count
# ---------------------------------------------------------------------------


def _tess_id(cid, *, bng):
    """Canonicalise a tessellate cell id for cross-tier comparison.

    Both tiers carry the id in the Long ``cellid`` field: quadbin ids ARE the
    Long; BNG stores ``BNG.parse(str)`` and does NOT expose RASTERX_CELL_ID in
    the Spark tile metadata map (it lives only on the GDAL Dataset), so we render
    the authoritative BNG String from the Long via ``pygx._bng.format`` — the
    same bijection both tiers use.
    """
    if not bng:
        return cid
    from databricks.labs.gbx.pygx import _bng

    return _bng.format(cid)


def _light_tessellate_ids(
    spark, raster, sql_name, resolution, mode, *, bng, coverage="complete"
):
    """Light tier: SQL LATERAL tessellate -> (canonical id_set, chip_count).

    ``mode`` is the assignment parameter (``centroid`` / ``covering``).
    ``coverage`` (``complete`` / ``sparse``) is the 4th SQL argument; defaults to
    ``complete`` to match the pre-Stage-2 3-arg call behaviour.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_ras_light_tess")
    rows = spark.sql(
        f"SELECT t.cellid AS cid FROM _ras_light_tess, "
        f"LATERAL {sql_name}(tile, {resolution}, '{mode}', '{coverage}') t"
    ).collect()
    ids = [_tess_id(r["cid"], bng=bng) for r in rows]
    return set(ids), len(ids)


def _heavy_tessellate_ids(
    spark, raster, fn_name, resolution, mode, *, bng, coverage="complete"
):
    """Heavy tier: DataFrame generator -> (canonical id_set, chip_count).

    ``mode`` is the assignment parameter (``centroid`` / ``covering``).
    ``coverage`` (``complete`` / ``sparse``) is forwarded to the Python wrapper;
    defaults to ``complete`` to match the pre-Stage-2 3-arg call behaviour.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    fn = getattr(hx, fn_name)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        hx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    rows = (
        df.select(fn(f.col("tile"), f.lit(resolution), mode, coverage).alias("tt"))
        .select(f.col("tt.cellid").alias("cid"))
        .collect()
    )
    ids = [_tess_id(r["cid"], bng=bng) for r in rows]
    return set(ids), len(ids)


@pytest.mark.parametrize("mode", ["covering", "centroid"])
def test_quadbin_tessellate_cellset_parity(spark_with_jar, mode):
    """quadbin tessellate: identical Long cell-id set + chip count, both modes."""
    spark = spark_with_jar
    raster = _quadbin_raster_4326()
    resolution = 12

    light_ids, light_n = _light_tessellate_ids(
        spark, raster, "gbx_rst_quadbin_tessellate", resolution, mode, bng=False
    )
    heavy_ids, heavy_n = _heavy_tessellate_ids(
        spark, raster, "rst_quadbin_tessellate", resolution, mode, bng=False
    )

    assert light_ids, f"light emitted no quadbin chips (mode={mode})"
    assert heavy_ids, f"heavy emitted no quadbin chips (mode={mode})"

    if light_ids != heavy_ids:
        pytest.fail(
            f"quadbin tessellate mode={mode} cell-set MISMATCH: "
            f"|light|={len(light_ids)} |heavy|={len(heavy_ids)} "
            f"light_only={sorted(light_ids - heavy_ids)[:8]} "
            f"heavy_only={sorted(heavy_ids - light_ids)[:8]}"
        )
    assert light_n == heavy_n, (
        f"quadbin tessellate mode={mode} chip-count mismatch: "
        f"light={light_n} heavy={heavy_n}"
    )


@pytest.mark.parametrize("mode", ["covering", "centroid"])
def test_bng_tessellate_cellset_parity(spark_with_jar, mode):
    """BNG tessellate: identical BNG-String cell-id set + chip count, both modes.

    27700-native fixture so BOTH tiers skip the warp (isolates enumeration
    parity from resampling).
    """
    spark = spark_with_jar
    raster = _bng_raster_27700()
    resolution = 3  # 1 km

    light_ids, light_n = _light_tessellate_ids(
        spark, raster, "gbx_rst_bng_tessellate", resolution, mode, bng=True
    )
    heavy_ids, heavy_n = _heavy_tessellate_ids(
        spark, raster, "rst_bng_tessellate", resolution, mode, bng=True
    )

    assert light_ids, f"light emitted no BNG chips (mode={mode})"
    assert heavy_ids, f"heavy emitted no BNG chips (mode={mode})"
    assert all(isinstance(c, str) for c in light_ids), "light BNG id must be String"
    assert all(isinstance(c, str) for c in heavy_ids), "heavy BNG id must be String"

    if light_ids != heavy_ids:
        pytest.fail(
            f"BNG tessellate mode={mode} cell-set MISMATCH: "
            f"|light|={len(light_ids)} |heavy|={len(heavy_ids)} "
            f"light_only={sorted(light_ids - heavy_ids)[:8]} "
            f"heavy_only={sorted(heavy_ids - light_ids)[:8]}"
        )
    assert light_n == heavy_n, (
        f"BNG tessellate mode={mode} chip-count mismatch: "
        f"light={light_n} heavy={heavy_n}"
    )


# ---------------------------------------------------------------------------
# rasterize_agg parity (quadbin + BNG) — shared grid, NoData-aware mask parity
# ---------------------------------------------------------------------------


def _quadbin_cells():
    """A small quadbin res-12 cell set over central London (Long ids)."""
    from shapely import set_srid, to_wkb
    from shapely.geometry import box

    from databricks.labs.gbx.pygx import _quadbin as qb

    ewkb = to_wkb(set_srid(box(-0.13, 51.50, -0.06, 51.55), 4326), include_srid=True)
    return qb.polyfill(ewkb, 12)


def _bng_cells():
    """A small BNG 1 km (res 3) cell set over London (String ids)."""
    from shapely.geometry import box

    from databricks.labs.gbx.pygx import _bng as bng

    poly = box(530000, 180000, 534000, 183000)
    return [bng.format(c) for c in bng.polyfill(poly, 3)]


def _compare_rasterize(spark, cells, *, grid, srid, resolution, heavy_cellid_col):
    """Run both tiers of rasterize_agg on a shared explicit grid; compare masks.

    Returns nothing; asserts NoData==-9999 both tiers, identical width/height,
    identical covered-pixel mask, and burned presence value 1.0 both tiers.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.pyrx.core import cellraster
    from databricks.labs.gbx.rasterx import functions as hx

    assert len(cells) >= 2, f"{grid}: need a multi-cell set, got {len(cells)}"

    # Shared canvas from the light gridspec helper (kring_pad=1) — identical for
    # both tiers, so the pixel-centroid burn is deterministic.
    xmin, ymin, xmax, ymax, pixel_size, width, height, out_srid = (
        cellraster.compute_gridspec(cells, srid=srid, kring_pad=1, grid=grid)
    )

    df = spark.createDataFrame([(c, "TX1") for c in cells], [heavy_cellid_col, "tx"])

    # --- LIGHT tier (Python UDF, resolved inline — no SQL-name collision) ---
    light_fn = getattr(prx, f"rst_{grid}_rasterize_agg")
    light_out = (
        df.groupBy("tx")
        .agg(
            light_fn(
                heavy_cellid_col,
                value=None,
                out_srid=f.lit(out_srid),
                pixel_size=f.lit(pixel_size),
                xmin=f.lit(xmin),
                ymin=f.lit(ymin),
                xmax=f.lit(xmax),
                ymax=f.lit(ymax),
                width=f.lit(width),
                height=f.lit(height),
                mode=f.lit("centroids"),
                kring_pad=f.lit(1),
            ).alias("tile")
        )
        .collect()
    )
    assert len(light_out) == 1
    light_tile = light_out[0]["tile"]
    assert light_tile is not None and light_tile["raster"] is not None

    # --- HEAVY tier (JAR, 12-arg call_function) ---
    hx.register(spark)
    heavy_fn = getattr(hx, f"rst_{grid}_rasterize_agg")
    heavy_out = (
        df.groupBy("tx")
        .agg(
            heavy_fn(
                f.col(heavy_cellid_col),
                f.lit(None).cast("double"),
                f.lit(out_srid),
                f.lit(pixel_size),
                f.lit(xmin),
                f.lit(ymin),
                f.lit(xmax),
                f.lit(ymax),
                f.lit(width),
                f.lit(height),
                f.lit("centroids"),
                f.lit(1),
            ).alias("tile")
        )
        .collect()
    )
    assert len(heavy_out) == 1
    heavy_tile = heavy_out[0]["tile"]
    assert heavy_tile is not None and heavy_tile["raster"] is not None

    with _serde.open_tile(bytes(light_tile["raster"])) as lds:
        light_arr = lds.read(1)
        assert lds.nodata == _NODATA, f"{grid} light band NoData must be -9999"
        assert (
            lds.width == width and lds.height == height
        ), f"{grid} light grid {lds.width}x{lds.height} != {width}x{height}"
    with _serde.open_tile(bytes(heavy_tile["raster"])) as hds:
        heavy_arr = hds.read(1)
        assert hds.nodata == _NODATA, f"{grid} heavy band NoData must be -9999"
        assert (
            hds.width == width and hds.height == height
        ), f"{grid} heavy grid {hds.width}x{hds.height} != {width}x{height}"

    light_mask = light_arr != _NODATA
    heavy_mask = heavy_arr != _NODATA
    assert int(light_mask.sum()) >= len(cells), f"{grid} light covered < cell count"
    assert int(heavy_mask.sum()) >= len(cells), f"{grid} heavy covered < cell count"

    diverging = np.where(light_mask != heavy_mask)
    n_div = len(diverging[0])
    if n_div > 0:
        rows = diverging[0][:5].tolist()
        cols = diverging[1][:5].tolist()
        pytest.fail(
            f"{grid} rasterize_agg mask parity FAILED: {n_div} pixel(s) differ "
            f"(grid {width}x{height}, {len(cells)} cells, srid={out_srid}). "
            f"First diverging (row,col): {list(zip(rows, cols))}. "
            f"light_covered={int(light_mask.sum())} heavy_covered={int(heavy_mask.sum())}."
        )

    assert np.all(light_arr[light_mask] == 1.0), f"{grid} light burn != 1.0"
    assert np.all(heavy_arr[heavy_mask] == 1.0), f"{grid} heavy burn != 1.0"


def test_quadbin_rasterize_agg_mask_parity(spark_with_jar):
    """quadbin rasterize_agg: NoData==-9999 + identical covered-pixel mask (4326)."""
    _compare_rasterize(
        spark_with_jar,
        _quadbin_cells(),
        grid="quadbin",
        srid=4326,
        resolution=12,
        heavy_cellid_col="cellid",
    )


def test_bng_rasterize_agg_mask_parity(spark_with_jar):
    """BNG rasterize_agg: NoData==-9999 + identical covered-pixel mask (27700-native)."""
    _compare_rasterize(
        spark_with_jar,
        _bng_cells(),
        grid="bng",
        srid=27700,
        resolution=3,
        heavy_cellid_col="cellid",
    )


# ---------------------------------------------------------------------------
# Stage-2 gate: coverage × assignment parity for rastertogrid (all 3 grids)
# ---------------------------------------------------------------------------
#
# Grid configurations for the new parametrized parity tests.
#
# CENTROID resolution: same as the existing single-function tests so these
# share the well-understood fixture regime.
# COVERING resolution: coarser so the raster fits in very few cells (covering
# is O(pixels × candidate-cells-per-pixel); coarse resolution → 1-4 cells →
# fast geometry intersection).
_RTG_CENTROID_RES = {"h3": 7, "bng": 3, "quadbin": 12}
_RTG_COVERING_RES = {
    "h3": 4,
    "bng": 3,
    "quadbin": 8,
}  # bng=3 (1km) matches _small_covering_bng_raster fixture

# Tessellate configs indexed by grid name.
_TESS_CONFIG = {
    "h3": ("gbx_rst_h3_tessellate", "rst_h3_tessellate", False),
    "bng": ("gbx_rst_bng_tessellate", "rst_bng_tessellate", True),
    "quadbin": ("gbx_rst_quadbin_tessellate", "rst_quadbin_tessellate", False),
}
_TESS_CENTROID_RES = {"h3": 5, "bng": 3, "quadbin": 12}
_TESS_COVERING_RES = {"h3": 4, "bng": 1, "quadbin": 8}


def _parity_cmp(light, heavy, *, label, tol=1e-9):
    """Assert cross-tier parity for a {(band, cellID): measure} dict pair.

    Rules:
    - Cell-set equality: every key present in both tiers.
    - Both None → empty-cell parity (skip value comparison).
    - One None, one not → FAIL (one-sided None is a real divergence).
    - Both float → ``abs(lv - hv) <= tol * max(1.0, abs(hv))`` (relative tol).

    ``tol`` defaults to 1e-9.  Use a wider value (e.g. 1e-6) for covering aggs
    where JTS (heavy) and shapely (light) area fractions may differ slightly.
    """
    light_keys = set(light.keys())
    heavy_keys = set(heavy.keys())
    if light_keys != heavy_keys:
        pytest.fail(
            f"{label} cell-set MISMATCH: "
            f"|light|={len(light_keys)} |heavy|={len(heavy_keys)} "
            f"light_only={sorted(light_keys - heavy_keys)[:8]} "
            f"heavy_only={sorted(heavy_keys - light_keys)[:8]}"
        )
    for key in light_keys:
        lv, hv = light[key], heavy[key]
        if lv is None and hv is None:
            continue  # covered-but-empty cell: both tiers agree → parity
        if lv is None or hv is None:
            pytest.fail(
                f"{label} one-sided None at {key}: light={lv} heavy={hv} — "
                "real cross-tier divergence (one tier emits an empty cell, the other does not)"
            )
        lv_f, hv_f = float(lv), float(hv)
        threshold = tol * max(1.0, abs(hv_f))
        assert abs(lv_f - hv_f) <= threshold, (
            f"{label} cell {key} measure diverged: "
            f"light={lv_f} heavy={hv_f} (diff={abs(lv_f - hv_f):.3e} > tol={threshold:.3e})"
        )


def _is_chip_all_nodata(raster_bytes):
    """True iff the chip raster contains only NoData pixels (no valid data).

    Used to verify that covered-but-empty chips are genuinely all-NoData in both tiers.
    This function only checks whether every pixel equals the declared NoData value.
    Returns True for ``None`` input (a null raster is treated as all-NoData).
    """
    if raster_bytes is None:
        return True
    from databricks.labs.gbx.pyrx import _serde

    with _serde.open_tile(raster_bytes) as ds:
        nodata = ds.nodata
        arr = ds.read(1)
        if nodata is None:
            # No declared NoData — treat NaN as NoData for float rasters.
            return bool(np.all(np.isnan(arr.astype("float64"))))
        nd_f = float(nodata)
        if np.isnan(nd_f):
            return bool(np.all(np.isnan(arr.astype("float64"))))
        return bool(np.all(arr == nodata))


@pytest.mark.parametrize("agg", ["avg", "count"])
@pytest.mark.parametrize("assignment", ["centroid", "covering"])
@pytest.mark.parametrize("coverage", ["sparse", "complete"])
@pytest.mark.parametrize("grid", ["h3", "bng", "quadbin"])
def test_rastertogrid_coverage_assignment_parity(
    spark_with_jar, grid, coverage, assignment, agg
):
    """Cross-tier rastertogrid parity for all coverage × assignment combinations.

    Exercises the Stage-2 coverage/assignment extension for all three grid families
    (h3, bng, quadbin) with two representative aggregations:

    * ``avg`` — the core averaging path (both centroid and covering).
    * ``count`` — exercises the count→Double change: both tiers now emit Double
      (not int) for pixel count; covering count is the area-fraction sum (may be
      fractional).  Compare as float with relative 1e-9 tolerance.

    Assertions:
    * Exact cell-set equality (both tiers emit the same cell keys).
    * Per-cell measure within relative tolerance 1e-9 (default for centroid,
      1e-6 for covering where JTS vs shapely geometry areas may differ slightly).
    * Both None ⇒ empty-cell parity; one None one not ⇒ FAIL.
    * ``complete`` cell set ⊇ corresponding ``sparse`` cell set (for both tiers).

    PERFORMANCE: covering mode uses a small 4×4 raster at a coarser resolution so
    the O(pixels × intersection-ops) geometry work completes quickly.
    """
    spark = spark_with_jar
    is_bng = grid == "bng"

    # Select raster and resolution based on assignment mode.
    if assignment == "covering":
        raster = (
            _small_covering_bng_raster() if is_bng else _small_covering_raster_4326()
        )
        resolution = _RTG_COVERING_RES[grid]
        # JTS (heavy) and shapely (light) intersection areas may differ at ~1e-10;
        # widen tolerance slightly for covering aggs.
        tol = 1e-6
    else:
        raster = _bng_raster_27700() if is_bng else _quadbin_raster_4326()
        resolution = _RTG_CENTROID_RES[grid]
        tol = 1e-9

    label = f"{grid} {coverage}/{assignment} {agg}"

    # Collect LIGHT first (both tiers share the gbx_rst_* SQL names; light must
    # be materialised before heavy re-registers).
    if is_bng:
        light = _light_reducer_rows(
            spark, raster, resolution, agg, coverage=coverage, assignment=assignment
        )
        heavy = _heavy_reducer_rows(
            spark, raster, resolution, agg, coverage=coverage, assignment=assignment
        )
    else:
        light = _light_grid_rows(
            spark,
            raster,
            resolution,
            grid,
            agg,
            coverage=coverage,
            assignment=assignment,
        )
        heavy = _heavy_grid_rows(
            spark,
            raster,
            resolution,
            grid,
            agg,
            coverage=coverage,
            assignment=assignment,
        )

    assert light, f"{label}: light emitted no cells"
    assert heavy, f"{label}: heavy emitted no cells"

    _parity_cmp(light, heavy, label=label, tol=tol)

    # BNG covering non-degeneracy guard: the origin-offset fixture must produce genuinely
    # fractional area-fraction weights (< 1.0).  For the ``count`` agg in covering mode,
    # the cell count equals the sum of area fractions from all overlapping pixels.  If ALL
    # weights were 1.0 (degenerate grid-aligned fixture), every cell count would be an
    # integer.  At least one fractional count proves the fixture exercises partial coverage.
    if grid == "bng" and assignment == "covering" and agg == "count":
        non_integer_found = any(
            v is not None and abs(v - round(v)) > 1e-6 for v in light.values()
        )
        assert non_integer_found, (
            "BNG covering fixture is degenerate: all count values are integers "
            "(area-fraction weights all equal 1.0). "
            "Fix: stagger the raster origin so pixels straddle BNG cell boundaries."
        )

    # complete ⊇ sparse: verify the superset relationship using a second sparse call.
    if coverage == "complete":
        if is_bng:
            light_sparse = _light_reducer_rows(
                spark, raster, resolution, agg, coverage="sparse", assignment=assignment
            )
        else:
            light_sparse = _light_grid_rows(
                spark,
                raster,
                resolution,
                grid,
                agg,
                coverage="sparse",
                assignment=assignment,
            )

        sparse_keys = set(light_sparse.keys())
        assert sparse_keys <= set(
            light.keys()
        ), f"{label}: sparse cell set is NOT a subset of complete (light tier)"
        assert sparse_keys <= set(
            heavy.keys()
        ), f"{label}: sparse cell set is NOT a subset of complete (heavy tier)"


# ---------------------------------------------------------------------------
# Stage-2 gate: coverage × assignment parity for tessellate (all 3 grids)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("assignment", ["centroid", "covering"])
@pytest.mark.parametrize("coverage", ["sparse", "complete"])
@pytest.mark.parametrize("grid", ["h3", "bng", "quadbin"])
def test_tessellate_coverage_assignment_parity(
    spark_with_jar, grid, coverage, assignment
):
    """Cross-tier tessellate parity for all coverage × assignment combinations.

    For each combination the test asserts:
    * Exact cell-id-set equality (same canonical ids in both tiers).
    * Identical chip count (same number of emitted rows).
    * ``centroid``: per cell, equal chip width, height and geotransform (1e-9),
      and the same source pixels ``(row, col, value)`` in both tiers' chips.
    * ``complete``: the cell set contains the ``sparse`` one, and every
      covered-but-empty cell is all-NoData in both tiers; under ``centroid`` it is
      the same 1×1 chip in both. Covering chips are clipped to the cell geometry,
      and their dimensions are not compared here.

    PERFORMANCE: covering mode uses a coarser resolution so the raster's footprint
    fits in very few cells, keeping the clip-to-cell-geometry work fast.
    """
    spark = spark_with_jar
    sql_name, fn_name, is_bng = _TESS_CONFIG[grid]

    raster = _bng_raster_27700() if is_bng else _quadbin_raster_4326()
    if assignment == "covering":
        resolution = _TESS_COVERING_RES[grid]
    else:
        resolution = _TESS_CENTROID_RES[grid]

    label = f"{grid} tessellate {coverage}/{assignment}"

    def _id(cid):
        return _tess_id(cid, bng=is_bng)

    # Collect LIGHT first, then register heavy (both share the gbx_rst_* SQL name).
    light_rows = tp.light_chips(
        spark, raster, sql_name, resolution, assignment, coverage, _id
    )
    heavy_rows = tp.heavy_chips(
        spark, raster, fn_name, resolution, assignment, coverage, _id
    )
    light = tp.by_cell(light_rows, f"{label} light")
    heavy = tp.by_cell(heavy_rows, f"{label} heavy")
    light_ids, heavy_ids = set(light), set(heavy)

    assert light_ids, f"{label}: light emitted no chips"
    assert heavy_ids, f"{label}: heavy emitted no chips"
    assert not tp.error_rows(light_rows), f"{label}: light error rows"
    assert not tp.error_rows(heavy_rows), f"{label}: heavy error rows"

    if light_ids != heavy_ids:
        pytest.fail(
            f"{label} cell-set MISMATCH: "
            f"|light|={len(light_ids)} |heavy|={len(heavy_ids)} "
            f"light_only={sorted(light_ids - heavy_ids)[:8]} "
            f"heavy_only={sorted(heavy_ids - light_ids)[:8]}"
        )

    assert len(light_rows) == len(
        heavy_rows
    ), f"{label} chip-count mismatch: light={len(light_rows)} heavy={len(heavy_rows)}"

    if assignment == "centroid":
        tp.assert_centroid_chips_match(light, heavy, tp.source_transform(raster), label)

    # complete ⊇ sparse: verify superset (at cell-id level) when coverage=complete.
    if coverage == "complete":
        light_sparse_ids, _ = _light_tessellate_ids(
            spark,
            raster,
            sql_name,
            resolution,
            assignment,
            bng=is_bng,
            coverage="sparse",
        )
        heavy_sparse_ids, _ = _heavy_tessellate_ids(
            spark,
            raster,
            fn_name,
            resolution,
            assignment,
            bng=is_bng,
            coverage="sparse",
        )
        assert (
            light_sparse_ids <= light_ids
        ), f"{label}: sparse cell set not subset of complete (light)"
        assert (
            heavy_sparse_ids <= heavy_ids
        ), f"{label}: sparse cell set not subset of complete (heavy)"

        # Cells in complete but NOT in sparse are covered-but-empty: their chips
        # must be all-NoData in BOTH tiers, and 1×1 under centroid.
        all_empty_ids = (light_ids - light_sparse_ids) | (heavy_ids - heavy_sparse_ids)
        for cid in all_empty_ids:
            assert _is_chip_all_nodata(light.get(cid)), (
                f"{label}: light chip for empty cell {cid} contains valid pixels "
                "(expected all-NoData for covered-but-empty cell)"
            )
            assert _is_chip_all_nodata(heavy.get(cid)), (
                f"{label}: heavy chip for empty cell {cid} contains valid pixels "
                "(expected all-NoData for covered-but-empty cell)"
            )
            if assignment == "centroid":
                for tier, chips in (("light", light), ("heavy", heavy)):
                    size = tp.chip_shape(chips[cid])[:2]
                    assert size == (1, 1), f"{label}: {tier} empty chip {cid} is {size}"


# dtype -> (fallback NoData, rows of real pixels equal to it, rows of real 0s that stay
# valid). For uint8 the fallback IS 0, so its real 0s become NoData.
_NO_NODATA_CASES = {
    "uint16": (65535.0, (2,), (0, 1)),
    "uint8": (0.0, (0, 1), ()),
}


def _no_nodata_raster(dtype, *, epsg, origin, px, n):
    """n×n ramp (1..200) with NO declared NoData, plus the rows of ``_NO_NODATA_CASES``."""
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    fallback, fallback_rows, zero_rows = _NO_NODATA_CASES[dtype]
    data = (np.arange(n * n, dtype="uint32").reshape(n, n) % 200 + 1).astype(dtype)
    for r in zero_rows:
        data[r, :] = 0
    for r in fallback_rows:
        data[r, :] = fallback
    prof = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=1,
        dtype=dtype,
        crs=f"EPSG:{epsg}",
        transform=from_origin(origin[0], origin[1], px, px),
    )
    with MemoryFile() as mf:
        with mf.open(**prof) as dst:
            dst.write(data, 1)
        return mf.read(), data


@pytest.mark.parametrize("assignment", ["centroid", "covering"])
@pytest.mark.parametrize("grid", ["h3", "bng", "quadbin"])
@pytest.mark.parametrize("dtype", ["uint16", "uint8"])
def test_tessellate_without_nodata_parity(spark_with_jar, dtype, grid, assignment):
    """A source without NoData tessellates the same way in both tiers.

    Both tiers stamp the shared dtype fallback (65535 for uint16, 0 for uint8) on
    every chip and emit the same rows, with no error rows. Every valid chip pixel is
    a source pixel with the source's value: the clip fill is NoData, and source
    pixels equal to the fallback are NoData too (not moved to a neighbouring value),
    while uint16's real 0s stay valid. Under ``centroid`` the chips also match per
    cell, and together they hold every source pixel that is not the fallback.
    """
    spark = spark_with_jar
    sql_name, fn_name, is_bng = _TESS_CONFIG[grid]
    fallback, fallback_rows, zero_rows = _NO_NODATA_CASES[dtype]
    if is_bng:
        n = 8
        raster, data = _no_nodata_raster(
            dtype, epsg=27700, origin=(530000.0, 181000.0), px=100.0, n=n
        )
    else:
        n = 16
        raster, data = _no_nodata_raster(
            dtype, epsg=4326, origin=(-0.13, 51.55), px=0.01, n=n
        )
    res_map = _TESS_COVERING_RES if assignment == "covering" else _TESS_CENTROID_RES
    resolution = res_map[grid]
    label = f"{grid} {dtype} no-nodata {assignment}"

    def _id(cid):
        return _tess_id(cid, bng=is_bng)

    light_rows = tp.light_chips(
        spark, raster, sql_name, resolution, assignment, "complete", _id
    )
    heavy_rows = tp.heavy_chips(
        spark, raster, fn_name, resolution, assignment, "complete", _id
    )
    assert light_rows, f"{label}: light emitted no chips"
    assert not tp.error_rows(light_rows), f"{label}: light error rows"
    assert not tp.error_rows(heavy_rows), f"{label}: heavy error rows"
    assert len(light_rows) == len(
        heavy_rows
    ), f"{label} row-count mismatch: light={len(light_rows)} heavy={len(heavy_rows)}"

    src_tf = tp.source_transform(raster)
    for tier, rows in (("light", light_rows), ("heavy", heavy_rows)):
        nodatas = {tp.chip_shape(b)[3] for _, b, _ in rows}
        assert nodatas == {fallback}, f"{label}: {tier} chip NoData {nodatas}"
        valid = {px for _, b, _ in rows for px in tp.chip_pixels(b, src_tf)}
        kept = sorted(px for px in valid if px[0] in fallback_rows)[:5]
        assert (
            not kept
        ), f"{label}: {tier} pixels equal to the fallback stay valid: {kept}"
        wrong = sorted(
            px
            for px in valid
            if not (0 <= px[0] < n and 0 <= px[1] < n)
            or px[2] != (float(data[px[0], px[1]]),)
        )[:5]
        assert (
            not wrong
        ), f"{label}: {tier} valid chip pixels not from the source: {wrong}"
        if zero_rows:
            zeros = {px for px in valid if px[2] == (0.0,)}
            assert zeros, f"{label}: {tier} chips lost the real zero pixels"

    if assignment == "centroid":
        light = tp.by_cell(light_rows, f"{label} light")
        heavy = tp.by_cell(heavy_rows, f"{label} heavy")
        tp.assert_centroid_chips_match(light, heavy, src_tf, label)
        covered = {px[:2] for b in light.values() for px in tp.chip_pixels(b, src_tf)}
        expected = n * n - len(fallback_rows) * n
        assert (
            len(covered) == expected
        ), f"{label}: chips hold {len(covered)} of {expected} pixels"


def _masked_uint16_raster(*, epsg, origin, px, n):
    """n×n uint16 ramp (1..200), NO declared NoData, real 65535s (the fallback) in
    row 6, and an internal per-dataset mask that hides rows 2-3 and the left half of
    row 5."""
    import rasterio
    from rasterio.enums import MaskFlags
    from rasterio.io import MemoryFile
    from rasterio.transform import from_origin

    data = (np.arange(n * n, dtype="uint32").reshape(n, n) % 200 + 1).astype("uint16")
    data[6, :] = 65535
    mask = np.full((n, n), 255, dtype="uint8")
    mask[2:4, :] = 0
    mask[5, : n // 2] = 0
    prof = dict(
        driver="GTiff",
        width=n,
        height=n,
        count=1,
        dtype="uint16",
        crs=f"EPSG:{epsg}",
        transform=from_origin(origin[0], origin[1], px, px),
    )
    with rasterio.Env(GDAL_TIFF_INTERNAL_MASK=True):
        with MemoryFile() as mf:
            with mf.open(**prof) as dst:
                dst.write(data, 1)
                dst.write_mask(mask)
            with mf.open() as chk:
                flags = chk.mask_flag_enums[0]
                assert flags == [
                    MaskFlags.per_dataset
                ], "fixture needs a per-dataset mask"
            return mf.read(), data, mask == 0


# h3 is left out: heavy clips hexagons with an all-touched cutline, and GDAL does
# not apply a source mask band under such a cutline (a separate, known limitation).
# The quadbin and BNG cells here split the raster on pixel edges, so gdalwarp
# applies the source mask under their cutlines.
@pytest.mark.parametrize("grid", ["bng", "quadbin"])
def test_tessellate_covering_masked_source_parity(spark_with_jar, grid):
    """Covering on a source without NoData but with its own mask band.

    The source's masked pixels stay NoData in both tiers' chips, and every other
    valid chip pixel carries the source value: the fallback NoData that covering
    stamps must not override the source mask, also when the source holds pixels
    equal to the fallback. Heavy keeps gdalwarp's handling of those
    fallback-valued pixels on a masked source, so they are not compared.
    """
    spark = spark_with_jar
    sql_name, fn_name, is_bng = _TESS_CONFIG[grid]
    if is_bng:
        n = 8
        raster, data, masked = _masked_uint16_raster(
            epsg=27700, origin=(530000.0, 181000.0), px=100.0, n=n
        )
    else:
        n = 16
        raster, data, masked = _masked_uint16_raster(
            epsg=4326, origin=(-0.13, 51.55), px=0.01, n=n
        )
    resolution = _TESS_COVERING_RES[grid]
    label = f"{grid} masked-source covering"

    def _id(cid):
        return _tess_id(cid, bng=is_bng)

    light_rows = tp.light_chips(
        spark, raster, sql_name, resolution, "covering", "complete", _id
    )
    heavy_rows = tp.heavy_chips(
        spark, raster, fn_name, resolution, "covering", "complete", _id
    )
    assert light_rows, f"{label}: light emitted no chips"
    assert not tp.error_rows(light_rows), f"{label}: light error rows"
    assert not tp.error_rows(heavy_rows), f"{label}: heavy error rows"
    assert len(light_rows) == len(heavy_rows), f"{label} row-count mismatch"

    src_tf = tp.source_transform(raster)
    for tier, rows in (("light", light_rows), ("heavy", heavy_rows)):
        valid = {px for _, b, _ in rows for px in tp.chip_pixels(b, src_tf)}
        inside = [px for px in valid if 0 <= px[0] < n and 0 <= px[1] < n]
        shown = sorted(px for px in inside if masked[px[0], px[1]])[:5]
        assert not shown, f"{label}: {tier} masked source pixels are valid: {shown}"
        wrong = (
            sorted(px for px in valid if px not in inside)[:5]
            + sorted(
                px
                for px in inside
                if data[px[0], px[1]] != 65535 and px[2] != (float(data[px[0], px[1]]),)
            )[:5]
        )
        assert (
            not wrong
        ), f"{label}: {tier} valid chip pixels not from the source: {wrong}"
        assert inside, f"{label}: {tier} chips hold no valid pixel"


# ---------------------------------------------------------------------------
# Regression: 2-arg default for rst_h3_tessellate == centroid + complete
# ---------------------------------------------------------------------------


def test_default_h3_tessellate_2arg_parity(spark_with_jar):
    """2-arg gbx_rst_h3_tessellate(tile, res) is centroid+complete in BOTH tiers.

    Task 8 aligned the light tier's default to match heavy: both now default to
    ``assignment="centroid"`` and ``coverage="complete"``.  This test is the
    regression gate for that alignment: if either tier's default diverges, the
    cell sets produced by the 2-arg call will differ.
    """
    spark = spark_with_jar
    raster = _quadbin_raster_4326()  # 16×16 4326 raster, valid for H3
    resolution = 5  # matches test_parity_h3_tessellate.py fixture

    # Light: 2-arg SQL (no explicit assignment/coverage) — collect FIRST.
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df2 = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df2.createOrReplaceTempView("_ras_default_h3")
    light_2arg_rows = spark.sql(
        f"SELECT t.cellid AS cid FROM _ras_default_h3, "
        f"LATERAL gbx_rst_h3_tessellate(tile, {resolution}) t"
    ).collect()
    light_2arg_ids = {r["cid"] for r in light_2arg_rows}
    light_2arg_n = len(light_2arg_rows)

    # Heavy: 2-arg Python wrapper (no explicit assignment/coverage) — register AFTER light.
    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    heavy_2arg_rows = (
        df2.select(hx.rst_h3_tessellate(f.col("tile"), f.lit(resolution)).alias("tt"))
        .select(f.col("tt.cellid").alias("cid"))
        .collect()
    )
    heavy_2arg_ids = {r["cid"] for r in heavy_2arg_rows}
    heavy_2arg_n = len(heavy_2arg_rows)

    assert light_2arg_ids, "light 2-arg emitted no cells"
    assert heavy_2arg_ids, "heavy 2-arg emitted no cells"

    if light_2arg_ids != heavy_2arg_ids:
        pytest.fail(
            f"2-arg default parity FAILED: "
            f"|light|={len(light_2arg_ids)} |heavy|={len(heavy_2arg_ids)} "
            f"light_only={sorted(light_2arg_ids - heavy_2arg_ids)[:8]} "
            f"heavy_only={sorted(heavy_2arg_ids - light_2arg_ids)[:8]}"
        )
    assert (
        light_2arg_n == heavy_2arg_n
    ), f"2-arg default chip-count mismatch: light={light_2arg_n} heavy={heavy_2arg_n}"

    # Cross-check: the 2-arg defaults equal the explicit centroid+complete call (light).
    # This proves the light tier's default is centroid+complete and not some other combo.
    light_explicit_ids, light_explicit_n = _light_tessellate_ids(
        spark,
        raster,
        "gbx_rst_h3_tessellate",
        resolution,
        "centroid",
        bng=False,
        coverage="complete",
    )
    assert (
        light_2arg_ids == light_explicit_ids
    ), "light 2-arg default != explicit centroid+complete — Task-8 alignment broken"
    assert (
        light_2arg_n == light_explicit_n
    ), "light 2-arg chip count != explicit centroid+complete — Task-8 alignment broken"
