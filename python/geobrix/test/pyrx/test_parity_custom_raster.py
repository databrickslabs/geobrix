"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for the 10 custom-grid
raster functions.

The capstone parity check. Both tiers are run on the SAME
in-memory raster + custom-grid struct and their outputs compared:

  * 8 custom reducers (``rst_custom_rastertogrid{avg,count,max,min,median,sum,
    variance,stddev}``) — EXACT cell-set equality + per-cell measure within 1e-9
    (centroid) or 1e-6 (covering, where JTS vs shapely area fractions may differ).
  * ``rst_custom_tessellate`` — identical Long cell-id set + chip count, both
    ``covering`` and ``centroid`` modes.
  * ``rst_custom_rasterize_agg`` — same NoData=-9999 in both tiers, identical
    covered-pixel mask + burned value 1.0 on the same explicit grid.

HARNESS REUSE (no new comparison framework invented):
  * Reducers + tessellate reuse the ``test_parity_bng_quadbin_raster_grid.py``
    two-phase pattern — register LIGHT first (prx.register), collect via SQL
    LATERAL or scalar, then register HEAVY (hx.register which OVERWRITES the
    gbx_rst_custom_* SQL names) and collect; sequential collection materialises
    before re-registration.
  * rasterize_agg: light via Python API (prx.rst_custom_rasterize_agg returns a
    tile struct); heavy via f.call_function after hx.register (UDAF returns tile
    struct). Both access ["raster"] bytes for mask comparison.

CUSTOM GRID DESIGN:
  * Grid: EPSG:27700, bounds (529000, 179000)-(533000, 183000) = 4000×4000 m;
    root_cell_size=4000, cell_splits=2 → resolution 1 yields 4 cells of 2000×2000 m.
  * Raster: EPSG:27700, 8×8 pixels at 100 m → extent (530000, 182200)-(530800, 183000),
    well within the grid bounds (R3 not triggered). Ramp values 1..64.
  * No CRS warping in either tier (raster already in grid's native CRS).

Heavy requires the geobrix JAR and the GDAL JNI libraries; both are present in the
geobrix-dev Docker container. Auto-skips when the JAR is absent or a JAR-free Spark
session is already live.

Run in geobrix-dev Docker:
    bash scripts/commands/gbx-test-parity.sh \\
        --path python/geobrix/test/pyrx/test_parity_custom_raster.py \\
        --skip-build --log stage3-t8.log
"""

import logging
from pathlib import Path

import pytest

rasterio = pytest.importorskip(
    "rasterio",
    reason="rasterio not installed (geobrix[light_env6] required)",
)
pytest.importorskip(
    "shapely",
    reason="shapely not installed (geobrix[light_env6] required)",
)
import numpy as np  # noqa: E402

pytestmark = pytest.mark.integration

_HERE = Path(__file__).resolve()
# parents[2] == python/geobrix (test/pyrx -> test -> python/geobrix)
_JARS = sorted((_HERE.parents[2] / "lib").glob("geobrix-*-jar-with-dependencies.jar"))

_NODATA = -9999.0

# ---------------------------------------------------------------------------
# Custom grid configuration
# Grid: EPSG:27700, 60000×60000 m extent, root_cell_size=20000, cell_splits=2
# Resolution 1: 6 cols × 6 rows = 36 cells of 10000×10000 m each.
#
# The 6-col × 6-row layout is necessary because the Scala polyfill uses a
# bidirectional over-scan: (first-1) to (last+1). With 6 cols, placing the
# raster in the INTERIOR (cols 1-4) ensures both lower (col 0) and upper (col 5)
# over-scan candidates are within bounds.
#
# Cell centroids: cx = 515000 + col*10000; cy = 170000 + row*10000
# (for bound_x_min=510000, 10000m cells at res=1).
#
# Centroid raster: 8x8@100m at (530000,182800) → col 2 [530000,540000], row 1 [175000,185000].
# Covering raster: 1x1@20000m at (521000,206000) → extent [521000,186000,541000,206000]
#   → contains centroids of cols 1-2, rows 2-3 (4 cells).
# ---------------------------------------------------------------------------
_GRID_BOUND_X_MIN = 510000
_GRID_BOUND_X_MAX = 570000
_GRID_BOUND_Y_MIN = 165000
_GRID_BOUND_Y_MAX = 225000
_GRID_CELL_SPLITS = 2
_GRID_ROOT_SIZE = 20000
_GRID_SRID = 27700
_GRID_RES = 1  # resolution 1 → 36 cells of 10000×10000 m

# Cell centroids at res=1:
#   cx = _GRID_BOUND_X_MIN + col*10000 + 5000 = 515000 + col*10000
#   cy = _GRID_BOUND_Y_MIN + row*10000 + 5000 = 170000 + row*10000
# Centroid raster (530000,182800): col=2 [530000,540000], row=1 [175000,185000]
# Covering raster (521000,206000,20000m): cols 1-2, rows 2-3

# SQL expression that builds the grid struct (both tiers share gbx_custom_grid).
_GRID_SQL = (
    f"gbx_custom_grid("
    f"{_GRID_BOUND_X_MIN}, {_GRID_BOUND_X_MAX}, "
    f"{_GRID_BOUND_Y_MIN}, {_GRID_BOUND_Y_MAX}, "
    f"{_GRID_CELL_SPLITS}, {_GRID_ROOT_SIZE}, {_GRID_ROOT_SIZE}, "
    f"{_GRID_SRID}"
    f")"
)


# ---------------------------------------------------------------------------
# Spark fixture — JAR loaded, scoped to module
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
                "gbx:test:python --path "
                "python/geobrix/test/pyrx/test_parity_custom_raster.py "
                "--with-integration"
            )

    session = (
        SparkSession.builder.master("local[2]")
        .appName("gbx-custom-raster-parity")
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
# In-memory raster fixture (identical bytes fed to BOTH tiers)
# ---------------------------------------------------------------------------


def _gtiff_bytes(data, *, epsg, origin, px, nodata=_NODATA):
    """Single-band north-up GTiff from a 2-D array at the given CRS/origin/pixel_size."""
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


def _custom_raster_27700():
    """8x8 EPSG:27700 raster in the custom grid's CRS, 100 m pixels, ramp 1..64.

    Origin (530000, 182800) → extent (530000,181800)-(530800,182800).
    Entirely within cell (col=0, row=0) of the 16-cell grid:
    x=[525000,535000], y=[175000,185000]. 2200 m margin from cell north y=185000
    ensures the polyfill over-scan only generates col/row 1 (which exists).
    Both tiers skip any CRS warp (raster already in grid native CRS 27700).
    """
    data = np.arange(1, 65, dtype="float32").reshape(8, 8)
    return _gtiff_bytes(data, epsg=27700, origin=(530000.0, 182800.0), px=100.0)


def _covering_raster():
    """1x1 EPSG:27700 raster (single 18000 m pixel) covering exactly 4 inner cells.

    Origin (521000, 204000) → extent [521000,186000,539000,204000].
    Strictly inside cols 1-2 [520000,540000) and rows 2-3 [185000,205000):
      last_x=trunc((539000-510000)/10000)=2, last_y=trunc((204000-165000)/10000)=3
      → no col 3 (x≥540000) or row 4 (y≥205000) overlap.

    Contains cell centroids:
      (525000,190000) ✓  (535000,190000) ✓
      (525000,200000) ✓  (535000,200000) ✓

    Bidirectional over-scan safety (Scala polyfill first-1..last+1):
      first_x=1 → lower col 0 cx=515000 ✓  last_x=2 → upper col 3 cx=545000 ✓
      first_y=2 → lower row 1 cy=180000 ✓  last_y=3 → upper row 4 cy=210000 ✓

    Non-degeneracy: each cell overlaps 9000×9000m of the 18000×18000m pixel →
    count weight = 0.25 per cell (fractional) → assertion holds.
    """
    data = np.array([[5.0]], dtype="float32").reshape(1, 1)
    return _gtiff_bytes(data, epsg=27700, origin=(521000.0, 204000.0), px=18000.0)


def _small_pixel_covering_raster():
    """9x9 EPSG:27700 raster, 2000 m pixels (SUB-CELL: the grid's cells are 10000 m).

    Origin (521000, 204000) → extent [521000, 186000, 539000, 204000], the SAME
    footprint as ``_covering_raster`` but paved with 81 sub-cell 2000 m pixels
    instead of one 18000 m pixel.  It spans the 4 inner cells (cols 1-2, rows 2-3).

    THIS is the regime the pre-fix light tier got wrong: each 2000 m pixel is far
    smaller than a 10000 m cell, so per-pixel centroid polyfill finds NO cell centre
    inside most pixels → the un-buffered light covering path returned zero cells for
    them and dropped their mass (missing cells, lost weight), while heavy's buffered
    ``coveringCandidateCells`` found the containing cell and kept it.  With the buffer
    fix the two tiers must agree exactly.

    Footprint is strictly interior (10000 m buffer stays within the grid bounds), so
    heavy's buffered enumeration does not hit the out-of-bounds guard.
    """
    data = np.arange(1, 82, dtype="float32").reshape(9, 9)
    return _gtiff_bytes(data, epsg=27700, origin=(521000.0, 204000.0), px=2000.0)


# ---------------------------------------------------------------------------
# Helper: add grid struct column to a DataFrame
# ---------------------------------------------------------------------------


def _add_grid_col(df):
    """Append a CUSTOM_GRID_SCHEMA struct literal column named 'grid'."""
    from pyspark.sql import functions as f

    return df.withColumn(
        "grid",
        f.struct(
            f.lit(_GRID_BOUND_X_MIN).cast("long").alias("bound_x_min"),
            f.lit(_GRID_BOUND_X_MAX).cast("long").alias("bound_x_max"),
            f.lit(_GRID_BOUND_Y_MIN).cast("long").alias("bound_y_min"),
            f.lit(_GRID_BOUND_Y_MAX).cast("long").alias("bound_y_max"),
            f.lit(_GRID_CELL_SPLITS).alias("cell_splits"),
            f.lit(_GRID_ROOT_SIZE).alias("root_cell_size_x"),
            f.lit(_GRID_ROOT_SIZE).alias("root_cell_size_y"),
            f.lit(_GRID_SRID).alias("srid"),
        ),
    )


# ---------------------------------------------------------------------------
# Reducer helpers (rastertogrid 8 functions)
# ---------------------------------------------------------------------------


def _tile_df(spark, raster):
    """Build a one-row DataFrame with tile + grid columns."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    raw = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    return _add_grid_col(raw)


def _light_reducer_rows(
    spark, raster, agg, *, coverage="complete", assignment="centroid"
):
    """Light tier: SQL LATERAL → {(band, cellID): measure}.

    Registers pyrx (light), then collects via the registered gbx_rst_custom_* UDTF.
    """
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = _tile_df(spark, raster)
    df.createOrReplaceTempView("_rst_cust_light")
    rows = spark.sql(
        f"SELECT t.band AS band, t.cellID AS cellID, t.measure AS measure "
        f"FROM _rst_cust_light, "
        f"LATERAL gbx_rst_custom_rastertogrid{agg}("
        f"tile, grid, {_GRID_RES}, '{coverage}', '{assignment}') t"
    ).collect()
    return {(r["band"], r["cellID"]): r["measure"] for r in rows}


def _heavy_reducer_rows(
    spark, raster, agg, *, coverage="complete", assignment="centroid"
):
    """Heavy tier: scalar ARRAY expression → {(band, cellID): measure}.

    Registers rasterx (which overwrites the gbx_rst_custom_* SQL names with GDAL
    expressions), then collects via scalar SQL with posexplode/explode.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    df = _tile_df(spark, raster)
    df.createOrReplaceTempView("_rst_cust_heavy")
    rows = (
        spark.sql(
            f"SELECT posexplode("
            f"  gbx_rst_custom_rastertogrid{agg}("
            f"    tile, grid, {_GRID_RES}, '{coverage}', '{assignment}')"
            f") AS (band0, cells) "
            f"FROM _rst_cust_heavy"
        )
        .select(
            (f.col("band0") + 1).alias("band"),
            f.explode("cells").alias("c"),
        )
        .select(
            "band",
            f.col("c.cellID").alias("cellID"),
            f.col("c.measure").alias("measure"),
        )
        .collect()
    )
    return {(r["band"], r["cellID"]): r["measure"] for r in rows}


# ---------------------------------------------------------------------------
# Parity comparison helper
# ---------------------------------------------------------------------------


def _parity_cmp(light, heavy, *, label, tol=1e-9):
    """Assert cross-tier parity for a {(band, cellID): measure} dict pair."""
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
            continue
        if lv is None or hv is None:
            pytest.fail(f"{label} one-sided None at {key}: light={lv} heavy={hv}")
        lv_f, hv_f = float(lv), float(hv)
        threshold = tol * max(1.0, abs(hv_f))
        assert abs(lv_f - hv_f) <= threshold, (
            f"{label} cell {key} diverged: "
            f"light={lv_f} heavy={hv_f} (diff={abs(lv_f - hv_f):.3e} > tol={threshold:.3e})"
        )


# ---------------------------------------------------------------------------
# Tests: rastertogrid (all 8 aggregations, centroid + complete)
# ---------------------------------------------------------------------------

_REDUCERS = ["avg", "count", "max", "min", "median", "sum", "variance", "stddev"]


@pytest.mark.parametrize("agg", _REDUCERS)
def test_custom_rastertogrid_centroid_complete_parity(spark_with_jar, agg):
    """All 8 custom rastertogrid aggs: exact cell-set + measure within 1e-9.

    27700-native fixture: both tiers skip any CRS warp, so any divergence is a
    real cell-math or reducer difference (do NOT loosen tolerance — fix upstream).
    """
    spark = spark_with_jar
    raster = _custom_raster_27700()

    # Collect LIGHT first (both tiers share the gbx_rst_custom_* SQL names).
    light = _light_reducer_rows(
        spark, raster, agg, coverage="complete", assignment="centroid"
    )
    heavy = _heavy_reducer_rows(
        spark, raster, agg, coverage="complete", assignment="centroid"
    )

    assert light, f"custom {agg} centroid/complete: light emitted no cells"
    assert heavy, f"custom {agg} centroid/complete: heavy emitted no cells"
    assert all(isinstance(k[1], int) for k in light), "light cellID must be Long int"
    assert all(isinstance(k[1], int) for k in heavy), "heavy cellID must be Long int"

    _parity_cmp(light, heavy, label=f"custom {agg} centroid/complete", tol=1e-9)


@pytest.mark.parametrize("agg", ["avg", "count"])
def test_custom_rastertogrid_covering_sparse_parity(spark_with_jar, agg):
    """Custom rastertogrid covering+sparse: exact cell-set + measure within 1e-6.

    Uses a single large pixel (18000 m) whose footprint overlaps all 4 inner cells.
    Companion to the sub-cell case below: this locks the pixel>=cell regime while
    ``test_custom_rastertogrid_covering_smallpixel_parity`` exercises the sub-cell
    regime that used to diverge. Both tiers must produce the same 4-cell set with
    fractional (0.25) count weights.

    Tolerance 1e-6 because JTS vs shapely area fractions can differ at ~1e-10.
    """
    spark = spark_with_jar
    raster = _covering_raster()

    # Collect LIGHT first.
    light = _light_reducer_rows(
        spark, raster, agg, coverage="sparse", assignment="covering"
    )
    heavy = _heavy_reducer_rows(
        spark, raster, agg, coverage="sparse", assignment="covering"
    )

    assert light, f"custom {agg} covering/sparse: light emitted no cells"
    assert heavy, f"custom {agg} covering/sparse: heavy emitted no cells"

    _parity_cmp(light, heavy, label=f"custom {agg} covering/sparse", tol=1e-6)

    # Non-degeneracy guard for count: each cell gets area_fraction = cell_area / pixel_area
    # = 10000^2 / 20000^2 = 0.25 (fractional) → all counts are non-integer.
    if agg == "count":
        non_integer = any(
            v is not None and abs(v - round(v)) > 1e-6 for v in light.values()
        )
        assert non_integer, (
            "Custom covering fixture degenerate: all count values are integers. "
            "Expected fractional counts from area-fraction weighting (0.25 per cell). "
            "Check that the 20000m pixel and 10000m cells produce fractional overlap."
        )


@pytest.mark.parametrize("agg", ["avg", "count", "sum"])
def test_custom_rastertogrid_covering_smallpixel_parity(spark_with_jar, agg):
    """Custom rastertogrid covering+sparse in the SUB-CELL regime: exact parity.

    Regression gate for the load-bearing bug: 2000 m pixels over a 10000 m custom
    grid.  Pre-fix, the light per-pixel centroid polyfill found no cell centre in a
    sub-cell pixel and returned zero cells for it → those pixels were dropped (fewer
    cells and/or smaller measures than heavy).  With the covering-candidate buffer
    fix (mirroring heavy ``CustomGridSystem.coveringCandidateCells``) light finds the
    containing cell for every sub-cell pixel, so the cell SETS are equal and every
    per-cell measure matches heavy within tolerance.

    Asserting full cell-set equality here is the honest gate: if light silently
    dropped any sub-cell pixel's cell, ``_parity_cmp`` fails on the set mismatch.

    Tolerance 1e-6 because JTS vs shapely area fractions can differ at ~1e-10.
    """
    spark = spark_with_jar
    raster = _small_pixel_covering_raster()

    # Collect LIGHT first (both tiers share the gbx_rst_custom_* SQL names).
    light = _light_reducer_rows(
        spark, raster, agg, coverage="sparse", assignment="covering"
    )
    heavy = _heavy_reducer_rows(
        spark, raster, agg, coverage="sparse", assignment="covering"
    )

    assert light, f"custom {agg} covering/sparse (small pixel): light emitted no cells"
    assert heavy, f"custom {agg} covering/sparse (small pixel): heavy emitted no cells"
    # The footprint spans the 4 inner cells; both tiers must cover all of them.
    assert len(light) == 4, (
        f"small-pixel covering must cover the 4 inner cells; light has {len(light)} "
        f"— a smaller count means sub-cell pixels were dropped (the fixed bug)"
    )

    _parity_cmp(
        light, heavy, label=f"custom {agg} covering/sparse (small pixel)", tol=1e-6
    )


# ---------------------------------------------------------------------------
# Tests: tessellate (both modes)
# ---------------------------------------------------------------------------


def _light_tessellate_ids(spark, raster, mode, *, coverage="complete"):
    """Light tier: SQL LATERAL → (canonical id_set, chip_count)."""
    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = _tile_df(spark, raster)
    df.createOrReplaceTempView("_rst_cust_light_tess")
    rows = spark.sql(
        f"SELECT t.cellid AS cid "
        f"FROM _rst_cust_light_tess, "
        f"LATERAL gbx_rst_custom_tessellate("
        f"tile, grid, {_GRID_RES}, '{mode}', '{coverage}') t"
    ).collect()
    ids = [r["cid"] for r in rows]
    return set(ids), len(ids)


def _heavy_tessellate_ids(spark, raster, mode, *, coverage="complete"):
    """Heavy tier: generator SQL LATERAL → (canonical id_set, chip_count)."""
    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    df = _tile_df(spark, raster)
    df.createOrReplaceTempView("_rst_cust_heavy_tess")
    rows = spark.sql(
        f"SELECT t.cellid AS cid "
        f"FROM _rst_cust_heavy_tess, "
        f"LATERAL gbx_rst_custom_tessellate("
        f"tile, grid, {_GRID_RES}, '{mode}', '{coverage}') t"
    ).collect()
    ids = [r["cid"] for r in rows]
    return set(ids), len(ids)


@pytest.mark.parametrize("mode", ["covering", "centroid"])
def test_custom_tessellate_cellset_parity(spark_with_jar, mode):
    """Custom tessellate: identical Long cell-id set + chip count, both modes.

    Centroid mode: uses the 8×8 100m raster (one cell).
    Covering mode: uses the single 20000m pixel (4 cells) — same rationale as
    the covering rastertogrid test (single large pixel = both tiers agree).
    """
    spark = spark_with_jar
    raster = _covering_raster() if mode == "covering" else _custom_raster_27700()

    # Collect LIGHT first (both tiers share the SQL name).
    light_ids, light_n = _light_tessellate_ids(spark, raster, mode)
    heavy_ids, heavy_n = _heavy_tessellate_ids(spark, raster, mode)

    assert light_ids, f"custom tessellate mode={mode}: light emitted no chips"
    assert heavy_ids, f"custom tessellate mode={mode}: heavy emitted no chips"
    assert all(isinstance(c, int) for c in light_ids), "light cellID must be int"
    assert all(isinstance(c, int) for c in heavy_ids), "heavy cellID must be int"

    if light_ids != heavy_ids:
        pytest.fail(
            f"custom tessellate mode={mode} cell-set MISMATCH: "
            f"|light|={len(light_ids)} |heavy|={len(heavy_ids)} "
            f"light_only={sorted(light_ids - heavy_ids)[:8]} "
            f"heavy_only={sorted(heavy_ids - light_ids)[:8]}"
        )
    assert light_n == heavy_n, (
        f"custom tessellate mode={mode} chip-count mismatch: "
        f"light={light_n} heavy={heavy_n}"
    )


# ---------------------------------------------------------------------------
# Test: rasterize_agg (NoData-aware mask parity)
# ---------------------------------------------------------------------------


def _custom_cells():
    """4 custom-grid resolution-1 cells from the inner 2×2 block.

    Use only the inner 4 cells (col 0-1, row 0-1 of the 16-cell grid) to keep
    cell centroids far from the grid boundary → no out-of-bounds UDAF failure.
    """
    from shapely.geometry import box

    from databricks.labs.gbx.pygx._custom import CustomGridConf
    from databricks.labs.gbx.pygx._custom import polyfill as _custom_polyfill

    conf = CustomGridConf(
        bound_x_min=_GRID_BOUND_X_MIN,
        bound_x_max=_GRID_BOUND_X_MAX,
        bound_y_min=_GRID_BOUND_Y_MIN,
        bound_y_max=_GRID_BOUND_Y_MAX,
        cell_splits=_GRID_CELL_SPLITS,
        root_cell_size_x=_GRID_ROOT_SIZE,
        root_cell_size_y=_GRID_ROOT_SIZE,
        srid=_GRID_SRID,
    )
    # Polyfill inner region containing cols 1-2, rows 2-3 (same 4 cells as covering raster).
    # Cell centroids: (525000,190000),(535000,190000),(525000,200000),(535000,200000)
    # Far from grid boundaries: 10000m margin to nearest boundary.
    poly = box(522000, 187000, 539000, 203000)  # strict interior → 4 cells
    return list(_custom_polyfill(conf, poly, _GRID_RES))


def test_custom_rasterize_agg_mask_parity(spark_with_jar):
    """Custom rasterize_agg: NoData==-9999 + identical covered-pixel mask.

    Light uses Python API (returns tile struct). Heavy uses f.call_function
    (heavy UDAF also returns tile struct). Both access ["raster"] bytes for
    pixel-mask comparison. Auto-derives the canvas from the cell set.
    """
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import _serde
    from databricks.labs.gbx.pyrx import functions as prx
    from databricks.labs.gbx.rasterx import functions as hx

    spark = spark_with_jar

    cells = _custom_cells()
    assert len(cells) >= 2, f"need >=2 custom cells, got {len(cells)}"

    df = spark.createDataFrame([(int(c), "TX1") for c in cells], ["cellid", "tx"])
    df = _add_grid_col(df)

    # Use pixel_size=5000m (half cell_size): auto-derived canvas = 2×2 pixels,
    # one pixel per cell, so all 4 cells get exactly 1 burned pixel each.
    # Default pixel_size=cell_size=10000m would produce a 1×1 canvas (all cells
    # map to the same pixel, failing the ">=1 pixel per cell" assertion).
    _PX_SIZE = 5000.0

    # --- LIGHT tier (Python API, cell collected FIRST before heavy re-registers) ---
    prx.register(spark)
    light_out = (
        df.groupBy("tx")
        .agg(
            prx.rst_custom_rasterize_agg(
                "cellid",
                "grid",
                pixel_size=f.lit(_PX_SIZE),
                kring_pad=f.lit(0),
            ).alias("tile")
        )
        .collect()
    )
    assert len(light_out) == 1
    light_tile = light_out[0]["tile"]
    assert (
        light_tile is not None and light_tile["raster"] is not None
    ), "light rasterize_agg returned None tile"

    # --- HEAVY tier (UDAF via call_function, after hx.register overwrites SQL names) ---
    hx.register(spark)
    heavy_out = (
        df.groupBy("tx")
        .agg(
            f.call_function(
                "gbx_rst_custom_rasterize_agg",
                f.col("cellid"),
                f.lit(None).cast("double"),  # value → presence mask
                f.col("grid"),
                f.lit(_GRID_SRID),  # out_srid
                f.lit(_PX_SIZE),  # pixel_size = 5000m
                f.lit(None).cast("double"),  # xmin → auto
                f.lit(None).cast("double"),  # ymin
                f.lit(None).cast("double"),  # xmax
                f.lit(None).cast("double"),  # ymax
                f.lit(None).cast("int"),  # width
                f.lit(None).cast("int"),  # height
                f.lit("centroids"),  # mode
                f.lit(0),  # kring_pad=0
            ).alias("tile")
        )
        .collect()
    )
    assert len(heavy_out) == 1
    heavy_tile = heavy_out[0]["tile"]
    assert (
        heavy_tile is not None and heavy_tile["raster"] is not None
    ), "heavy rasterize_agg returned None tile"

    # Compare pixel masks.
    with _serde.open_tile(bytes(light_tile["raster"])) as lds:
        light_arr = lds.read(1)
        assert lds.nodata == _NODATA, f"light band NoData must be {_NODATA}"
    with _serde.open_tile(bytes(heavy_tile["raster"])) as hds:
        heavy_arr = hds.read(1)
        assert hds.nodata == _NODATA, f"heavy band NoData must be {_NODATA}"

    light_mask = light_arr != _NODATA
    heavy_mask = heavy_arr != _NODATA
    assert int(light_mask.sum()) >= len(
        cells
    ), f"light covered pixels ({light_mask.sum()}) < cell count ({len(cells)})"
    assert int(heavy_mask.sum()) >= len(
        cells
    ), f"heavy covered pixels ({heavy_mask.sum()}) < cell count ({len(cells)})"

    # UNCONDITIONAL canvas + mask parity: both tiers auto-derive the same snapped,
    # lattice-aligned canvas from the same 4-cell set (pixel_size=5000, kring_pad=0),
    # so the shapes MUST be equal and the NoData masks bit-identical.  (Do not gate
    # this on "if shapes match" — that would let a real canvas divergence pass green.)
    assert light_arr.shape == heavy_arr.shape, (
        f"custom rasterize_agg canvas shape mismatch: "
        f"light={light_arr.shape} heavy={heavy_arr.shape} ({len(cells)} cells)"
    )
    diverging = np.where(light_mask != heavy_mask)
    n_div = len(diverging[0])
    if n_div > 0:
        rows = diverging[0][:5].tolist()
        cols_list = diverging[1][:5].tolist()
        pytest.fail(
            f"custom rasterize_agg mask parity FAILED: {n_div} pixel(s) differ "
            f"({light_arr.shape[1]}x{light_arr.shape[0]}, {len(cells)} cells). "
            f"First diverging (row,col): {list(zip(rows, cols_list))}. "
            f"light_covered={int(light_mask.sum())} heavy_covered={int(heavy_mask.sum())}."
        )
    assert np.array_equal(
        light_mask, heavy_mask
    ), "custom rasterize_agg NoData mask mismatch (shapes equal but masks differ)"
    assert np.all(light_arr[light_mask] == 1.0), "light burn != 1.0 (presence mask)"
    assert np.all(heavy_arr[heavy_mask] == 1.0), "heavy burn != 1.0 (presence mask)"
