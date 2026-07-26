"""Cross-tier (light pyrx vs heavy Scala/GDAL) parity for the 9 BNG/quadbin
raster-grid functions.

THE Phase-2 parity def-of-done. For each of the nine functions, both tiers are
run on the SAME in-memory sample raster + resolution and their outputs compared
against the design §4.1 parity bar:

  * 5 BNG reducers (``rst_bng_rastertogrid{avg,count,max,min,median}``) — EXACT
    BNG-String cell-set equality + per-cell measure within 1e-9.
  * ``rst_quadbin_tessellate`` / ``rst_bng_tessellate`` — identical emitted
    cell-id set (quadbin Long / BNG String) + identical chip count, for both
    ``covering`` and ``centroid`` modes.
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
    "rasterio", reason="rasterio not installed (geobrix[light] required)"
)
pytest.importorskip("shapely", reason="shapely not installed (geobrix[light] required)")
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


# ---------------------------------------------------------------------------
# Reducer parity (5 BNG functions) — exact cell-set + measure within 1e-9
# ---------------------------------------------------------------------------

_REDUCERS = ["avg", "count", "max", "min", "median", "sum"]


def _heavy_reducer_rows(spark, raster, resolution, agg):
    """Heavy tier: ARRAY<ARRAY<struct(cellID,measure)>> -> flat {(band,cellID): measure}."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    fn = getattr(hx, f"rst_bng_rastertogrid{agg}")
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        hx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    rows = (
        df.select(fn(f.col("tile"), f.lit(resolution)).alias("bands"))
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


def _light_reducer_rows(spark, raster, resolution, agg):
    """Light tier: SQL LATERAL over the registered pyrx UDTF -> {(band,cellID): measure}."""
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
        f"LATERAL gbx_rst_bng_rastertogrid{agg}(tile, {resolution}) t"
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


def _heavy_grid_rows(spark, raster, resolution, grid, agg):
    """Heavy tier (Long cell id grids): -> {(band,cellID): measure}."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    fn = getattr(hx, f"rst_{grid}_rastertogrid{agg}")
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        hx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    rows = (
        df.select(fn(f.col("tile"), f.lit(resolution)).alias("bands"))
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


def _light_grid_rows(spark, raster, resolution, grid, agg):
    """Light tier (Long cell id grids): SQL LATERAL over the pyrx UDTF."""
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
        f"LATERAL gbx_rst_{grid}_rastertogrid{agg}(tile, {resolution}) t"
    ).collect()
    return {(r["band"], r["cellID"]): r["measure"] for r in rows}


@pytest.mark.parametrize("grid,resolution", [("quadbin", 12), ("h3", 7)])
def test_grid_rastertogridsum_parity(spark_with_jar, grid, resolution):
    """h3/quadbin sum: exact Long cell-set + per-cell measure within relative tol.

    Real multi-value tile (ramp 1..256) so cells carry several pixels and the
    sum exercises real accumulation, not a single-pixel degenerate case.
    """
    spark = spark_with_jar
    raster = _quadbin_raster_4326()  # 16x16 4326 ramp — valid for both h3 & quadbin

    light = _light_grid_rows(spark, raster, resolution, grid, "sum")
    heavy = _heavy_grid_rows(spark, raster, resolution, grid, "sum")

    assert light, f"light emitted no cells for {grid} sum"
    assert heavy, f"heavy emitted no cells for {grid} sum"

    light_keys, heavy_keys = set(light), set(heavy)
    if light_keys != heavy_keys:
        pytest.fail(
            f"{grid} sum cell-set MISMATCH: "
            f"|light|={len(light_keys)} |heavy|={len(heavy_keys)} "
            f"light_only={sorted(light_keys - heavy_keys)[:8]} "
            f"heavy_only={sorted(heavy_keys - light_keys)[:8]}"
        )

    # within_tol (same summation-order class as avg): relative tolerance.
    for key in light_keys:
        lv, hv = float(light[key]), float(heavy[key])
        assert abs(lv - hv) <= 1e-9 * max(1.0, abs(hv)), (
            f"{grid} sum cell {key} diverged beyond within_tol: "
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


def _light_tessellate_ids(spark, raster, sql_name, resolution, mode, *, bng):
    """Light tier: SQL LATERAL tessellate -> (canonical id_set, chip_count)."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.pyrx import functions as prx

    prx.register(spark)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        prx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    df.createOrReplaceTempView("_ras_light_tess")
    rows = spark.sql(
        f"SELECT t.cellid AS cid FROM _ras_light_tess, "
        f"LATERAL {sql_name}(tile, {resolution}, '{mode}') t"
    ).collect()
    ids = [_tess_id(r["cid"], bng=bng) for r in rows]
    return set(ids), len(ids)


def _heavy_tessellate_ids(spark, raster, fn_name, resolution, mode, *, bng):
    """Heavy tier: DataFrame generator -> (canonical id_set, chip_count)."""
    from pyspark.sql import functions as f

    from databricks.labs.gbx.rasterx import functions as hx

    hx.register(spark)
    fn = getattr(hx, fn_name)
    df = spark.createDataFrame([(bytearray(raster),)], ["raster"]).select(
        hx.rst_fromcontent("raster", f.lit("GTiff")).alias("tile")
    )
    rows = (
        df.select(fn(f.col("tile"), f.lit(resolution), mode).alias("tt"))
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
                srid=f.lit(out_srid),
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
