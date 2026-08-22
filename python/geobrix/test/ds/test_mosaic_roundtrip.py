"""Task 6 (SDD — Phase A native mini-COG mosaic): end-to-end round-trip integration.

Proves the full native mosaic pipeline composes correctly in a single Docker test:

  1. prepare → expand → rst_avg:
     Write a raster as a native mosaic via the full Spark ``cog_gbx`` DataSource
     (mini-COGs + ``mosaic.vrt``); read back via ``raster_gbx`` loading the VRT
     (one row per member); apply ``rst_avg`` — per-tile means must match the source
     pixel values within each tile's spatial extent.

  2. windowed via mint_vrt:
     Build a transient VRT over the mini-COGs with ``mint_vrt``; open it with
     rasterio; read a cross-tile viewport — pixels must equal the same window
     from the original source raster.

  3. bbox-filtered VRT read (cheap probe):
     Load ``mosaic.vrt`` with a ``clipPolygons`` option whose polygon covers exactly
     one tile's spatial extent — assert exactly one row is returned, and that the
     row belongs to the expected tile.

Pure Python (no JAR, no osgeo).

Run (in Docker):
    bash scripts/commands/gbx-test-python.sh \\
        --path python/geobrix/test/ds/test_mosaic_roundtrip.py \\
        --log mosaic-roundtrip.log
"""

from __future__ import annotations

import glob
import os

import numpy as np
import rasterio
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, StructField, StructType
from rasterio.transform import from_origin
from rasterio.windows import Window

from databricks.labs.gbx.ds._mosaic import mint_vrt
from databricks.labs.gbx.ds.cog import CogGbxDataSource
from databricks.labs.gbx.ds.raster import RasterGbxDataSource
from databricks.labs.gbx.pyrx.functions import rst_avg

# ---------------------------------------------------------------------------
# Shared raster geometry
# ---------------------------------------------------------------------------

_SRC_W = 200  # source width in pixels
_SRC_H = 120  # source height in pixels
_TILE_SIZE = 100  # tileSize option (square grid cells, pixels)
_PIXEL_SIZE = 10.0  # metres per pixel (EPSG:32632)
_ORIGIN_X = 400000.0  # easting of upper-left corner
_ORIGIN_Y = 5000000.0  # northing of upper-left corner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _path_schema() -> StructType:
    return StructType([StructField("path", StringType(), False)])


def _write_src(
    path: str, w: int = _SRC_W, h: int = _SRC_H, dtype: str = "uint16"
) -> str:
    """Write a deterministic GTiff raster with nonzero pixel values.

    Pixel value = ((row * w + col + 1) % maxval) clamped to [1, maxval].
    Values are always nonzero so no tile is all-zero and NoData pruning is safe.
    """
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    profile = dict(
        driver="GTiff",
        width=w,
        height=h,
        count=1,
        dtype=dtype,
        crs="EPSG:32632",
        transform=from_origin(_ORIGIN_X, _ORIGIN_Y, _PIXEL_SIZE, _PIXEL_SIZE),
    )
    data = (np.arange(w * h, dtype=np.uint32).reshape(1, h, w) + 1).astype(dtype)
    maxval = np.iinfo(dtype).max
    data = data % maxval
    data[data == 0] = 1  # guarantee no all-zero tiles
    with rasterio.open(path, "w", **profile) as ds:
        ds.write(data)
    return path


def _write_mosaic_spark(spark, src_path: str, out_dir: str) -> str:
    """Write *src_path* as a native mosaic via the full Spark cog_gbx DataSource.

    Returns the path to the written ``mosaic.vrt`` (``out_dir/mosaic.vrt``).
    """
    spark.dataSource.register(CogGbxDataSource)
    df = spark.createDataFrame([{"path": src_path}], schema=_path_schema())
    (
        df.write.format("cog_gbx")
        .option("mosaic", "true")
        .option("gridSystem", "none")
        .option("tileSize", str(_TILE_SIZE))
        .mode("overwrite")
        .save(out_dir)
    )
    vrt_path = os.path.join(out_dir, "mosaic.vrt")
    assert os.path.exists(vrt_path), f"mosaic.vrt not produced at {vrt_path}"
    return vrt_path


def _member_cog_paths(out_dir: str):
    """Return sorted list of mini-COG tile paths in *out_dir*."""
    return sorted(glob.glob(os.path.join(out_dir, "tile_*.tif")))


# ---------------------------------------------------------------------------
# Test 1: prepare → expand → rst_avg
#
# Full Spark write (cog_gbx mosaic) → read (raster_gbx VRT expansion) →
# rst_avg transform.  Asserts:
#   (a) row count == member count
#   (b) rst_avg output per row is non-null
#   (c) spark_avg == direct mean from rasterio (tiles are faithfully encoded)
#   (d) member pixel mean == source region pixel mean (write is pixel-perfect)
# ---------------------------------------------------------------------------


def test_prepare_expand_rst_avg(spark, tmp_path):
    """End-to-end: cog_gbx mosaic write → raster_gbx VRT expand → rst_avg values."""
    src_path = str(tmp_path / "src" / "input.tif")
    out_dir = str(tmp_path / "mosaic")

    _write_src(src_path)
    vrt_path = _write_mosaic_spark(spark, src_path, out_dir)

    # ── Read the mosaic VRT: expect one row per member mini-COG ───────────────
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(vrt_path)

    member_paths = _member_cog_paths(out_dir)
    n_members = len(member_paths)
    assert n_members > 0, "no mini-COG tiles found after mosaic write"

    count = df.count()
    assert (
        count == n_members
    ), f"VRT expansion produced {count} rows, expected {n_members} (one per member)"

    # ── Apply rst_avg per tile; collect (path, avg) ───────────────────────────
    rows = df.select(
        col("tile.path").alias("member_path"),
        rst_avg(col("tile")).alias("avg"),
    ).collect()
    assert len(rows) == n_members, f"select/collect length mismatch: {len(rows)}"

    # ── Per-tile value assertions ─────────────────────────────────────────────
    with rasterio.open(src_path) as ref:
        for row in rows:
            member_path = row["member_path"]
            avg_list = row["avg"]
            basename = os.path.basename(member_path)

            # (b) rst_avg returned a non-null, non-empty list
            assert avg_list is not None, f"rst_avg returned None for {basename!r}"
            assert len(avg_list) >= 1, f"rst_avg avg list is empty for {basename!r}"
            assert all(
                v is not None for v in avg_list
            ), f"rst_avg has None band value for {basename!r}: {avg_list}"

            spark_avg = float(avg_list[0])

            # (c) rst_avg output matches a direct rasterio mean of the member file
            with rasterio.open(member_path) as tile_ds:
                tile_data = tile_ds.read().astype(np.float64)
                direct_mean = float(np.mean(tile_data))

            np.testing.assert_allclose(
                spark_avg,
                direct_mean,
                rtol=1e-5,
                err_msg=(
                    f"rst_avg mean ({spark_avg:.6f}) != rasterio mean ({direct_mean:.6f}) "
                    f"for {basename!r}"
                ),
            )

            # (d) member pixel mean matches source region pixel mean
            #     — proves the write is pixel-perfect for this tile's window.
            with rasterio.open(member_path) as tile_ds:
                tile_bounds = tile_ds.bounds
                # Snap floating-point window offsets/shape to whole pixels.
                # round_shape is deprecated in rasterio ≥ 2.0; compute manually.
                raw_win = ref.window(*tile_bounds)
                col_off = int(np.floor(raw_win.col_off))
                row_off = int(np.floor(raw_win.row_off))
                width = int(np.ceil(raw_win.col_off + raw_win.width) - col_off)
                height = int(np.ceil(raw_win.row_off + raw_win.height) - row_off)
                src_win = Window(col_off, row_off, width, height)
                src_region = ref.read(window=src_win).astype(np.float64)
                src_mean = float(np.mean(src_region))

            np.testing.assert_allclose(
                direct_mean,
                src_mean,
                rtol=1e-5,
                err_msg=(
                    f"member mean ({direct_mean:.6f}) != source region mean "
                    f"({src_mean:.6f}) for {basename!r}: write is not pixel-perfect"
                ),
            )


# ---------------------------------------------------------------------------
# Test 2: windowed read via mint_vrt
#
# Build a transient VRT over the written mini-COGs with mint_vrt;
# read a cross-tile viewport and compare to the original source raster.
# ---------------------------------------------------------------------------


def test_windowed_via_mint_vrt(spark, tmp_path):
    """mint_vrt over the mini-COGs + cross-tile windowed read equals source pixels."""
    src_path = str(tmp_path / "src" / "input.tif")
    out_dir = str(tmp_path / "mosaic")

    _write_src(src_path)
    _write_mosaic_spark(spark, src_path, out_dir)

    member_paths = _member_cog_paths(out_dir)
    assert member_paths, "no mini-COG tiles found — prerequisite write failed"

    # Build a transient VRT (absolute SourceFilename paths).
    minted_vrt = mint_vrt(member_paths)
    assert os.path.exists(minted_vrt), f"mint_vrt produced no file: {minted_vrt!r}"

    # The minted VRT must cover the full source extent.
    with rasterio.open(minted_vrt) as vrt_ds:
        assert (
            vrt_ds.width == _SRC_W
        ), f"minted VRT width {vrt_ds.width} != expected {_SRC_W}"
        assert (
            vrt_ds.height == _SRC_H
        ), f"minted VRT height {vrt_ds.height} != expected {_SRC_H}"

    # Cross-tile viewport: cols 40–160, rows 20–80.
    # This straddles the tile_0_0 / tile_0_1 column boundary at col 100.
    viewport = Window(40, 20, 120, 60)

    with rasterio.open(src_path) as ref_ds:
        ref_data = ref_ds.read(window=viewport)

    with rasterio.open(minted_vrt) as vrt_ds:
        vrt_data = vrt_ds.read(window=viewport)

    np.testing.assert_array_equal(
        vrt_data,
        ref_data,
        err_msg=(
            "mint_vrt cross-tile windowed read differs from source pixels "
            "(viewport col_off=40, row_off=20, w=120, h=60)"
        ),
    )


# ---------------------------------------------------------------------------
# Test 3: bbox-filtered VRT read touches only intersecting members
#
# Load mosaic.vrt with clipPolygons covering the spatial extent of tile_0_0
# only.  The reader must return exactly one row (for tile_0_0).
#
# Grid layout (200×120 source, tileSize=100, EPSG:32632, pixel=10 m):
#   tile_0_0: cols 0:100,   rows 0:100   → geo x=[400000,401000], y=[4999000,5000000]
#   tile_0_1: cols 100:200, rows 0:100   → geo x=[401000,402000], y=[4999000,5000000]
#   tile_1_0: cols 0:100,   rows 100:120 → geo x=[400000,401000], y=[4998800,4999000]
#   tile_1_1: cols 100:200, rows 100:120 → geo x=[401000,402000], y=[4998800,4999000]
#
# The test polygon is inset 5 m from tile_0_0's edges to avoid touching
# adjacent tiles at their shared boundaries.
# ---------------------------------------------------------------------------

# Inset-polygon that covers only tile_0_0 (x=[400000,401000], y=[4999000,5000000]).
_BBOX_TILE_0_0 = (
    "POLYGON ((400005 4999005, 400995 4999005, "
    "400995 4999995, 400005 4999995, 400005 4999005))"
)


def test_bbox_filtered_vrt_read(spark, tmp_path):
    """clipPolygons over tile_0_0's extent → exactly one row from the mosaic VRT."""
    src_path = str(tmp_path / "src" / "input.tif")
    out_dir = str(tmp_path / "mosaic")

    _write_src(src_path)
    vrt_path = _write_mosaic_spark(spark, src_path, out_dir)

    spark.dataSource.register(RasterGbxDataSource)
    df = (
        spark.read.format("raster_gbx")
        .option("clipPolygons", _BBOX_TILE_0_0)
        .option("clipCrs", "EPSG:32632")
        .load(vrt_path)
    )
    rows = df.collect()

    assert len(rows) == 1, (
        f"bbox-filtered read over tile_0_0 extent returned {len(rows)} rows, "
        f"expected exactly 1"
    )

    member_path = rows[0]["tile"]["path"]
    # Tile names carry a per-source discriminator: tile_<disc>_<row>_<col>.tif.
    assert member_path.endswith("_0_0.tif"), (
        f"Expected the returned row to belong to the (0,0) tile; "
        f"got member_path={member_path!r}"
    )
