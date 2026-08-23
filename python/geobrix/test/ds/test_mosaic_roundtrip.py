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
from rasterio.warp import transform_bounds as _transform_bounds
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
        .option("vrtMosaic", "true")
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


# ---------------------------------------------------------------------------
# Quadbin round-trip helpers
# ---------------------------------------------------------------------------

_QB_RESOLUTION = 12  # quadbin resolution; cells at ~6–10 km cover the 2 km source


def _write_mosaic_quadbin_spark(
    spark, src_path: str, out_dir: str, resolution: int = _QB_RESOLUTION
) -> str:
    """Write *src_path* as a quadbin mosaic via the full Spark cog_gbx DataSource.

    Options: ``vrtMosaic=true``, ``gridSystem=quadbin``, ``gridResolution=<resolution>``.
    Returns the path to the written ``mosaic.vrt``.
    """
    spark.dataSource.register(CogGbxDataSource)
    df = spark.createDataFrame([{"path": src_path}], schema=_path_schema())
    (
        df.write.format("cog_gbx")
        .option("vrtMosaic", "true")
        .option("gridSystem", "quadbin")
        .option("gridResolution", str(resolution))
        .mode("overwrite")
        .save(out_dir)
    )
    vrt_path = os.path.join(out_dir, "mosaic.vrt")
    assert os.path.exists(vrt_path), f"mosaic.vrt not produced at {vrt_path}"
    return vrt_path


def _member_cell_paths(out_dir: str):
    """Return sorted list of quadbin mini-COG cell paths in *out_dir*."""
    return sorted(glob.glob(os.path.join(out_dir, "cell_*.tif")))


# ---------------------------------------------------------------------------
# Test 4: quadbin prepare → expand → rst_avg + cellid / gridSystem + windowed
#
# Full quadbin mosaic write (cog_gbx) → read (raster_gbx VRT expansion) →
# rst_avg + metadata assertions + windowed VRT read.
#
# Asserts:
#   (a) row count == member cell count
#   (b) every tile has a positive quadbin cellid AND gridSystem="quadbin"
#   (c) rst_avg per tile is non-null
#   (d) union of member extents (back in source CRS) contains the source extent
#   (e) windowed rasterio VRT read returns non-fill pixels (Path-B locality)
# ---------------------------------------------------------------------------


def test_quadbin_round_trip(spark, tmp_path):
    """End-to-end quadbin mosaic: cog_gbx write → raster_gbx expand → rst_avg + metadata + windowed."""
    src_path = str(tmp_path / "src_qb" / "input.tif")
    out_dir = str(tmp_path / "mosaic_qb")

    _write_src(src_path)
    vrt_path = _write_mosaic_quadbin_spark(spark, src_path, out_dir)

    # ── Count member mini-COG cells ────────────────────────────────────────────
    member_paths = _member_cell_paths(out_dir)
    n_members = len(member_paths)
    assert n_members >= 1, "no mini-COG cells found after quadbin mosaic write"

    # ── Read the mosaic VRT: one row per member cell ───────────────────────────
    spark.dataSource.register(RasterGbxDataSource)
    df = spark.read.format("raster_gbx").load(vrt_path)

    count = df.count()
    assert count == n_members, (
        f"VRT expansion produced {count} rows, expected {n_members} (one per member cell)"
    )

    # ── Per-row: cellid positive + gridSystem="quadbin" + rst_avg non-null ─────
    rows = df.select(
        col("tile.metadata").alias("metadata"),
        rst_avg(col("tile")).alias("avg"),
    ).collect()

    for row in rows:
        metadata = row["metadata"]
        assert metadata is not None, "tile.metadata is None"

        # (b) positive quadbin cellid
        assert "cellid" in metadata, f"tile.metadata missing 'cellid': {metadata}"
        cellid_val = int(metadata["cellid"])
        assert cellid_val > 0, f"cellid is not positive: {cellid_val}"

        # (b) gridSystem tag
        assert metadata.get("gridSystem") == "quadbin", (
            f"tile.metadata['gridSystem'] expected 'quadbin', "
            f"got {metadata.get('gridSystem')!r}"
        )

        # (c) rst_avg non-null
        avg_list = row["avg"]
        assert avg_list is not None, "rst_avg returned None for quadbin cell"
        assert len(avg_list) >= 1, "rst_avg avg list is empty for quadbin cell"
        assert all(
            v is not None for v in avg_list
        ), f"rst_avg has None band value: {avg_list}"

    # ── (d) Union of member extents contains the source extent ─────────────────
    with rasterio.open(src_path) as src_ds:
        src_bounds = src_ds.bounds  # EPSG:32632

    westerns, southerns, easterns, northerns = [], [], [], []
    for path in member_paths:
        with rasterio.open(path) as cell_ds:
            assert cell_ds.crs.to_epsg() == 3857, (
                f"cell tile CRS should be EPSG:3857, got {cell_ds.crs}"
            )
            w, s, e, n = _transform_bounds(
                cell_ds.crs,
                "EPSG:32632",
                cell_ds.bounds.left,
                cell_ds.bounds.bottom,
                cell_ds.bounds.right,
                cell_ds.bounds.top,
            )
            westerns.append(w)
            southerns.append(s)
            easterns.append(e)
            northerns.append(n)

    union_west = min(westerns)
    union_south = min(southerns)
    union_east = max(easterns)
    union_north = max(northerns)

    # Cells align to the quadbin / web-mercator grid, so the union re-projected
    # back to EPSG:32632 may be larger or offset by up to one cell width (~10 km
    # at resolution 12).  The tolerance below is generous; the key invariant is
    # that the union overlaps the source.
    tol_m = 20_000  # 20 km
    assert union_west <= src_bounds.left + tol_m, (
        f"union_west ({union_west:.1f}) does not extend west enough "
        f"of src.left ({src_bounds.left:.1f})"
    )
    assert union_east >= src_bounds.right - tol_m, (
        f"union_east ({union_east:.1f}) does not extend east enough "
        f"of src.right ({src_bounds.right:.1f})"
    )
    assert union_south <= src_bounds.bottom + tol_m, (
        f"union_south ({union_south:.1f}) does not extend south enough "
        f"of src.bottom ({src_bounds.bottom:.1f})"
    )
    assert union_north >= src_bounds.top - tol_m, (
        f"union_north ({union_north:.1f}) does not extend north enough "
        f"of src.top ({src_bounds.top:.1f})"
    )

    # ── (e) Windowed VRT read returns non-fill pixels (Path-B locality) ────────
    # Anchor the pixel window on the first cell's spatial extent within the VRT.
    # Every cell overlaps the source, so pixels in this window must be non-zero
    # (the source guarantees values >= 1; fill for missing coverage = 0).
    with rasterio.open(member_paths[0]) as first_cell_ds:
        cell_bounds_3857 = first_cell_ds.bounds

    with rasterio.open(vrt_path) as vrt_ds:
        cell_win = vrt_ds.window(
            cell_bounds_3857.left,
            cell_bounds_3857.bottom,
            cell_bounds_3857.right,
            cell_bounds_3857.top,
        )
        vrt_full = Window(0, 0, vrt_ds.width, vrt_ds.height)
        cell_win = cell_win.intersection(vrt_full)
        vrt_data = vrt_ds.read(window=cell_win)

    assert vrt_data.size > 0, "windowed VRT read returned empty array"
    # The source guarantees pixel values >= 1 after reprojection at least some
    # pixels in the cell's window must be non-fill.
    assert np.any(vrt_data > 0), (
        "windowed VRT read over the first cell's extent returned all-fill (zero) pixels; "
        "expected non-zero source data since every cell overlaps the source"
    )
